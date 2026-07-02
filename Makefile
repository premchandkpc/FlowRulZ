RUST_DIR := runtime
GO_PKG   := ./server/cmd/flowrulz
SIM_PKG  := ./simulator/cmd/simulator
BIN      := flowrulz
SIM_BIN  := sim
CGO      := CGO_ENABLED=1

.PHONY: all rust go sim test test-local bench clean vet run-flowrulz run-sim \
        docker docker-sim kind-up kind-load kind-deploy helm-install helm-uninstall k8s-deploy k8s-delete \
        full file deploy

all: rust go

full: clean rust go sim test vet bench
	@echo "=== All done ==="

file: full

deploy: full docker kind-up
	kubectl apply -k k8s/base

rust:
	cd $(RUST_DIR) && cargo build --release

go: rust
	$(CGO) go build -o $(BIN) $(GO_PKG)

sim: rust
	$(CGO) go build -o $(SIM_BIN) $(SIM_PKG)

run-flowrulz: go
	@echo "=== starting flowrulz production node ==="
	./$(BIN)

run-sim: sim
	@echo "=== starting simulator with dashboard on :8081 ==="
	./$(SIM_BIN) --interactive --dashboard-addr :8081

test-rust:
	cd $(RUST_DIR) && cargo test

test-go:
	$(CGO) go test ./server/... ./simulator/... -count=1

test: test-rust test-go

test-local: go sim
	$(CGO) go test ./server/... ./simulator/... -count=1
	@echo "--- smoke: sim --scenarios ---"
	./$(SIM_BIN) --scenarios
	@echo "--- smoke: sim 1s run ---"
	./$(SIM_BIN) --scenario ramp-up --duration 1s --rate 10 --dashboard=false

bench:
	cd $(RUST_DIR) && cargo bench --bench flowrulz_bench

vet:
	$(CGO) go vet ./server/... ./simulator/...

clean:
	cd $(RUST_DIR) && cargo clean
	rm -f $(BIN) $(SIM_BIN) simulator/simulator
	rm -rf runtime/target

e2e-up:
	docker compose up -d --build
	@echo "Waiting for cluster..."
	@sleep 8

e2e-test: e2e-up
	E2E=1 go test ./e2e/... -v -count=1 -timeout=120s

e2e-down:
	docker compose down -v

e2e: e2e-test e2e-down

docker:
	docker build --target flowrulz -t flowrulz:latest .

docker-sim:
	docker build --target sim -t flowrulz-sim:latest .

kind-up:
	kind create cluster --name flowrulz --config k8s/kind-config.yaml 2>/dev/null || true
	kind load docker-image flowrulz:latest --name flowrulz

kind-deploy: docker kind-up
	kubectl apply -k k8s/base

helm-install: docker
	helm upgrade --install flowrulz k8s/helm --namespace flowrulz --create-namespace

helm-uninstall:
	helm uninstall flowrulz --namespace flowrulz

k8s-deploy: docker
	kubectl apply -k k8s/base

k8s-delete:
	kubectl delete -k k8s/base
