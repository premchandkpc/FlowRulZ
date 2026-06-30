RUST_DIR := rust
GO_PKG   := ./go/cmd/flowrulz
SIM_PKG  := ./simulator/cmd/simulator
BIN      := flowrulz
SIM_BIN  := sim
CGO      := CGO_ENABLED=1

.PHONY: all rust go sim test test-local bench clean vet run-flowrulz run-sim \
        docker docker-sim kind-up kind-load kind-deploy helm-install helm-uninstall k8s-deploy k8s-delete \
        full file deploy

all: rust go

# Run all functionalities sequentially (clean → build → test → vet → bench)
full: clean rust go sim test vet bench
	@echo "=== All done ==="

file: full

# Full deploy pipeline: build → test → docker → kind cluster → deploy
deploy: full docker kind-up
	kubectl apply -k k8s/base

rust:
	cd $(RUST_DIR) && cargo build --release

go: rust
	$(CGO) go build -o $(BIN) $(GO_PKG)

sim: rust
	$(CGO) go build -o $(SIM_BIN) $(SIM_PKG)

# Run production node (background, persists logs)
run-flowrulz: go
	@echo "=== starting flowrulz production node ==="
	./$(BIN)

# Run simulator with dashboard (background)
run-sim: sim
	@echo "=== starting simulator with dashboard on :8081 ==="
	./$(SIM_BIN) --interactive --dashboard-addr :8081

test-rust:
	cd $(RUST_DIR) && cargo test

test-go:
	$(CGO) go test ./go/... ./simulator/... -count=1

test: test-rust test-go

# Quick local: Go tests + binary smoke tests, skip Rust cargo test
test-local: go
	$(CGO) go test ./go/... ./simulator/... -count=1
	@echo "--- smoke: simulate --scenarios ---"
	./$(BIN) simulate --scenarios
	@echo "--- smoke: simulate 1s run ---"
	./$(BIN) simulate --scenario ramp-up --duration 1s --rate 10 --dashboard=false

bench:
	cd $(RUST_DIR) && cargo bench --bench flowrulz_bench

vet:
	$(CGO) go vet ./go/... ./simulator/...

clean:
	cd $(RUST_DIR) && cargo clean
	rm -f $(BIN) $(SIM_BIN) simulator/simulator
	rm -rf rust/target

# === Docker ===
docker:
	docker build --target flowrulz -t flowrulz:latest .

docker-sim:
	docker build --target sim -t flowrulz-sim:latest .

# === kind (local K8s) ===
kind-up:
	kind create cluster --name flowrulz --config k8s/kind-config.yaml 2>/dev/null || true
	kind load docker-image flowrulz:latest --name flowrulz

kind-deploy: docker kind-up
	kubectl apply -k k8s/base

# === Helm ===
helm-install: docker
	helm upgrade --install flowrulz k8s/helm --namespace flowrulz --create-namespace

helm-uninstall:
	helm uninstall flowrulz --namespace flowrulz

# === kubectl (kustomize) ===
k8s-deploy: docker
	kubectl apply -k k8s/base

k8s-delete:
	kubectl delete -k k8s/base
