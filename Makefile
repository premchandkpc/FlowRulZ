###############################################################################
# Project
###############################################################################

RUST_DIR := runtime

GO_PKG   := ./server/cmd/flowrulz
SIM_PKG  := ./simulator/cmd/simulator

BIN      := flowrulz
SIM_BIN  := sim

IMAGE     := flowrulz:latest
SIM_IMAGE := flowrulz-sim:latest

CLUSTER := flowrulz

APP_NAME := flowrulz
WORKLOAD := statefulset

CGO := CGO_ENABLED=1

###############################################################################
# PHONY
###############################################################################

.PHONY: \
all build rust go sim \
run-flowrulz run-sim \
test test-rust test-go test-local bench vet clean \
docker docker-sim \
kind-up kind-down kind-load \
k8s-deploy k8s-delete \
helm-install helm-uninstall \
e2e-up e2e-test e2e-down e2e \
logs pods svc describe shell port-forward restart status \
up down rebuild reset deploy full

###############################################################################
# Build
###############################################################################

all: build

build: rust go sim

full: clean build test vet bench
	@echo ""
	@echo "==================================="
	@echo " Build Completed Successfully"
	@echo "==================================="

###############################################################################
# Rust
###############################################################################

rust:
	cd $(RUST_DIR) && cargo build --release

###############################################################################
# Go
###############################################################################

go: rust
	$(CGO) go build -o $(BIN) $(GO_PKG)

sim: rust
	$(CGO) go build -o $(SIM_BIN) $(SIM_PKG)

###############################################################################
# Run
###############################################################################

run-flowrulz: go
	./$(BIN)

run-sim: sim
	./$(SIM_BIN) --interactive --dashboard-addr :8081

###############################################################################
# Tests
###############################################################################

test-rust:
	cd $(RUST_DIR) && cargo test

test-go:
	$(CGO) go test ./server/... ./simulator/... -count=1

test: test-rust test-go

test-local: build
	$(CGO) go test ./server/... ./simulator/... -count=1
	./$(SIM_BIN) --scenarios
	./$(SIM_BIN) --scenario ramp-up --duration 1s --rate 10 --dashboard=false

bench:
	cd $(RUST_DIR) && cargo bench --bench flowrulz_bench

vet:
	$(CGO) go vet ./server/... ./simulator/...

###############################################################################
# Lint/Format
###############################################################################

gofmt-check:
	$(CGO) gofmt -d -s ./server/internal ./simulator

goimports-check:
	$(CGO) goimports -d ./server/internal ./simulator

fmt:
	$(CGO) gofmt -w -s ./server/internal ./simulator

imports:
	$(CGO) goimports -w ./server/internal ./simulator

lint: fmt vet imports

lint-fast: fmt imports

###############################################################################
# Docker
###############################################################################

docker:
	docker build --target flowrulz -t $(IMAGE) .
	docker build --target sim -t $(SIM_IMAGE) .

docker-sim:
	docker build --target sim -t $(SIM_IMAGE) .

###############################################################################
# Kind
###############################################################################

kind-up:
	kind create cluster \
		--name $(CLUSTER) \
		--config k8s/kind-config.yaml 2>/dev/null || true

kind-load:
	kind load docker-image $(IMAGE) --name $(CLUSTER)

kind-down:
	kind delete cluster --name $(CLUSTER)

###############################################################################
# Kubernetes
###############################################################################

k8s-deploy:
	kubectl apply -k k8s/base

k8s-delete:
	kubectl delete -k k8s/base

status:
	kubectl get all -A

pods:
	kubectl get pods -A

svc:
	kubectl get svc -A

logs:
	kubectl logs -f $$(kubectl get pod -l app=$(APP_NAME) -o jsonpath='{.items[0].metadata.name}')

describe:
	kubectl describe $(WORKLOAD) $(APP_NAME)

restart:
	kubectl rollout restart $(WORKLOAD)/$(APP_NAME)
	kubectl rollout status $(WORKLOAD)/$(APP_NAME)

wait:
	kubectl rollout status $(WORKLOAD)/$(APP_NAME) --timeout=300s

shell:
	kubectl exec -it $$(kubectl get pod -l app=flowrulz -o jsonpath='{.items[0].metadata.name}') -- sh

port-forward:
	kubectl port-forward svc/flowrulz 8080:8080

###############################################################################
# Helm
###############################################################################

helm-install:
	helm upgrade \
		--install flowrulz \
		k8s/helm \
		--namespace flowrulz \
		--create-namespace

helm-uninstall:
	helm uninstall flowrulz --namespace flowrulz

###############################################################################
# End-to-End
###############################################################################

e2e-up:
	docker compose up -d --build
	sleep 8

e2e-test: e2e-up
	E2E=1 go test ./e2e/... -v -count=1 -timeout=120s

e2e-down:
	docker compose down -v

e2e: e2e-test e2e-down

###############################################################################
# Cleanup
###############################################################################

clean:
	cd $(RUST_DIR) && cargo clean
	rm -rf runtime/target
	rm -f $(BIN)
	rm -f $(SIM_BIN)
	rm -f simulator/simulator

###############################################################################
# One Command Local Kubernetes
###############################################################################

up: full docker kind-up kind-load k8s-deploy wait
	@echo ""
	@echo "=========================================="
	@echo " FlowRulZ Successfully Deployed"
	@echo "=========================================="
	@kubectl get pods -A

down:
	kubectl delete -k k8s/base || true
	kind delete cluster --name $(CLUSTER) || true

reset: down clean

rebuild: reset up

###############################################################################
# Default Deploy
###############################################################################

deploy: up