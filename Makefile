RUST_DIR := rust
GO_PKG   := ./go/cmd/flowrulz
SIM_PKG  := ./simulator/cmd/simulator
BIN      := flowrulz
SIM_BIN  := sim
CGO      := CGO_ENABLED=1

.PHONY: all rust go sim test test-local bench clean vet run-flowrulz run-sim

all: rust go

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
