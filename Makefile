RUST_DIR := rust
GO_PKG   := ./go/cmd/flowrulz
BIN      := flowrulz
CGO      := CGO_ENABLED=1

.PHONY: all rust go test bench clean vet

all: rust go

rust:
	cd $(RUST_DIR) && cargo build --release

go: rust
	$(CGO) go build -o $(BIN) $(GO_PKG)

test-rust:
	cd $(RUST_DIR) && cargo test

test-go:
	$(CGO) go test ./go/... -count=1

test: test-rust test-go

bench:
	cd $(RUST_DIR) && cargo bench --bench flowrulz_bench

vet:
	$(CGO) go vet ./go/...

clean:
	cd $(RUST_DIR) && cargo clean
	rm -f $(BIN)
	rm -rf rust/target
