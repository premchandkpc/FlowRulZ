# Development Guide

Building, testing, contributing.

## Prerequisites

- Go 1.21+
- Rust toolchain (rustup)
- protobuf compiler (protoc)
- Docker (optional, for Kafka/Redis)

## Project Structure

```
server/       Go control plane
runtime/      Rust VM
sdk/          Client SDKs (Go, Python, Java, JS, Rust)
simulator/    40+ services, 8 modes, 50+ scenarios
docs/         This documentation
k8s/          Kubernetes manifests
proto/        Protobuf definitions
```

## Building

### Go Server

```bash
cd server
go build -o flowrulz ./cmd/flowrulz/
```

### Rust VM

```bash
cd runtime
cargo build --release
```

### Full Build

```bash
make build
```

## Testing

### Go Tests

```bash
cd server
go test -race ./internal/...
go test -race ./bridge/...
```

### Rust Tests

```bash
cd runtime
cargo test
```

### Simulator

```bash
cd simulator
go test ./...
```

### All Tests

```bash
make test
```

## Linting

### Go

```bash
cd server
golangci-lint run
```

### Rust

```bash
cd runtime
cargo clippy
```

## Code Generation

### Protobuf

```bash
protoc --go_out=. --go-grpc_out=. proto/eventbus.proto
```

## Architecture

### Go Packages

| Package | Description |
|---------|-------------|
| `cmd/flowrulz` | Main entrypoint |
| `internal/node` | HTTP handlers, plan execution |
| `internal/scheduler` | Work-stealing scheduler |
| `internal/engine` | Rule management |
| `internal/cluster` | Raft consensus |
| `internal/transport` | Message bus |
| `internal/plandist` | Plan distribution |
| `internal/registry` | Service discovery |
| `internal/execstate` | Execution state |
| `internal/reliability` | Resilience patterns |
| `internal/admin` | Admin API |
| `internal/observability` | Metrics, tracing |
| `internal/flow` | Flow DSL |
| `internal/compiler` | DSL compilation |
| `bridge` | CGo FFI |
| `pkg/` | Public interfaces |

### Rust Modules

| Module | Description |
|--------|-------------|
| `dsl/` | Lexer, parser, optimizer |
| `bytecode/` | Instruction set |
| `executor/` | VM engine |
| `memory/` | Arena, interning |
| `ffi/` | C FFI |
| `tracing/` | Spans |

## Key Gotchas

- `ExecutionContext`: use `State()/SetVariable()/Variable()` accessors (sync.Mutex)
- `TimerWheel`: `Stop()` waits for callbacks (sync.WaitGroup)
- `ReplyRouter`: uses `PendingRequest.closeOnce()` to prevent double-close
- `SpanRingBuffer`: drain before emitting in tests
- `execTask`: has `defer recover()` - panics write error to ResultCh
- `Scheduler.Stop()`: releases mutex before wg.Wait() to prevent deadlock
- TCP conn pool: uses closed flag + closeMu to prevent panic on send to closed channel
- DLQ: captures replayFn under lock before executing outside lock
- `CircuitBreaker.State()`: acquires mutex (was previously racy)
- `goServiceCaller` (CGo): has `defer recover()` - panics don't crash process
- All data persistence files written with 0600 (not 0644)
- TLS cipher suites: explicit allowlist (ECDHE+AES-GCM only)

## Debugging

### Go Race Detector

```bash
go test -race ./...
```

### Rust Backtraces

```bash
RUST_BACKTRACE=1 cargo test
```

### Logging

```bash
FLOWRULZ_LOG=debug go run ./cmd/flowrulz/
```

### Metrics

```bash
curl http://localhost:8080/metrics
```

## CI/CD

GitHub Actions workflow at `.github/workflows/ci.yml`:

- Go tests with race detector
- Rust tests
- Linting (golangci-lint, cargo clippy)
- Build verification

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes
4. Run tests
5. Submit a pull request

### Code Style

- Go: follow `golangci-lint` rules
- Rust: follow `cargo clippy` suggestions
- No comments unless asked
- Keep responses concise
