# FlowRulZ — Kafka Message Routing VM

Two-layer rule engine: Rust core (bytecode VM + DSL compiler) + Go I/O shell.

## Project Map

```
FlowRulZ/
├── rust/          # Core: DSL toolchain, bytecode VM, memory management
│   ├── src/
│   │   ├── bytecode/   # Instruction set, plan format, const pool
│   │   ├── dsl/        # Lexer, parser, optimizer, compiler
│   │   ├── executor/   # VM dispatch loop + op handlers
│   │   └── memory/     # Arena allocator, slab pool, string interning
│   └── Cargo.toml
├── go/            # Go I/O shell
│   ├── cmd/flowrulz/   # Entry point (HTTP admin + Kafka consumer)
│   └── internal/
│       ├── bridge/         # cgo bindings to Rust FFI
│       ├── engine/         # Rule lifecycle management
│       ├── flow/           # Flow orchestration
│       ├── transport/      # Kafka consumer/producer
│       ├── admin/          # Admin HTTP API
│       ├── observability/  # Metrics counters
│       └── reliability/    # Circuit breaker
├── docs/
│   ├── specs/
│   │   ├── dsl-syntax.md
│   │   ├── bytecode-format.md
│   │   ├── vm-architecture.md
│   │   ├── memory-management.md
│   │   ├── ffi-api.md
│   │   └── kafka-semantics.md
│   └── development.md
├── Makefile
├── go.mod
└── README.md
```

## Quick Start

```bash
# Full build + all tests
make && make test

# Run server (HTTP admin on :8080)
./flowrulz
```

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Rust hot path, Go I/O | Performance-critical execution in Rust; Go for admin, observability, transport |
| 8-byte packed instructions | Cache-friendly, easy to snapshot/serialize |
| Slab pool for messages | Zero-alloc message lifecycle via `flowrulz_msg_alloc` / `flowrulz_msg_release` |
| DSL → bytecode compiler | Compile once, execute many; no parse cost per message |
| DAG as embedded sub-language | Complex routing expressed declaratively; validated at compile time |
| Go service caller bridge | Rust VM calls back into Go via `//export` + C helper; enables Go service dispatch |
