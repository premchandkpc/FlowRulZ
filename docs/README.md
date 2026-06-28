# FlowRulZ Documentation

Two-layer rule engine: Rust core (bytecode VM + DSL compiler) + Go I/O shell.

## Project Map

```
FlowRulZ/
├── rust/          # Core: DSL toolchain, bytecode VM, memory management
│   ├── src/
│   │   ├── bytecode/   # Instruction set, plan format, const pool, type system
│   │   ├── dsl/        # Lexer, parser, optimizer, compiler
│   │   ├── executor/   # VM dispatch loop + op handlers + expr engine
│   │   ├── tracing/    # Lock-free span ring buffer
│   │   └── memory/     # Arena allocator, slab pool, string interning
│   ├── benches/        # Criterion benchmarks
│   └── Cargo.toml
├── go/            # Go I/O shell
│   ├── cmd/flowrulz/   # Entry point (HTTP admin + Kafka consumer)
│   └── internal/
│       ├── bridge/         # cgo bindings to Rust FFI (sync.Map caller dispatch)
│       ├── engine/         # Rule lifecycle, versioning, lane routing, persistence
│       ├── flow/           # Flow orchestration
│       ├── transport/      # Kafka consumer/producer
│       ├── admin/          # HTTP API (rules CRUD, validate, promote, lanes)
│       ├── observability/  # Metrics counters
│       └── reliability/    # Circuit breaker
├── docs/
│   ├── specs/
│   │   ├── dsl-syntax.md
│   │   ├── bytecode-format.md
│   │   ├── vm-architecture.md
│   │   ├── memory-management.md
│   │   ├── ffi-api.md
│   │   ├── kafka-semantics.md
│   │   └── flow-architecture.md
│   ├── development.md
│   └── README.md
├── CLAUDE.md
├── Makefile
├── go.mod
└── README.md
```

## Quick Start

```bash
# Full build + all tests
make && make test

# Benchmarks
make bench

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
| Go service caller bridge | Rust VM calls back into Go via `sync.Map` + C helper; concurrent callers by ctxID |
| Complexity scoring | Compile-time cost estimate → lane assignment (fast/normal/heavy) |
| Schema-typed fields | Runtime type validation via `TypeGuard` opcode; no silent type coercion |
| File-based persistence | Rules saved/loaded as JSON; no external DB dependency |
