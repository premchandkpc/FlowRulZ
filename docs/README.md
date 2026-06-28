# FlowRulZ Documentation

Distributed programmable event runtime and message bus. One platform for publish/subscribe, request/reply, rule execution, and workflow orchestration.

## Project Map

```
FlowRulZ/
├── rust/          # Core: DSL toolchain, bytecode VM, event model, memory management
│   ├── src/
│   │   ├── bytecode/   # Event, ExecutionContext, Instruction set, plan format, type system
│   │   ├── dsl/        # Lexer, parser, optimizer, compiler (with type checking)
│   │   ├── executor/   # VM dispatch loop + op handlers + ExecutionRuntime + expr engine
│   │   ├── tracing/    # Lock-free span ring buffer
│   │   └── memory/     # Arena allocator, slab pool, string interning
│   ├── benches/        # Criterion benchmarks
│   └── Cargo.toml
├── go/            # Go data plane + SDK
│   ├── cmd/flowrulz/   # Entry point (ExecutionNode)
│   ├── flow/           # Client SDK (Publish, Request, Execute, Stream)
│   └── internal/
│       ├── bridge/         # cgo bindings to Rust FFI (sync.Map caller dispatch)
│       ├── engine/         # Rule lifecycle, versioning, lane routing, persistence
│       ├── execnode/       # ExecutionNode process (engine + transport + admin lifecycle)
│       ├── transport/      # Kafka consumer/producer
│       ├── admin/          # HTTP API (rules CRUD, validate, promote, lanes)
│       ├── flow/           # Flow orchestration
│       ├── registry/       # ServiceRegistry — service name → healthy endpoints, LB
│       ├── replyrouter/    # ReplyRouter — correlation ID → pending request channel
│       ├── observability/  # Metrics counters
│       └── reliability/    # Circuit breaker
├── docs/
│   ├── specs/
│   │   ├── flow-architecture.md  # Distributed Event Runtime — architecture, Event model, ExecutionContext, flows
│   │   ├── dsl-syntax.md         # DSL language specification
│   │   ├── bytecode-format.md    # ExecutionPlan, Instruction, opcodes, types
│   │   ├── vm-architecture.md    # VM dispatch, opcode handlers, ExecutionContext
│   │   ├── memory-management.md  # Slab pool, arena, interning, message lifecycle
│   │   ├── ffi-api.md            # C FFI surface for Go bridge
│   │   ├── kafka-semantics.md    # Consumer groups, offset commit, DLQ
│   │   └── cluster-model.md      # Single-leader cluster, membership, plan distribution, service registry
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
| Event as universal type | Opaque payload — any serialization (JSON, Protobuf, Avro, MessagePack, binary) |
| 8-byte packed instructions | Cache-friendly, easy to snapshot/serialize |
| Slab pool for messages | Zero-alloc message lifecycle via `flowrulz_msg_alloc` / `flowrulz_msg_release` |
| DSL → bytecode compiler | Compile once, execute many; no parse cost per message |
| ExecutionContext | Services enrich context (body + outputs + variables) instead of replacing a single JSON blob |
| DAG as embedded sub-language | Complex routing expressed declaratively; validated at compile time |
| Go service caller bridge | Rust VM calls back into Go via `sync.Map` + C helper; concurrent callers by ctxID |
| Complexity scoring | Compile-time cost estimate → lane assignment (fast/normal/heavy) |
| Schema-typed fields | Runtime type validation via `TypeGuard` opcode; compile-time Gate/Map operator checking |
| Enum types | Field validation against allowed value set; `enum[val1|val2|...]` syntax |
| File-based persistence | Rules saved/loaded as JSON; atomic write via `.tmp` + `os.Rename` |
| 4 communication models | Publish (async), Request (sync), Execute (rule), Stream (subscription) — single SDK |
| Single-leader cluster | Lowest-ID alive node is leader; no Raft/Paxos — Kafka provides durability |
| Seed-based membership | Nodes discover via seed peers; heartbeat on `_flowrulz_members` compacted topic |
| Service Registry | Nodes register services in heartbeat; leader aggregates → publishes combined view |
| Reply Router | Per-node pending request tracker by correlation_id; timeout/cleanup goroutine |
