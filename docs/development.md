# Development Guide

## Prerequisites

- Rust 1.70+ (edition 2021)
- Go 1.26+
- No other system dependencies

## Build

```bash
# Full build (Rust cdylib + Go binary)
make

# Rust only
cd rust && cargo build --release

# Go only (requires prebuilt Rust cdylib)
make go
```

The Rust library is built as both `cdylib` and `rlib`. The `cdylib` (`libflowrulz_core.dylib`/`.so`) is linked by the Go shell via cgo.

## Test

```bash
# All tests (Rust 119 + Go all packages)
make test

# Rust only
cd rust && cargo test

# Go only
CGO_ENABLED=1 go test -count=1 ./go/... ./simulator/...

# Go lint
CGO_ENABLED=1 go vet ./go/... ./simulator/...
```

## Bench

```bash
make bench
```

Criterion benchmarks: compile (5 DSL variants), VM execute, full pipeline, gate eval, DAG.

## Project Layout

```
rust/src/
├── lib.rs              # C FFI exports, module declarations
├── bytecode/           # Instruction set, plan types, event model, type system
│   ├── mod.rs
│   ├── event.rs        # Event, Mode, EventMetadata — universal message type
│   ├── execution.rs    # ExecutionContext — event + body + variables + outputs
│   ├── opcode.rs       # Opcode enum (0–22) + GateOp, ChunkMode, RetryStrategy
│   ├── instruction.rs  # 8-byte packed Instruction
│   ├── consts.rs       # ConstantPool
│   ├── services.rs     # ServiceTable
│   ├── dag_table.rs    # DAGTable + DAGNode + DAGFailurePolicy + MergeStrategy
│   ├── mapexpr.rs      # MapExpr
│   ├── resolved_type.rs# ResolvedType enum (incl. Enum), FieldSchema, Schema
│   └── plan.rs         # ExecutionPlan
├── dsl/                # Language toolchain
│   ├── mod.rs
│   ├── lexer.rs        # Tokenizer
│   ├── parser.rs       # AST builder
│   ├── optimizer.rs    # AST optimizations
│   └── compiler.rs     # AST → ExecutionPlan (complexity scoring, schema, type checking)
├── executor/           # Virtual machine
│   ├── mod.rs          # VM dispatch loop + TypeGuard handler (operates on ExecutionContext)
│   ├── context.rs      # Re-exports bytecode::execution::ExecutionContext
│   ├── runtime.rs      # ExecutionRuntime — Chunk/Buffer orchestration
│   ├── next.rs         # Service call + retry
│   ├── parallel.rs     # Parallel fan-out
│   ├── gate.rs         # Conditional branch
│   ├── emit.rs         # Fire-and-forget
│   ├── map.rs          # Field transformation
│   ├── dag.rs          # DAG execution (parent merging, failure policies, merge strategies)
│   ├── chunk.rs        # Chunk processing
│   ├── helpers.rs      # JSON utilities
│   └── expr.rs         # Expression engine (22 builtins)
├── ffi.rs              # extern "C" exports for Go bridge
├── tracing/            # Span ring buffer
│   ├── mod.rs          # Span struct + thread_local buffer + emit_span
│   └── ring_buffer.rs  # Lock-free ring buffer (atomic head/tail)
└── memory/             # Memory management
    ├── mod.rs
    ├── arena.rs        # Bump allocator
    ├── slab.rs         # Slab pool
    └── intern.rs       # String interning

go/
├── bridge/                 # cgo bindings to Rust FFI
│   ├── bridge.go           # Go wrappers + sync.Map caller dispatch
│   ├── caller_bridge.c     # C helper for function pointer callback
│   └── bridge_test.go      # Integration tests
├── cmd/flowrulz/           # Entry point (uses execnode package)
├── flow/                   # Client SDK (Publish, Request, Execute, Stream)
│   └── client.go
└── internal/
    ├── engine/             # Rule lifecycle, versioning, lane routing, persistence
    ├── execnode/           # ExecutionNode: process wrapping engine + transport + admin
    ├── transport/          # Kafka consumer/producer (internal topics: _flowrulz_members, _plans, _acks, _replies)
    ├── admin/              # HTTP API (rules CRUD, validate, promote, lanes)
    ├── flow/               # Flow orchestrator with state machine
    ├── registry/           # ServiceRegistry — service name → healthy endpoints, LB, health checks
    ├── replyrouter/        # ReplyRouter — correlation ID → pending request channel, timeout/cleanup
    ├── scheduler/          # Priority queue (fast/normal/heavy), concurrency limits, backpressure
    ├── plandist/           # PlanDistributor — plan/ack topics, versioned ACK quorum, activation
    ├── observability/      # MetricsCollector — counters, gauges, histograms, global shortcuts
    └── reliability/        # DLQ, rate limiter, circuit breaker

simulator/                  # Simulator for testing rules, services, and cluster behavior
├── cmd/simulator/          # CLI entry point (--scenario, --interactive, --dashboard)
├── config/                 # SimConfig, ChaosConfig
├── dashboard/              # HTTP dashboard + admin API (live metrics, send/rules/services)
├── dispatcher/             # Hash-based message routing to nodes
├── execution/              # ExecutionContext, Plan, queues (ReadyQueue, WaitingQueue)
├── loadgen/                # Traffic generation by scenario
├── metrics/                # Metrics collector (throughput, latency, error rates)
├── network/                # Simulated network (latency, drop, slow, duplicate)
├── scheduler/              # Per-node worker pool, PlanCache, executeContext/executeBridge
├── scenarios/              # Built-in scenarios (ramp-up, black-friday, payment-outage, spike-test, chaos-monkey)
├── services/               # MockService with configurable latency/failure, ServiceRegistry
├── timeline/               # Event timeline store
├── simulator.go            # Simulator struct — orchestrates all components
├── client.go               # Client — Send, RegisterService, AddRule
├── admin.go                # Admin HTTP handlers (registered on dashboard mux)
└── client_test.go          # Client tests
```

## Adding a New Opcode

1. Define opcode in `bytecode/opcode.rs` — add variant to `OpCode` enum
2. Add builder in `bytecode/instruction.rs` — `Instruction::your_op()`
3. Handle in `dsl/lexer.rs` — add token variant and lex logic
4. Handle in `dsl/parser.rs` — add AST node and parse logic
5. Handle in `dsl/optimizer.rs` — add optimization rules if needed
6. Emit in `dsl/compiler.rs` — compile AST to instruction
7. Execute in `executor/mod.rs` — add arm in `dispatch()`
8. Add op handler with test coverage

## Adding a Built-in Function (Expression Engine)

1. Define in `executor/expr.rs`:
   - Add function name to `eval_expr()` match
   - Add logic in `call_builtin()` match (takes `&[serde_json::Value]`)
2. Add test covering the new function

## Conventions

- **Naming:** snake_case for functions/vars, CamelCase for types
- **Errors:** Use `thiserror` derive macros; return `Result<_, ExecError>` or `Result<_, CompileError>`
- **Testing:** Rust unit tests inline in source files (`#[cfg(test)]`); Go test files alongside source
- **FFI safety:** All `extern "C"` functions check null pointers; return error codes
- **Go cgo pattern:** Callbacks use `//export` + `sync.Map` caller dispatch by `ctx_id`; no mutex in hot path

## Performance Considerations

- VM dispatch uses `match` on opcode — compiler generates jump table
- Service calls are FFI-bound (C callbacks into Go); overhead dominated by serialization, not dispatch
- Slab pool should be sized to workload peak concurrency
- Expression engine uses simple recursive descent — no parser generator dependency
- Span ring buffer is thread-local + lock-free; zero contention per thread
