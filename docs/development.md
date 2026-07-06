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
cd runtime && cargo build --release

# Go only (requires prebuilt Rust cdylib)
make go
```

The Rust library is built as both `cdylib` and `rlib`. The `cdylib` (`libflowrulz_core.dylib`/`.so`) is linked by the Go shell via cgo.

## Test

```bash
# All tests (Rust 401 + Go all packages)
make test

# Rust only
cd runtime && cargo test

# Go only
CGO_ENABLED=1 go test -count=1 ./server/... ./simulator/...

# E2E cluster tests (3-node docker-compose)
make e2e

# Go lint
CGO_ENABLED=1 go vet ./server/... ./simulator/...
```

## Bench

```bash
make bench
```

Criterion benchmarks: compile (5 DSL variants), VM execute, full pipeline, gate eval, DAG.

## Project Layout

```
runtime/src/
├── lib.rs              # C FFI exports, module declarations
├── bytecode/           # Instruction set, plan types, event model, type system
│   ├── mod.rs
│   ├── event.rs        # Event, Mode, EventMetadata — universal message type
│   ├── execution.rs    # ExecutionContext — event + body + variables + outputs
│   ├── opcode.rs       # Opcode enum (0–24) + GateOp, ChunkMode, RetryStrategy
│   ├── instruction.rs  # 8-byte packed Instruction
│   ├── consts.rs       # ConstantPool
│   ├── services.rs     # ServiceTable
│   ├── dag_table.rs    # DAGTable + DAGNode + DAGFailurePolicy + MergeStrategy
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
│   ├── runtime.rs      # ExecutionRuntime — Chunk/Buffer orchestration
│   ├── next.rs         # Service call + retry
│   ├── parallel.rs     # Parallel fan-out
│   ├── gate.rs         # Conditional branch
│   ├── emit.rs         # Fire-and-forget
│   ├── map.rs          # Field transformation + WASM plugin dispatch
│   ├── plugin.rs       # WASM plugin runtime (wasmtime sandbox, registry, caching)
│   ├── dag.rs          # DAG execution (parent merging, failure policies, merge strategies)
│   ├── chunk.rs        # Chunk processing
│   ├── helpers.rs      # JSON utilities
│   └── expr.rs         # Expression engine (31 builtins)
├── ffi.rs              # extern "C" exports for Go bridge
├── tracing/            # Span ring buffer
│   ├── mod.rs          # Span struct + thread_local buffer + emit_span
│   # (ring buffer implemented inline in mod.rs — no separate file)
└── memory/             # Memory management
    ├── mod.rs
    ├── arena.rs        # Bump allocator
    └── intern.rs       # String interning

server/
├── bridge/                 # cgo bindings to Rust FFI
│   ├── bridge.go           # Go wrappers + sync.Map caller dispatch
│   ├── caller_bridge.c     # C helper for function pointer callback
│   └── bridge_test.go      # Integration tests
├── cmd/flowrulz/           # Entry point (uses bootstrap.NodeBuilder)
├── pkg/                    # Public interfaces (13 packages for DI/testability)
│   ├── transport/eventbus.go  # EventBus, Message, Handler, Subscription types
│   ├── cluster/               # Raft + membership interfaces
│   ├── scheduler/             # Task scheduling + lane interfaces
│   ├── engine/                # Rule lifecycle interfaces
│   ├── node/                  # Node interface + Dependencies (ExecRegistry, NodeEngine, etc.)
│   ├── plandist/              # Plan distribution interfaces
│   ├── partition/             # Partition management interfaces
│   ├── membership/            # Node membership interfaces
│   ├── store/                 # Execution state persistence interfaces
│   ├── registry/              # Service registry interfaces
│   ├── reliability/           # Circuit breaker, DLQ, rate limit, dedup, saga interfaces
│   ├── replyrouter/           # Reply router interface
│   └── vm/                    # Plan compilation + execution interfaces
└── internal/
    ├── node/               # ProdNode — composition root with sub-components
    │   ├── prod.go           # ProdNode struct + NewNode() constructor
    │   ├── interfaces.go     # 16 DI interfaces (LeadershipStrategy, TransportFactory, etc.)
    │   ├── layers.go         # 6 dependency bags (Cluster, Transport, Execution, etc.)
    │   ├── execution_engine.go # VM step-loop + circuit breakers + saga
    │   ├── ingress_pipeline.go # Rate limit → dedup → execute → DLQ
    │   ├── message_router.go   # 5-topic consumer demux
    │   ├── admin_http.go       # HTTP API (health, metrics, executions, partitions)
    │   ├── leadership.go       # Strategy pattern: Raft or SingleLeader
    │   ├── recovery.go         # Resume in-flight executions from state store
    │   ├── production_invoker.go # Protocol-aware service dispatch (HTTP/gRPC/TCP)
    │   └── cluster_adapter.go   # Cluster → TransportFactory bridge
    ├── bootstrap/          # NodeBuilder — DI composition root
    ├── engine/             # Rule lifecycle, versioning, lane routing, persistence
    ├── scheduler/          # Priority lanes + work stealing
    ├── cluster/            # gRPC p2p Cluster Bus + Gossip + transport adapter
    ├── transport/          # Pluggable transport factory (Kafka, cluster, memory)
    │   ├── factory.go         # TransportFactory with backend switching
    │   ├── registry.go        # In-memory transport registration
    │   └── kafka/             # Sarama-backed Kafka producer/consumer
    ├── cache/              # Pluggable cache (memory, Redis)
    │   ├── cache.go           # Cache + CacheProvider interfaces
    │   ├── memory.go          # In-memory backend with TTL + LRU eviction
    │   └── redis.go           # Redis backend
    ├── flow/               # Flow DSL — high-level workflow language
    │   ├── lexer.go           # Hand-written tokenizer (40+ tokens)
    │   ├── parser.go          # Recursive descent parser → AST
    │   ├── ast.go             # AST node types (Flow, Service, WorkflowStep, etc.)
    │   ├── semantic.go        # Semantic analysis (service/event reference validation)
    │   ├── ir.go              # AST → IR graph compilation
    │   ├── codegen.go         # IR → Go/Rust/Java/Python source generation
    │   ├── graph.go           # IR → Graphviz DOT / Mermaid diagrams
    │   ├── formatter.go       # Canonical .flow formatting
    │   ├── cli.go             # CLI (fmt, validate, graph, codegen, info)
    │   ├── lsp.go             # LSP server (completion, hover, diagnostics, formatting)
    │   ├── watcher.go         # File watcher with debounced hot-reload
    │   └── registry.go        # Runtime store with cache-backed IR
    ├── admin/              # HTTP API (rules CRUD, validate, promote, lanes)
    ├── plandist/           # Plan distribution + ack protocol
    ├── partition/          # Key-space shard mgmt + rebalancing
    ├── membership/         # Gossip, leader lease, heartbeat eviction
    ├── execstate/          # In-memory + file execution state persistence
    ├── reliability/        # DLQ, saga, circuit breaker, dedup (16-shard LRU), rate limiter
    ├── registry/           # Service registry via HTTP heartbeat
    ├── replyrouter/        # ReplyRouter — correlation ID → pending request channel
    ├── observability/      # OTel tracing, Prometheus metrics
    ├── compiler/           # DSL compiler abstraction (local/remote)
    ├── plugins/            # WASM plugin loader
    ├── flowengine/         # Flow orchestration state machine
    ├── adapters/           # Adapters implementing pkg/ interfaces
    └── ports/              # Port interfaces

simulator/                  # Virtual Enterprise Platform (40+ services, 8 modes, 50+ scenarios)
├── cmd/simulator/          # CLI entry point (--scenario, --mode, --interactive)
├── config/                 # SimConfig, ChaosConfig
├── dashboard/              # HTTP dashboard + admin API (live metrics, send/rules/services)
├── dispatcher/             # Hash-based message routing to nodes
├── execution/              # ExecutionContext, Plan, 25+ execution plans
├── loadgen/                # Traffic generation by scenario (constant, burst, ramp-up)
├── metrics/                # Metrics collector (throughput, latency, error rates)
├── modes.go                # 8 simulator modes (simple, enterprise, chaos, performance, distributed, multi-region, interview, learning)
├── network/                # Simulated network (latency, drop, slow, duplicate)
├── scheduler/              # Per-node worker pool, PlanCache, executeContext/executeBridge
├── scenarios/              # 50+ built-in scenarios across 6 categories
│   ├── registry.go         # Scenario definitions (descriptive)
│   └── scenarios.go        # Executable scenarios (Apply/Setup functions)
├── services/               # 40+ mock services with configurable latency/failure
├── timeline/               # Event timeline store
├── simulator.go            # Simulator struct — orchestrates all components
├── client.go               # Client — Send, RegisterService, AddRule
├── handlers.go             # Admin HTTP handlers (registered on dashboard mux)
├── routes.go               # HTTP route definitions
└── client_test.go          # Client tests
```

## Writing a WASM Plugin

WASM plugins are sandboxed WebAssembly modules called from DSL via `w:plugin.func()`. Each plugin exports `memory` and one or more `process`-style functions.

**Plugin calling convention:**
- Function signature: `(input_ptr: i32, input_len: i32) → output_len_or_packed: i64`
- Host writes input JSON at `input_ptr` in linear memory before calling
- Function reads input, processes it, writes output to linear memory
- Returns `(output_ptr << 32) | output_len` as i64
- 100k fuel limit prevents infinite loops

**Example plugin (WAT):**
```wat
(module
  (memory (export "memory") 1)
  (func (export "verify") (param $ptr i32) (param $len i32) (result i64)
    ;; read input at $ptr, write result, return packed pointer+length
    (i64.or
      (i64.shl (i64.extend_i32_u (local.get $ptr)) (i64.const 32))
      (i64.extend_i32_u (local.get $len))
    )
  )
)
```

**Deployment:**
1. Compile your WASM module to `.wasm`
2. Place it in the directory pointed to by `FLOWRULZ_PLUGIN_DIR` (default: none)
3. Filename without `.wasm` extension becomes the plugin name
4. Reference in DSL as `w:<filename>.<funcname>`

**Wiring in code:**
- Rust: `plugin::register("name", &wasm_bytes)` / `plugin::call("name", "func", &input)`
- Go: `bridge.RegisterPlugin("name", wasmBytes)` / `plugins.LoadDir("/path/to/dir")`

## Flow DSL CLI

The Flow DSL (`server/internal/flow/`) is a high-level, block-structured workflow language separate from the Rust bytecode DSL. It compiles to an IR graph and can generate Go, Rust, Java, or Python source code.

```bash
# Format .flow files in canonical style
flow fmt *.flow

# Validate (parse + semantic analysis)
flow validate signup.flow

# Generate graph (dot or mermaid)
flow graph -format dot signup.flow
flow graph -format mermaid signup.flow

# Generate source code
flow codegen -target go signup.flow
flow codegen -target rust signup.flow
flow codegen -target java signup.flow
flow codegen -target python signup.flow

# Print summary
flow info signup.flow

# Help
flow help
```

**Example .flow file:**
```flow
version 1

flow UserSignup

service auth
    type grpc
    address auth:50051

service email
    type http
    url https://email.internal

workflow

Start
-> auth.CreateUser
-> email.SendWelcome
-> End
```

See `docs/flow-dsl.md` for the full language specification.

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
- **Errors:** `FfiError` enum with `Display` impl; `CompileError` for DSL errors; return `Result<_, CompileError>` or error codes for FFI
- **Testing:** Rust unit tests inline in source files (`#[cfg(test)]`); Go test files alongside source
- **FFI safety:** All `extern "C"` functions check null pointers; return error codes
- **Go cgo pattern:** Callbacks use `//export` + `sync.Map` caller dispatch by `ctx_id`; no mutex in hot path

## Performance Considerations

- VM dispatch uses `match` on opcode — compiler generates jump table
- Service calls are FFI-bound (C callbacks into Go); overhead dominated by serialization, not dispatch
- `std::alloc` for FFI message allocation (no custom allocator)
- Expression engine uses simple recursive descent with quote-aware argument parsing — no parser generator dependency
- Span ring buffer is thread-local + lock-free; zero contention per thread
