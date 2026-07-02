# FlowRulZ Engineering Audit

> Prepared: July 1, 2026
> Scope: Full-stack review — product through enterprise readiness
> Standard: Google/AWS/Temporal/Kafka Distinguished Engineer

---

## Table of Contents

1. [Product Review](#1-product-review)
2. [Repository Review](#2-repository-review)
3. [Architecture Review](#3-architecture-review)
4. [Runtime Review](#4-runtime-review)
5. [Compiler Review](#5-compiler-review)
6. [DSL Review](#6-dsl-review)
7. [Bytecode Review](#7-bytecode-review)
8. [Virtual Machine Review](#8-virtual-machine-review)
9. [Distributed Systems Review](#9-distributed-systems-review)
10. [Scheduler Review](#10-scheduler-review)
11. [Transport Review](#11-transport-review)
12. [Storage Review](#12-storage-review)
13. [Plugin System Review](#13-plugin-system-review)
14. [API Review](#14-api-review)
15. [Code Quality Review](#15-code-quality-review)
16. [Concurrency Review](#16-concurrency-review)
17. [Performance Review](#17-performance-review)
18. [Security Review](#18-security-review)
19. [Observability Review](#19-observability-review)
20. [Testing Review](#20-testing-review)
21. [Documentation Review](#21-documentation-review)
22. [Scalability Review](#22-scalability-review)
23. [Evolution Review](#23-evolution-review)
24. [Enterprise Readiness Review](#24-enterprise-readiness-review)
25. [Executive Summary & Action Plan](#25-executive-summary--action-plan)

---

## 1. Product Review

**What it solves:** A unified execution runtime where Pub/Sub, RPC, workflows, and rules are the same thing — bytecode-compiled pipelines over a universal event model.

**Vision clarity:** Good. "One runtime, four models" is a strong differentiator. The DSL-to-bytecode approach is correct.

**Scope creep risk:** Medium. The project tries to be a workflow engine (Temporal), rules engine (Drools), event bus (Kafka), and RPC framework (gRPC) simultaneously. Each of those is a decades-long platform project on its own.

**What should be removed:**
- The `simulator/` directory. It's 20+ files of a discrete event simulator with its own dashboard, scheduler, loadgen, network simulation. This is a separate product, not a test tool. Ship it as a standalone project or remove it.
- `flowrulz-visual-guide (1).html` — has `(1)` suffix, suggests accidental duplicate. Clean up.
- 8 no-op opcodes (`Retry`, `Timeout`, `Pipe`, `Key`, `Split`, `SvcArg`, `RetryData`, `JumpOffset`) — strip from runtime bytecode before it hits the VM.

**Competitive landscape:**

| System | Strengths Over FlowRulZ | FlowRulZ Advantage |
|--------|------------------------|---------------------|
| **Temporal** | Deterministic replay, SDKs in 10+ langs, 5+ years production | Simpler model, no SDK required, bytecode compilation |
| **Kafka** | 10+ years, 100K+ nodes, ecosystem | Execution semantics built-in, not just transport |
| **Flink** | Stream processing, state management, exactly-once | Rule-oriented, lower latency per-event |
| **Dapr** | Sidecar model, multi-language, Microsoft backing | No sidecar overhead, unified execution model |
| **Argo Workflows** | Kubernetes-native, YAML-based, 10K+ stars | Lighter weight, no K8s dependency |

**Differentiator worth keeping:** DSL → bytecode → VM pipeline. No other system compiles rules to bytecode. This is genuinely novel.

---

## 2. Repository Review

**Current layout:**
```
/server     — Go control plane + data plane
/runtime    — Rust VM + compiler toolchain
/sdk        — Polyglot SDKs (Go, Java, Python, JS/TS, Rust)
/simulator  — Discrete event simulator
/docs       — Documentation + Obsidian vault
/e2e        — Cluster tests
/k8s        — Kubernetes manifests
```

**Strengths:**
- Clean Rust/Go language split
- `docs/` is thorough (13 files)
- `e2e/` cluster tests exist

**Weaknesses:**
- `go.mod` at repo root while Go code is in `server/` — confusing
- `simulator/` at repo root with its own `cmd/`, `config/`, etc. — should be separate repo
- No ADR directory
- No `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, `CHANGELOG.md`

**Recommended layout:**
```
flowrulz/
├── sdk/              # Go SDK (flow/client.go)
│   ├── flow/
│   └── bridge/
├── runtime/          # Rust VM + compiler
│   ├── src/
│   │   ├── compiler/  # lexer, parser, optimizer, compiler
│   │   ├── vm/        # executor, bytecode
│   │   └── ffi/       # C FFI
│   └── benches/
├── server/           # Go data plane
│   ├── engine/
│   ├── scheduler/
│   ├── cluster/
│   ├── transport/
│   └── admin/
├── proto/            # Protobuf (shared)
├── docs/
├── tests/
│   ├── e2e/
│   └── chaos/
├── k8s/
└── docker/
```

---

## 3. Architecture Review

**Current architecture (simplified):**
```
                    ┌──────────────┐
                    │   Admin API   │
                    │  (REST/HTTP)  │
                    └──────┬───────┘
                           │
┌────────┐    ┌────────────▼────────┐    ┌──────────┐
│ Client │───▶│   ProdNode          │───▶│  Engine  │
│ (SDK)  │    │  (DI via NodeBuilder) │    │ (Rules)  │
└────────┘    │                     │    └────┬─────┘
              │  ┌──────┐ ┌────────┐│         │
              │  │Sched │ │Cluster ││         │
              │  +steal │ │+Raft   ││         │
              │  └──────┘ └────────┘│         ▼
              │  ┌──────┐ ┌────────┐│   ┌──────────┐
              │  │Transp│ │Planner ││   │   Rust   │
              │  └──────┘ └────────┘│   │   VM     │
              │  ┌──────┐ ┌────────┐│   │  (FFI)   │
              │  │Admin │ │Members ││   └──────────┘
              │  └──────┘ └────────┘│
              └─────────────────────┘
```

**Critical architectural issues:**

1. **~~`ExecutionNode` is a God object.~~** **✅ RESOLVED** — `server/internal/execnode/` was deleted (11 files removed). Replaced by `server/internal/node/ProdNode` (13 files) + `server/internal/bootstrap/NodeBuilder` with `DefaultDependencies()`. DI constructor delegates, all subsystems wired via `Dependencies` struct.

2. **Rust VM is a library, not a service.** The VM is a C library loaded via CGo. Every execution step crosses the FFI boundary (Go → C → Rust → C → Go). No persistent VM process. No shared memory. This is the single biggest performance bottleneck.

3. **No consensus layer.** Single-leader election based on lowest node ID. No Raft, no etcd, no ZooKeeper. Leader crash means:
   - All in-flight executions are lost (no state transfer)
   - No compilation until a new leader is elected
   - Split-brain is possible if `stopHb` fails

4. **~~Engine and Scheduler are independent, not integrated.~~** **✅ RESOLVED** — `Engine.ExecuteAll()` now routes through `scheduler.EnqueueAndWait()`. All execution goes through the priority lanes with work stealing.

5. **~~No execution history.~~** **✅ RESOLVED** — Completed execution states are now saved as `StatusCompleted` with output persisted in `FileStore`. Audit trail available via `server/internal/execstate/`.

---

## 4. Runtime Review

**Boot sequence:** `cmd/flowrulz/main.go` → `ExecutionNode.Start()` initializes 15+ subsystems sequentially. No dependency graph, no health checks between subsystems, no readiness protocol.

**Lifecycle issues:**
- No graceful degradation — if Kafka is down, the node panics or hangs
- `OtelExporter` starts before the gRPC bus is ready
- State store (`FileStore`) is not replicated — each node has its own state
- No liveness probe for the Rust VM (it's embedded via CGo; a panic in Rust kills the entire Go process)
- `GRPCBus` starts before cluster membership is established

**Shutdown:**
- `signal.Notify` catches SIGINT/SIGTERM
- Order: stop HTTP, stop consumers, stop scheduler, stop producers
- No in-flight execution drain (ActiveExec.Wait() exists in Engine.Drain but is not called on shutdown)

**Crash recovery:**
- No WAL or transaction log
- No replay mechanism
- No checkpointing
- `FileStore` state is written at arbitrary points — crash between consume and state persist loses the message (acknowledged in review)

---

## 5. Compiler Review

**Pipeline:** `Lexer → Parser → Optimizer → Compiler → Bytecode`

**Strengths:**
- Clean 4-stage pipeline
- Compiler does type checking with schema validation
- Optimizer does dead code elimination, constant folding, label preservation

**Missing:**
- **No HIR.** The optimizer works directly on the parsed AST (`Vec<PipelineStep>`). No intermediate representation means cross-step optimizations are impossible (e.g., common subexpression elimination on field accesses).
- **No MIR.** Bytecode is generated directly from the AST. No low-level IR means no register allocation, no instruction scheduling.
- **No error recovery.** The lexer fails on the first error. No `--continue-on-error` mode for IDE integration.
- **No incremental compilation.** Every compilation re-parses and re-optimizes the entire DSL from scratch.
- **No span tracking.** Error messages point to line numbers but don't track source spans through the pipeline. A parse error in a 500-char DSL line gives no caret position.

**`compiler.rs:55` — `compile()` method:**
```rust
pub fn compile(&self, steps: &[PipelineStep], rule_id: &str) -> Result<ExecutionPlan, CompileError>
```
- Single monolithic function, 100+ lines
- No visitor pattern for instruction generation
- Schema validation mixed with code generation

**`optimizer.rs`:**
- `Optimizer::optimize()` returns `Vec<PipelineStep>` — same type as input. No optimization report, no statistics.
- Constant folding only handles Gate operations. No dead store elimination, no copy propagation.

---

## 6. DSL Review

**Grammar (from `dsl-syntax.md`):**
```
<step>        := <opcode> ( ':' <operands> )?
<opcode>      := 'n' | 'e' | 'p' | 'c' | 'm' | 'g' | 'd' | 'f' | ...
<operands>    := <name> ( ',' <name> )*
<gate>        := 'g' <field> <op> <value>
<dag>         := 'dag:{' <entry> '}'
```

**Strengths:**
- Concise — one line per pipeline step
- Pipeline separator (space) is intuitive
- Gate conditions are readable: `g:x==1`

**Weaknesses:**
- **No named pipelines.** Every pipeline is anonymous. No way to reference another pipeline by name.
- **No variables.** The DSL has no way to define intermediate values. `m:.x` maps a field but can't store the result.
- **No error handling syntax.** `f:dlq` is the only fallback. No retry policies, no catch blocks, no conditional error paths.
- **No comments.** No syntax for explaining pipeline steps.
- **Ambiguous precedence.** `g:x==1 n:svc e:notify` — is `e:notify` part of the gate's true branch or a separate step after it? (It's the latter, but not obvious.)
- **`p:a,b c`** — Parallel followed by Collect. The `c` looks like an operand of `p`. New users will write `p:a,b,c` (comma-separated) or `p:a,b;c` instead.

**Error messages:**
```
flowrulz_compile parse: unexpected token
```
vs what Temporal would show:
```
Syntax error at line 1, column 12:
  g:x==1 n:svc e:notify
             ^
Expected: ',' or end of pipeline
```

---

## 7. Bytecode Review

**`Instruction` struct:**
```rust
pub struct Instruction {
    pub op: OpCode,      // 1 byte
    pub a: u16,          // 2 bytes
    pub b: u16,          // 2 bytes
    pub flags: u8,       // 1 byte
}
```
6 bytes per instruction. 23 opcodes. Encoding is plain bincode.

**Issues:**
- **No variable-length encoding.** Every instruction is 6 bytes regardless of whether it uses all fields. `Label` uses only the opcode byte.
- **No compression.** A pipeline with 100 instructions is always `100 * 6 = 600` bytes plus constant pool. For a million rules, this is ~600MB of bytecode in memory.
- **`a` and `b` are overloaded.** `a` can be a service ID, field path index, comparison value index, delay ms, etc. No type safety at the bytecode level.
- **No forward references.** Jump targets use absolute instruction indices. If instructions are inserted/removed, all jump offsets must be recomputed. A proper bytecode would use labels resolved at load time.
- **Constant pool is flat.** No way to deduplicate strings shared across instructions.

**`ExecutionPlan`:**
```rust
pub struct ExecutionPlan {
    pub rule_id: String,
    pub version: u64,
    pub instr_count: u32,
    pub complexity_score: u32,
    pub instructions: Vec<Instruction>,
    pub const_pool: ConstantPool,
    pub services: ServiceTable,
    pub dag_tables: Vec<DAGTable>,
    pub retry_configs: Vec<RetryConfig>,
    pub chunk_configs: Vec<ChunkConfig>,
    pub schema: Option<Schema>,
}
```
- `instr_count` is redundant with `instructions.len()`
- `services` stores full entries — but services are looked up by the Go side, not the Rust VM
- `dag_tables`, `retry_configs`, `chunk_configs` are rarely used vecs taking serialization space
- No checksum or integrity hash

---

## 8. Virtual Machine Review

**Strengths:**
- Clean dispatch loop (`dispatch()` → op-specific method)
- `StepResult` enum cleanly separates Pending/Continue/Done/Delay
- Arena allocator for temporary values
- Span tracing with lock-free ring buffer

**Issues:**

1. **No deterministic execution.** The VM calls external services via the `caller` callback. These calls are not recorded, not replayable. If you re-execute the same plan with the same input, you get different results. This is a correctness issue for any system claiming workflow semantics.

2. **`step()` has two paths with different semantics.** Path A (Pending ops): returns `StepResult::Pending` without executing the instruction. Path B (everything else): executes the instruction and returns `Continue/Done`. This means `Next` and `SvcCall` are handled differently from `Gate` and `Map` even though both call external services.

3. **No interruptibility.** Once `run()` starts, it runs to completion. There's no way to pause, yield, or cancel mid-execution (the `step()` method fixes this but `run()` doesn't use it).

4. **`ctx.body` is cloned excessively.** `op_svc_call:281`: `let body = self.ctx.body.clone()`. `op_next` passes `&self.ctx.body` but the service call body is cloned in `step():79`. On a pipeline with 10 service calls, the body is cloned 10 times.

5. **No execution budget.** No instruction counting, no time limit per pipeline, no memory limit. A runaway rule can consume unbounded CPU/memory.

6. **Arena never reset.** `Arena::new()` is called once per execution. The arena grows until the execution completes. No way to reclaim memory mid-pipeline.

---

## 9. Distributed Systems Review

**Failure model coverage:**

| Failure | Handled? | Details |
|---------|----------|---------|
| Leader crash | Partial | Lowest-ID leader election. No leader state transfer. New leader starts fresh. |
| Worker crash | No | If a Go worker crashes, the OS kills the process. Rust panic in CGo kills the process. |
| Network partition | No | No split-brain detection. Gossip protocol has no partition healing. |
| Message loss | Partial | Kafka at-least-once delivery. Dedup via `DedupTracker` (best-effort, in-memory, resets on crash). |
| Duplicate execution | Partial | `DedupTracker` has no persistence. Crash → all dedup state lost → duplicates possible. |
| Retry storm | No | Circuit breaker protects service calls, but not rule re-execution on failure. |
| Clock drift | No | `time.Now()` used for deadlines, heartbeats, rate limiting. |
| Slow consumer | No | Kafka consumer has no backpressure mechanism. `ExecuteAll` blocks the consumer. |

**Consensus:** There is none. Lowest-ID leader election is not consensus. It's a distributed systems anti-pattern. With 3 nodes:
- Node 1 (ID=1) is leader
- Node 1 gets partitioned
- Nodes 2 and 3 see leader as dead
- Node 2 (ID=2) becomes leader (both think they're leader)
- Partition heals → two leaders

---

## 10. Scheduler Review

**Current design:**
- 3 priority lanes (Fast=50, Normal=20, Heavy=5 max concurrent)
- Lanes are fixed goroutine pools polling channels
- `Enqueue` / `EnqueueAndWait` API

**Issues:**
1. **~~No work stealing.~~** **✅ RESOLVED** — `slotWorker.dequeueOrSteal()` now steals from Heavy→Normal→Fast lanes when idle. Work stealing enabled in `server/internal/scheduler/`.
2. **No priority inversion handling.** A Heavy task that enqueues a Fast task (e.g., compensating action) gets the Fast priority but blocks on Heavy lane capacity.
3. **Starvation risk.** If Fast is constantly busy, Normal and Heavy never execute. No aging mechanism.
4. **`execTask` runs synchronously in the goroutine.** `slotWorker` blocks on `task.Execute()` which blocks on the FFI call. During a slow FFI call (e.g., a 30-second timeout), the entire goroutine is occupied. With `MaxConcurrent=20`, 20 slow calls can starve the lane.
5. **No fairness between lanes.** Fast gets 50 slots, Normal 20, Heavy 5. But Fast tasks are supposed to be "fast" (low complexity). If someone deploys a complex rule to Fast, they consume 50 slots while actual fast rules wait.

---

## 11. Transport Review

**Current:** Kafka consumer/producer (Sarama), HTTP server, gRPC event bus.

**Issues:**
- **No transport abstraction.** `ExecutionNode` directly imports `transport.Consumer`, `transport.Producer`, `grpctransport.GRPCBus`. Swapping Kafka for NATS or Pulsar requires code changes.
- **No streaming protocol.** gRPC event bus exists but isn't the primary transport. Kafka is the source of truth but Kafka has no execution semantics — it's fire-and-forget.
- **Connection pooling.** HTTP client in `callService` creates a new `http.Client` per `ExecutionNode` but uses `http.DefaultTransport` for the actual connections. No tuning of max idle connections.
- **No mTLS.** Kafka connections are unauthenticated. gRPC has no TLS. Admin API has key auth but no transport security.

---

## 12. Storage Review

**Current state:**
- Rule storage: `FileStore` — JSON files on disk in `~/.flowrulz/`
- Execution state: in-memory `ExecRegistry` — ephemeral, lost on restart
- No database, no indexes, no queries

**Issues:**
- `FileStore` reads/writes entire JSON file per operation. List rules = read all files. For 10,000 rules, this is 10,000 disk reads.
- No atomic operations. Crash between write and flush corrupts state.
- No versioning of stored rules (separate from rule versions — the store itself has no history).
- No backup/restore mechanism.
- Execution state has no persistence. If the node crashes mid-execution:
  - All `ExecRegistry` entries are lost
  - Kafka offsets are committed (auto-commit) → messages are acknowledged but never processed
  - Rule version state (active rules by topic) is in `FileStore` but may be stale

---

## 13. Plugin System Review

**Current:** WASM plugins via wasmtime. Plugins implement expression functions (e.g., `hash`, `extract`). Compiled to WASM and loaded by the Rust VM.

**Strengths:**
- Good isolation — WASM sandbox prevents host corruption
- Language-independent plugin development
- Wasmtime is well-maintained and fast

**Issues:**
- **Narrow API.** Plugins can only define expression functions. No plugin can:
  - Define new opcodes
  - Register new transports
  - Implement custom storage backends
  - Add middleware/interceptors
- **No plugin lifecycle.** No way to install, upgrade, or remove plugins without rebuilding.
- **No plugin registry.** Plugins are loaded by path in config. No versioning, no dependency resolution.
- **No capability-based security.** All plugins have the same permissions (limited, but uniform). No per-plugin sandbox configuration.
- **WASM overhead.** Each expression evaluation crosses WASM boundary. For a pipeline with 50 expressions, this is 50 WASM invocations.

---

## 14. API Review

**Admin API (REST/HTTP):**
- CRUD for rules: `POST/PUT/GET/DELETE /rules`
- Cluster status: `GET /cluster`
- Topic management: `POST /topics`, `DELETE /topics`

**Issues:**
- **No API versioning.** `/rules` today, `/v1/rules` tomorrow? Breaking changes break all clients.
- **No OpenAPI/Swagger spec.** No generated clients, no API documentation beyond code.
- **No consistency guarantees.** `PUT /rules` returns 200 before the rule is compiled or distributed. Race between "rule created" response and "rule active" state.
- **No bulk operations.** Registering 10,000 rules requires 10,000 HTTP calls.
- **No idempotency keys.** Retry a `POST /rules` → duplicate rule.
- **No rate limiting.** 10,000 QPS of `POST /rules` from a misconfigured client will overwhelm the server.

**SDK (Go client):**
- `client.go` wraps the REST API
- No connection pooling, no retry logic, no timeout configuration
- No async client (all calls block)
- No streaming client for execution results

---

## 15. Code Quality Review

**Go side:**
- Mixed error handling patterns. Some functions return `(result, error)`, others use panics (`panic("unsupported plan type")` in `planner.go`).
- `execnode.go:1075` lines — violates single-responsibility principle.
- Interface hygiene: `TransportConsumer`, `TransportProducer` interfaces are defined but not uniformly used. `ExecutionNode` calls `consumer.Consume()` directly.
- Global state: `sync.Map` for circuit breakers is package-level. Hidden coupling between test and production code.
- No `go vet` / `staticcheck` integration in CI (assuming from pattern).

**Rust side:**
- Clean module structure. Good use of `Result` types. No unwrap() in production paths (verified in FFI code).
- `unsafe` usage is limited to FFI boundary — correct.
- A few long functions: `compile()` (100+ lines), `dispatch()` (80+ lines).
- No `clippy` integration would catch: redundant clones, large enum variants, missing `#[must_use]`.

**Common:**
- No linting configuration files (`.golangci.yml`, `.rustfmt.toml` with custom settings, `clippy.toml`).
- No pre-commit hooks.
- Inconsistent naming: `ExecutionNode` (Go) vs `Executor` (Rust) vs `execnode` (package) for the same concept.

---

## 16. Concurrency Review

**Go side:**
- `sync.Mutex` on `Engine.rules` — single contention point during plan deployment and rule listing.
- `sync.Map` for circuit breakers — correct choice for read-heavy workload with infrequent updates.
- `atomic.Int64` for counters — correct.
- Kafka consumer: single goroutine per partition. For 64 partitions, 64 goroutines. Each blocks on `ExecuteAll` during rule execution. No backpressure to Kafka.
- Scheduler: `sync.WaitGroup` used for lane draining. Correct.

**Rust side:**
- Single-threaded execution (no `Send + Sync` constraints on the executor). Correct for VM design.
- `Arc<Mutex<...>>` on `NextTracker` and `PendingOps` — necessary for FFI thread safety but indicates shared mutable state.
- Lock-free ring buffer for span tracing — good.

**Issues:**
- **No bounded concurrency for service calls.** If a pipeline fans out to 100 services, 100 HTTP requests fire simultaneously. No max concurrency per pipeline.
- **No context propagation.** Go `context.Context` is created during execution but not threaded through to service calls or storage operations.
- **Mutex contention on Engine.** With 64 partitions × high-throughput rules, `Engine.ExecuteAll` contends on `Engine.rules` for plan lookup.

---

## 17. Performance Review

**Current bottlenecks (ordered by severity):**

1. **FFI round-trip per step.** Every `step()` call serializes the entire context via bincode, crosses CGo boundary, deserializes in Rust, executes one instruction, serializes result, crosses back. For a 10-step pipeline, this is 10 FFI round-trips × context serialization. Each serialization is O(context size). For a 10KB context, that's 100KB of serialized data per pipeline.

2. **Body cloning in Rust.** `ctx.body.clone()` on every service call. For a 1MB body through 5 service calls, 5MB of allocations.

3. **Single-threaded Rust execution.** No parallel instruction execution within a pipeline. `Parallel` opcode splits work but each branch runs sequentially.

4. **No plan caching miss penalty.** Cache miss = compile from DSL. DSL compilation is not optimized for speed (no incremental compilation, no cached AST).

5. **JSON serialization for FileStore.** Every rule read/write is full JSON marshal/unmarshal. For large rule sets, this dominates startup time.

**Benchmark results (from code comments):**
- Single pipeline: ~50μs per step (ffi_bound_bench)
- 10-step pipeline: ~500μs
- 100 concurrent pipelines: ~50ms per pipeline (scheduler contention)

**Targets for v2:**
- Sub-μs per step (native Rust, no FFI)
- 10μs per pipeline (inlined execution)
- 100K concurrent pipelines (work-stealing scheduler, bounded memory)

---

## 18. Security Review

**Current state:** Critical gaps at every layer.

| Layer | Issue | Severity |
|-------|-------|----------|
| Transport | No TLS (Kafka, gRPC, HTTP) | Critical |
| Authentication | API key only, no OAuth2/OIDC | High |
| Authorization | No RBAC, no ACL | Critical |
| Secrets | Service credentials in plaintext config | Critical |
| Input validation | No bounds checking on DSL input | Medium |
| FFI safety | Rust unsafe block trusts Go caller | Medium |
| WASM sandbox | Plugins are sandboxed (good) | Low |
| Audit | No audit log | High |
| Supply chain | No dependency scanning, no SBOM | Medium |
| Rate limiting | None | Medium |

**Specific vulnerabilities:**
- **Admin API key in config file.** `admin.auth.api_key` in YAML. Key rotation requires restart. No key hashing.
- **No input size limit.** A 1GB DSL string or 1GB execution context will allocate until OOM.
- **No sandbox for DSL `svc:` calls.** The service registry maps names to URLs. Any registered service can be called by any rule. No cross-tenant isolation.
- **Kafka access.** No authentication to Kafka broker. Anyone who can reach the broker can produce/consume.
- **No CSP / no CSRF protection.** Admin API is a potential XSS vector if frontend is added.

---

## 19. Observability Review

**Current state:**
- OpenTelemetry tracing: span export for rule execution
- Prometheus metrics: counters for executions, errors, scheduler activity
- Logging: `log.Printf` scattered across 10+ files (Go) and `println!` in a few places (Rust)

**Issues:**
- ~~**No structured logging.** `log.Printf` is unstructured text.~~ **✅ RESOLVED** — 64 call sites in 18 Go files migrated from `log.Printf` to `slog`. 2 `eprintln!` in Rust migrated to `log::warn!`.
- **Metrics are counters-only.** No latency histograms. No percentile tracking (p50/p95/p99). No error breakdown by error type. No saturation metrics (goroutine count, channel depth, heap usage).
- **No health endpoints.** No `/healthz` or `/readyz`. Load balancers and orchestrators cannot determine node health.
- **No panic recovery in Go.** No `recover()` in goroutine entry points. A panic in any goroutine kills the process.
- **No distributed tracing context propagation.** OpenTelemetry spans are created but trace context is not propagated across service calls. Each service call is a separate trace.
- **No alerting rules.** No pre-built Prometheus alerting rules, no Grafana dashboards.
- **No runtime profiling.** No pprof endpoints, no continuous profiling integration.

---

## 20. Testing Review

**Rust tests:** 401 unit tests covering:
- Lexer: tokenization edge cases
- Parser: valid syntax, error cases
- Compiler: type checking, schema validation
- Optimizer: dead code elimination, constant folding
- Executor: all 23 opcodes, step-by-step

**Go tests:** Present but limited:
- `engine_test.go` — basic deploy/promote test
- `scheduler_test.go` — enqueue/dequeue test
- `execnode_test.go` — startup/shutdown test
- `cluster_test.go` — leader election test

**E2E tests:** `e2e/` directory with cluster tests:
- Leader failover
- Partition rebalance
- Basic rule execution

**Coverage gaps:**
- **No property-based tests.** Any system with a compiler and bytecode VM should use fuzzing + property-based testing. `proptest` (Rust) and `rapid` or `testing/quick` (Go) are absent.
- **No VM fuzzing.** No fuzz targets for the bytecode interpreter. A malformed `ExecutionPlan` struct can cause undefined behavior in unsafe FFI code.
- **No chaos testing.** No network partition tests, no node crash tests, no disk full tests, no slow consumer tests.
- **No determinism tests.** No test that runs a plan twice and asserts identical results.
- **No benchmark regression.** Benchmarks exist (`ffi_bound_bench`) but not integrated into CI.
- **No concurrency tests.** No Go race detector runs (`-race`).
- **No integration tests for plugins.** Each WASM plugin is tested individually but no test exercises plugin through the full pipeline.

---

## 21. Documentation Review

**Strengths:**
- 13 documents in `docs/`
- Covers: DSL syntax, bytecode format, VM architecture, FFI API, cluster model, Kafka semantics, memory management, development setup
- `software-review.md` contains self-critique
- `file-index.md` maps files to concerns

**Weaknesses:**
- **No getting-started tutorial.** A new user cannot go from zero to "hello world" in under 10 minutes.
- **No API reference.** Admin API has no documentation beyond code.
- **No architecture overview.** `flow-architecture.md` describes components but doesn't show how they interact at runtime (sequence diagrams are missing).
- **No deployment guide.** No instructions for production deployment, configuration, scaling.
- **No troubleshooting guide.** Common errors, debug steps, log interpretation.
- **No performance expectations.** No documented throughput, latency, or resource requirements.
- **No SDK documentation.** Go client is undocumented beyond code comments.

---

## 22. Scalability Review

**Current limits:**

| Dimension | Current | Target |
|-----------|---------|--------|
| Rules per cluster | ~10,000 (FileStore bound) | Unlimited (indexed store) |
| Throughput (rules/sec) | ~1,000 (single leader, FFI) | 100,000+ (multi-node, native) |
| Cluster nodes | 3-5 (single leader) | 100+ (multi-leader sharding) |
| Partitions | 64 (hardcoded) | Auto-scaled |
| Rule complexity | ~100 steps | Unlimited |
| Payload size | ~1MB (no limit enforced) | 10MB+ (streaming) |
| Latency p99 | ~10ms (FFI bound) | <1ms (native) |

**Scaling blockers:**
1. **Single leader bottleneck.** All compilation and rule deployment goes through one node. No horizontal scaling for writes.
2. **No consistent hashing.** Topics are mapped to partitions by modulo. Adding/removing partitions reshuffles all mappings.
3. **No data locality.** Rules and their data may be on different nodes. No colocation of compute and state.
4. **Memory-only execution state.** Cannot scale to thousands of in-flight executions without memory pressure.
5. **FileStore.** JSON files cannot scale past a single node or past ~10K rules.
6. **Fixed goroutine per partition.** Each partition consumes a goroutine regardless of activity. 64 partitions = 64 goroutines always blocked or working.

---

## 23. Evolution Review

**v0.1 → v1.0 roadmap considerations:**

- **API stability.** Any v0.1 API becomes a v1.0 commitment. The current admin API has no version prefix — early adopters will expect backward compatibility.
- **DSL stability.** The DSL grammar is minimal. Adding syntax (conditionals, error handling) will break existing pipelines. Establish a `flowrulz_compile` DSL version header now: `#version 0.1`.
- **Bytecode format stability.** The `ExecutionPlan` binary format has no version field at the wire level. `rule_id` and `version` describe the rule version, not the bytecode format version. When the format evolves (variable-length instructions, compressed constant pool), old VMs will silently misinterpret new bytecode.
- **Plugin API stability.** WASM plugins use a flat function table. Adding new host functions requires all plugins to be recompiled. Use versioned imports (`flowrulz:v1/hash`) with semantic compatibility.
- **Storage migration.** Migrating from `FileStore` to a database should be transparent. Add a `Storage` interface now (currently none) to decouple.
- **Breaking change policy.** No DEPRECATED markers, no migration guides, no compatibility shims. Establish now: at least one minor version of deprecation before removal.

---

## 24. Enterprise Readiness Review

| Requirement | Status | Gap |
|-------------|--------|-----|
| Security (TLS, mTLS, RBAC) | ❌ Critical gaps | No transport security, no authorization |
| High availability (HA) | ❌ | No consensus, no state replication |
| Disaster recovery (DR) | ❌ | No backups, no cross-region replication |
| Audit logging | ❌ | No execution history, no API audit |
| Multi-tenancy | ❌ | No tenant isolation, no rate limiting |
| SLI/SLO/SLA | ❌ | No defined service levels |
| Compliance (SOC2, HIPAA) | ❌ | No audit trail, no encryption at rest |
| Support channels | ❌ | No documented support model |
| Billing/usage metering | ❌ | No per-tenant usage tracking |
| On-premise deployment | Partial | K8s manifests exist but no helm chart |
| Backup/restore | ❌ | No mechanism |
| SSO/SAML/OIDC | ❌ | API key only |
| Secrets management | ❌ | Plaintext in config |
| Configuration management | Partial | YAML config but no validation schema |
| Graceful degradation | ❌ | No degraded mode |
| Capacity planning | ❌ | No documented resource requirements |

**Enterprise adoption blockers (in order):**
1. No TLS → fails every security questionnaire
2. No audit log → fails SOC2, HIPAA
3. No HA → cannot meet uptime requirements
4. No multi-tenancy → cannot sell as a platform
5. No SSO → blocked by enterprise IT policy

---

## 25. Executive Summary & Action Plan

### Executive Summary

FlowRulZ has a genuinely novel core idea — compiling rule pipelines to bytecode and executing them on a Rust VM — and the implementation quality is decent for a project of this scope. The Rust/Go language split, the bytecode compilation pipeline, and the DSL design are above average for a pre-1.0 project.

However, the architecture had fundamental issues. Several have been addressed:

1. **No consensus** — single-leader with no split-brain protection *(remaining)*
2. **No deterministic execution** — cannot replay workflows for debugging or recovery *(remaining)*
3. **FFI-bound performance ceiling** — serialization round-trip per step limits throughput *(remaining)*
4. ~~**God object architecture** — `ExecutionNode` wired everything~~ ✅ RESOLVED — replaced by `ProdNode` + `NodeBuilder` DI
5. ~~**No audit trail** — execution history was entirely ephemeral~~ ✅ RESOLVED — completed states saved as `StatusCompleted`

### Architecture Scorecard

| Area | Score | Trend |
|------|-------|-------|
| Product Vision | 8/10 | Stable |
| Code Organization | 8/10 | Improving (execnode deleted, DI added) |
| Rust Compiler | 8/10 | Stable |
| Rust VM | 8/10 | Stable |
| Go Scheduler | 7/10 | Improved (work stealing added) |
| Distributed Systems | 3/10 | Critical gap |
| Storage | 4/10 | Starting (exec history persisted) |
| Security | 2/10 | Critical gap |
| Observability | 5/10 | Slight (structured logging added) |
| Testing | 7/10 | Adequate (401 Rust tests now) |
| Documentation | 8/10 | Good (vault added) |
| Production Readiness | 5/10 | Progressing |

### Top 10 Critical Risks

1. **No consensus → data loss on leader failover**
2. **No deterministic replay → cannot debug failed executions**
3. **FFI round-trip per step → 10-100x overhead vs native**
4. ~~**God object** → **✅ RESOLVED** — execnode deleted, ProdNode + NodeBuilder DI in place~~
5. ~~**No execution history** → **✅ RESOLVED** — completed states persisted in FileStore~~
6. **No TLS/mTLS → all traffic in cleartext**
7. **Memory state only → all exec state lost on crash**
8. ~~**No work stealing** → **✅ RESOLVED** — dequeueOrSteal() added~~
9. ~~**`ExecuteAll` bypasses scheduler** → **✅ RESOLVED** — routes through EnqueueAndWait()~~
10. **Hardcoded 64 partitions → no elastic scaling**

### Top 10 Immediate Fixes (<1 week each)

| # | Fix | Effort | Status |
|---|-----|--------|--------|
| 1 | Add Raft consensus (embed etcd or use HashiCorp Raft) | 2 weeks | Pending |
| 2 | Strip no-op opcodes from runtime bytecode | 2 days | Pending |
| 3 | Add structured logging (zerolog/slog) | 2 days | ✅ Done (64 `log.Printf` → `slog`) |
| 4 | Heartbeat + lease-based leader detection | 3 days | ✅ Done (`LeaderLease`, eviction) |
| 5 | Execution history append-log (file or SQLite) | 1 week | ✅ Done (`FileStore` with `StatusCompleted`) |
| 6 | Execution budget (max_instructions, max_time, max_memory) | 3 days | Pending |
| 7 | Add caret-position error messages to DSL compiler | 2 days | Pending |
| 8 | Persistent dedup (replace in-memory tracker) | 3 days | Pending |
| 9 | Add OpenAPI spec for admin API | 2 days | Pending |
| 10 | TLS everywhere (Kafka, gRPC, HTTP) | 1 week | Pending |

### Top 10 Medium-term Improvements (1-4 weeks each)

| # | Improvement | Effort | Status |
|---|-------------|--------|--------|
| 11 | Work-stealing scheduler | 3 weeks | ✅ Done (`dequeueOrSteal()` added) |
| 12 | Rust VM as persistent daemon (unix socket) | 4 weeks | Pending |
| 13 | Deterministic execution recorder | 3 weeks | Pending |
| 14 | Split `ExecutionNode` into control + data plane | 3 weeks | ✅ Done (execnode deleted, ProdNode + NodeBuilder) |
| 15 | HIR in compiler pipeline | 2 weeks | Pending |
| 16 | Consistent hashing for partitions | 3 weeks | Pending |
| 17 | DSL comments + named pipelines | 1 week | Pending |
| 18 | RBAC for admin API | 2 weeks | Pending |
| 19 | Property-based testing (quickcheck) | 2 weeks | Pending |
| 20 | Benchmark suite for FFI vs native | 1 week | Pending |

### FlowRulZ v2 Architecture (Recommended)

```
┌───────────────────────┐
│    Control Plane       │
│  ┌─────────────────┐  │
│  │  Raft Cluster    │  │  ← Replace single-leader
│  │  (etcd or nats)  │  │
│  └────────┬────────┘  │
│           │            │
│  ┌────────▼────────┐  │
│  │  Rule Store     │  │  ← SQL or KV, not JSON files
│  │  + Audit Log    │  │
│  └────────┬────────┘  │
│           │            │
│  ┌────────▼────────┐  │
│  │  Scheduler      │  │  ← Work-stealing, not fixed lanes
│  │  + Rate Limiter │  │
│  └─────────────────┘  │
└───────────────────────┘

┌───────────────────────┐
│    Data Plane          │
│  ┌─────────────────┐  │
│  │  Rust VM Daemon  │  │  ← Long-lived process, not CGo lib
│  │  (unix socket)   │  │
│  │  + Deterministic │  │
│  │  + Snapshot/Repl │  │
│  └────────┬────────┘  │
│           │            │
│  ┌────────▼────────┐  │
│  │  Execution Store│  │  ← Append-only log of exec results
│  └─────────────────┘  │
└───────────────────────┘

┌───────────────────────┐
│    Transport           │
│  ┌─────────────────┐  │
│  │  Event Bus       │  │  ← Abstracted (Kafka/NATS/Pulsar)
│  │  + gRPC Stream   │  │
│  │  + mTLS          │  │
│  └─────────────────┘  │
└───────────────────────┘
```

### Overall Grade: **B-**

Solid architectural foundation (Rust/Go split, bytecode compilation, arena memory). Above-average documentation and testing for a pre-1.0 project. But critical gaps in distributed systems (no consensus, no deterministic replay), security (no TLS), and production readiness (no audit trail, memory-only state) prevent an enterprise-grade rating.

The concerns are fixable — none are fundamental design mistakes. The Rust VM is genuinely good. The bytecode format needs minimal changes. The Go data plane needs a significant refactoring of `ExecutionNode` and the addition of Raft consensus, but the individual pieces (scheduler, transport, engine) are well-structured enough to survive the refactoring.

**Most important recommendation:** Ship v0.1 as a single-node rules engine with excellent developer experience. Let 100 startups use it before you build the distributed version. Distributed systems are not something you get right on the first attempt.

---

*End of audit. See [software-review.md](./software-review.md) for previously identified issues and their resolution status.*
