# FlowRulZ

Distributed execution runtime. Pub/Sub, RPC, workflows, and rules are all execution plans running on the same VM.

> **AI rules** — On each conversation start, read `docs/` dir. After any code change, update relevant `.md` files in `docs/` to stay in sync. Never let docs go stale.

## Build & Test

```bash
make all       # rust release + go binary
make test      # all rust (119) + go tests (63+)
make bench     # criterion benchmarks
make vet       # go vet
make clean     # cargo clean + remove binary
```

## Architecture

- **Control Plane** (Go): Rule registry, DSL compiler, scheduling, leader election. Simple single-leader — no Raft. No WAL/storage beyond rules JSON file.
- **Data Plane** (Go + Rust): Partition workers, ExecutionRuntime, service callers, span collection. Multiple nodes scale horizontally.
- **Execution Node** (`go/internal/execnode/`): process wrapping Engine + Bridge + Runtime + PlanDistributor + transport consumers + admin HTTP. Leader/follower role via `SetLeader()`/`IsLeader()`. Leader distributes plans via `Engine.AfterDeploy`/`AfterPromote` hooks that call `PlanDist.PublishPlan()` + ACK quorum + `ActivatePlan()`.
- **Kafka** is the durable event log; FlowRulZ is a consumer group with programmable execution
- C FFI prefix: `flowrulz_` — all exported functions use `#[no_mangle] pub extern "C"`
- Bridge: `sync.Map callerMap` + `atomic.Uint64 nextExecID` — no mutex in hot path
- Span tracing: `thread_local!` ring buffer, lock-free atomic head/tail, drained via `flowrulz_get_spans`
- **Service Registry** (`go/internal/registry/`): Maps service names → healthy endpoints, round-robin/random/least-loaded LB, passive+active health checks
- **Reply Router** (`go/internal/replyrouter/`): Per-node pending request tracker by correlation_id, timeout-based cleanup, duplicate detection
- **Scheduler** (`go/internal/scheduler/`): Lane-based priority queues (fast/normal/heavy), semaphore-based concurrency limits, reject-on-full backpressure
- **Plan Distribution** (`go/internal/plandist/`): Leader publishes plans to `_flowrulz_plans`, followers ACK on `_flowrulz_acks`, quorum-based activation with `WaitForAcks`. Wired in execnode via `handlePlanMessage`/`handleAckMessage` — plan/ack consumers listen on `_flowrulz_plans`/`_flowrulz_acks`, call `Engine.AddVersion()` for "plan" type and `Engine.Promote()` for "activate". Term-based rejection prevents stale plans.
- **Metrics** (`go/internal/observability/`): Counter, Gauge, Histogram with thread-safe per-name dedup and global shortcuts
- **Circuit Breaker** (`go/internal/reliability/circuitbreaker.go`): Three-state (Closed/Open/HalfOpen) per-svcID breaker wired in `execnode` svcCaller (threshold=5, recovery=30s)
- **DLQ** (`go/internal/reliability/dlq.go`): Bounded dead-letter queue, Kafka-backed via `WithDLQProducer()`, per-entry replay, bulk ReplayAll, JSON export
- **Rate Limiter** (`go/internal/reliability/ratelimit.go`): Token bucket per name, configurable rate/burst, isolation across services
- **Dedup** (`go/internal/reliability/dedup.go`): Bounded in-memory dedup tracker with TTL eviction, wired in execnode handler via MessageID

## Key Layers

| Layer | Dir | Description |
|---|---|---|
| Event | `rust/src/bytecode/event.rs` | `Event` + `Mode` — universal message type |
| Execution | `rust/src/bytecode/execution.rs` | `ExecutionContext` — body + variables + outputs |
| DSL | `rust/src/dsl/` | Lexer → Parser → Optimizer → Compiler |
| Bytecode | `rust/src/bytecode/` | OpCode (0-22), Instruction (8 bytes), ExecutionPlan |
| VM | `rust/src/executor/` | `VM::run()` dispatches opcodes, operates on `ExecutionContext` |
| Runtime | `rust/src/executor/runtime.rs` | `ExecutionRuntime` wraps VM, handles Chunk/Buffer at runtime level |
| FFI | `rust/src/ffi.rs` | `flowrulz_compile`, `flowrulz_execute`, `flowrulz_get_spans`, etc. |
| Bridge | `go/bridge/` | CGo bindings + C caller bridge |
| Engine | `go/internal/engine/` | `VersionedPlan`, lane routing, persistence, `ExecuteAll`, `AddVersion`, `LaneForScore` |
| ExecNode | `go/internal/execnode/` | Data plane process: engine + transport + admin + lifecycle |
| Admin | `go/internal/admin/` | HTTP API with API key auth, rule CRUD, validate, lanes |
| SDK | `go/flow/` | Client SDK — `Publish`, `Request`, `Execute`, `Stream` |
| Simulator | `simulator/` | Simulator for testing rules, services, and cluster behavior |
| Registry | `go/internal/registry/` | `ServiceRegistry` — service name → healthy endpoints, LB, health checks |
| ReplyRouter | `go/internal/replyrouter/` | `ReplyRouter` — correlation ID → pending request channel, timeout/cleanup |
| Scheduler | `go/internal/scheduler/` | Priority queue per lane (fast/normal/heavy), semaphore-based concurrency limits, reject-on-full backpressure |
| PlanDist | `go/internal/plandist/` | `PlanDistributor` — plan/ack topics, versioned ACK quorum, activation |
| Metrics | `go/internal/observability/` | `MetricsCollector` — counters, gauges, histograms, global shortcuts |
| DLQ | `go/internal/reliability/dlq.go` | Dead-letter queue with replay, bounded size, JSON export |
| RateLimiter | `go/internal/reliability/ratelimit.go` | Token bucket per name, configurable rate/burst |

## Cluster Model

- **Single-leader cluster** — no Raft, no Paxos. Leader = lowest-ID alive node.
- **Membership**: Seed-based discovery; heartbeat via `_flowrulz_members` internal Kafka topic (compacted).
- **Leader election**: Sort alive nodes by ID ascending — lowest is leader. On leader failure, next-lowest promotes itself.
- **Plan distribution**: Leader publishes ExecutionPlan to `_flowrulz_plans` → followers ACK on `_flowrulz_acks` → leader activates.
- **Partition ownership**: Kafka consumer group protocol per lane. Leader tracks partition → node mapping.
- **Service Registry**: Nodes register services in heartbeat. Leader aggregates → publishes combined registry.
- **Reply Router**: Per-node component tracking pending request/reply by correlation_id. Replies hash to origin node's partition.
- **Node lifecycle**: Join (announce → catch-up → consume), Drain (rebalance → drain execs → leave), Crash (rejoin with same ID → catch-up).
- **Scheduler**: Lane-based priority queues; Fast (50 concurrent, 5k queue), Normal (20, 2k), Heavy (5, 500). RejectOnFull for heavy lane.
- **Plan Distribution**: `PlanDistributor` publishes `PlanMessage{type, rule_id, version, plan, dsl}` to `_flowrulz_plans`. Followers send `AckMessage{node_id, rule_id, version, status}` to `_flowrulz_acks`. `WaitForAcks(ruleID, version, quorum, timeout)` implements ACK-quorum activation. Wired in execnode via `handlePlanMessage`/`handleAckMessage` — plan/ack consumers listen on `_flowrulz_plans`/`_flowrulz_acks`, call `Engine.AddVersion()` for "plan" type and `Engine.Promote()` for "activate". Term-based rejection prevents stale plans.

## Conventions

- `caller_cb_t` signature: `int(uint64_t ctx_id, uint16_t svc_id, const u8* body, size_t body_len, u8* resp, size_t* resp_len)`
- `Instruction` is 8 bytes: `{op: u8, flags: u8, a: u16, b: u16, c: u16}`
- Schema DSL: `schema:{field:type,!required_field:type}` — emits `TypeGuard` opcode (22)
- Compile-time type inference: when `schema:{...}` is present, the compiler pre-pass validates Gate operators (`type_check_gate()`) and Map expressions (`type_check_map()`) against declared field types, emitting `TypeMismatch` errors for incompatible operations
- DAGTable fields: `failure_policy` (AbortAll/ContinueOthers/SkipDependents), `node_timeouts`, `merge_strategy` (LastWins/ArrayConcat/DeepMerge/ExplicitMap), `distributed`
- DAGNode has `parent_ids: Vec<u16>` — populated during compile from deps, used at runtime to merge parent results into downstream node input
- DAG exec_dag.rs implements all three failure policies: AbortAll (immediate error), ContinueOthers (record failure, continue), SkipDependents (skip nodes with failed parents)
- DAG merge_dag_results implements MergeStrategy: LastWins (keyed JSON object), ArrayConcat (JSON array), DeepMerge (recursive), ExplicitMap (same as LastWins, no explicit map config yet)
- Schema DSL: `enum[val1|val2|...]` syntax for `ResolvedType::Enum(Vec<String>)`
- Persistence: atomic write via write-to-temp-then-rename pattern (`saveRules()` uses `os.WriteFile` to `.tmp` then `os.Rename`)
- ExecutionRuntime owns the plan and handles Buffer (accumulate) and Chunk (split+execute) opcodes at the runtime level, not inside the VM
- `ExecutionContext` holds `event` + `body` + `variables` + `outputs` — services enrich context instead of replacing a single body
- `Mode` enum: `Publish`, `Request`, `Reply`, `Stream`, `Workflow`, `Internal` — determines delivery semantics per event
- Client SDK at `go/flow/` — `Publish()`, `Request()`, `Execute()`, `Stream()` — all operations go through the same runtime

## Expression Builtins

`to_string`, `parse_int`, `parse_float`, `coalesce`, `default`, `contains`, `keys`, `merge`, `epoch`, `hash`, `uuid`, `now`, `lower`, `upper`, `trim`, `length`, `concat`, `base64`, `json`, `substring`, `replace`

`call_builtin` takes `&[serde_json::Value]` (not `&[&str]`).

## Persistence

Engine accepts `persistPath` on creation. Saves/loads rules as JSON. Set via `FLOWRULZ_PERSIST_PATH` env var.

## Admin API

All endpoints (except `/health`) require `Authorization: Bearer <FLOWRULZ_API_KEY>` header when `FLOWRULZ_API_KEY` is set.

- `POST /rules` — deploy rule
- `DELETE /rules/{id}` — remove rule (drains active execs)
- `GET /rules` — list rules with versions
- `GET /rules/{id}` — get rule detail with lane info
- `GET /rules/{id}/versions` — list versions
- `POST /rules/{id}/validate` — compile-only, returns validity + complexity
- `POST /rules/{id}/promote?version=N` — promote version
- `POST /rules/{id}/rollback` — same as promote
- `GET /lanes` — lane configs
- `GET /health` — health check

## Opcodes (0-22)

0=Next, 1=Parallel, 2=Collect, 3=Fallback, 4=Gate, 5=Split, 6=Map, 7=Emit, 8=Drop, 9=Buffer, 10=Key, 11=Retry, 12=Pipe, 13=Timeout, 14=Async, 15=Chunk, 16=Dag, 17=Jmp, 18=Label, 19=SvcArg, 20=RetryData, 21=JumpOffset, 22=TypeGuard

## Known Issues — Status

| Issue | Status | File(s) |
|-------|--------|---------|
| P0 DLQ persistence | In-memory cache + Kafka producer (`_flowrulz_dlq`) when `WithDLQProducer()` configured | `go/internal/reliability/dlq.go` |
| P0 Circuit breaker | Wired per-svcID in svcCaller (threshold=5, recovery=30s). Three-state FSM tested (6 tests). Stub caller never trips — real HTTP/gRPC caller needed | `reliability/circuitbreaker.go`, `execnode/execnode.go` |
| P1 Leader epoch | `PlanMessage.Term` field (uint64), `PlanDistributor.SetTerm()`/`CurrentTerm()`. Term-based rejection in execnode `handlePlanMessage` (rejects plans with older term) | `go/internal/plandist/plandist.go`, `go/internal/execnode/execnode.go` |
| P1 Workflow state | File-based checkpointing (`NewOrchestratorWithCheckpointDir`). Per-flow JSON files, atomic write, restore on start | `go/internal/flow/flow.go` |
| P1 Message dedup | Bounded in-memory `DedupTracker` (10k, 5min TTL). Wired by MessageID in handler. Cleanup goroutine every 30s | `reliability/dedup.go`, `execnode/execnode.go` |
| P2 Leader election | Auto-elected via heartbeat on `_flowrulz_members` — lowest-ID alive node wins. `startHeartbeat()` goroutine sends `HeartbeatMessage` every 3s. `runLeaderElection()` promotes/step-down on every heartbeat receipt. Transport is stubbed (log-only) — real Kafka heartbeat not verified | `execnode/execnode.go`, `membership/membership.go` |
| P2 AckQuorum | `WaitForAcks` with quorum=0 or -1 always returns immediately. Real quorum requires membership counting via `_flowrulz_members` topic | `plandist/plandist.go` |
| P2 Plan distribution | `AfterDeploy`/`AfterPromote` callbacks wired in execnode. `distributePlan()` calls `PublishPlan()` + `WaitForAcks()` + `ActivatePlan()`. Works with stubs — needs real transport + membership for ACK quorum | `execnode/execnode.go`, `engine/engine.go` |

## Progress
### Done
- Added `ResultCh` + `Output` fields to `ExecutionContext` for client result delivery.
- Added `sendResult()` helper to `Scheduler` — sends result to channel on completion/failure.
- Added `Client` type in `simulator/client.go` with `Send()`, `RegisterService()`, `AddRule()`, `Plans()`, `Services()`.
- Simulator extracted from `go/simulator/` to `simulator/` at project root.
- Bridge moved from `go/internal/bridge/` to `go/bridge/` (needed to allow import from outside `go/`).
- `sendResult` fires on all exit paths (stop, error, done) in both `executeContext` and `executeBridge`.
- `PlanCache.List()` method added.
- `Scheduler.Stop()` made idempotent (stopped bool guard).
- All 4 client tests pass: Send with bridge rule, rule not found, AddRule across nodes, RegisterService.
- 17 Go packages, `go vet ./go/...` clean.

### Next Steps
1. Let user write sender/receiver code using `Client.Send()` + `Client.RegisterService()` + `Client.AddRule()`.
2. Wire rule deployment through admin API + plan distribution for end-to-end test.
3. Full test suite after each change.
