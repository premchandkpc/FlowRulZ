# FlowRulZ

Distributed Rule Broker — a unified execution runtime combining service discovery, rule engine, scheduler, router, and message broker. Services self-register with their capabilities; rules route events between them.

```
                 Producer
                     │
                     ▼
        ┌─────────────────────┐
        │      FlowRulZ       │
        │  Registry · Rules   │
        │  Runtime · Router   │
        │  Scheduler · Queue  │
        └──────────┬──────────┘
                   │
       ┌─────┬─────┼──────┬──────┐
       │     │     │      │      │
    Payment Email Fraud Inventory ...
```

Services register themselves (`POST /register` with methods, version, protocol, zone). Rules reference them by name (`n:payment`). FlowRulZ resolves → load-balances → calls → routes → response. No service knows about any other service.

> **AI rules** — On each conversation start, read `docs/` dir. After any code change, update relevant `.md` files in `docs/` to stay in sync. Never let docs go stale.

## Build & Test

```bash
make all       # rust release + go binary
make test      # all rust (140) + go tests (20+ suites)
make bench     # criterion benchmarks
make vet       # go vet
make clean     # cargo clean + remove binary
```

## Architecture

- **Control Plane** (Go): Rule registry, DSL compiler, scheduling, leader election. Simple single-leader — no Raft. No WAL/storage beyond rules JSON file.
- **Data Plane** (Go + Rust): Partition workers, ExecutionRuntime, service callers, span collection. Multiple nodes scale horizontally.
- **Execution Node** (`go/internal/execnode/`): process wrapping Engine + Bridge + Runtime + Registry + PlanDistributor + cluster transport + admin HTTP. Leader/follower role via `SetLeader()`/`IsLeader()`. Leader distributes plans via `Engine.AfterDeploy`/`AfterPromote` hooks that call `PlanDist.PublishPlan()` + ACK quorum + `ActivatePlan()`.
- **Cluster Bus** (`go/internal/cluster/`): gRPC-based peer-to-peer overlay — no Kafka, no ZK, no external deps. `ClusterNode` manages Publish/Subscribe, peer membership, topic handlers. `ClusterProducer`/`ClusterConsumer` adapters implement `transport.MessageProducer`/`transport.MessageConsumer`. Default transport; Kafka (`go/internal/transport/kafka.go`) remains as legacy fallback when `FLOWRULZ_KAFKA_BROKERS` is explicitly set.
- **Service Registry** (`go/internal/registry/`): Rich registry — services self-register with `ServiceInstance` (ID, name, version, methods, capabilities, endpoint, zone, weight, tags, metadata, heartbeat). Two registration paths: legacy `Register(name, endpoint)` and rich `RegisterInstance(inst)`. Heartbeat expiry (default 30s) marks unhealthy. LB strategies: random, round-robin, least-loaded, local-prefer. `LookupInstance(name, method)` selects method-aware healthy instance.
- **Registration API**: `POST /register` (service announces name, version, methods, address, port, protocol, zone, weight) and `POST /heartbeat` (keeps instance alive). `GET /services` lists all registered services with full instance details.
- **Method syntax in rules**: `n:payment.authorize` — method name embedded in service string. `bridge.ParseServiceMethod("payment.authorize")` → `("payment", "authorize")`. Rust DSL lexer captures everything after `n:` (dot included). No Rust changes needed.
- **Plan service resolution**: `bridge.InternLookup(svcID)` is **broken** for plan-local IDs — global intern table has pre-filled strings at IDs 0-6 (`"content-type"`, etc.). Use `bridge.PlanServices(plan)` → `map[uint16]string` in `executePlan`; pass `(svcName, method, body)` to `callService`.
- **EventBus** (`go/pkg/transport/eventbus.go`) is the canonical pub/sub abstraction; `Message`, `Handler`, `Subscription` types are shared across `go/` and `simulator/`. Cluster Bus implements the same patterns over gRPC.
- C FFI prefix: `flowrulz_` — all exported functions use `#[no_mangle] pub unsafe extern "C"`
- `Compiler::new()` is no-arg (was `new(&[])` — all callers passed empty slice)
- `Error` enum removed — only `FfiError` remains (was never constructed in any path)
- `ExecutionPlan::map_exprs` field removed — was always empty, vestigial from earlier design
- Slab pool (`memory::slab`) removed — `flowrulz_msg_alloc`/`release` use `std::alloc` directly
- Bridge: `sync.Map callerMap` + `atomic.Uint64 nextExecID` — no mutex in hot path
- Span tracing: `thread_local!` ring buffer, lock-free atomic head/tail, drained via `flowrulz_get_spans`
- **Reply Router** (`go/internal/replyrouter/`): Per-node pending request tracker by correlation_id, timeout-based cleanup, duplicate detection
- **Scheduler** (`go/internal/scheduler/`): Lane-based priority queues (fast/normal/heavy), semaphore-based concurrency limits, reject-on-full backpressure
- **Plan Distribution** (`go/internal/plandist/`): Leader publishes plans to `_flowrulz_plans`, followers ACK on `_flowrulz_acks`, quorum-based activation with `WaitForAcks`. Transport-agnostic — uses `MessageProducer`/`MessageConsumer` interfaces backed by Cluster Bus or Kafka. Wired in execnode via `handlePlanMessage`/`handleAckMessage`. Term-based rejection prevents stale plans.
- **Metrics** (`go/internal/observability/`): Counter, Gauge, Histogram with thread-safe per-name dedup and global shortcuts
- **Circuit Breaker** (`go/internal/reliability/circuitbreaker.go`): Three-state (Closed/Open/HalfOpen) per-svcID breaker wired in `execnode` svcCaller (threshold=5, recovery=30s)
- **DLQ** (`go/internal/reliability/dlq.go`): Bounded dead-letter queue, in-memory cache with optional Kafka producer, per-entry replay, bulk ReplayAll, JSON export
- **Rate Limiter** (`go/internal/reliability/ratelimit.go`): Token bucket per name, configurable rate/burst, isolation across services
- **Dedup** (`go/internal/reliability/dedup.go`): Bounded in-memory dedup tracker with TTL eviction, wired in execnode handler via MessageID

## Key Layers

| Layer | Dir | Description |
|---|---|---|
| EventBus (interface) | `go/pkg/transport/eventbus.go` | Canonical `EventBus` interface — `Publish`, `Subscribe`, `Request`, `Reply`, `Broadcast` |
| EventBus (impl) | `simulator/eventbus/` | In-memory pub/sub with Go channels: Publish, Subscribe, Request/Reply, Broadcast, Delay, Drop, Duplicate |
| Cluster Bus | `go/internal/cluster/` | `ClusterNode` — gRPC p2p overlay, Publish/Subscribe, peer management, topic handlers |
| Transport (legacy) | `go/internal/transport/` | Kafka/Sarama producer/consumer (optional, when `FLOWRULZ_KAFKA_BROKERS` set) |
| Transport (gRPC) | `go/internal/transport/grpc/` | `GRPCBus` — low-level gRPC transport used by Cluster Bus |
| Event | `rust/src/bytecode/event.rs` | `Event` + `Mode` — universal message type |
| Execution | `rust/src/bytecode/execution.rs` | `ExecutionContext` — body + variables + outputs |
| DSL | `rust/src/dsl/` | Lexer → Parser → Optimizer → Compiler |
| Bytecode | `rust/src/bytecode/` | OpCode (0-22), Instruction (8 bytes), ExecutionPlan |
| VM | `rust/src/executor/` | `VM::run()` dispatches opcodes, operates on `ExecutionContext` |
| Runtime | `rust/src/executor/runtime.rs` | `ExecutionRuntime` wraps VM, handles Chunk/Buffer at runtime level |
| FFI | `rust/src/ffi.rs` | `flowrulz_compile`, `flowrulz_execute`, `flowrulz_get_spans`, etc. |
| Bridge | `go/bridge/` | CGo bindings + C caller bridge |
| Engine | `go/internal/engine/` | `VersionedPlan`, lane routing, persistence, `ExecuteAll`, `AddVersion`, `LaneForScore` |
| ExecNode | `go/internal/execnode/` | Data plane process: engine + cluster + transport + admin + lifecycle |
| Admin | `go/internal/admin/` | HTTP API with API key auth, rule CRUD, validate, lanes |
| SDK | `go/flow/` | Client SDK — `Publish`, `Request`, `Execute`, `Stream` |
| Simulator | `simulator/` | Simulator for testing rules, services, and cluster behavior |
| Registry | `go/internal/registry/` | `ServiceRegistry` — service name → healthy endpoints, LB, health checks |
| ReplyRouter | `go/internal/replyrouter/` | `ReplyRouter` — correlation ID → pending request channel, timeout/cleanup |
| Scheduler | `go/internal/scheduler/` | Priority queue per lane (fast/normal/heavy), semaphore-based concurrency limits |
| PlanDist | `go/internal/plandist/` | `PlanDistributor` — plan/ack topics, versioned ACK quorum, activation |
| Metrics | `go/internal/observability/` | `MetricsCollector` — counters, gauges, histograms, global shortcuts |
| DLQ | `go/internal/reliability/dlq.go` | Dead-letter queue with replay, bounded size, JSON export |
| RateLimiter | `go/internal/reliability/ratelimit.go` | Token bucket per name, configurable rate/burst |

## Cluster Model

- **Single-leader cluster** — no Raft, no Paxos. Leader = lowest-ID alive node.
- **Transport**: gRPC-based Cluster Bus (peer-to-peer overlay). No Kafka, no ZK, no external dependencies.
- **Membership**: Seed-based discovery via `FLOWRULZ_SEEDS` env var (comma-separated `node:port`). Each node connects to peers, broadcasts heartbeat via cluster bus topic `_flowrulz_members`.
- **Leader election**: Sort alive nodes by ID ascending — lowest is leader. On leader failure, next-lowest promotes itself. Heartbeat via cluster bus every 3s.
- **Plan distribution**: Leader publishes ExecutionPlan to `_flowrulz_plans` cluster bus topic → followers ACK on `_flowrulz_acks` → leader activates.
- **Partition ownership**: Round-robin partition assignment per lane. Leader tracks partition → node mapping.
- **Service Registry**: Nodes register services in heartbeat. Leader aggregates → publishes combined registry.
- **Reply Router**: Per-node component tracking pending request/reply by correlation_id. Replies route via cluster bus topic to origin node.
- **Node lifecycle**: Join (announce → catch-up → consume), Drain (rebalance → drain execs → leave), Crash (rejoin with same ID → catch-up).
- **Scheduler**: Lane-based priority queues; Fast (50 concurrent, 5k queue), Normal (20, 2k), Heavy (5, 500). RejectOnFull for heavy lane.
- **Plan Distribution**: `PlanDistributor` publishes `PlanMessage{type, rule_id, version, plan, dsl}` to `_flowrulz_plans`. Followers send `AckMessage{node_id, rule_id, version, status}` to `_flowrulz_acks`. `WaitForAcks(ruleID, version, quorum, timeout)` implements ACK-quorum activation. Term-based rejection prevents stale plans.

## Conventions

- `caller_cb_t` signature: `int(uint64_t ctx_id, uint16_t svc_id, const u8* body, size_t body_len, u8* resp, size_t* resp_len)`
- `Instruction` is 8 bytes: `{op: u8, flags: u8, a: u16, b: u16, c: u16}`
- Schema DSL: `schema:{field:type,!required_field:type}` — emits `TypeGuard` opcode (22)
- Opcodes 23 (`SvcCall`) and 24 (`Delay`) exist but are never emitted by the compiler
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

## Expression Builtins (31 total)

`to_string`, `parse_int`, `parse_float`, `parse_bool`, `coalesce`, `default`, `contains`, `keys`, `merge`, `epoch`, `hash`, `uuid`, `now`, `lower`, `upper`, `trim`, `length`, `concat`, `base64`, `base64_decode`, `json`, `substring`, `replace`, `split`, `abs`, `round`, `ceil`, `floor`, `min`, `max`, `typeof`

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

## Opcodes (0-24)

0=Next, 1=Parallel, 2=Collect, 3=Fallback, 4=Gate, 5=Split, 6=Map, 7=Emit, 8=Drop, 9=Buffer, 10=Key, 11=Retry, 12=Pipe, 13=Timeout, 14=Async, 15=Chunk, 16=Dag, 17=Jmp, 18=Label, 19=SvcArg, 20=RetryData, 21=JumpOffset, 22=TypeGuard, 23=SvcCall, 24=Delay

## Known Issues — Status

| Issue | Status | File(s) |
|-------|--------|---------|
| P0 DLQ persistence | In-memory cache + optional Kafka producer (`_flowrulz_dlq`) when `WithDLQProducer()` configured | `go/internal/reliability/dlq.go` |
| P0 Circuit breaker | Wired per-svcID in svcCaller (threshold=5, recovery=30s). Three-state FSM tested (6 tests). Stub caller never trips — real HTTP/gRPC caller needed | `reliability/circuitbreaker.go`, `execnode/execnode.go` |
| P1 Leader epoch | `PlanMessage.Term` field (uint64), `PlanDistributor.SetTerm()`/`CurrentTerm()`. Term-based rejection in execnode `handlePlanMessage` (rejects plans with older term) | `go/internal/plandist/plandist.go`, `go/internal/execnode/execnode.go` |
| P1 Workflow state | File-based checkpointing (`NewOrchestratorWithCheckpointDir`). Per-flow JSON files, atomic write, restore on start | `go/internal/flow/flow.go` |
| P1 Message dedup | Bounded in-memory `DedupTracker` (10k, 5min TTL). Wired by MessageID in handler. Cleanup goroutine every 30s | `reliability/dedup.go`, `execnode/execnode.go` |
| P2 Leader election | Auto-elected via heartbeat on `_flowrulz_members` topic — lowest-ID alive node wins. Uses Cluster Bus transport (resolved). Quorum counting via membership tracking still pending | `execnode/execnode.go`, `membership/membership.go` |
| P2 AckQuorum | `WaitForAcks` with quorum=0 or -1 always returns immediately. Real quorum requires membership counting | `plandist/plandist.go` |
| P2 Plan distribution | `AfterDeploy`/`AfterPromote` callbacks wired in execnode. Uses Cluster Bus transport (resolved). Distributes + Waits + Activates | `execnode/execnode.go`, `engine/engine.go` |

## Progress
### Done
- In-memory EventBus: Go channel pub/sub with Publish, Subscribe, Request/Reply, Broadcast, Delay, Drop, Duplicate. 12 tests covering delivery, timeout, unsubscribe, delayed delivery, duplicate, close semantics, topic isolation.
- EventBus wired into simulator: `Client.Send()` uses `Bus.Request("execution", msg)` → scheduler subscribers receive → execute → reply. Replaces direct scheduler dispatch for interactive/API usage.
- Animated execution graph dashboard: SVG service DAG with active path highlighting, pulsing execution dots, real-time refresh
- Dashboard API: `/api/executions` (list all), `/api/executions/{id}` (per-execution), `/api/metrics` (latency/throughput/counts), `/api/nodes` (per-node queues), `/api/events` (timeline), `/api/stats` (event type counts)
- Service latency cards with p50/p95/error rate + bar chart
- Execution flow visualization per execution: service list, status badges (completed/failed/running), event count
- Per-node queue bars (ready/waiting) in sidebar
- Event type distribution in sidebar
- `fmt()` JS helper properly converts Go nanosecond durations to human-readable ms/µs/s
- `handleExecutions` endpoint groups timeline events by execution, extracts service list and status
- Both `sim` (in-memory EventBus based simulator) and `flowrulz` (Cluster Bus + Rust VM production node) binaries build and work independently
- 154 Rust tests + 20+ Go test suites pass; `go vet` clean
- All docs rewritten and verified against actual codebase — every file path, type, function, enum variant, and export is documented correctly across all 12 .md files
- Cluster Bus benchmark: ~12K msg/s publish, ~44µs latency, ~92µs request/reply (Apple M5)

### Phase 1–3: Rust cleanup (complete)
- Deleted 3 dead files: `bytecode/mapexpr.rs`, `executor/context.rs`, `memory/slab.rs`
- Deleted 6 dead functions: `execute_chunked_seq`, `execute_chunked_par`, `exec_chunked_call`, `format_now`, `merge_json_array`, `alloc_str`
- Deleted `Error` enum (only `FfiError` remains, with own `Display` impl)
- Removed unused Cargo deps: `bytes`, `rayon`, `thiserror`, `libc`, `crossbeam-queue`
- Removed `CompileError::UnknownTarget` / `Compiler::targets` — `Compiler::new()` now no-arg
- Removed `ExecutionPlan::map_exprs` and `MapExpr`/`MapKV` types
- Removed `SLAB_POOL` — `flowrulz_msg_alloc`/`release` use `std::alloc` directly
- Fixed 22 clippy warnings: all `extern "C"` → `pub unsafe extern "C"`
- Fixed `now_iso()` date calc: replaced heuristic with `civil_from_days` algorithm
- Consolidated `merge_json` → `dag::deep_merge` (single source of truth)
- `extract_json_field` returns `&[u8]` (was `&mut [u8]`)
- 140 Rust tests pass, `cargo check` clean

### Phase 4–5: Go prod cleanup (complete)
- Deleted orphaned `go/internal/transport/http.go`
- Removed dead `Endpoint.nodeID`, `dlqMessage`, `ErrRateLimited`, `RecordTiming`/`GetHistogram`
- Removed dead replyrouter methods: `Cancel`/`EvictedCount`/`RouteOrStore`
- Removed dead execnode methods: `JoinCluster`/`LeaveCluster`/`AliveCount`
- Removed `RemoteCompiler.Validate`, `Rollback` alias
- Fixed `Rules()` shallow copy
- `go vet` clean, all Go tests pass

### Phase 6: Simulator cleanup (complete)
- Removed multiple dead methods, fields, variables across simulator
- All simulator + Go tests pass

### Phase 7: Kafka removal → Cluster Bus (complete)
- Designed and implemented **Cluster Bus** (`go/internal/cluster/`): gRPC peer-to-peer overlay
  - `ClusterNode`: Publish/Subscribe, peer management, topic handlers
  - `ClusterProducer`/`ClusterConsumer`: adapters implementing `transport.MessageProducer`/`transport.MessageConsumer`
- Wired into execnode: default transport when no Kafka brokers configured
- Updated `main.go` with `FLOWRULZ_GRPC_ADDR` and `FLOWRULZ_SEEDS` env vars
- Removed Kafka/ZK from docker-compose.yml (3-node cluster bus setup)
- Removed `k8s/kafka.yaml`, `k8s/zookeeper.yaml`
- Updated `k8s/flowrulz.yaml` to StatefulSet with gRPC port + cluster seeds
- Cleaned `k8s/configmap.yaml` of Kafka env vars
- Removed `kafka-transport` skill
- Build clean, `go vet` clean, all 154 Rust + all Go tests pass

### Next Steps
1. Cluster Bus: real peer-to-peer gossip, quorum-based ACK counting
2. Cluster Bus: partition rebalancing on node join/leave
3. E2E tests with 3-node docker-compose cluster

## Env Vars

| Var | Default | Description |
|---|---|---|
| `FLOWRULZ_HTTP_ADDR` | `:8080` | HTTP admin API address |
| `FLOWRULZ_GRPC_ADDR` | `:9090` | gRPC Cluster Bus address |
| `FLOWRULZ_NODE_ID` | `node-1` | Unique node identifier |
| `FLOWRULZ_SEEDS` | `` | Comma-separated `host:port` of seed nodes for cluster discovery |
| `FLOWRULZ_PERSIST_PATH` | `` | Path to rules JSON file |
| `FLOWRULZ_TOPIC` | `flowrulz-input` | Input topic name |
| `FLOWRULZ_API_KEY` | `` | Admin API auth key (optional) |
| `FLOWRULZ_KAFKA_BROKERS` | `` | Kafka brokers (legacy; empty = use Cluster Bus) |
| `FLOWRULZ_KAFKA_GROUP_ID` | `flowrulz` | Kafka consumer group (legacy) |
| `FLOWRULZ_KAFKA_ACKS` | `` | Kafka acks level (legacy) |

## Docker Compose

```bash
docker compose up --build
```

3-node cluster with Cluster Bus (no Kafka, no ZK). Nodes auto-discover via `FLOWRULZ_SEEDS`. Exposes HTTP on 8080-8082, gRPC on 9090-9092.

## Admin API (Interactive Mode)

Start the simulator in interactive mode:
```bash
make sim
./sim --interactive --dashboard-addr :8081
```

Then use curl to interact:
```bash
# Add a rule
curl -X POST localhost:8081/api/admin/rules \
  -d '{"id":"my-rule","dsl":"n:validate n:echo"}'

# Register a service
curl -X POST localhost:8081/api/admin/services \
  -d '{"name":"echo","base_latency_ms":5}'

# Send a message
curl -X POST localhost:8081/api/admin/send \
  -d '{"rule":"my-rule","body":"{\"hello\":\"world\"}"}'
# → {"body":"...","duration":"..."}
```
