# FlowRulZ Project Context

## Overview
Event-driven DAG execution engine. Compiles DSL rules → bytecode plans, distributes across Raft cluster, executes via Rust VM with work-stealing scheduler.

## Repo Structure
```
server/           Go control plane (cluster, scheduler, transport, DI)
  internal/
    node/         ProdNode — central struct wiring all modules
    bootstrap/    NodeBuilder — DI composition root
    cluster/      Raft (hashicorp/raft), peer mgmt, FSM
    scheduler/    Priority lanes + work stealing (dequeueOrSteal)
    transport/    Kafka, gRPC bus, cluster transport
    reliability/  DLQ, Saga, Circuit Breaker, Dedup, Rate Limiter
    engine/       Rule lifecycle (deploy, compile, compile)
    plandist/     Plan distribution + ack protocol
    partition/    Key-space shard mgmt + rebalancing
    membership/   Gossip, leader lease, heartbeat eviction
    execstate/    FileStore — JSON execution records
    registry/     Service registry via HTTP heartbeat
    admin/        Admin HTTP API
    observability/ OTel tracing, Prometheus metrics
  bridge/         CGo FFI → Rust runtime
  pkg/            Public interfaces (interfaces for DI/testability)

runtime/          Rust bytecode VM
  src/
    executor/     MAP, GATE, SERVICE_CALL step handlers
    bytecode/     Opcodes, plan/schema types
    memory/       Arena allocator (bumpalo)
    tracing/      Span ring buffer
    ffi/          C FFI exports for Go bridge

sdk/
  flow/           Go client library (Publish, Request, Execute, Stream)
  java/           Java SDK (Maven, com.flowrulz)
  python/         Python SDK (pip, flowrulz)
  javascript/     JS/TS SDK (npm, flowrulz)
  rust/           Rust SDK (cargo, flowrulz-sdk)
simulator/        Load gen + scenario testing (16 services, 9 scenarios, dashboard)
docs/obsidian-vault/  Obsidian vault (arch map, 26 notes, 1 canvas)
```

## Architecture
- **Raft consensus** for leader election + log replication (hashicorp/raft)
- **Priority lanes**: Fast (50 workers) > Normal (20) > Heavy (5)
- **Work stealing**: idle workers steal from Heavy→Normal→Fast lanes
- **Execution**: Go scheduler → CGo bridge → Rust VM → HTTP service calls
- **Persistence**: JSON FileStore for execution records + DLQ replay
- **DI**: Manual constructor injection via NodeBuilder.WithDefaults()

## Hexagonal Architecture (Ports & Adapters)
The codebase uses a **ports & adapters** pattern with two port layers:

### `pkg/` — Primary Ports (active, 13 packages)
All internal domain packages consume `pkg/` interfaces for DI and testability. Each pkg interface has a matching internal implementation:
```
pkg/interface          →  internal/implementation
├── pkg/cluster/       →  internal/cluster/pkgsupport.go  (ClusterMember wrapper)
├── pkg/engine/        →  internal/engine/pkgsupport.go   (Engine wrapper)
├── pkg/membership/    →  internal/membership/membership.go
├── pkg/node/          →  internal/node/prod.go           (ProdNode)
├── pkg/partition/     →  internal/partition/manager.go   (Manager, RebalanceNotifier)
├── pkg/plandist/      →  internal/plandist/distributor.go
├── pkg/registry/      →  internal/registry/pkgsupport.go (Registry wrapper)
├── pkg/reliability/   →  internal/reliability/pkgsupport.go (CircuitBreaker, Deduplicator)
├── pkg/replyrouter/   →  internal/replyrouter/router.go
├── pkg/scheduler/     →  internal/scheduler/pkgsupport.go
├── pkg/store/         →  internal/execstate/pkgsupport.go (FileStore wrapper)
├── pkg/transport/     →  internal/transport/memory/bus.go + grpc/bus.go
└── pkg/vm/            →  server/bridge/vm_adapter.go     (BridgeVM — CGo FFI adapter)
```

### `internal/policy/` — Policy Resolution Engine (new)
Hierarchical policy resolution system implementing the **Policy Resolution Pattern**:
- **9-level hierarchy**: Platform → Environment → Tenant → Application → Service → Endpoint → Method → Workflow → Runtime
- **Deep merging**: Non-nil fields override, nil fields inherit from parent levels
- **Map merging**: Feature flags and metadata merge correctly across levels
- **Caching**: Resolved policies cached for performance (invalidated on policy changes)
- **Validation**: Comprehensive validation with custom rule support
- **Storage**: Memory and file-based backends with atomic writes
- **Thread-safe**: All operations protected by RWMutex

Key types:
- `Policy`: Complete execution policy with timeout, retry, rate limit, circuit breaker, auth, tracing, etc.
- `Resolver`: Hierarchical policy resolution with caching
- `Validator`: Policy validation with built-in and custom rules
- `Store`: Persistence interface (MemoryStore, FileStore)

### `internal/ports/` — Secondary Ports (nascent, 1 of 5 active)
An alternate port hierarchy started but never connected to domain code. Only `ports/messaging/` has an adapter (`internal/adapters/messaging/memory/adapter.go`). The other 4 port packages (`cluster`, `execution`, `storage`, `vm`) were removed as dead code.

### Key Adapter: `bridge/vm_adapter.go`
The critical adapter implementing `pkg/vm` interfaces. Wraps CGo FFI calls (`Compile`, `ExecuteStep`, `InitContext`, `PlanServices`) behind `PlanCompiler` + `VMRunner` interfaces.

### Key Adapter: `adapters/messaging/memory/adapter.go`
Bridges between `pkg/transport` (topic-as-separate-param) and `ports/messaging` (topic-inside-message) with `toPkgMsg()` / `fromPkgMsg()` conversions.

### Hexagonal Gaps (known)
| Gap | Detail |
|---|---|
| Orphaned interfaces | Removed: `pkg/cluster/Gossiper`, `pkg/transport/MessageProducerFactory`, `ports/{cluster,execution,storage,vm}` |
| Reliability mismatch | `pkg/reliability/DLQ`, `RateLimiter`, `SagaOrchestrator` have different APIs than `internal/reliability/` concrete implementations. Adapter helpers added (`AllowWithCtx`, `WaitCtx`, `CompensateCtx`, `StatusInfo`) but no compile-time compliance yet. DI uses concrete types (`*reliability.DLQ`) in `node/factory.go`. |
| Mixed DI types | `Dependencies` struct at `node/prod.go:35` mixes interface types (Cluster, Scheduler, ReplyRouter) with concrete types (Engine, PlanDist, DLQ, GRPCBus) |
| Signature differences | `pkg/transport.Publish(ctx, topic, msg)` vs `ports/messaging.Publish(ctx, msg)` — topic placement differs.

## Platform Architecture Maturity

FlowRulZ was evaluated against the Principal Platform Architect blueprint (16 sections, 80+ items). 

### Strong areas (75%+ complete)
| Section | Score | Key implementations |
|---|---|---|
| OO Design Patterns | 82% | Builder, Factory, Strategy, Adapter, Facade, Options, Manual DI |
| Failure Scenarios | 67% | CB, DLQ replay, saga compensation, dedup, rate limit, timeout, atomic file persistence |
| Deliverables | 75% | Full docs suite, file index, Obsidian vault, AGENTS.md, 274+ tests |
| Execution Pipeline | 60% | Rate limit → Dedup → CB → Saga → DLQ; Rust VM with retry opcodes |
| Scalability | 64% | Work-stealing, priority lanes, Raft, key-space partitioning, Kafka, arena allocator |
| Runtime SDK | 60% | 5 language SDKs, CGo FFI bridge, WASM plugin support |
| Control Plane | 68% | Service registry + heartbeat, rule versioning, lane priority, admin CRUD API |

### Improvement areas
| Section | Score | Key gaps |
|---|---|---|
| Security | 18% | No RBAC/ABAC, no mTLS/JWT, no audit trail, no service-to-service auth |
| Dynamic Config | 25% | No watch/notify pattern, no atomic pointer swap, no feature flags, no SDK watcher |
| Plugin/Extension | 36% | No pluggable auth/serialization/routing, no plugin lifecycle mgmt, no extension registration API |
| DDD | 44% | No bounded context boundaries, no ACLs, no domain event model |
| Policy Resolver | 56% | No hierarchical policy merging (platform→env→service→endpoint), no inheritance |
| Metadata Cache | 43% | No distributed cache, no push invalidation, no prewarming |

## Refactoring Gaps (completed)
1. **Structured logging**: `log.Printf`→`slog` (64 call sites in 18 Go files); `eprintln!`→`log::warn!` (Rust)
2. **Split execnode God object**: deleted `server/internal/execnode/` (11 files dead code); exported `MakeProducerFromCluster`/`MakeConsumerFromCluster` to transport pkg
3. **ExecuteAll bypasses scheduler**: routes through `scheduler.EnqueueAndWait`
4. **Execution history**: completed states saved as `StatusCompleted` + output (not deleted)
5. **Work stealing**: `slotWorker.dequeueOrSteal()` steals from Heavy→Normal→Fast when idle
6. **DI migration**: `NodeBuilder.WithDefaults()` delegates to `DefaultDependencies()` factory

## Race Conditions Fixed (Q2 2026 — 14 data races)
### Production Server (8 races)
| File | Issue | Fix |
|---|---|---|
| `transport/grpc/bus.go` | Map iteration after `RUnlock` raced with concurrent writes | Hold `RLock` during `deliverToTopic`/`Publish` iteration |
| `scheduler/prod.go` | `l.wg.Add(1)` in goroutine raced with `l.wg.Wait()` in `Stop()` | Move `wg.Add(N)` to `Start()` before goroutines |
| `scheduler/worker.go` | `tickOnce` fired callbacks inside lock — re-entrancy deadlock | Collect callbacks, release lock, then fire |
| `scheduler/worker.go` | Callback goroutines untracked on `Stop()` | Added `sync.WaitGroup`, `Stop()` waits |
| `replyrouter/router.go` | `Deliver` + `Cancel` + `cleanup` all `close(pr.ReplyCh)` — double-close panic | Hold lock during close; collect expired, release, then close |
| `node/exec_registry.go` | `Cancel()` released lock before calling `cancel()` — TOCTOU | Delete + cancel under same lock |
| `cluster/node.go` | Unbounded goroutines per peer with `context.Background()` | Added 30s timeout per peer publish |
| `reliability/dlq.go` | Kafka producer error silently swallowed | Return `err` to caller |
### Simulator (6 races)
| File | Issue | Fix |
|---|---|---|
| `eventbus/eventbus.go` | `Unsubscribe` wrote to topics map while `dispatch` iterated | Copy handlers map under lock via `handlersFor()` |
| `execution/context.go` | `State`/`Variables` fields unsynchronized between worker and test | Added `sync.Mutex`, private fields, accessor methods |
| `scheduler/scheduler_test.go` | Timer wheel callback wrote to `order` slice without sync | Added `sync.Mutex` |
| `eventbus/eventbus_test.go` | Handler wrote to shared `msgID` without sync | Replaced with buffered channel |
| `eventbus/eventbus_test.go` | `dispatch` + `Unsubscribe` map race | Copy handlers under lock |
| `scheduler/scheduler.go` | Bus dispatch goroutine blocked on `<-resultCh` | Wrapped reply in `go func()` |

## Functional Improvements (Q2 2026)
### Rust VM
| File | Issue | Fix |
|---|---|---|
| `executor/vm.rs` | `op_svc_call` returned `Err(e)` on failure, short-circuiting fallback handlers. `op_next` returned `Ok(())` — inconsistent | Now returns `Ok(())` on failure, matching `op_next` |
| `tracing/mod.rs` | Thread-local `RefCell<SpanRingBuffer>` panicked if Go poller (`flowrulz_get_spans`) + Rust VM (`emit_span`) overlapped. Cross-thread lost spans. | Replaced with global `Lazy<Mutex<SpanRingBuffer>>` |

### Simulator
| File | Issue | Fix |
|---|---|---|
| `loadgen/loadgen.go` | `time.Second / time.Duration(0)` panics on `Rate=0` | Guard `if rate <= 0 { rate = 1 }` |
| `loadgen/loadgen.go` | `rampUp` ticker leaked on context cancellation | Refactored into `runStep` helper with `defer ticker.Stop()` |
| `execution/context.go` | `concurrent` counter tracked dispatch rate, not actual concurrency | Added `OnDone` callback; increment at dispatch, decrement on completion |
| `simulator.go` | EventBus never closed on Stop | Added `s.Bus.Close()` |

### Server
| File | Issue | Fix |
|---|---|---|
| `partition/manager.go` | `HandleAssignmentMessage` applied assignments from any node — no leader/auth check | Validates `NodeID == leaderID` + `Term >= currentTerm` (skipped when no leader) |
| `scheduler/prod.go` | `EnqueueAndWait` returned on context cancel but task still ran with side effects unreported | Spawns background goroutine to drain `ResultCh` on cancellation |
| `node/execute_plan.go` | HTTP 5xx response body not drained → connection pool exhaustion | `io.ReadAll(resp.Body)` before returning error |

## Flaky Tests Fixed
- **`simulator_test.go`**: replaced `time.Sleep(200ms)` with polling loops + 5s timeout
- **`simulator_test.go`**: set deterministic `FailureRate = 0.0` for all tests (except `TestServiceFailure` which explicitly tests failures)
- **`scheduler/scheduler_test.go`**: `TestContextCancellation` added `defer s.Stop()` / `defer cancel()` for cleanup guarantee
- **`scheduler/scheduler_test.go`**: `TestTimerWheelOrder` added `sync.Mutex` for shared `order` slice

## Tests
- Go: `cd server && go test ./internal/... ./bridge/...` (274 tests, all pass with `-race`)
- Rust: `cd runtime && cargo test` (401 tests, all pass)
- Simulator: `go test ./simulator/...` (all pass with `-race`)
- **Bridge `TestExecuteStepMultiCall`**: root cause was `sync.Pool` buffer aliasing in `Compile`, `Execute`, `InitContext`. Fixed by `make+copy`.
- **Scheduler `TestPriorityOrdering`**: data race on `execOrder` slice. Fixed with `sync.Mutex`.

## Key Patterns
- **ExecutionContext**: has `sync.Mutex`. Use `State()`/`SetVariable()`/`Variable()` accessors. `OnDone` callback fires after `MarkDone`/`MarkFailed`.
- **TimerWheel**: `sync.WaitGroup` tracks callback goroutines. `Stop()` waits for all callbacks before returning.
- **ReplyRouter**: channel closes happen under lock to prevent double-close panics.
- **SpanRingBuffer**: global `once_cell::sync::Lazy<std::sync::Mutex<SpanRingBuffer>>`. Use `SPAN_BUFFER.lock()` for access. Rust tests using global buffer must drain via `drain_global_buffer()` before emitting.

## Docs
`docs/` — architecture guides, format specs, review documents. 18 files + 1 vault.

| File | Purpose |
|---|---|
| `flow-architecture.md` | Distributed Event Runtime — architecture, Event model, ExecutionContext, flows |
| `vm-architecture.md` | VM dispatch loop, opcode handlers, DAG evaluation |
| `bytecode-format.md` | ExecutionPlan, Instruction packing, opcodes, type system |
| `dsl-syntax.md` | DSL language specification (rules, services, fallback, DAG) |
| `memory-management.md` | Arena allocator, string interning, message lifecycle |
| `ffi-api.md` | C FFI surface — `flowrulz_compile`, `flowrulz_execute_step`, `flowrulz_get_spans` |
| `cluster-model.md` | Single-leader cluster, membership, plan distribution, service registry |
| `flows.md` | Every data path: membership → deployment → execution → DLQ → metrics |
| `file-index.md` | Every source file: package, purpose, key exports (~1078 lines) |
| `kafka-semantics.md` | Kafka transport — partitions, offsets, idempotent producer, consumer groups |
| `development.md` | Dev setup, test commands, build pipeline |
| `software-review.md` | Multi-layer codebase review (architecture, bugs, security, ops) |
| `engineering-audit.md` | Engineering audit findings |
| `architecture-review-complete.md` | Full architecture review (SOLID, DRY, decoupling) |
| `restructure-plan.md` | Codebase restructure plan |
| `ultimate-review-prompt.md` | Architecture review prompt template |
| `README.md` | Docs index + project map + key design decisions |
| `obsidian-vault/` | Obsidian vault — 26 notes + 1 `.canvas` arch map |

## Obsidian Vault
`docs/obsidian-vault/` — 26 notes + 1 `.canvas` map. Architecture, modules, concepts, all linked via wikilinks.
