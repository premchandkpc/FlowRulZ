# File Index

Every source file in the project, grouped by package, with its purpose and key exports.

---

## Go (21 source files + 18 simulator files)

### `go/cmd/flowrulz/main.go`
**Package:** `main`

Entry point — reads env vars (`FLOWRULZ_PERSIST_PATH`, `FLOWRULZ_HTTP_ADDR`, `FLOWRULZ_TOPIC`), creates `execnode.Config`, and calls `execnode.New(cfg).Start()`.

---

### `go/flow/client.go`
**Package:** `flow`

Public client SDK. Provides four communication models: `Publish` (async), `Request` (sync), `Execute` (rule), `Stream` (subscription). All operations are methods on the `Client` struct, configured with `Config{Address, APIKey, HTTPClient}`.

**Exports:** `Client`, `Config`, `Event`, `Mode` constants, `Publish()`, `Request()`, `Execute()`, `Stream()`, `ExecuteOption`, `WithExecuteTimeout()`, `WithExecuteHeaders()`

---

### `go/internal/flow/flow.go`
**Package:** `flow` (internal)

Internal workflow state machine. Tracks execution state (`Pending`/`Running`/`Completed`/`Failed`) and collects per-service responses. The `Orchestrator` manages concurrent workflow instances by ID.

File-based checkpointing: `NewOrchestratorWithCheckpointDir(dir)` persists each flow as `<id>.json` after state transitions (Start, StoreResponse, Complete, Fail). Atomic write via `.tmp` + `Rename`. `loadCheckpoints()` scans `dir` on creation and restores all flows.

**Exports:** `FlowState`, `Flow`, `Orchestrator`, `NewOrchestrator()`, `NewOrchestratorWithCheckpointDir()`, `Start()`, `Get()`, `StoreResponse()`, `Complete()`, `Fail()`, `Remove()`, `List()`

---

### `go/internal/admin/server.go`
**Package:** `admin`

HTTP admin server. Serves rule CRUD, validation, promote/rollback, lane listing, DLQ management, health check, and metrics. API key auth via `Authorization: Bearer <key>` header on all endpoints except `/health` and `/metrics`.

**Exports:** `Server`, `New()`, `RegisterDLQ()`, `Handler()`
**Endpoints:** `POST/DELETE/GET /rules`, `GET /rules/{id}/versions`, `POST /rules/{id}/validate`, `POST /rules/{id}/promote`, `POST /rules/{id}/rollback`, `GET /lanes`, `GET /health`, `GET /dlq`, `POST /dlq/replay/{id}`, `POST /dlq/replay`, `DELETE /dlq`

---

### `go/bridge/bridge.go`
**Package:** `bridge`

CGo FFI bridge to the Rust shared library. Functions map 1:1 to `extern "C"` calls:
- `Compile` → `flowrulz_compile` — DSL string → bytecode plan
- `Execute` → `flowrulz_execute` — plan + body → result (synchronous, callback-based)
- `ExecuteStep` → `flowrulz_execute_step` — cooperative single-step execution (plan + serialized ctx + optional response → StepOutput)
- `PlanComplexity` → `flowrulz_plan_complexity`
- `PlanServices` → `flowrulz_plan_services` — extract service IDs from plan
- `GetSpans` → `flowrulz_get_spans` — drain span ring buffer
- `MsgAlloc` / `MsgRelease` — C-heap memory management
- `Intern` / `InternLookup` — string interning via Rust `InternTable`

The Go-side service caller bridge uses `sync.Map` (callerMap) + `atomic.Uint64` (nextExecID) — no mutex in hot path.

**Exports:** `ServiceCaller` func type, `ExecContext`, `StepResult` (Done/Pending/Continue), `StepOutput`, `ServiceEntry`, `Compile()`, `Execute()`, `ExecuteStep()`, `MsgAlloc()`, `MsgRelease()`, `Intern()`, `InternLookup()`, `GetSpans()`, `PlanServices()`, `PlanComplexity()`

---

### `go/bridge/caller_bridge.c`
**Language:** C

Static C function bridging the Rust `caller_cb_t` function pointer to the Go-exported `goServiceCaller`. Provides `getCallerBridgePtr()` which returns a function pointer the Rust FFI layer registers as the service call callback.

**Exports:** `callerBridge()`, `getCallerBridgePtr()`

---

### `go/internal/engine/engine.go`
**Package:** `engine`

Core rule engine. Maintains a `map[string]*Rule` of versioned plans in memory. Each `Rule` holds a slice of `VersionedPlan`s with an `ActiveVersion` index. `Deploy()` compiles DSL via bridge, assigns a lane (fast/normal/heavy) by `PlanComplexity` score, persists to disk, and returns. `AddVersion()` stores a pre-compiled plan (used by followers receiving plans via PlanDistributor) without auto-activating. `Promote()` activates a specific version.

Callback hooks: `AfterDeploy` and `AfterPromote` are set by execnode to trigger plan distribution on the leader. After a successful deploy, `AfterDeploy(id, dsl, plan, version)` is called (leader spawns `distributePlan`). After promote, `AfterPromote(id, version)` is called (leader spawns `distributeActivate`).

Persistence: atomic write via `os.WriteFile(path.tmp)` + `os.Rename(path.tmp, path)`.

**Exports:** `Lane`, `LaneConfig`, `DefaultLanes`, `VersionedPlan`, `Rule`, `Engine`, `New()`, `Deploy()`, `AddVersion()`, `Promote()`, `Drain()`, `Remove()`, `Rules()`, `ExecuteAll()`, `ActivePlanBytes()`, `LaneForScore()`

---

### `go/internal/execnode/execnode.go`
**Package:** `execnode`

Data-plane process. `New()` wires together: Engine, PlanDistributor (with plan/ack producers), Scheduler, ReplyRouter, DLQ, RateLimiter, CircuitBreakers (per-svcID), MetricsCollector, Admin server, and `httpClient`. Sets `Engine.AfterDeploy` and `Engine.AfterPromote` callbacks that trigger plan distribution when the node is leader.

Leader/follower role managed by `SetLeader()`/`IsLeader()`. `SetLeader(true)` marks the node as leader; `SetTerm(n)` synchronizes the cluster term to both the node and PlanDistributor.

`Start()`:
1. Creates the ingress `MessageHandler` — rate-limits, dedup by MessageID, calls `executeAll(msg)` which iterates active plans from `Engine.ActivePlanBytes()`
2. `executeAll()` delegates to `executePlan(plan, body)` per active rule — a cooperative loop using `bridge.ExecuteStep()`:
   - `StepDone` → return output
   - `StepPending` → `callService(svcID, body)` which checks circuit breaker, resolves via `ServiceResolver`, makes HTTP call, records metrics
   - `StepContinue` → advance to next instruction
3. Creates plan/ack consumers via `mkConsumer()` (real Sarama or stub depending on `KafkaBrokers` config) that listen on `_flowrulz_plans`/`_flowrulz_acks`:
   - `handlePlanMessage` deserializes `PlanMessage`, rejects stale terms, calls `Engine.AddVersion()` for "plan" type (and sends ACK) or `Engine.Promote()` for "activate" type
   - `handleAckMessage` deserializes `AckMessage` and calls `PlanDist.RecordAck()`
4. Creates producers via `mkProducer()` (real Sarama `SyncProducer` or stub)
5. Starts `PlanDistributor`
6. Starts the HTTP server (admin + health + metrics)
7. Blocks on SIGINT/SIGTERM

`Shutdown()`: stops consumers → stops PlanDistributor → stops scheduler → stops reply router cleanup → closes producers → shuts down HTTP server.

Leader-only distribution flow:
- After `Engine.Deploy()` → `AfterDeploy` callback → spawns `distributePlan()` goroutine:
  1. Increments cluster term
  2. `PlanDist.PublishPlan()` → `PlanDist.WaitForAcks()` → `PlanDist.ActivatePlan()`
- After `Engine.Promote()` → `AfterPromote` callback → spawns `distributeActivate()` goroutine:
  1. `PlanDist.ActivatePlan()` publishes activate message

**Exports:** `Config`, `NewConfig()`, `ExecutionNode`, `New()`, `Start()`, `Shutdown()`, `SetLeader()`, `IsLeader()`, `SetTerm()`, `CurrentTerm()`, `callService()`, `executePlan()`, `executeAll()`

---

### `go/internal/observability/metrics.go`
**Package:** `observability`

Thread-safe metrics collection. `Counter` (atomic int64), `Gauge` (atomic int64), `Histogram` (sorted buckets + atomic counters). Per-name dedup via `sync.RWMutex`-guarded maps. A package-level `defaultCollector` provides global shortcut functions.

Histogram `Observe()`: linear scan of sorted bucket bounds, increments the first matching bucket or the overflow bucket. Not lock-protected (atomic counts are sufficient; total and bucket increments are not atomic together — acceptable skew for metrics).

**Exports:** `Counter`, `Gauge`, `Histogram`, `MetricsCollector`, `MetricSnapshot`, `NewMetricsCollector()`, `GetCounter()`, `GetGauge()`, `RecordExec()`, `RecordError()`

---

### `go/internal/plandist/plandist.go`
**Package:** `plandist`

Plan distribution across the cluster. The leader publishes `PlanMessage{type:"plan"}` with compiled bytecode to `_flowrulz_plans`, then waits for ACKs from a quorum of followers on `_flowrulz_acks`. Once quorum is reached, publishes `PlanMessage{type:"activate"}`.

Wired in execnode: `handlePlanMessage` deserializes `PlanMessage`, rejects plans with stale terms (lower than current cluster term), calls `Engine.AddVersion()` for "plan" type (and sends ACK via `PlanDist.SendAck()`) or `Engine.Promote()` for "activate" type. `handleAckMessage` deserializes `AckMessage` and calls `PlanDist.RecordAck()`.

Plan/ACK consumers listen on `_flowrulz_plans`/`_flowrulz_acks` respectively, registered in `execnode.Start()`.

`WaitForAcks()`: creates a `pendingAck` entry in a `sync.Map`, blocks on a `done` channel with timeout. `RecordAck()` (called by the ACK consumer handler) increments the atomic counter and signals the channel when quorum is met.

**Exports:** `PlanMessage`, `AckMessage`, `PlanHandler`, `AckHandler`, `PlanDistributor`, `New()`, `Start()`, `Stop()`, `PublishPlan()`, `ActivatePlan()`, `SendAck()`, `WaitForAcks()`, `RecordAck()`, `PlanMessageFromBytes()`, `AckMessageFromBytes()`

---

### `go/internal/plugins/loader.go`
**Package:** `plugins`

WASM plugin loader. `LoadDir(dir)` scans a directory for `.wasm` files, reads each file, and registers it via `bridge.RegisterPlugin()`. The filename without extension becomes the plugin name. Skips non-existent directories with a log message.

Configured via `execnode.Config.PluginDir` or `FLOWRULZ_PLUGIN_DIR` env var. Registered plugins become available in DSL as `w:<name>.<func>()`.

**Exports:** `LoadDir()`

---

### `go/internal/registry/registry.go`
**Package:** `registry`

Service registry mapping service names → healthy endpoints. Supports four load-balancing strategies: `Random`, `RoundRobin` (atomic counter), `LocalPrefer` (same node first), `LeastLoaded` (lowest load value).

`Register()` defaults `Protocol` to HTTP and `NodeID` to local node ID. `Pick()` returns a single healthy endpoint per the configured strategy. `Snapshot()` deep-copies the entire registry for leader aggregation.

**Exports:** `Protocol`, `LBStrategy`, `Endpoint`, `ServiceInfo`, `ServiceRegistry`, `New()`, `Register()`, `Lookup()`, `Pick()`, `PickWithStrategy()`, `MarkUnhealthy()`, `MarkHealthy()`, `ListServices()`, `ListEndpoints()`, `Snapshot()`

---

### `go/internal/reliability/dedup.go`
**Package:** `reliability`

Bounded in-memory dedup tracker. `Mark(id)` stores an entry; `Seen(id)` checks if already processed. Evicts oldest when at capacity (default 10k). Background cleanup goroutine removes entries past TTL (default 5min). Wired in `execnode` handler by MessageID.

**Exports:** `DedupEntry`, `DedupTracker`, `NewDedupTracker()`, `Seen()`, `Mark()`, `StartCleanup()`, `Len()`, `Clear()`

---

### `go/internal/reliability/circuitbreaker.go`
**Package:** `reliability`

Circuit breaker with three states: `Closed` (normal), `Open` (rejecting all), `HalfOpen` (probing). Transitions: `Closed` → `Open` when failure count reaches threshold; `Open` → `HalfOpen` after recovery timeout; `HalfOpen` → `Closed` on success, or → `Open` on failure.

Allows up to `halfOpenMaxReqs` (default 3) concurrent probing requests. State and half-open counter use atomics for lock-free reads in the hot path.

**Wiring:** `execnode/execnode.go` creates per-svcID instances in `callService()` (threshold=5, recovery=30s). Before calling the service, `Allow()` is checked — returns "circuit breaker open" error if tripped. On success `Success()` resets the breaker; on error `Failure()` advances the failure count.

**Exports:** `State`, `CircuitBreaker`, `NewCircuitBreaker()`, `Allow()`, `Success()`, `Failure()`

---

### `go/internal/reliability/dlq.go`
**Package:** `reliability`

Bounded dead-letter queue with Kafka persistence. `Send()` appends to an in-memory slice (oldest evicted FIFO at capacity, default 10k) and, when a `transport.MessageProducer` is configured via `WithDLQProducer()`, also produces to `_flowrulz_dlq`. Always succeeds (no-fail design).

`Replay()`: removes entry from DLQ by ID, calls `replayFn`. `ReplayAll()`: drains all entries, replays each, re-enqueues any that fail again. `ToJSON()` serializes all entries for export. `SetReplayFn()` configures the callback (set by execnode to re-run `executeAll`).

**Exports:** `DeadLetterEntry`, `DLQ`, `NewDLQ()`, `DLQOption`, `WithDLQProducer()`, `WithDLQTopic()`, `DefaultDLQTopic`, `SetReplayFn()`, `Send()`, `LoadFromTopic()`, `Replay()`, `ReplayAll()`, `List()`, `Len()`, `Clear()`, `ToJSON()`

---

### `go/internal/reliability/ratelimit.go`
**Package:** `reliability`

Token-bucket rate limiter. Per-name buckets with configurable rate (tokens/sec) and burst cap. Lazy refill: `AllowN()` computes elapsed time since last refill, adds `elapsed * rate` tokens (capped at burst), then subtracts `n`.

`Bucket()` uses double-checked locking — first `RLock` to check if bucket exists, fallback to `Lock` + create with defaults (rate=100, burst=100). `SetBucket()` replaces the bucket for a name entirely.

**Exports:** `TokenBucket`, `RateLimiter`, `NewRateLimiter()`, `NewTokenBucket()`, `Bucket()`, `SetBucket()`, `Allow()`, `AllowN()`

---

### `go/internal/replyrouter/router.go`
**Package:** `replyrouter`

Per-node reply router for request/reply pattern. `Send(corrID, timeout)` registers a `PendingRequest` in a `map[string]*PendingRequest` and returns a buffered `chan []byte`. The caller waits on the channel for the reply.

`Route(corrID, response)` looks up the entry, deletes it, sends the response on the channel, and closes it. `RouteOrStore()` is a best-effort variant that silently ignores not-found (reply arrived after timeout).

Cleanup goroutine runs every 1s, evicting expired entries (past `Deadline`) and closing their channels. Duplicate correlation IDs and capacity limits (default 10,000) are rejected with typed errors.

**Exports:** `PendingRequest`, `ReplyRouter`, `New()`, `Send()`, `Route()`, `PendingCount()`, `StartCleanup()`, `StopCleanup()`, `ErrPendingNotFound`, `ErrPendingExpired`, `ErrPendingLimit`, `ErrDuplicateCorrID`

---

### `go/internal/scheduler/scheduler.go`
**Package:** `scheduler`

Lane-based priority scheduler. Three lanes: `Fast` (50 concurrent, 5k queue), `Normal` (20, 2k), `Heavy` (5, 500, reject-on-full). Each lane has a buffered channel as its queue and a semaphore channel for concurrency limiting.

`Enqueue()`: Fast/Normal lanes block on the queue channel (backpressure propagates to the consumer). Heavy lane uses non-blocking send + `ErrQueueFull` on capacity.

Lane workers: one goroutine per lane, each acquires a semaphore slot, dequeues a task, and spawns `execTask` as a goroutine (which releases the semaphore on completion). `PriorityForScore()` maps complexity scores to lanes.

**Exports:** `Priority`, `Task`, `TaskResult`, `LaneConfig`, `Scheduler`, `New()`, `Start()`, `Stop()`, `Enqueue()`, `EnqueueAndWait()`, `QueuedCount()`, `RunningCount()`, `TotalEnqueued()`, `TotalDequeued()`, `TotalRejected()`, `DefaultLanes`, `PriorityForScore()`, `ErrQueueFull`

---

### `go/pkg/transport/eventbus.go`
**Package:** `transport`

Public `EventBus` interface — the canonical pub/sub abstraction for FlowRulZ. Defines `Publish`, `Subscribe`, `Request`, `Reply`, `Broadcast`, `PublishToPartition`, `Unsubscribe`, `TopicStats`, and `Close`. Also defines `Message` (universal message type with ID, Type, Topic, Body, Headers, CorrelationID, etc.), `Handler` callback type, and `Subscription` struct.

This interface lives in `go/pkg/` (not `go/internal/`) so it can be imported by both `go/...` and `simulator/...` packages. The in-memory implementation is at `simulator/eventbus/eventbus.go`; production Kafka/HTTP implementations are at `go/internal/transport/`.

**Exports:** `EventBus`, `Message`, `MessageType`, `TypePublish`, `TypeRequest`, `TypeReply`, `TypeBroadcast`, `TypeExecution`, `Handler`, `Subscription`

---

### `go/internal/transport/consumer.go`
**Package:** `transport`

In-memory message consumer. Accepts injected messages via `Inject(msg)`, dispatches to a `MessageHandler` in a `Start()` loop. Stops via `Stop()` closing `stopCh`. Buffered channel of 100 messages between inject and handler.

**Exports:** `Consumer`, `NewConsumer()`, `Topic()`, `Start()`, `Inject()`, `Stop()`

---

### `go/internal/transport/producer.go`
**Package:** `transport`

In-memory/log-only message producer. `Send()` logs the topic/key/size and returns nil. `Close()` is a no-op. Stub for development and testing.

**Exports:** `Producer`, `NewProducer()`, `Send()`, `Close()`

---

### `go/internal/transport/types.go`
**Package:** `transport`

Core transport interfaces. `MessageHandler` func type and `MessageConsumer`/`MessageProducer` interfaces used by all transport implementations.

**Exports:** `MessageHandler`, `MessageConsumer`, `MessageProducer`

---

### `go/internal/transport/kafka.go`
**Package:** `transport`

Real Sarama-backed Kafka transport. `KafkaConsumer` wraps `sarama.ConsumerGroup` with round-robin partition strategy — implements `sarama.ConsumerGroupHandler` (`Setup`/`Cleanup`/`ConsumeClaim`), dispatches messages to a `MessageHandler`, marks them consumed. Falls back to in-memory channel mode when no brokers configured.

`KafkaProducer` wraps `sarama.SyncProducer` with `WaitForLocal` ack level — `Send()` returns partition/offset on success. Lazy-init: producer created on first `Send()` call. Log-only mode (no-op) when no brokers configured.

Both implement `MessageConsumer`/`MessageProducer` interfaces, swappable with stub implementations for testing.

**Exports:** `KafkaConfig`, `KafkaConsumer`, `KafkaProducer`, `NewKafkaConsumer()`, `NewKafkaProducer()`, `Topic()`, `Start()`, `Stop()`, `Inject()`, `Send()`, `Close()`

---



### `simulator/simulator.go`
**Package:** `simulator`

Top-level orchestrator. Creates ServiceRegistry, Timeline, Metrics, Network, Nodes (Scheduler instances), Dispatcher, LoadGen, Dashboard. `New()` compiles default plans via bridge.Compile, assigns compiled bytecode to per-node plan copies. `Run()` starts all components, runs for configured duration, prints results. `Stop()` tears down orderly.

**Exports:** `Simulator`, `New()`, `Run()`, `Stop()`, `Client()`

---

### `simulator/client.go`
**Package:** `simulator`

Client for programmatic interaction. `Send(ruleID, body)` dispatches a message via the Dispatcher and blocks until the execution completes, returning the output body and duration. `RegisterService(svc)` adds a mock service. `AddRule(id, dsl)` compiles a DSL rule and registers on all nodes. `Plans()`/`Services()` list registered items.

**Exports:** `Client`, `SendResult`, `Send()`, `RegisterService()`, `AddRule()`, `Plans()`, `Services()`

---

### `simulator/admin.go`
**Package:** `simulator`

Admin HTTP handlers for interactive mode. Registered on the dashboard mux via `RegisterAdminHandlers()`. Provides `POST /api/admin/send` (dispatch message to rule), `GET/POST /api/admin/rules` (list/add rules), `GET/POST /api/admin/services` (list/add services).

**Exports:** `RegisterAdminHandlers()`

---

### `simulator/config/config.go`
**Package:** `config`

Configuration structs for the simulator. `SimConfig` holds Nodes count, Workers per node, Scenario name, Duration, Rate, Speed multiplier, Dashboard address, Chaos settings. `ChaosConfig` configures packet drop, slow network, duplication.

**Exports:** `SimConfig`, `ChaosConfig`

---

### `simulator/dashboard/dashboard.go`
**Package:** `dashboard`

HTTP dashboard + admin API server. Serves real-time metrics, event timeline, node status, and execution detail. Uses `ServeMux` with JSON API endpoints. Admin endpoints can be injected via `AddHandler()`. Serves an HTML/JS SPA at `/`.

**Exports:** `Dashboard`, `New()`, `Start()`, `Stop()`, `AddHandler()`

---

### `simulator/dispatcher/dispatcher.go`
**Package:** `dispatcher`

Hash-based message dispatcher. Maps execution context ID to a node via FNV-32a hash, enqueues on the target node's Scheduler. `DispatchByKey()` allows routing by explicit key.

**Exports:** `Dispatcher`, `New()`, `Dispatch()`, `DispatchByKey()`, `StartAll()`, `StopAll()`

---

### `simulator/execution/context.go`
**Package:** `execution`

`ExecutionContext` — state object flowing through the simulator. Holds Plan, IP (instruction pointer), State (Created/Ready/Running/WaitingForService/Completed/Failed), Variables map, IncomingBody, Output, WaitingService, ResultCh for client delivery. `Transition()` records state changes, `MarkDone()`/`MarkFailed()` set final state.

**Exports:** `State`, `Result`, `ExecutionContext`, `StateChange`, `NewContext()`, `Transition()`, `MarkDone()`, `MarkFailed()`, `AddEvent()`

---

### `simulator/execution/plan.go`
**Package:** `execution`

Plan and Instruction types for the simulator's instruction-based execution path. Defines OpCodes: OpNop, OpCallService, OpValidate, OpBranch, OpPublish, OpReturn. Also holds `PlanBytes` (compiled bytecode for real VM) and `ServiceNames` map. Provides built-in flows: OrderFlow, OrderFlowAlt, PaymentFlow, RefundFlow.

**Exports:** `OpCode`, `Instruction`, `Plan`, `NewPlan()`, builtin plan vars

---

### `simulator/execution/queue.go`
**Package:** `execution`

ReadyQueue (FIFO with mutex, signaled via channel) and WaitingQueue (map of correlation ID → ExecutionContext). Used by Scheduler for execution management and suspension.

**Exports:** `ReadyQueue`, `WaitingQueue`, `NewReadyQueue()`, `NewWaitingQueue()`

---

### `simulator/loadgen/loadgen.go`
**Package:** `loadgen`

Traffic generator. `Config` holds RequestsPerSec, Duration, Pattern. `Generator` dispatches messages at the configured rate using ticker-based pacing. Supports pattern-based message selection (0=random, 1=sequential, 2=weighted by service).

**Exports:** `Config`, `Generator`, `DefaultConfig()`, `New()`, `Start()`, `Stop()`

---

### `simulator/metrics/metrics.go`
**Package:** `metrics`

Metrics collector. Tracks completed/failed/dropped counts, throughput, latency percentiles (P50/P95/P99), and per-service latency/error rates. `Snapshot()` returns all current values for dashboard display.

**Exports:** `Collector`, `Snapshot`, `NodeStats`, `ServiceStats`, `NewCollector()`, `RecordCompleted()`, `RecordFailed()`, `RecordDropped()`, `RecordServiceCall()`, `Snapshot()`

---

### `simulator/network/network.go`
**Package:** `network`

Simulated network layer. `CallService()` applies configurable latency (min/max jitter), optional chaos (packet drop, slow network, duplication). ChaosConfig controls drop probability, slow factor, duplicate percentage.

**Exports:** `Config`, `ChaosConfig`, `Network`, `New()`, `SetChaos()`, `CallService()`

---

### `simulator/scheduler/scheduler.go`
**Package:** `scheduler`

Per-node execution scheduler. Uses worker pool (goroutines) pulling from a ReadyQueue. Two execution paths:
- **Instruction-based**: loops over `Plan.Instructions`, dispatches OpCodes (CallService, Validate, Branch, Publish, Return)
- **Bridge-based** (PlanBytes != nil): cooperative loop calling `bridge.ExecuteStep()`, handles StepPending (service call via Network) and StepDone (captures output, records metrics)

`PlanCache` maps rule IDs to Plans. `sendResult()` delivers completion/failure to `ctx.ResultCh` for client integration.

**Exports:** `Result`, `Scheduler`, `PlanCache`, `New()`, `NewPlanCache()`, `Start()`, `Stop()`, `Enqueue()`, `Snapshot()`

---

### `simulator/scenarios/scenarios.go`
**Package:** `scenarios`

Built-in test scenarios. Each scenario defines a network config and loadgen config. Available: ramp-up (gradual load increase), black-friday (high rate spike), payment-outage (payment 100% failure), spike-test (intermittent bursts), chaos-monkey (random failures + slow network).

**Exports:** `Scenario`, `All`, `ByName()`, `DefaultPlans()`

---

### `simulator/services/service.go`
**Package:** `services`

Mock service with configurable behavior. `MockService` has BaseLatency, LatencyJitter, FailureRate, MaxConcurrent. `Call()` applies latency then returns `{"status":"ok"}` or fails based on failure rate. `ServiceRegistry` maps names to services. `DefaultServices()` pre-populates 9 common services (validate, inventory, fraud, payment, email, loyalty, invoice, shipping, notification).

**Exports:** `MockService`, `CallRecord`, `CallResult`, `ServiceRegistry`, `NewRegistry()`, `Register()`, `Get()`, `Names()`, `DefaultServices()`

---

### `simulator/timeline/timeline.go`
**Package:** `timeline`

Event timeline store. Records all execution events (created, instruction, service call, response, completion, failure) with timestamps. Supports `Recent(n)`, `ForExec(id)`, and aggregate `Stats()`.

**Exports:** `Event`, `Store`, `EventType` constants, `NewStore()`, `Record()`, `Recent()`, `ForExec()`, `Stats()`

---

## Rust (34 source files)

### `rust/src/lib.rs`
**Package:** `flowrulz_core`

Crate root. Declares all modules: `bytecode`, `dsl`, `executor`, `memory`, `tracing`. Exports the top-level types used by the FFI boundary.

**Exports:** `ExecutionPlan`, `VM`

---

### `rust/src/ffi.rs`
**Package:** `flowrulz_core`

C FFI boundary. All functions use `#[no_mangle] pub extern "C"` with the `flowrulz_` prefix:
- `flowrulz_compile(dsl, rule_id)` — DSL string → zero-copy `Vec<u8>` plan bytes
- `flowrulz_execute(plan, body, caller_cb, ctx)` — deserialize plan, create `VM`, run, return `ctx.body`
- `flowrulz_get_spans(buf, len)` — drain the thread-local span ring buffer into the given buffer
- `flowrulz_free(ptr)` — free C-heap allocated memory
- `flowrulz_version()` — return semver string
- `flowrulz_plan_complexity(plan)` — count instructions for lane assignment
- `flowrulz_intern(string)` / `flowrulz_intern_lookup(id)` — string interning
- `flowrulz_register_plugin(name, wasm_bytes)` — register a WASM plugin module by name

The service call caller registers the C function pointer via `caller_cb_t` signature.

**Exports:** All `flowrulz_*` extern C functions

---

### `rust/src/error.rs`
**Package:** `flowrulz_core`

Unified error types. `Error` enum covers DSL lex/parse/compile errors, VM execution errors, schema validation errors, and memory errors. Implements `Display` and `std::error::Error`.

**Exports:** `Error`, `FfiError`

---

### `rust/src/bytecode/mod.rs`
**Package:** `flowrulz_core::bytecode`

Module declaration. Re-exports all bytecode sub-module types with `pub use *`.

---

### `rust/src/bytecode/opcode.rs`
**Package:** `flowrulz_core::bytecode`

Opcode enum (23 variants: 0–22). Also defines `GateOp` (comparison operators), `ChunkMode`, and `RetryStrategy` enums used by instruction operands.

**Exports:** `OpCode`, `GateOp`, `ChunkMode`, `RetryStrategy`

---

### `rust/src/bytecode/instruction.rs`
**Package:** `flowrulz_core::bytecode`

The 8-byte `Instruction` struct: `{op: u8, flags: u8, a: u16, b: u16, c: u16}`. Provides named constructors for every opcode (`op_next()`, `op_gate()`, `op_dag()`, etc.) and accessor methods (`svc_id()`, `timeout_ms()`, `has_retry()`, `label()`).

**Exports:** `Instruction`

---

### `rust/src/bytecode/event.rs`
**Package:** `flowrulz_core::bytecode`

Universal message type. `Event` holds `id`, `topic`, `payload`, `headers`, `metadata`. `EventMetadata` contains `mode`, `reply_to`, `correlation_id`, `trace_id`, `content_type`, `schema_name`, `schema_version`, `partition`, `offset`. `Mode` enum with 6 variants.

**Exports:** `Event`, `EventMetadata`, `Mode`

---

### `rust/src/bytecode/execution.rs`
**Package:** `flowrulz_core::bytecode`

`ExecutionContext` — the state object flowing through the VM. Holds `event`, `body` (current working payload), `variables` (intermediate state), `outputs` (service call results keyed by service name), `failed` flag, `errors` vec, `hop_count`, `retry_count`, and `deadline_ms`.

**Exports:** `ExecutionContext`

---

### `rust/src/bytecode/plan.rs`
**Package:** `flowrulz_core::bytecode`

`ExecutionPlan` — the compiled output of the DSL compiler. Contains: `instructions` (flat vec), `constant_pool`, `service_table`, `dag_tables`, `map_expressions`, `retry_configs`, `chunk_configs`, `schema`, version metadata.

Also holds helper types: `RetryConfig` (max_attempts, strategy, fixed_ms), `ChunkConfig` (mode, count).

**Exports:** `ExecutionPlan`, `RetryConfig`, `ChunkConfig`

---

### `rust/src/bytecode/consts.rs`
**Package:** `flowrulz_core::bytecode`

`ConstantPool` — interned string table mapping `String → u16` IDs. Used by instructions to reference field names, values, and labels. Provides `intern()`, `lookup()`, `get_or_intern()`, and iterators.

**Exports:** `ConstantPool`

---

### `rust/src/bytecode/services.rs`
**Package:** `flowrulz_core::bytecode`

`ServiceTable` — maps `String` service names to `u16` numeric IDs and holds `ServiceEntry` metadata (name, id, endpoint info). Generated at compile time from the DSL service declarations.

**Exports:** `ServiceTable`, `ServiceEntry`

---

### `rust/src/bytecode/resolved_type.rs`
**Package:** `flowrulz_core::bytecode`

Type system for schema validation. `ResolvedType` enum: `String`, `Integer`, `Float`, `Boolean`, `Object`, `Array`, `Enum(Vec<String>)`, `Null`, `Any`. `FieldSchema` holds field name, type, and required flag. `Schema` is a map of field names to `FieldSchema`, with `is_valid()` that validates a JSON value against the schema.

Enum syntax in DSL: `enum[val1|val2|...]`.

**Exports:** `ResolvedType`, `FieldSchema`, `Schema`

---

### `rust/src/bytecode/mapexpr.rs`
**Package:** `flowrulz_core::bytecode`

Map expression types. `MapExpr` is a sequence of `MapKV` entries (field extraction, transformation, or constant assignment). Used by the `Map` opcode to restructure `ctx.body`.

**Exports:** `MapExpr`, `MapKV`

---

### `rust/src/bytecode/dag_table.rs`
**Package:** `flowrulz_core::bytecode`

DAG execution metadata. `DAGTable` holds: `nodes` (service IDs + parent IDs), `layers` (topologically sorted by level), `terminal_nodes`, `failure_policy`, `node_timeouts`, `merge_strategy`, `distributed` flag.

`DAGFailurePolicy`: `AbortAll`, `ContinueOthers`, `SkipDependents`.
`MergeStrategy`: `LastWins` (keyed JSON object), `ArrayConcat` (JSON array), `DeepMerge` (recursive), `ExplicitMap` (not yet implemented — falls back to LastWins).

**Exports:** `DAGTable`, `DAGNode`, `DAGFailurePolicy`, `MergeStrategy`

---

### `rust/src/dsl/mod.rs`
**Package:** `flowrulz_core::dsl`

Module declaration. Re-exports all DSL sub-module types.

---

### `rust/src/dsl/lexer.rs`
**Package:** `flowrulz_core::dsl`

Lexer — converts DSL source text to a stream of `Token` values. Handles: pipeline operators (`|`, `>`, `~`), labels (`n:` / `m:` / `p:` / `e:`), strings (single/double quoted), numbers, keys, gate conditions, schema definitions, retry configs, DAG blocks, and comments.

Emits `LexError` on invalid input with position info.

**Exports:** `Token`, `LexError`, `lex()`

---

### `rust/src/dsl/parser.rs`
**Package:** `flowrulz_core::dsl`

Parser — consumes `Token` stream, produces an `ASTNode`-based `Pipeline`. Validates structural rules: retry must follow a service call, collect after parallel, etc. Each DS L step is one `ASTNode` in the pipeline.

**Exports:** `ASTNode`, `Pipeline`, `ParseError`, `parse()`

---

### `rust/src/dsl/optimizer.rs`
**Package:** `flowrulz_core::dsl`

Optimizer — transforms `Pipeline` → `OptimizedPipeline`. Performs: timeout hoisting (push timeouts down to individual service calls), emit merging (combine adjacent emits), dead code elimination, NOP removal, and retry merging.

**Exports:** `OptimizedPipeline`, `Optimizer`, `Optimizer::optimize()`

---

### `rust/src/dsl/compiler.rs`
**Package:** `flowrulz_core::dsl`

Compiler — consumes `OptimizedPipeline`, produces `ExecutionPlan`. Handles: schema parsing from DSL, compile-time type checking (Gate conditions and Map expressions against declared field types), label resolution for jumps, DAG compilation (node/layer generation from DAG blocks), service table construction, constant pool population.

Emits `CompileError` with span information for all DSL-level errors.

**Exports:** `Compiler`, `CompileError`, `Compiler::compile()`

---

### `rust/src/executor/mod.rs`
**Package:** `flowrulz_core::executor`

The `VM` struct — main execution engine. `VM::run()` loops over `plan.instructions`, calling `dispatch()` on each. Dispatch matches `OpCode` to handler functions: `Next` → `exec_next`, `Gate` → `exec_jmp_if_false`, `Dag` → `exec_dag`, `Map` → `exec_map` (delegates `w:` prefix to plugin runtime), `Emit` → `exec_emit`, `Drop` → halt, `Jmp` → jump, `TypeGuard` → schema validation, etc.

Module declarations: declares `pub mod plugin;` for WASM plugin runtime.

After each dispatch, emits a `Span` (opcode, service_id, duration, status) via `crate::tracing::emit_span()`.

**Exports:** `VM`, `VM::new()`, `VM::run()`

---

### `rust/src/executor/runtime.rs`
**Package:** `flowrulz_core::executor`

`ExecutionRuntime` — higher-level wrapper around `VM`. `execute(body)` checks the first instruction's opcode:
- **Buffer (9)**: store body in accumulator, return (no VM run). Subsequent `buffer_push()` calls merge JSON; `buffer_flush()` returns the full accumulator.
- **Chunk (15)**: split body into N chunks, run VM on each, collect results into a JSON array.
- **Other**: delegate to `run_vm(body)`.

**Exports:** `ExecutionRuntime`

---

### `rust/src/executor/next.rs`
**Package:** `flowrulz_core::executor`

`exec_next()` — service call handler for the `Next` and `Async` opcodes. Extracts `svc_id` and `timeout` from the instruction. If retry is configured (`has_retry()`), delegates to `exec_with_retry()` which loops up to `max_attempts` with delay (exponential/linear/fixed) and jitter.

`find_retry_config()` reads from `plan.retry_configs[instr.c]`, defaulting to 3 attempts with exponential backoff.

**Exports:** `exec_next()`, `exec_with_retry()`, `find_retry_config()`

---

### `rust/src/executor/gate.rs`
**Package:** `flowrulz_core::executor`

`exec_jmp_if_false()` — gate/conditional opcode. Extracts a JSON field from `ctx.body` via dot-path, compares against a value using `GateOp` (eq, neq, gt, gte, lt, lte, contains, exists, !exists). If condition is false, sets the skip offset to jump over the gated block.

**Exports:** `exec_jmp_if_false()`

---

### `rust/src/executor/map.rs`
**Package:** `flowrulz_core::executor`

`exec_map()` — map/transform opcode. If the expression starts with `w:`, delegates to `plugin::call_plugin()` for WASM plugin execution. Otherwise evaluates `MapExpr` against `ctx.body`, producing a new JSON body. Handles field extraction (dot-path), key=value rewrites, constant assignments, and expression evaluation.

**Exports:** `exec_map()`

---

### `rust/src/executor/plugin.rs`
**Package:** `flowrulz_core::executor`

WASM plugin runtime. Maintains two global registries: `PLUGIN_BYTES` (name → raw WASM bytes) and `MODULE_CACHE` (name → compiled wasmtime `Engine`+`Module`). Plugins are lazily compiled on first `call()` invocation.

`register(name, wasm_bytes)`: stores raw bytes for later compilation.
`call(name, func_name, input)`: compiles (or loads from cache), instantiates, writes input at end of linear memory, calls the exported function with `(input_offset, input_len)` params, reads output from returned `(output_ptr << 32) | output_len` packed i64.
`call_plugin(expr, body)`: parses `w:plugin.func()` expression, extracts plugin name and function name, delegates to `call()`.

Calling convention: plugin exports `memory` + `process(ptr: i32, len: i32) → i64`. Fuel limit: 100k instructions.

**Exports:** `register()`, `call()`, `call_plugin()`

---

### `rust/src/executor/parallel.rs`
**Package:** `flowrulz_core::executor`

`exec_parallel()` — fan-out to multiple services concurrently. Calls each target service via the caller callback, collects all results, and merges them into a JSON array. `exec_collect()` simply increments `hop_count` as a synchronization marker.

**Exports:** `exec_parallel()`, `exec_collect()`

---

### `rust/src/executor/emit.rs`
**Package:** `flowrulz_core::executor`

`exec_emit()` — fire-and-forget output opcode. Calls the caller callback for each target service but discards the return value. Used to produce events to output topics as a side effect.

**Exports:** `exec_emit()`

---

### `rust/src/executor/context.rs`
**Package:** `flowrulz_core::executor`

Thin re-export of `ExecutionContext` from `bytecode::execution`.

**Exports:** `ExecutionContext` (re-export)

---

### `rust/src/executor/expr.rs`
**Package:** `flowrulz_core::executor`

Expression evaluator for `Map` expressions. Parses and evaluates: field references (`@.field.subfield`), string concatenation, and builtin function calls. ~30+ builtins: `to_string`, `parse_int`, `parse_float`, `coalesce`, `default`, `contains`, `keys`, `merge`, `epoch`, `hash`, `uuid`, `now`, `lower`, `upper`, `trim`, `length`, `concat`, `base64`, `json`, `substring`, `replace`.

`call_builtin(name, &[Value])` dispatches by name.

**Exports:** `eval_map_expression()`, `call_builtin()`

---

### `rust/src/executor/helpers.rs`
**Package:** `flowrulz_core::executor`

Utility functions: `merge_json_array()` (concatenate two JSON arrays), `extract_json_field()` (dot-path field lookup on `serde_json::Value`), `compare_values()` (type-coercing comparison for Gate opcodes).

**Exports:** `merge_json_array()`, `extract_json_field()`, `compare_values()`

---

### `rust/src/executor/dag.rs`
**Package:** `flowrulz_core::executor`

`exec_dag()` — DAG execution handler. Iterates layers in topological order. For each node:
1. **SkipDependents check**: if any parent is in the failed set, skip.
2. **Build input body**: merge parent results via `deep_merge()`.
3. **Service call**: `caller(svc_id, merged_body, timeout)`.
4. **Failure handling**: `AbortAll` → return Err immediately; `ContinueOthers` → record failure, continue; `SkipDependents` → same as ContinueOthers (downstream skip handled at step 1).

After all layers, calls `merge_dag_results()` with the configured `MergeStrategy`:
- **LastWins**: `{"svc_name": result}` JSON object
- **ArrayConcat**: `[result, result]` JSON array
- **DeepMerge**: recursive JSON object merge of all terminal node results
- **ExplicitMap**: falls back to LastWins

Results are allocated on the `Arena` bump allocator and returned as `&mut [u8]`.

**Exports:** `exec_dag()`

---

### `rust/src/executor/chunk.rs`
**Package:** `flowrulz_core::executor`

`split_chunks()` — splits a body into N roughly equal chunks by byte length. `execute_chunked_seq()` runs VM sequentially on each chunk. All results are assembled into a single JSON array.

**Exports:** `split_chunks()`, `execute_chunked_seq()`, `execute_chunked_par()`

---

### `rust/src/memory/mod.rs`
**Package:** `flowrulz_core::memory`

Module declaration. Re-exports all memory sub-module types.

---

### `rust/src/memory/arena.rs`
**Package:** `flowrulz_core::memory`

`Arena` — wrapper around `bumpalo::Bump`, a bump allocator for fast temporary allocations during execution. Used primarily in `exec_dag()` for merge results.

**Exports:** `Arena`

---

### `rust/src/memory/slab.rs`
**Package:** `flowrulz_core::memory`

`SlabPool` — pre-allocated pool of `Arena`s in three size classes (small: 4KB, medium: 16KB, large: 64KB). Uses `crossbeam::SegQueue` for lock-free acquire/put. Borrow-checked via runtime lease pattern.

**Exports:** `SlabPool`

---

### `rust/src/memory/intern.rs`
**Package:** `flowrulz_core::memory`

`InternTable` — concurrent string interner. Forward map: `RwLock<HashMap<String, u16>>`. Reverse map: `boxcar::Vec` (lock-free append-only). ID generation via `AtomicU16`. Used by both the DSL compiler and the runtime for string dedup.

**Exports:** `InternTable`

---

### `rust/src/tracing/mod.rs`
**Package:** `flowrulz_core::tracing`

Lock-free span tracing. `SpanRingBuffer` is a thread-local fixed-size ring buffer (1024 entries) with atomic `head`/`tail` counters.

`push(span)`: loads head/tail (Relaxed/Acquire), computes index `head % 1024`, writes span, stores `head+1` (Release). Drops if buffer full.

`drain(out)`: loops loading tail (Acquire) and head (Relaxed), reads span at `tail % 1024`, copies to output buffer, stores `tail+1` (Release). Returns bytes written.

`emit_span()`: the `thread_local!` convenience — called from `VM::dispatch()` after every opcode.

**Exports:** `Span`, `SpanRingBuffer`, `SPAN_BUFFER`, `emit_span()`

---

## Build & Config

### `Makefile`
**Location:** project root

Top-level build orchestration. Targets:
- `make all` — builds Rust release + Go binary
- `make test` — runs all Rust tests + Go tests
- `make bench` — runs Criterion benchmarks
- `make vet` — runs `go vet`
- `make clean` — `cargo clean` + removes binary

### `go.mod`
**Location:** project root
**Module:** `github.com/premchandkpc/FlowRulZ`

Go module definition. Standard dependencies.

### `Cargo.toml`
**Location:** `rust/`
**Crate:** `flowrulz_core`

Rust crate definition with dependencies: `bumpalo`, `serde`, `serde_json`, `crossbeam`, `boxcar`, `criterion` (dev).

---

## Doc Index

| File | Summary |
|------|---------|
| `docs/README.md` | Project overview, directory layout, features table, quick start |
| `docs/development.md` | Dev setup, package tree, adding opcodes, benchmarks |
| `docs/specs/flow-architecture.md` | Distributed Event Runtime — architecture, Event model, ExecutionContext, flows |
| `docs/specs/dsl-syntax.md` | DSL language specification |
| `docs/specs/bytecode-format.md` | ExecutionPlan, Instruction, opcodes, types |
| `docs/specs/vm-architecture.md` | VM dispatch, opcode handlers, ExecutionContext |
| `docs/specs/memory-management.md` | Slab pool, arena, interning, message lifecycle |
| `docs/specs/ffi-api.md` | C FFI surface for Go bridge |
| `docs/specs/kafka-semantics.md` | Consumer groups, offset commit, DLQ, internal topics |
| `docs/specs/cluster-model.md` | Single-leader cluster, membership, plan distribution, service registry |
| `docs/specs/flows.md` | Every data path: membership → deployment → execution → DLQ → metrics |
| `docs/specs/file-index.md` | This file |

## Summary

| Layer | Files | Lines |
|-------|-------|-------|
| Go source (prod) | 22 | ~2,550 |
| Go source (simulator) | 18 | ~2,200 |
| Rust source | 37 | ~6,400 |
| C source | 1 | 14 |
| Build/config | 3 | — |
| Docs | 12 | — |
