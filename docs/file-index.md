# File Index

Every source file in the project, grouped by package, with its purpose and key exports.

---

## Go Server (~50 source files + 19 simulator files)

### `server/cmd/flowrulz/main.go`
**Package:** `main`

Entry point — reads env vars (`NODE_ID`, `HTTP_ADDR`, `GRPC_ADDR`, `SEEDS`, `PERSIST_PATH`, `TOPIC`, `API_KEY`, `KAFKA_BROKERS`, `COMPILER_ADDR`, `PLUGIN_DIR`, `EXEC_STATE_DIR`, `KAFKA_GROUP_ID`, `KAFKA_ACKS`, `KAFKA_IDEMPOTENT`, `LIST_SCENARIOS`), builds config, creates `NodeBuilder` via `bootstrap.New()`, calls `WithDefaults().Build()` to produce `ProdNode`, then `Start()`.

**Exports:** `func main()`

---

### `server/bridge/bridge.go`
**Package:** `bridge`

CGo FFI bridge to the Rust shared library. Functions map 1:1 to `extern "C"` calls:
- `InitContext(body)` → `flowrulz_init_context` — create bincode-serialized `ExecutionContext` from body bytes
- `Compile(dsl, ruleID)` → `flowrulz_compile` — DSL string → bytecode plan
- `ExecuteStep(plan, ctxBytes, respBytes, extra)` → `flowrulz_execute_step` — cooperative single-step execution
- `PlanServices(plan)` → `flowrulz_plan_services` — extract service IDs from plan
- `GetSpans()` → `flowrulz_get_spans` — drain span ring buffer
- `SpanSize()` — returns Rust `Span` struct size
- `RegisterPlugin(name, wasm)` → `flowrulz_register_plugin`

Go-side service caller uses `sync.Map` (callerMap) + `atomic.Uint64` (nextExecID) — no mutex in hot path.

**Exports:** `StepResult`, `StepOut`, `ServiceEntry`, `InitContext()`, `Compile()`, `ExecuteStep()`, `PlanServices()`, `GetSpans()`, `SpanSize()`, `RegisterPlugin()`, `ParseServiceMethod()`, `ParseCompensation()`

---

### `server/bridge/bridge_test.go`
**Package:** `bridge`

Tests: `TestParseServiceMethod`, `TestParseServiceMethodNoMethod`, `TestParseCompensation`, `TestParseCompensationNoComp`, `TestServiceEntryRoundTrip`, `TestCompileAndPlanServices`, `TestStepResults`, `TestGetSpans`, `TestCgoEnabled`

---

### `sdk/flow/client.go`
**Package:** `flow` (SDK)

Public client SDK. Provides four communication models: `Publish` (async), `Request` (sync), `Execute` (rule), `Stream` (subscription). Also: `DeployRule`, `RemoveRule`, `ListRules`, `GetRule`, `ValidateRule`, `GetLanes`, `GetHealth`, `RegisterService`, `ListServices`.

**Exports:** `Client`, `RuleConfig`, `RuleStatus`, `ServiceInstance`, `SendResult`, `RequestResult`, `New()`, `WithAPIKey()`, `Publish()`, `Request()`, `Execute()`, `Stream()`, `DeployRule()`, `RemoveRule()`, `ListRules()`, `GetRule()`, `ValidateRule()`, `GetLanes()`, `GetHealth()`, `RegisterService()`, `ListServices()`

---

### `sdk/flow/client_test.go`
**Package:** `flow`

Tests: `TestPublish`, `TestRequest`, `TestExecute`, `TestDeployRule`, `TestRemoveRule`, `TestListRules`, `TestValidateRule`, `TestGetLanes`, `TestRegisterService`, `TestListServices`, `TestStream`, `TestHealth`, `TestWithAPIKey`

---

### `server/pkg/transport/eventbus.go`
**Package:** `transport` (public)

Canonical pub/sub abstraction. `EventBus` interface defines `Publish`, `Subscribe`, `Request`, `Reply`, `Broadcast`, `Unsubscribe`, `Close` — the single contract consumed by both production code and the simulator.

Also defines `Message`, `Handler`, `Subscription` types. Constants: `TypePublish`, `TypeRequest`, `TypeReply`, `TypeBroadcast`, `TypeStream`, `TypeStreamData`, `TypeStreamComplete`.

**Exports:** `EventBus`, `Message`, `Handler`, `Subscription`, all type constants

---

### `server/internal/admin/admin.go`
**Package:** `admin`

HTTP admin API server. Serves rule CRUD, validation, promote/rollback, lane listing, DLQ management, health check, metrics. API key auth via `Authorization: Bearer <key>` on all endpoints except `/health`.

**Exports:** `Server`, `New()`, `NewWithCompiler()`, `RegisterDLQ()`, `Handler()`
**Endpoints:** `POST /rules`, `DELETE /rules/{id}`, `GET /rules`, `GET /rules/{id}`, `GET /rules/{id}/versions`, `POST /rules/{id}/validate`, `POST /rules/{id}/promote`, `POST /rules/{id}/rollback`, `GET /lanes`, `GET /dlq`, `POST /dlq/replay/{id}`, `POST /dlq/replay`, `DELETE /dlq`, `GET /health`

---

### `server/internal/admin/admin_test.go`
**Package:** `admin_test` (external)

Tests: `TestPostAndGetRule`, `TestPostAndListRules`, `TestDeleteRule`, `TestGetVersions`, `TestPromote`, `TestAuth`, `TestAuthSkippedForHealth`

---

### `server/internal/admin/admin_lanes_test.go`
**Package:** `admin`

Test: `TestHandleGetLanes`

---

### `server/internal/engine/engine.go`
**Package:** `engine`

Core rule engine. Maintains `map[string]*Rule` of versioned plans. Each `Rule` holds `[]*VersionedPlan` with an `ActiveVersion` index. `Deploy()` compiles DSL via bridge, assigns lane by complexity score, persists to disk. `AddVersion()` stores a pre-compiled plan without auto-activating. `Promote()` activates a version.

Callback hooks: `AfterDeploy`, `AfterPromote` — set by ProdNode for plan distribution. Persistence: atomic write via `.tmp` + `os.Rename`.

**Exports:** `VersionedPlan`, `Rule`, `Engine`, `New()`, `NewWithCompiler()`, `Deploy()`, `AddVersion()`, `Promote()`, `Rollback()`, `Remove()`, `ActivePlanBytes()`, `ActivePlan()`, `LaneForScore()`, `GetRule()`, `Rules()`, `ExecuteAll()`

---

### `server/internal/engine/engine_test.go`
**Package:** `engine`

Tests: `TestDeployAndActive`, `TestAddVersionAndPromote`, `TestRollback`, `TestRemove`, `TestLaneForScore`, `TestPersistence`, `TestRulesSnapshot`, `TestExecuteAll`, `TestCompileError`, `TestAfterDeployHook`, `TestAfterPromoteHook`, `TestMultipleRules`, `TestActivePlanBytesEmpty`

---

### `server/internal/compiler/compiler.go`
**Package:** `compiler`

DSL compiler abstraction — local (CGo bridge) or remote (HTTP) compilation. `NewLocal()` returns nil (local is default via bridge). `NewRemote(endpoint)` creates HTTP client that POSTs to `{endpoint}/compile`.

**Exports:** `Client`, `Local`, `NewLocal()`, `NewRemote()`, `Compile()`

---

### `server/internal/compiler/compiler_test.go`
**Package:** `compiler`

Tests: `TestLocalCompile`, `TestRemoteCompileError`

---

### `server/internal/transport/transport.go`
**Package:** `transport`

Core transport interfaces. `MessageHandler` func type, `MessageConsumer`/`MessageProducer` interfaces, in-memory `Producer`/`Consumer` implementations with `Inject()` for testing. `KafkaConfig` struct for legacy Kafka transport.

**Exports:** `MessageHandler`, `MessageConsumer`, `MessageProducer`, `Producer`, `Consumer`, `KafkaConfig`, `KafkaAcksLevel`

---

### `server/internal/transport/kafka/` (3 files: config.go, consumer.go, producer.go)


Legacy Kafka transport (Sarama-backed). Only active when `FLOWRULZ_KAFKA_BROKERS` is explicitly set. Default is Cluster Bus.

**Exports:** `KafkaProducer`, `KafkaConsumer`, `NewKafkaProducer()`, `NewKafkaConsumer()`, `AcksLevelFromString()`

---

### `server/internal/transport/kafka_test.go` (legacy, test code moved into kafka/ as package-level tests)
**Package:** `transport`

Tests: `TestKafkaProducerSend`, `TestKafkaConsumerConsume`

---

### `server/internal/transport/grpc/bus.go`
**Package:** `grpctransport`

Low-level gRPC transport used by Cluster Bus. `GRPCBus` manages gRPC server with topic-based publish/subscribe. `GRPCClient` connects as subscriber. `BusMessage` carries Id, Topic, Body, PartitionKey, Headers.

**Exports:** `GRPCBus`, `GRPCClient`, `BusMessage`, `PublishRequest`, `PublishResponse`, `SubscribeRequest`, `NewGRPCBus()`, `NewGRPCClient()`, `Start()`, `Stop()`, `Publish()`, `PublishRaw()`, `Connect()`, `Close()`

---

### `server/internal/transport/grpc/bus_test.go`
**Package:** `grpctransport`

Tests: `TestGRPCPublishSubscribe`, `TestGRPCRequestReply`, `TestGRPCBroadcast`, `TestGRPCUnsubscribe`

### `server/internal/transport/grpc/bench_test.go`
**Package:** `grpctransport`

Benchmarks: `BenchmarkPublishThroughput` (~12K msg/s), `BenchmarkPublishLatency` (~44µs), `BenchmarkRequestReply` (~92µs)

---

### `server/internal/replyrouter/replyrouter.go`
**Package:** `replyrouter`

Per-node pending request tracker by correlation_id. `Register(corrID)` creates pending entry, returns receive channel. `Route(corrID, msg)` delivers to pending channel. Timeout cleanup goroutine. Max pending limit.

**Exports:** `ReplyRouter`, `New()`, `WithCleanupInterval()`, `WithMaxPending()`, `Register()`, `Route()`, `PendingCount()`, `StartCleanup()`, `StopCleanup()`

---

### `server/internal/replyrouter/replyrouter_test.go`
**Package:** `replyrouter`

Tests: `TestRegisterAndRoute`, `TestRouteNonExistent`, `TestCleanupTimeout`, `TestMaxPendingRejection`, `TestStartStopCleanup`, `TestDuplicateCleanup`

---

### `server/internal/registry/registry.go`
**Package:** `registry`

Service registry mapping service names → healthy endpoints. `RegisterInstance(inst)` for rich registration (methods, capabilities, zone, weight, tags, metadata). `LookupInstance(name, method)` for method-aware instance selection. Heartbeat expiry (default 30s) marks unhealthy. HTTP handlers for `POST /register`, `POST /heartbeat`, `GET /services`.

**Exports:** `ServiceInstance`, `Endpoint`, `ServiceRegistry`, `New()`, `Register()`, `RegisterInstance()`, `Heartbeat()`, `MarkUnhealthy()`, `LookupInstance()`, `LookupAll()`, `SetHeartbeatTimeout()`, `StartHeartbeatChecker()`, `RegisterHTTPHandler()`, `HeartbeatHTTPHandler()`, `ListServicesHTTPHandler()`

---

### `server/internal/registry/registry_test.go`
**Package:** `registry`

Tests: `TestRegisterAndLookup`, `TestHeartbeat`, `TestHeartbeatTimeout`, `TestMarkUnhealthy`, `TestLoadBalancerRandom`, `TestHTTPRegister`, `TestHTTPHeartbeat`

---

### `server/internal/registry/loadbalancer.go`
**Package:** `registry`

Load balancing strategies: `StrategyRandom`, `StrategyRoundRobin`, `StrategyLeastLoaded`, `StrategyLocalPrefer`. Thread-safe round-robin via `sync.Map` counters.

**Exports:** `Strategy`, `LoadBalancer`, `NewLoadBalancer()`, `Select()`, `SetStrategy()`

---

### `server/internal/registry/endpoint.go`
**Package:** `registry`

Endpoint URL construction from `ServiceInstance`. `URL()` builds `{protocol}://{address}:{port}`. `ParseEndpoint()` parses `host:port` or `protocol://host:port`.

**Exports:** `Endpoint.URL()`, `ParseEndpoint()`

---

### `server/internal/partition/partition.go`
**Package:** `partition`

Partition management — assignments, rebalancing, ownership tracking. Default 64 partitions. Round-robin assignment across alive nodes. FNV-32a key routing. `RebalanceNotifier` triggers on membership changes. HTTP endpoints: `GET /partitions`, `POST /partitions/rebalance`.

**Exports:** `Manager`, `RebalanceNotifier`, `AssignmentMessage`, `New()`, `SetProducer()`, `OnLeaderChange()`, `HandleAssignmentMessage()`, `Rebalance()`, `Assignments()`, `PartitionsForNode()`, `NumPartitions()`, `PublishAssignments()`, `NewRebalanceNotifier()`, `CheckAndRebalance()`

---

### `server/internal/partition/partition_test.go`
**Package:** `partition`

Tests: `TestRebalance`, `TestPartitionsForNode`, `TestLeaderChangeResets`, `TestHandleAssignment`, `TestRebalanceNotifier`

---

### `server/internal/scheduler/scheduler.go`
**Package:** `scheduler`

Lane-based priority scheduler: `Fast` (50 concurrent, 5k queue), `Normal` (20, 2k), `Heavy` (5, 500, reject-on-full). Each lane has a buffered channel as queue and semaphore for concurrency limiting.

**Exports:** `LaneConfig`, `Scheduler`, `Task`, `TaskResult`, `New()`, `Start()`, `Stop()`, `Enqueue()`, `LaneNames()`, `LaneConfigs()`, `SetLaneConfig()`

---

### `server/internal/execstate/execstate.go`
**Package:** `execstate`

Execution state types and `Store` interface for persisting in-flight executions. `State` holds `ID`, `RuleID`, `Version`, `PlanBytes`, `CtxBytes`, `Status`, `PendingSvc`, `PendingBody`, `Error`, `Output`, timestamps. `Status` enum: `Created`, `Running`, `WaitingForService`, `Completed`, `Failed`.

**Exports:** `Status`, `State`, `Store` (interface: `Create`, `Save`, `Load`, `List`, `Delete`, `Close`)

---

### `server/internal/execstate/filestore.go`
**Package:** `execstate`

File-based `Store` implementation. Atomic write-to-temp-then-rename per state file. Directory created on `NewFileStore()`.

**Exports:** `FileStore`, `NewFileStore()`

---

### `server/internal/execstate/execstate_test.go`
**Package:** `execstate`

Tests: `TestFileStoreCreateLoad`, `TestFileStoreList`, `TestFileStoreSaveDelete`, `TestFileStoreDuplicate`, `TestFileStoreAtomicity`

---

### `server/internal/membership/membership.go`
**Package:** `membership`

Cluster membership tracking with heartbeat-based leader election (lowest-ID wins). `AliveCount()`, `AliveNodes()`, `LeaderID()`. Lease expiry detection with `LeaderLease` (default 8s). `StartEviction()` goroutine evicts stale heartbeats. `OnLeaseExpiry()` callback.

**Exports:** `NodeInfo`, `LeaseCallback`, `Membership`, `New()`, `SetLeaderLease()`, `OnLeaseExpiry()`, `Add()`, `Remove()`, `MarkDead()`, `MarkAlive()`, `Heartbeat()`, `AliveCount()`, `AliveNodes()`, `LeaderID()`, `Snapshot()`, `Lookup()`, `LeaderLastSeen()`, `StartLeaderLeaseChecker()`, `StartEviction()`

---

### `server/internal/membership/membership_test.go`
**Package:** `membership`

Tests (13): `TestNew`, `TestAdd`, `TestRemove`, `TestMarkDead`, `TestAliveNodes`, `TestSnapshot`, `TestLookup`, `TestLeaderID`, `TestLeaderIDPicksLowestAlive`, `TestHeartbeatAutoAdds`, `TestEvictStaleWithLeaseCallback`, `TestStartEviction`, `TestStartLeaderLeaseCheckerExpires`

---

### `server/internal/cluster/node.go`
**Package:** `cluster`

gRPC-based peer-to-peer cluster overlay. `ClusterNode` manages Publish/Subscribe, peer membership (AddPeer/RemovePeer), and topic handlers. `Publish()` sends to local bus + all peers (goroutine per peer). Default cluster transport for ProdNode.

**Exports:** `Peer`, `ClusterNode`, `SubscribeHandler`, `NewClusterNode()`, `Start()`, `Stop()`, `Publish()`, `Subscribe()`, `Unsubscribe()`, `AddPeer()`, `RemovePeer()`

---

### `server/internal/cluster/gossip.go`
**Package:** `cluster`

Epidemic gossip protocol for membership propagation. Push (every 2s, fanout=2) + Pull anti-entropy (every 10s, 1 random peer). Conflict resolution: higher epoch wins. `GossipState` per node with `Term`/`Epoch`.

**Exports:** `GossipState`, `GossipMessage`, `Gossiper`, `NewGossiper()`, `SetState()`, `UpdateState()`, `GetState()`, `AllStates()`, `GetMyState()`, `Start()`, `Stop()`, `HandleGossipMessage()`

---

### `server/internal/cluster/transport.go`
**Package:** `cluster`

Transport adapters implementing `transport.MessageProducer`/`transport.MessageConsumer` for the Cluster Bus.

**Exports:** `ClusterProducer`, `ClusterConsumer`, `NewClusterProducer()`, `NewClusterConsumer()`

---

### `server/internal/plandist/plandist.go`
**Package:** `plandist`

Plan distribution across cluster. Leader publishes `PlanMessage{type:"plan"}` with compiled bytecode to `_flowrulz_plans`, waits for ACKs from quorum on `_flowrulz_acks`, then publishes `PlanMessage{type:"activate"}`. Term-based rejection prevents stale plans. `WaitForAcks()` blocks with timeout. `QuorumProvider` interface for membership counting.

**Exports:** `PlanMessage`, `AckMessage`, `PlanHandler`, `AckHandler`, `QuorumProvider`, `PlanDistributor`, `New()`, `Start()`, `Stop()`, `SetTerm()`, `CurrentTerm()`, `PublishPlan()`, `ActivatePlan()`, `SendAck()`, `WaitForAcks()`, `RecordAck()`, `PlanMessageFromBytes()`, `AckMessageFromBytes()`

---

### `server/internal/plandist/plandist_test.go`
**Package:** `plandist`

Tests: `TestPublishAndReceivePlan`, `TestSendAndReceiveAck`, `TestWaitForAcks`, `TestWaitForAcksTimeout`, `TestQuorumZeroWithMajority`, `TestQuorumNegativeAll`, `TestQuorumZeroSingleNode`, `TestSetTerm`, `TestHandleAckNoPending`, `TestHandleAckDuplicate`, `TestPublishPlanNoProducer`, `TestActivatePlan`

---

### `server/internal/observability/metrics.go`
**Package:** `observability`

In-memory metrics collector. `Counter` (atomic int64), `Gauge` (atomic int64), `Histogram` (sorted buckets + atomic counters). Per-name dedup via `sync.RWMutex`. Global shortcuts: `GetCounter()`, `GetGauge()`, `RecordExec()`, `RecordError()`.

**Exports:** `Counter`, `Gauge`, `Histogram`, `MetricsCollector`, `MetricSnapshot`, `NewMetricsCollector()`, `Snapshot()`, global shortcuts

---

### `server/internal/observability/metrics_test.go`
**Package:** `observability`

Tests: `TestCounter`, `TestCounterDedup`, `TestGauge`, `TestHistogram`, `TestSnapshot`, `TestGlobalShortcuts`

---

### `server/internal/observability/tracer.go`
**Package:** `observability`

OpenTelemetry span exporter. Reads raw span bytes from `bridge.GetSpans()`, parses `Span` struct (opcode, service_id, layer, duration_ns, status), creates OTLP spans. Ticker loop every 5s.

**Exports:** `SpanExporter`, `NewSpanExporter()`, `Start()`, `Stop()`

---

### `server/internal/observability/tracer_test.go`
**Package:** `observability`

Tests: `TestSpanExporterNilWhenEmptyEndpoint`, `TestSpanExporterStartStop`, `TestSpanSize`

---

### `server/internal/reliability/circuitbreaker.go`
**Package:** `reliability`

Three-state circuit breaker per service: `Closed` → `Open` (threshold=5 failures) → `HalfOpen` (recovery=30s). Lock-free state via atomics. Wired in ProdNode `callService()`.

**Exports:** `State`, `CircuitBreaker`, `NewCircuitBreaker()`, `Allow()`, `Success()`, `Failure()`

---

### `server/internal/reliability/circuitbreaker_test.go`
**Package:** `reliability`

Tests: `TestInitiallyClosed`, `TestTripsAfterThreshold`, `TestSuccessResets`, `TestHalfOpenRecovery`, `TestHalfOpenLimitsRequests`, `TestSuccessClosesFromHalfOpen`

---

### `server/internal/reliability/dlq.go`
**Package:** `reliability`

Dead-letter queue. Bounded in-memory cache (default 10k, FIFO evict). Optional Kafka producer via `WithDLQProducer()`. Per-entry replay, bulk `ReplayAll()`, JSON export, file persistence via `WithDLQDir()`. No-fail design: `Send()` always succeeds.

**Exports:** `DeadLetterEntry`, `DLQ`, `DLQOption`, `NewDLQ()`, `SetReplayFn()`, `Send()`, `Replay()`, `ReplayAll()`, `List()`, `Len()`, `Clear()`, `ToJSON()`

---

### `server/internal/reliability/dlq_test.go`
**Package:** `reliability`

Tests: `TestDLQSendAndList`, `TestDLQMaxSize`, `TestDLQReplay`, `TestDLQReplayAll`, `TestDLQClear`, `TestDLQToJSON`

---

### `server/internal/reliability/dedup.go`
**Package:** `reliability`

Bounded in-memory dedup tracker. `Mark(id)`, `Seen(id)`. Default 10k entries, 5min TTL. Evicts oldest at capacity. Background cleanup goroutine. Wired in ProdNode handler by MessageID.

**Exports:** `DedupEntry`, `DedupTracker`, `NewDedupTracker()`, `Seen()`, `Mark()`, `StartCleanup()`, `Len()`, `Clear()`

---

### `server/internal/reliability/dedup_test.go`
**Package:** `reliability`

Tests: `TestDedupSeenUnseen`, `TestDedupMarkAndSeen`, `TestDedupMaxSize`, `TestDedupClear`, `TestDedupCleanupExpired`, `TestDedupDefaults`, `TestDedupEvictsOldest`

---

### `server/internal/reliability/ratelimit.go`
**Package:** `reliability`

Token bucket rate limiter per name. Configurable rate/burst. Double-checked locking for bucket creation. Default: rate=100, burst=100.

**Exports:** `TokenBucket`, `RateLimiter`, `NewRateLimiter()`, `NewTokenBucket()`, `Bucket()`, `SetBucket()`, `Allow()`, `AllowN()`

---

### `server/internal/reliability/ratelimit_test.go`
**Package:** `reliability`

Tests: `TestTokenBucketBasic`, `TestTokenBucketRefill`, `TestAllowN`, `TestRateLimiter`, `TestRateLimiterDefaultBucket`, `TestRateLimiterIsolation`

---

### `server/internal/reliability/saga.go`
**Package:** `reliability`

Saga pattern tracker for compensating transactions. `RegisterStep(execID, step)` appends step with compensator info. `Compensate(execID)` calls compensators in reverse order. Optional disk persistence.

**Exports:** `SagaStep`, `CompensatorFunc`, `SagaTracker`, `NewSagaTracker()`, `NewSagaTrackerWithDir()`, `RegisterStep()`, `Compensate()`, `StepsFor()`, `Clear()`

---

### `server/internal/reliability/saga_test.go`
**Package:** `reliability`

Tests: `TestSagaTrackerRegisterCompensate`, `TestSagaTrackerNoCompensator`, `TestSagaTrackerCompensateError`, `TestSagaTrackerClear`

---

### `server/internal/flowengine/flow.go`
**Package:** `flowengine`

Workflow orchestration with file-based checkpointing. `FlowState`: `Pending`, `Running`, `Completed`, `Failed`. `Orchestrator` manages flows by ID, persists checkpoints as `<id>.json`. Atomic write via `.tmp` + rename.

**Exports:** `FlowState`, `Flow`, `Orchestrator`, `NewOrchestrator()`, `NewOrchestratorWithCheckpointDir()`, `Start()`, `Get()`, `StoreResponse()`, `Complete()`, `Fail()`, `Remove()`, `List()`

---

### `server/internal/flowengine/flow_test.go`
**Package:** `flowengine`

Tests: `TestStartFlow`, `TestGetFlow`, `TestStoreResponse`, `TestStoreResponseNonexistentFlow`

---

### `server/internal/plugins/loader.go`
**Package:** `plugins`

WASM plugin loader. `LoadDir(dir)` scans for `.wasm` files, registers each via `bridge.RegisterPlugin()`. Filename without extension becomes plugin name.

**Exports:** `LoadDir()`

---

### `server/internal/node/prod.go`
**Package:** `node`

Central production node wiring all modules via DI. Fields: Engine, PlanDistributor, Scheduler, ReplyRouter, DLQ, RateLimiter, CircuitBreakers, Dedup, MetricsCollector, Admin, Registry, Membership, SagaTracker, Store, ClusterNode, Partitions, Rebalancer, GRPCBus, SpanExporter. `Start()` initializes all components. `Shutdown()` tears down gracefully. `executePlan()` runs cooperative step loop via bridge. `callService()` uses CB + registry + HTTP. Leader-only: `distributePlan()`/`distributeActivate()`.

**Exports:** `ProdNode`, `NewNode()`, `Start()`, `Shutdown()`, `SetLeader()`, `IsLeader()`, `SetTerm()`, `CurrentTerm()`

---

### `server/internal/node/prod_test.go`
**Package:** `node`

Tests: `TestProdNodeStartShutdown`, `TestSetLeader`

---

### `server/internal/bootstrap/builder.go`
**Package:** `bootstrap`

DI composition root. `NodeBuilder` constructs ProdNode with all dependencies. `WithDefaults()` configures default implementations. `DefaultDependencies()` factory for production wiring. Supports optional override methods for testing.

**Exports:** `NodeBuilder`, `New()`, `WithDefaults()`, `Build()`, `DefaultDependencies()`

---

### `server/internal/adapters/adapter.go`
**Package:** `adapters`

Bridge layer between `pkg/` interfaces and `internal/` implementations. Wraps concrete types (e.g., `scheduler.Scheduler` → `pkg/scheduler.Scheduler` interface) for decoupled DI.

**Exports:** various adapter types for engine, scheduler, transport, cluster, registry, store

---

### `server/internal/ports/ports.go`
**Package:** `ports`

Secondary port interfaces for external system interactions (HTTP caller, filesystem, time). Enables testability without real I/O.

**Exports:** `HTTPCaller`, `FileSystem`, `Clock`

---

### `server/pkg/cluster/` — cluster interfaces and types
### `server/pkg/engine/` — engine interfaces and types
### `server/pkg/membership/` — membership interfaces and types
### `server/pkg/node/` — node interfaces and types
### `server/pkg/partition/` — partition interfaces and types
### `server/pkg/plandist/` — plan distribution interfaces and types
### `server/pkg/registry/` — registry interfaces and types
### `server/pkg/reliability/` — reliability interfaces (CB, DLQ, RL, dedup, saga)
### `server/pkg/replyrouter/` — reply router interface
### `server/pkg/scheduler/` — scheduler + lane interfaces
### `server/pkg/store/` — execution state store interface
### `server/pkg/vm/` — VM compilation + execution interfaces

---

## SDK (5 languages)

### `sdk/java/` — Java SDK (Maven, com.flowrulz)
Publish, Request, Execute, Stream API. Maven artifact with auto-retry.

### `sdk/python/` — Python SDK (pip, flowrulz)
Async-first SDK. Publish/Request/Execute/Stream with asyncio.

### `sdk/javascript/` — JS/TS SDK (npm, flowrulz)
TypeScript SDK. Publish/Request/Execute/Stream with typed events.

### `sdk/rust/` — Rust SDK (cargo, flowrulz-sdk)
Native Rust SDK. Publish/Request/Execute/Stream via async-std.

---

## Simulator (19 files)

### `simulator/simulator.go`
**Package:** `simulator`

Top-level orchestrator. Creates ServiceRegistry, Timeline, Metrics, Network, Scheduler Nodes, Dispatcher, LoadGen, Dashboard. Compiles plans via `bridge.Compile`. `Run()` starts all components, runs for duration, prints results.

**Exports:** `Simulator`, `New()`, `Run()`, `Stop()`, `Client()`

---

### `simulator/simulator_test.go`
**Package:** `simulator`

Tests: `TestOrderFlowExecution`, `TestSuspensionResume`, `TestServiceFailure`, `TestMultiNodeDispatch`, `TestFullSimulatorRun`, `TestPaymentOutageAllFail`

---

### `simulator/client.go`
**Package:** `simulator`

Programmatic client. `Send(ruleID, body)` dispatches via EventBus or direct dispatch, returns output + duration. `RegisterService(svc)`, `AddRule(id, dsl)`, `Plans()`, `Services()`. Also implements `ScenarioClient` interface for scenario setup: `SetLoadGenPlan(plan)`, `SetLoadGenBodyFunc(fn)`.

**Exports:** `Client`, `SendResult`, `ServiceInfo`, `Send()`, `RegisterService()`, `AddRule()`, `Plans()`, `Plan()`, `RemoveRule()`, `Services()`, `SetLoadGenPlan()`, `SetLoadGenBodyFunc()`

---

### `simulator/client_test.go`
**Package:** `simulator`

Tests: `TestClientSendBridgeRule`, `TestClientSendRuleNotFound`, `TestClientAddRule`, `TestClientRegisterService`

---

### `simulator/modes.go`
**Package:** `simulator`

8 simulator modes with configuration: simple (4 services), enterprise (40+ services), chaos (high failure), performance (10K TPS), distributed (3 clusters), multi-region (US/Europe/Asia), interview (animated), learning (step-by-step). Each mode defines services, nodes, regions, TPS, workers, timeout, retry, failure rate, animation.

**Exports:** `Mode`, `ModeConfig`, `Modes()`, `GetMode()`

---

### `simulator/admin.go`
**Package:** `simulator`

Admin HTTP handlers for interactive mode. Registered on dashboard mux. Endpoints: `/api/admin/send`, `/api/admin/rules`, `/api/admin/services`, `/api/admin/lanes`, `/api/admin/validate`, `/api/admin/health`, `/api/admin/partitions`.

**Exports:** `RegisterAdminHandlers()`

---

### `simulator/cmd/simulator/main.go`
**Package:** `main`

CLI entry point. Flags: `--nodes`, `--workers`, `--scenario`, `--rate`, `--duration`, `--speed`, `--dashboard`, `--dashboard-addr`, `--drop`, `--slow`, `--scenarios`, `--verbose`, `--interactive`.

---

### `simulator/config/config.go`
**Package:** `config`

`SimConfig` holds Nodes, Workers, Scenario, Duration, Rate, Speed, Dashboard, DashboardAddr, Chaos config, Plans. `ChaosConfig`: drop probability, slow factor, duplicate percentage.

**Exports:** `SimConfig`, `ChaosConfig`

---

### `simulator/dashboard/dashboard.go`
**Package:** `dashboard`

HTTP dashboard server with embedded HTML/CSS/JS SPA. Real-time metrics, service DAG graph, latency cards, node queues, execution table, event timeline, rule creation form, send request form. API endpoints: `/api/metrics`, `/api/nodes`, `/api/events`, `/api/executions/`, `/api/executions/{id}`, `/api/stats`. Refresh interval 1s.

**Exports:** `Dashboard`, `New()`, `Start()`, `Stop()`, `AddHandler()`

---

### `simulator/dispatcher/dispatcher.go`
**Package:** `dispatcher`

Hash-based message dispatcher. Maps exec ID to node via FNV-32a. `Dispatch(ctx)` records EventCreated, enqueues on target node's Scheduler.

**Exports:** `Dispatcher`, `New()`, `Dispatch()`, `StartAll()`, `StopAll()`

---

### `simulator/eventbus/eventbus.go`
**Package:** `eventbus`

In-memory EventBus with Go channels. Publish, Subscribe, Request/Reply, Broadcast, Delay, Drop, Duplicate semantics. Fan-out to all handlers per topic.

**Exports:** `EventBus`, `New()`, `Publish()`, `PublishToPartition()`, `Subscribe()`, `Request()`, `Reply()`, `Broadcast()`, `Unsubscribe()`, `Close()`, `TopicStats()`

---

### `simulator/eventbus/eventbus_test.go`
**Package:** `eventbus`

Tests (12): `TestPublishSubscribe`, `TestPublishMultipleSubscribers`, `TestPublishToNoSubscribers`, `TestRequestReply`, `TestRequestTimeout`, `TestUnsubscribe`, `TestDelayedMessage`, `TestCloseRejectsPublish`, `TestMultipleTopics`, `TestMessageIDAutoAssign`, `TestTopicStats`

---

### `simulator/execution/plan.go`
**Package:** `execution`

Plan and Instruction types for simulator's instruction-based execution path. `OpCode`: `Nop`, `CallService`, `Validate`, `Branch`, `Publish`, `Return`. Also holds `PlanBytes` (compiled bytecode) and `ServiceNames`. 25+ pre-built plans across domains: customer, catalog, order, shipping, notification, analytics, AI, utility, complex workflows.

**Exports:** `OpCode`, `Instruction`, `Plan`, `NewPlan()`, `OrderFlow`, `PaymentFlow`, `RefundFlow`, `ShippingFlow`, `ServiceDiscoveryFlow`, `DeadLetterQueueFlow`, `CustomerRegistrationFlow`, `CustomerLoginFlow`, `SupportTicketFlow`, `ProductSearchFlow`, `RecommendationFlow`, `PriceCalculationFlow`, `OrderCancellationFlow`, `RefundProcessingFlow`, `SubscriptionRenewalFlow`, `ShippingScheduleFlow`, `WarehouseFulfillmentFlow`, `NotificationDispatchFlow`, `AnalyticsAggregationFlow`, `FraudDetectionFlow`, `DocumentProcessingFlow`, `ImageProcessingFlow`, `TranslationFlow`, `CurrencyConversionFlow`, `GeoLookupFlow`, `CompleteOrderWorkflow`, `EcommerceCheckoutFlow`

---

### `simulator/execution/context.go`
**Package:** `execution`

`ExecutionContext` flowing through simulator. Holds Plan, IP, State (Created/Ready/Running/Waiting/Completed/Failed), Variables, IncomingBody, Output, WaitingService, ResultCh.

**Exports:** `State`, `Result`, `ExecutionContext`, `StateChange`, `NewContext()`, `Transition()`, `MarkDone()`, `MarkFailed()`, `AddEvent()`

---

### `simulator/execution/queue.go`
**Package:** `execution`

`ReadyQueue` (FIFO with mutex + channel signal) and `WaitingQueue` (map correlationID → ExecutionContext).

**Exports:** `ReadyQueue`, `WaitingQueue`, `NewReadyQueue()`, `NewWaitingQueue()`

---

### `simulator/loadgen/loadgen.go`
**Package:** `loadgen`

Traffic generator. Ticker-based pacing at configured rate. Supports patterns: random, sequential, weighted. Can override plan selection via `SetPlanFunc(fn)` and request body via `BodyFunc` in `Config` / `SetBodyFunc(fn)`.

**Exports:** `Config`, `Generator`, `DefaultConfig()`, `New()`, `Start()`, `Stop()`, `SetPlanFunc()`, `SetBodyFunc()`

---

### `simulator/metrics/metrics.go`
**Package:** `metrics`

Metrics collector. Tracks completed/failed/dropped counts, throughput, latency percentiles (P50/P95/P99), per-service stats.

**Exports:** `Collector`, `Snapshot`, `NodeStats`, `ServiceStats`, `NewCollector()`, `RecordCompleted()`, `RecordFailed()`, `RecordDropped()`, `RecordServiceCall()`, `Snapshot()`

---

### `simulator/network/network.go`
**Package:** `network`

Simulated network layer. `CallService()` applies configurable latency (min/max jitter), chaos (drop, slow, duplicate).

**Exports:** `Config`, `ChaosConfig`, `Network`, `New()`, `SetChaos()`, `CallService()`

---

### `simulator/scheduler/scheduler.go`
**Package:** `scheduler`

Per-node execution scheduler. Worker pool pulling from ReadyQueue. Two paths: instruction-based (loop over Plan.Instructions) and bridge-based (cooperative `ExecuteStep` loop, initializes VM context via `bridge.InitContext` from `ctx.IncomingBody`). `PlanCache` maps rule IDs to Plans.

**Exports:** `Result`, `Scheduler`, `PlanCache`, `New()`, `NewPlanCache()`, `Start()`, `Stop()`, `Enqueue()`, `Snapshot()`

---

### `simulator/scenarios/scenarios.go`
**Package:** `scenarios`

Executable scenarios with `Apply`/`Setup` functions: black-friday, payment-outage, spike-test, chaos-monkey, ramp-up, order-routing, order-processing, metadata-updates, circuit-breaker. `ScenarioClient` interface provides `AddRule`, `RegisterService`, `Plan`, `SetLoadGenPlan`, `SetLoadGenBodyFunc` for scenario setup.

**Exports:** `Scenario`, `ScenarioClient`, `All`, `ByName()`, `DefaultPlans()`

---

### `simulator/scenarios/registry.go`
**Package:** `scenarios`

50+ scenario definitions across 6 categories (business, reliability, distributed, metadata, performance, chaos). Each `ScenarioDef` has Name, Category, Description, Mode, Steps, Duration. Functions: `ScenariosByCategory()`, `ScenariosByMode()`, `GetScenarioDef()`, `CategoryCount()`.

**Exports:** `ScenarioDef`, `Step`, `AllDefs`, `ScenariosByCategory()`, `ScenariosByMode()`, `GetScenarioDef()`, `CategoryCount()`

---

### `simulator/scenarios/order_processing.go`
**Package:** `scenarios`

`OrderProcessing` scenario — full order-to-dispatch workflow with retries, timeouts, and parallel execution. Configures payment (40ms, 2% failure), inventory (8ms, 2% failure), shipping (15ms, 1% failure), notification (3ms, 0.5% failure).

**Exports:** `OrderProcessing`

---

### `simulator/scenarios/metadata_updates.go`
**Package:** `scenarios`

`MetadataUpdates` scenario — live metadata updates and rule deployment without restart. Demonstrates dynamic configuration.

**Exports:** `MetadataUpdates`

---

### `simulator/scenarios/circuit_breaker_demo.go`
**Package:** `scenarios`

`CircuitBreakerDemo` scenario — circuit breaker behavior with fallback execution. Payment service at 95% failure triggers circuit, falling back to notification.

**Exports:** `CircuitBreakerDemo`

---

### `simulator/services/service.go`
**Package:** `services`

40+ mock services across 10 domains with configurable latency, jitter, failure rate, max concurrent. Functions: `DefaultServices()` (enterprise), `SimpleServices()` (4 core), `ChaosServices()` (high failure), `PerformanceServices()` (optimized). Domains: customer, catalog, order, shipping, notification, analytics, AI, utility, platform, infrastructure.

**Exports:** `MockService`, `MethodInfo`, `CallResult`, `ServiceRegistry`, `NewRegistry()`, `Register()`, `Get()`, `All()`, `Names()`, `ByDomain()`, `Domains()`, `DefaultServices()`, `SimpleServices()`, `EnterpriseServices()`, `ChaosServices()`, `PerformanceServices()`

---

### `simulator/timeline/timeline.go`
**Package:** `timeline`

Event timeline store. Records all execution events with timestamps. `Recent(n)`, `ForExec(id)`, `Stats()`.

**Exports:** `Event`, `Store`, `NewStore()`, `Record()`, `Recent()`, `ForExec()`, `Stats()`

---

## Rust (26 source files)

### `runtime/src/lib.rs`
**Package:** `flowrulz_core`

Crate root. Declares modules: `bytecode`, `dsl`, `error`, `executor`, `ffi`, `memory`, `tracing`. Re-exports `ExecutionPlan` and `VM`.

**Exports:** `ExecutionPlan`, `VM`

---

### `runtime/src/error.rs`
**Package:** `flowrulz_core::error`

`FfiError` enum: `NullPointer=-1`, `InvalidUtf8=-2`, `Lex=-3`, `Parse=-4`, `Compile=-5`, `Serialize=-6`, `BufferTooSmall=-7`, `Deserialize=-8`, `Exec=-9`. Implements `Display`.

**Exports:** `FfiError`, `FfiError::code()`

---

### `runtime/src/ffi.rs`
**Package:** `flowrulz_core::ffi`

All `#[no_mangle] pub unsafe extern "C"` functions:
- `flowrulz_init_context(body)` — create bincode-serialized `ExecutionContext` from body bytes
- `flowrulz_compile(dsl, rule_id)` — DSL string → bincode-serialized `ExecutionPlan`
- `flowrulz_execute(ctx_id, plan, body, caller_cb, out, err, msg_id, corr_id, trace_id, partition, offset)` — synchronous, callback-based
- `flowrulz_execute_step(ctx_id, plan, ctx_bytes, resp, caller_cb, out, err, pending_svc, pending_body, ctx_out)` — cooperative step-based
- `flowrulz_msg_alloc(size)` / `flowrulz_msg_release(ptr)` — `std::alloc` directly
- `flowrulz_intern(s)` / `flowrulz_intern_lookup(id)` — string interning
- `flowrulz_get_spans(out, cap)` — drain thread-local span ring buffer
- `flowrulz_register_plugin(name, wasm_bytes)` — WASM plugin registration
- `flowrulz_plan_services(plan)` — extract service ID→name map as JSON
- `flowrulz_plan_complexity(plan)` — returns `complexity_score`

**Global statics:** `INTERN_TABLE: Lazy<InternTable>` (prefilled with 7 standard headers)

---

### `runtime/src/bytecode/mod.rs`
**Package:** `flowrulz_core::bytecode`

Re-exports all sub-modules: `consts`, `dag_table`, `event`, `execution`, `instruction`, `opcode`, `plan`, `resolved_type`, `services`.

---

### `runtime/src/bytecode/opcode.rs`
**Package:** `flowrulz_core::bytecode::opcode`

`OpCode` enum (25 variants: 0=Next..24=Delay). `GateOp` (Eq/Ne/Gt/Lt/Gte/Lte/Contains). `ChunkMode` (Sequential/Parallel). `RetryStrategy` (Exponential/Linear/Fixed).

**Exports:** `OpCode`, `GateOp`, `ChunkMode`, `RetryStrategy`

---

### `runtime/src/bytecode/instruction.rs`
**Package:** `flowrulz_core::bytecode::instruction`

8-byte `Instruction` struct: `{op: OpCode, flags: u8, a: u16, b: u16, c: u16}`. Factory methods for every opcode: `next()`, `parallel()`, `gate()`, `dag()`, `emit()`, etc. Accessors: `delay_ms()`, `has_retry()`, `gate_op()`, `timeout_ms()`.

**Exports:** `Instruction`

---

### `runtime/src/bytecode/event.rs`
**Package:** `flowrulz_core::bytecode::event`

`Event` with `id`, `topic`, `payload`, `headers`, `metadata`. `EventMetadata` with `mode`, `reply_to`, `correlation_id`, `trace_id`, `content_type`, `schema_name`, `schema_version`, `partition`, `offset`. `Mode` enum: `Publish=0`, `Request=1`, `Reply=2`, `Stream=3`, `Workflow=4`, `Internal=5`.

**Exports:** `Event`, `EventMetadata`, `Mode`

---

### `runtime/src/bytecode/execution.rs`
**Package:** `flowrulz_core::bytecode::execution`

`ExecutionContext`: `event`, `body`, `variables`, `outputs`, `headers`, `failed`, `errors`, `hop_count`, `retry_count`, `deadline_ms`. Services enrich context via `set_service_output()`.

**Exports:** `ExecutionContext`

---

### `runtime/src/bytecode/plan.rs`
**Package:** `flowrulz_core::bytecode::plan`

`ExecutionPlan`: `rule_id`, `version`, `instr_count`, `complexity_score`, `instructions`, `const_pool`, `services`, `dag_tables`, `retry_configs`, `chunk_configs`, `schema`. `RetryConfig` (max_attempts, strategy, fixed_ms). `ChunkConfig` (count, mode).

**Exports:** `ExecutionPlan`, `RetryConfig`, `ChunkConfig`

---

### `runtime/src/bytecode/services.rs`
**Package:** `flowrulz_core::bytecode::services`

`ServiceTable`: `entries: Vec<ServiceEntry>`, `index: HashMap<String, u16>`. `ServiceEntry`: `id`, `name`.

**Exports:** `ServiceTable`, `ServiceEntry`

---

### `runtime/src/bytecode/consts.rs`
**Package:** `flowrulz_core::bytecode::consts`

`ConstantPool`: `entries: Vec<String>`, `index: HashMap<String, u16>`. Methods: `add()`, `get()`, `len()`, `entries()`.

**Exports:** `ConstantPool`

---

### `runtime/src/bytecode/resolved_type.rs`
**Package:** `flowrulz_core::bytecode::resolved_type`

`ResolvedType` enum: `String`, `Integer`, `Float`, `Boolean`, `Object`, `Array`, `Null`, `Any`, `Enum(Vec<String>)`. `FieldSchema`: `name`, `type`, `required`. `Schema`: `fields: Vec<FieldSchema>`. Methods: `field_type()`, `is_valid()`, `check()`, `supports_ordering()`, `supports_contains()`, `is_numeric()`.

**Exports:** `ResolvedType`, `FieldSchema`, `Schema`

---

### `runtime/src/bytecode/dag_table.rs`
**Package:** `flowrulz_core::bytecode::dag_table`

`DAGNode`: `service_id`, `layer`, `parent_ids`. `DAGTable`: `nodes`, `layers`, `terminal_nodes`, `failure_policy`, `node_timeouts`, `merge_strategy`, `distributed`. `DAGFailurePolicy`: `AbortAll`, `ContinueOthers`, `SkipDependents`. `MergeStrategy`: `LastWins`, `ArrayConcat`, `DeepMerge`, `ExplicitMap`.

**Exports:** `DAGNode`, `DAGTable`, `DAGFailurePolicy`, `MergeStrategy`

---

### `runtime/src/dsl/mod.rs`
**Package:** `flowrulz_core::dsl`

Re-exports sub-modules: `compiler`, `lexer`, `optimizer`, `parser`.

---

### `runtime/src/dsl/lexer.rs`
**Package:** `flowrulz_core::dsl::lexer`

`Token` enum (22 variants), `LexError` (17 variants). `pub fn lex(input) -> Result<Vec<Token>, LexError>`.

**Exports:** `Token`, `LexError`, `lex()`

---

### `runtime/src/dsl/parser.rs`
**Package:** `flowrulz_core::dsl::parser`

`ASTNode` enum (same variants as Token), `Pipeline` (nodes: Vec<ASTNode>), `ParseError` (18 variants). `pub fn parse(tokens) -> Result<Pipeline, ParseError>`.

**Exports:** `ASTNode`, `Pipeline`, `ParseError`, `parse()`

---

### `runtime/src/dsl/optimizer.rs`
**Package:** `flowrulz_core::dsl::optimizer`

`Optimizer` (unit struct). `OptimizedPipeline`. Passes: simplify gates, hoist timeouts, merge emits, remove dead code, merge retries, remove unused labels, eliminate redundant jumps, remove nops.

**Exports:** `Optimizer`, `OptimizedPipeline`, `Optimizer::new()`, `optimize()`

---

### `runtime/src/dsl/compiler.rs`
**Package:** `flowrulz_core::dsl::compiler`

`Compiler` (unit struct). `CompileError` (10 variants). `compile()` converts `OptimizedPipeline` → `ExecutionPlan`. `new()` is no-arg (was `new(&[])`). Internal: `type_check_gate()`, `type_check_map()`, `compile_dag()` (cycle detection, topological sort), `compile_schema()` (parses `{name:string,!age:int}`, `enum[val1|val2|...]`). Free function: `calc_complexity()`.

**Exports:** `Compiler`, `CompileError`, `Compiler::compile()`, `calc_complexity()`

---

### `runtime/src/executor/mod.rs`
**Package:** `flowrulz_core::executor`

`VM` struct: `plan`, `arena`, `caller`, `ctx`. `StepResult` enum: `Done`, `Continue`, `Pending{svc_id,body,timeout_ms}`, `Delay(u64)`. `VM::run()` — dispatch loop. `VM::step(response)` — cooperative single-step. Modules: `chunk`, `dag`, `emit`, `expr`, `gate`, `helpers`, `map`, `next`, `plugin`, `parallel`, `runtime`.

**Exports:** `VM`, `StepResult`, `VM::new()`, `VM::run()`, `VM::step()`

---

### `runtime/src/executor/next.rs`
**Package:** `flowrulz_core::executor::next`

`exec_next()` — service call with optional retry (exponential/linear/fixed backoff). `exec_with_retry()`, `find_retry_config()`.

**Exports:** `exec_next()`

---

### `runtime/src/executor/parallel.rs`
**Package:** `flowrulz_core::executor::parallel`

`exec_parallel()` — fan-out to multiple services in parallel, store results under `_parallel` key. `exec_collect()` — extract `_parallel` key.

**Exports:** `exec_parallel()`, `exec_collect()`

---

### `runtime/src/executor/gate.rs`
**Package:** `flowrulz_core::executor::gate`

`exec_jmp_if_false()` — field extraction + comparison using GateOp; skip instructions if false.

**Exports:** `exec_jmp_if_false()`

---

### `runtime/src/executor/map.rs`
**Package:** `flowrulz_core::executor::map`

`exec_map()` — field transformation. Supports `w:` prefix for WASM plugin dispatch, expression evaluation, field extraction by dot-path.

**Exports:** `exec_map()`

---

### `runtime/src/executor/emit.rs`
**Package:** `flowrulz_core::executor::emit`

`exec_emit()` — fire-and-forget calls to multiple services, discarding results.

**Exports:** `exec_emit()`

---

### `runtime/src/executor/expr.rs`
**Package:** `flowrulz_core::executor::expr`

Expression evaluator for map transformations. `eval_map_expression()` parses and evaluates expressions. **31 builtins**: `to_string`, `parse_int`, `parse_float`, `parse_bool`, `coalesce`, `default`, `contains`, `keys`, `merge`, `epoch`, `hash`, `uuid`, `now`, `lower`, `upper`, `trim`, `length`, `concat`, `base64`, `base64_decode`, `json`, `substring`, `replace`, `split`, `abs`, `round`, `ceil`, `floor`, `min`, `max`, `typeof`. Quote-aware `parse_args()`.

**Exports:** `eval_map_expression()`

---

### `runtime/src/executor/helpers.rs`
**Package:** `flowrulz_core::executor::helpers`

`extract_json_field()` — dot-path field extraction. `compare_values()` — type-coercing comparison for gates.

**Exports:** `extract_json_field()`, `compare_values()`

---

### `runtime/src/executor/dag.rs`
**Package:** `flowrulz_core::executor::dag`

`exec_dag()` — layer-by-layer DAG execution. Parent merging via `deep_merge()`. Failure policies: `AbortAll`, `ContinueOthers`, `SkipDependents`. Merge strategies: `LastWins`, `ArrayConcat`, `DeepMerge`, `ExplicitMap`.

**Exports:** `exec_dag()`, `deep_merge()`

---

### `runtime/src/executor/chunk.rs`
**Package:** `flowrulz_core::executor::chunk`

`split_chunks()` — split body into N chunks by byte length.

**Exports:** `split_chunks()`

---

### `runtime/src/executor/plugin.rs`
**Package:** `flowrulz_core::executor::plugin`

WASM plugin runtime via wasmtime. Global registries: `PLUGIN_BYTES` (name → raw bytes), `MODULE_CACHE` (name → compiled Engine+Module). Calling convention: `process(ptr: i32, len: i32) → i64` packed `(output_ptr << 32) | output_len`. 100k fuel limit.

**Exports:** `register()`, `call()`, `call_plugin()`

---

### `runtime/src/executor/runtime.rs`
**Package:** `flowrulz_core::executor::runtime`

`ExecutionRuntime` — high-level orchestration wrapping VM. Handles Buffer (accumulate) and Chunk (split+execute) at runtime level. Methods: `execute()`, `buffer_push()`, `buffer_flush()`, `buffer_remaining()`.

**Exports:** `ExecutionRuntime`

---

### `runtime/src/memory/mod.rs`
**Package:** `flowrulz_core::memory`

Re-exports: `arena`, `intern`.

---

### `runtime/src/memory/arena.rs`
**Package:** `flowrulz_core::memory::arena`

`Arena` — bump allocator wrapping `bumpalo::Bump`. Methods: `alloc()`, `alloc_copy()`, `reset()`, `allocated_bytes()`.

**Exports:** `Arena`

---

### `runtime/src/memory/intern.rs`
**Package:** `flowrulz_core::memory::intern`

`InternTable` — concurrent string interning. Forward map: `RwLock<HashMap<String, u16>>`. Reverse: `boxcar::Vec` (lock-free). AtomicU16 ID generation. Methods: `prefill()`, `intern()`, `lookup()`, `len()`.

**Exports:** `InternTable`

---

### `runtime/src/tracing/mod.rs`
**Package:** `flowrulz_core::tracing`

Lock-free thread-local span tracing. `Span` (repr(C)): `opcode: u8`, `service_id: u16`, `layer: u8`, `duration_ns: u64`, `status: u8`. `SpanRingBuffer`: 1024-entry ring buffer with atomic head/tail. `emit_span()` pushes to thread-local buffer. `drain()` copies to output.

**Exports:** `Span`, `SpanRingBuffer`, `SPAN_BUFFER`, `emit_span()`, `SPAN_BUFFER_CAPACITY`

---

### `docs/software-review.md`
Multi-layer codebase review: architecture, component analysis, bug findings, DSL/compiler/VM assessment, security, observability, testing, distributed systems, scalability. Scorecard with 12 areas, critical findings (UB bug, panic boundary, god object), and prioritized recommendations.

---

## Build & Config

### `Makefile`

Top-level build orchestration:
- `make all` — `cargo build --release` + `go build`
- `make test` — all Rust tests (`cargo test`) + all Go tests + `go vet`
- `make bench` — Criterion benchmarks
- `make vet` — `go vet ./server/... ./simulator/...`
- `make clean` — `cargo clean` + remove Go binary
- `make go` — Go build only (requires prebuilt Rust cdylib)
- `make e2e` — docker-compose up + e2e tests

### `go.mod`
**Module:** `github.com/premchandkpc/FlowRulZ`

Dependencies: Sarama (Kafka), OTel gRPC, gRPC, wasmtime-go, etc.

### `Cargo.toml`
**Crate:** `flowrulz_core`

Dependencies: `bumpalo` (arena), `boxcar` (lock-free vec), `serde`/`serde_json`, `uuid`, `rand`, `once_cell`, `bincode`, `wasmtime` (plugins). Dev: `criterion`, `wat`.

---

## Summary

| Layer | Source Files | Tests | Lines |
|-------|-------------|-------|-------|
| Go server + bridge + pkg + SDK | ~50 | ~100 tests | ~5,200 |
| Go simulator | 26 | ~25 tests | ~2,800 |
| Rust runtime | 26 | 401 tests | ~6,100 |
| C bridge | — | — | ~15 |
| Build/config | 3 | — | ~200 |
| Docs | 19 | — | ~4,500 |
