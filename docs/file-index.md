# File Index

Every source file in the project, grouped by package, with its purpose and key exports.

---

## Go Server (~45 source files + 19 simulator files)

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

Core rule engine. Maintains `map[string]*Rule` of versioned plans. Each `Rule` holds `[]*VersionedPlan` with an `ActiveVersion` index and `ActiveExec sync.WaitGroup` for in-flight tracking. `Deploy()` compiles DSL via bridge, assigns lane by complexity score, persists to disk. `AddVersion()` stores a pre-compiled plan without auto-activating. `Promote()` activates a version. `Drain()` gracefully removes a version by waiting on `ActiveExec.Wait()`. `Remove()` waits for all in-flight executions then deletes. Callback hooks: `AfterDeploy`, `AfterPromote` — set by ProdNode for plan distribution.

**Exports:** `VersionedPlan`, `Rule`, `Engine`, `New()`, `NewWithCompiler()`, `Deploy()`, `AddVersion()`, `Promote()`, `Rollback()`, `Drain()`, `Remove()`, `ActivePlanBytes()`, `ActivePlan()`, `LaneForScore()`, `GetRule()`, `Rules()`, `ExecuteAll()`, `SetAfterDeploy()`, `SetAfterPromote()`

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

16-shard LRU dedup tracker. `CheckAndMark(key) bool` — atomic check-and-mark preventing TOCTOU races. `Seen(id)`, `Mark(id)` for backward compatibility. Shard key via `maphash.Hash`. Default 10k entries total (625 per shard), 5min TTL. Background cleanup. `StartCleanup(ctx, interval)` for background eviction.

**Exports:** `DedupEntry`, `DedupTracker`, `NewDedupTracker()`, `Seen()`, `Mark()`, `CheckAndMark()`, `StartCleanup()`, `Len()`, `Clear()`

---

### `server/internal/reliability/dedup_test.go`
**Package:** `reliability`

Tests: `TestDedupSeenUnseen`, `TestDedupMarkAndSeen`, `TestDedupMaxSize`, `TestDedupClear`, `TestDedupCleanupExpired`, `TestDedupDefaults`, `TestDedupEvictsOldest`

Benchmarks: `BenchmarkDedupNewKeys`, `BenchmarkDedupDuplicates`, `BenchmarkDedupAtCapacity`, `BenchmarkDedupMark`, `BenchmarkDedupSeen`, `BenchmarkDedupConcurrent`

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

Central production node composition root. Composed of sub-components: `ExecutionEngine`, `IngressPipeline`, `MessageRouter`, `AdminHTTPServer`, `LeadershipStrategy`. `Dependencies` struct holds ~20 dependencies grouped into 6 bags (`ClusterDeps`, `TransportDeps`, `ExecutionDeps`, `ReliabilityDeps`, `APIDeps`, `PartitionDeps`). `NewNode()` constructor wires everything: selects leadership strategy, builds dependency bags, constructs sub-components, starts flow watcher.

**Exports:** `ProdNode`, `Dependencies`, `NodeConfig`, `NewNode()`, `Start()`, `Shutdown()`

---

### `server/internal/node/prod_test.go`
**Package:** `node`

Tests: `TestProdNodeStartShutdown`, `TestSetLeader`

---

### `server/internal/node/interfaces.go`
**Package:** `node`

16 DI interfaces forming the testability boundary: `ServiceInvoker`, `NodeEngine`, `NodeDLQ`, `NodeSagaTracker`, `GRPCService`, `AdminHandler`, `MetricsSnapshotProvider`, `SpanExporter`, `ClusterTransport`, `GossipProvider`, `RateLimiter`, `DedupChecker`, `ServiceLookup`, `TransportFactory`, `PlanDistributor`, `ProtocolDispatcher`, `LeadershipStrategy`, `CircuitBreakerRegistry`, `ExecTracker`, `ExecLister`, `StateStore`.

**Exports:** all interface types

---

### `server/internal/node/layers.go`
**Package:** `node`

6 dependency bags grouping ~20 dependencies: `ClusterDeps`, `TransportDeps`, `ExecutionDeps`, `ReliabilityDeps`, `APIDeps`, `PartitionDeps`. Pure structural organization — no logic.

**Exports:** `ClusterDeps`, `TransportDeps`, `ExecutionDeps`, `ReliabilityDeps`, `APIDeps`, `PartitionDeps`

---

### `server/internal/node/execution_engine.go`
**Package:** `node`

VM step-loop execution engine. Runs bytecode plans cooperatively via `bridge.ExecuteStep`. Handles circuit breakers (per-service), saga compensation on failure, and in-flight execution tracking via `ExecTracker`.

**Exports:** `ExecutionEngine`, `NewExecutionEngine()`

---

### `server/internal/node/ingress_pipeline.go`
**Package:** `node`

Reliability pipeline for inbound messages: rate limit → dedup (16-shard LRU, atomic `CheckAndMark`) → execute all active plans → DLQ on failure. Wired as the handler for user-topic transport consumer.

**Exports:** `IngressPipeline`, `NewIngressPipeline()`, `HandleMessage()`

---

### `server/internal/node/message_router.go`
**Package:** `node`

Transport demultiplexer. Sets up 5 consumers via `TransportFactory`: `_flowrulz_members` (node discovery), `_flowrulz_plans` (plan distribution), `_flowrulz_acks` (ack handling), `_flowrulz_partitions` (partition assignment), user topic (→ IngressPipeline). Handles term fencing on plan messages.

**Exports:** `MessageRouter`, `NewMessageRouter()`, `StartConsumers()`, `StopConsumers()`

---

### `server/internal/node/admin_http.go`
**Package:** `node`

HTTP API server. Endpoints: `/health`, `/readyz`, `/metrics`, `/register`, `/heartbeat`, `/services`, `/cluster/join` (auth required), `/executions` (list), `/executions/{id}` (cancel), `/partitions`, `/partitions/rebalance`. Delegates `/admin/*` to `admin.Server` for rules CRUD.

**Exports:** `AdminHTTPServer`, `NewAdminHTTPServer()`, `ServeHTTP()`, `Shutdown()`

---

### `server/internal/node/leadership.go`
**Package:** `node`

Strategy pattern for leadership determination. `LeadershipStrategy` interface with two implementations: `RaftLeadershipStrategy` (delegates to Raft cluster) and `SingleLeaderStrategy` (this node is always leader). Strategy selected in `NewNode()` based on whether Raft is configured.

**Exports:** `LeadershipStrategy`, `RaftLeadershipStrategy`, `SingleLeaderStrategy`, `NewRaftLeadershipStrategy()`, `NewSingleLeaderStrategy()`

---

### `server/internal/node/recovery.go`
**Package:** `node`

In-flight execution recovery on node startup. Queries `StateStore` for `Running`/`WaitingForService` executions, retries pending service calls, resumes VM step-loop from saved `CtxBytes`. In-memory only — executions lost on full process restart by design.

**Exports:** `recoverInFlight()`

---

### `server/internal/node/production_invoker.go`
**Package:** `node`

Protocol-aware service call dispatcher. Looks up service instance via `ServiceLookup`, applies per-service circuit breaker, dispatches via `ServiceCaller` (HTTP POST / gRPC / TCP with length-prefixed framing). Falls back to passthrough (echo) for unregistered services. Marks unhealthy on HTTP 5xx or TCP failures.

**Exports:** `ProductionInvoker`, `NewProductionInvoker()`

---

### `server/internal/node/cluster_adapter.go`
**Package:** `node`

Adapter bridging `ClusterNode` to the `TransportFactory` interface. Registers cluster gRPC producer/consumer factories.

**Exports:** `transportFactoryAdapter`, `serviceLookupAdapter`, `rateLimiterAdapter`, `dedupCheckerAdapter`, `planDistributorAdapter`

---

### `server/internal/node/message_router_test.go`
**Package:** `node`

Tests: `TestMessageRouterStartStopConsumers`

---

### `server/internal/cache/cache.go`
**Package:** `cache`

Pluggable cache interface: `Get`, `Set` (with TTL), `Delete`, `Exists`, `Clear`. `CacheProvider` factory interface. Provider registry with `RegisterProvider`, `GetProvider`, `NewFromConfig`. Falls back to memory if named provider not found.

**Exports:** `Cache`, `CacheProvider`, `Config`, `DefaultConfig()`, `RegisterProvider()`, `GetProvider()`, `NewFromConfig()`

---

### `server/internal/cache/memory.go`
**Package:** `cache`

In-memory cache backend. `sync.RWMutex`-protected map with TTL entries. Background cleanup goroutine (1s interval). Defensively copies values on Get/Set. `Len()` method beyond interface.

**Exports:** `MemoryCache`, `MemoryProvider`, `NewMemoryCache()`

---

### `server/internal/cache/redis.go`
**Package:** `cache`

Redis cache backend wrapping `github.com/redis/go-redis/v9`. `Get` returns `nil` on `redis.Nil`. `Clear` calls `FlushDB`. Adds `Ping` health-check.

**Exports:** `RedisCache`, `RedisProvider`, `NewRedisCache()`

---

### `server/internal/flow/ast.go`
**Package:** `flow`

AST node types for the Flow DSL. `Flow` (top-level), `ServiceDecl`, `EventDecl`, `WorkflowStep` interface with 10 implementations (`StepRef`, `IfBlock`, `SwitchBlock`, `ParallelBlock`, `WaitBlock`, `ForeachLoop`, `WhileLoop`, `EmitEvent`, `ReturnStep`). `ServiceType` enum (6 types: gRPC, HTTP, Kafka, Redis, Postgres, TCP).

**Exports:** `Flow`, `ServiceDecl`, `EventDecl`, `WorkflowStep`, `StepRef`, `IfBlock`, `SwitchBlock`, `ParallelBlock`, `WaitBlock`, `ForeachLoop`, `WhileLoop`, `EmitEvent`, `ReturnStep`, `ServiceType`

---

### `server/internal/flow/lexer.go`
**Package:** `flow`

Hand-written character-by-character tokenizer. 40+ token types: literals, 29 keywords, 6 service types, operators, delimiters. Handles `#`, `//`, `/* */` comments. Duration suffixes (`ms`, `s`, `m`, `h`, etc.). Version tokens (`v.1`).

**Exports:** `Token`, `TokenType` (40+ constants), `Lexer`, `NewLexer()`, `Tokenize()`, `FilterNewlines()`

---

### `server/internal/flow/parser.go`
**Package:** `flow`

Recursive descent parser consuming token stream into AST. Top-level: `version`, `flow <name>`, then sections (`description`, `variables`, `constants`, `service`, `event`, `workflow`, etc.). Workflow steps dispatched to 10 step types. Service options handle 6 shapes (bool, list, map, typed keyword, key-value).

**Exports:** `Parser`, `NewParser()`, `Parse()`

---

### `server/internal/flow/semantic.go`
**Package:** `flow`

Semantic analyzer. Registers declarations into lookup maps. Validates service references (`auth.CreateUser`), event references, `onError` cases, `compensate` entries. Returns `[]SemanticError` with line numbers.

**Exports:** `Analyzer`, `SemanticError`, `NewAnalyzer()`, `Analyze()`

---

### `server/internal/flow/ir.go`
**Package:** `flow`

AST → IR graph compilation. Creates `IR` with `[]IRNode` + `[]IREdge`. Node types: `start`, `end`, `step`, `if`, `merge`, `parallel`, `join`, `wait`, `emit`, `return`. JSON-serializable via `MarshalIR`/`UnmarshalIR`. Auto-generates node IDs (`n0`, `n1`, ...).

**Exports:** `IR`, `IRNode`, `IREdge`, `Compiler`, `NewCompiler()`, `Compile()`, `MarshalIR()`, `UnmarshalIR()`

---

### `server/internal/flow/codegen.go`
**Package:** `flow`

IR → source code generation. 4 targets: Go (struct + service interfaces + Execute), Rust (struct + traits + async fn), Java (class + CompletableFuture interfaces), Python (class + Protocol classes). Type mapping for `string`/`int`/`float`/`bool` to language equivalents.

**Exports:** `CodeGenerator`, `NewCodeGenerator()`, `Generate()`

---

### `server/internal/flow/graph.go`
**Package:** `flow`

IR → graph visualization. Two output formats: Graphviz DOT (services=yellow ellipses, nodes=blue boxes, if=yellow diamonds, parallel=pink hexagons, emit=cyan notes, start/end=green ellipses) and Mermaid (`flowchart TD`).

**Exports:** `GraphGenerator`, `NewGraphGenerator()`, `GenerateDOT()`, `GenerateMermaid()`

---

### `server/internal/flow/formatter.go`
**Package:** `flow`

Canonical `.flow` formatting. Reads parsed AST and re-emits in standardized indentation and spacing.

**Exports:** `Formatter`, `NewFormatter()`, `Format()`

---

### `server/internal/flow/cli.go`
**Package:** `flow`

CLI commands wiring the full pipeline. 5 commands: `fmt`, `validate`, `graph` (dot/mermaid), `codegen` (go/rust/java/python), `info`. `ParseArgs()` utility for `--flag value` parsing.

**Exports:** `CLI`, `NewCLI()`, `Run()`, `ParseArgs()`

---

### `server/internal/flow/lsp.go`
**Package:** `flow`

Language Server Protocol implementation. Tracks open documents, parses on open/change, runs semantic analysis. Methods: `initialize`, `textDocument/didOpen`, `textDocument/didChange`, `textDocument/didClose`, `textDocument/formatting`, `textDocument/completion` (keywords + service names), `textDocument/hover` (service type info). Library implementation — needs JSON-RPC transport.

**Exports:** `LSPServer`, `NewLSPServer()`, `HandleRequest()`, `Graph()`, `Diagnostics()`

---

### `server/internal/flow/watcher.go`
**Package:** `flow`

Filesystem hot-reload. `FileWatcher` polls at configurable interval (default 5s), checks `ModTime` of `.flow` files. `DebouncedWatcher` wraps with debounce delay for editor-save scenarios. Context-based cancellation.

**Exports:** `FileWatcher`, `NewFileWatcher()`, `Start()`, `Stop()`, `DebouncedWatcher`, `NewDebouncedWatcher()`

---

### `server/internal/flow/registry.go`
**Package:** `flow`

Runtime store for flow definitions. Thread-safe via `sync.RWMutex`. IR cached under `flow:<name>:ir` (5-min TTL). Topic-to-flow routing cached under `flow:route:<topic>`. Methods: `LoadFile`, `LoadDirectory`, `Register`, `Get`, `GetByTopic`, `List`, `Delete`, `Format`.

**Exports:** `Registry`, `FlowState`, `NewRegistry()`, `LoadFile()`, `LoadDirectory()`, `Register()`, `Get()`, `GetByTopic()`, `List()`, `Delete()`, `Format()`

---

### `server/internal/flow/flow_test.go`
**Package:** `flow`

Tests: lexer tokenization, operators, comments, durations; parser simple flow, retry, parallel, error recovery; formatter round-trip; semantic analyzer (valid and unknown service); IR compiler; lexer edge cases.

---

### `server/internal/flow/codegen_test.go`
**Package:** `flow`

Tests: DOT graph generation, Mermaid graph generation, Go codegen, Rust codegen, LSP server (open, completion, hover, format), LSP diagnostics, CLI help.

---

### `server/internal/flow/registry_test.go`
**Package:** `flow`

Tests: full registry integration (register, get, list, format, delete), multiple flows, semantic error propagation, cache hit after memory deletion.

---

### `server/internal/cluster/transport_factory.go`
**Package:** `cluster`

Registration adapter plugging cluster's gRPC producer/consumer into `TransportFactory`. 25 lines. Registers `KindCluster` factories delegating to `cluster.NewClusterProducer`/`NewClusterConsumer`.

**Exports:** `RegisterClusterTransport()`

---

### `server/internal/transport/factory.go`
**Package:** `transport`

Pluggable transport factory with kind-based switching. 4 kinds: Kafka, Cluster, Memory, Noop. `NewProducer(topic)` / `NewConsumer(topic, handler)` look up factory for active kind. `SetKind()` for runtime switching. Thread-safe via `sync.RWMutex`.

**Exports:** `TransportFactory`, `NewTransportFactory()`, `SetKind()`, `NewProducer()`, `NewConsumer()`

---

### `server/internal/transport/registry.go`
**Package:** `transport`

In-memory transport registration. `RegisterMemory(factory)` registers `KindMemory` factories using simple buffered channels.

**Exports:** `RegisterMemory()`

---

### `server/internal/transport/kafka/registry.go`
**Package:** `kafka`

Kafka transport registration. `RegisterKafka(factory, cfg)` registers `KindKafka` factories. Returns early if brokers empty. `NewTransportFactoryFromConfig()` convenience constructor.

**Exports:** `RegisterKafka()`, `NewTransportFactoryFromConfig()`, `RegistrationConfig`

---

### `server/pkg/node/dependencies.go`
**Package:** `node` (public)

Public interface definitions for external consumers: `ExecRegistry`, `NodeEngine`, `GRPCService`, `AdminHandler`, `SpanExporter`, `ClusterTransport`. Minimal subset of the 16 internal interfaces.

**Exports:** `ExecRegistry`, `NodeEngine`, `GRPCService`, `AdminHandler`, `SpanExporter`, `ClusterTransport`

---

### `server/internal/bootstrap/builder.go`
**Package:** `bootstrap`

DI composition root. `NodeBuilder` constructs ProdNode with all dependencies. `WithDefaults()` configures default implementations. `DefaultDependencies()` factory for production wiring. Supports optional override methods for testing.

**Exports:** `NodeBuilder`, `New()`, `WithDefaults()`, `Build()`, `DefaultDependencies()`

---

---

### `server/pkg/cluster/` — cluster interfaces and types
### `server/pkg/membership/` — membership interfaces and types
### `server/pkg/node/` — node types only (`ID`, `ExecuteRequest`, `ExecuteResponse`)
### `server/pkg/partition/` — partition interfaces and types
### `server/pkg/plandist/` — plan distribution interfaces and types
### `server/pkg/replyrouter/` — reply router interface
### `server/pkg/scheduler/` — scheduler + lane interfaces
### `server/pkg/transport/` — transport interfaces and types

> **Deleted (2026-07-06):** `pkg/engine/`, `pkg/registry/`, `pkg/store/`, `pkg/reliability/`, `pkg/vm/` — Potemkin abstractions, never used as DI types. `internal/adapters/`, `internal/ports/` — zero-import dead code. `bridge/vm_adapter.go` — dead code (`NewBridgeVM` never called).

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
| Go server + bridge + pkg + SDK | ~70 | ~120 tests | ~7,500 |
| Go simulator | 26 | ~25 tests | ~2,800 |
| Rust runtime | 26 | 401 tests | ~6,100 |
| C bridge | — | — | ~15 |
| Build/config | 3 | — | ~200 |
| Docs | 25 | — | ~5,500 |
