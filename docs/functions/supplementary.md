# Supplementary Function Reference

Covers packages and functions not in the primary docs: bootstrap, cache, compiler, cluster (detailed), plugins, plandist (detailed), transport factory, node messages.

---

## internal/bootstrap/builder.go

### `NewNodeBuilder(cfg node.Config) *NodeBuilder`
Creates builder with config and lifecycle registry.

### `(b *NodeBuilder) WithDefaults() *NodeBuilder`
Calls `node.DefaultDependencies(cfg)` to populate all dependencies.

### `(b *NodeBuilder) Build() (*node.ProdNode, error)`
**Flow:**
1. Check accumulated errors.
2. Call `node.NewNode(cfg, deps)`.
3. Return node or first error.

**Edge Cases:** Returns first error if any were accumulated during build.

### `(b *NodeBuilder) BuildWithLifecycle(ctx) (*node.ProdNode, error)`
Same as `Build()` — lifecycle registration is done separately via `Lifecycle()`.

### `(b *NodeBuilder) Lifecycle() *common.LifecycleRegistry`
Returns the lifecycle registry for external service registration.

### Lifecycle Adapters
- `engineService` — `Start`/`Stop` are no-ops (engine doesn't need lifecycle).
- `schedulerService` — delegates to `Scheduler.Start()`/`Stop()`.

---

## internal/cache/cache.go (Interface)

### `Cache` Interface
```go
type Cache interface {
    Get(ctx, key) ([]byte, error)
    Set(ctx, key, value, ttl) error
    Delete(ctx, key) error
    Exists(ctx, key) (bool, error)
    Clear(ctx) error
    Close() error
}
```

### `CacheProvider` Interface
```go
type CacheProvider interface {
    Name() string
    New(config map[string]interface{}) (Cache, error)
}
```

### `DefaultConfig() Config`
Returns `{Provider: "memory", Options: {}}`.

---

## internal/cache/memory.go

### `NewMemoryCache() *MemoryCache`
Creates in-memory cache with background cleanup goroutine (1s interval).

### `(c *MemoryCache) Get(ctx, key) ([]byte, error)`
**Flow:**
1. Read-lock → check map.
2. If not found → return nil.
3. If expired → delete + return nil.
4. Return copy of value.

**Edge Cases:** Returns copy (not reference) to prevent aliasing.

### `(c *MemoryCache) Set(ctx, key, value, ttl) error`
Stores copy of value. If `ttl > 0`, sets expiry.

### `(c *MemoryCache) Delete(ctx, key) error`
Removes entry from map.

### `(c *MemoryCache) Exists(ctx, key) (bool, error)`
Read-lock check.

### `(c *MemoryCache) Clear(ctx) error`
Replaces map with empty one.

### `(c *MemoryCache) Close() error`
Stops cleanup goroutine.

### `(c *MemoryCache) Len() int`
Returns count under read-lock.

### `NewFromConfig(cfg Config) (Cache, error)`
Looks up provider by name, falls back to memory.

### `RegisterProvider(p CacheProvider)` / `GetProvider(name)`
Global provider registry.

---

## internal/cache/redis.go

### `NewRedisCache(addr, password, db) *RedisCache`
Creates Redis client with go-redis.

### `(c *RedisCache) Get(ctx, key) ([]byte, error)`
Redis GET. Returns nil on `redis.Nil`.

### `(c *RedisCache) Set(ctx, key, value, ttl) error`
Redis SET with optional TTL.

### `(c *RedisCache) Delete(ctx, key) error` — Redis DEL.
### `(c *RedisCache) Exists(ctx, key) (bool, error)` — Redis EXISTS.
### `(c *RedisCache) Clear(ctx) error` — Redis FLUSHDB.
### `(c *RedisCache) Close() error` — Closes Redis connection.
### `(c *RedisCache) Ping(ctx) error` — Health check.

### `RedisProvider` — Registers as `"redis"` provider. Options: `addr`, `password`, `db`.

---

## internal/compiler/compiler.go

### `Compiler` Interface
```go
type Compiler interface {
    Compile(dsl, ruleID string) (*Result, error)
}
```

### `NewLocal() *LocalCompiler`
Local compiler using bridge FFI.

### `(c *LocalCompiler) Compile(dsl, ruleID) (*Result, error)`
Calls `bridge.Compile(dsl, ruleID)` → `bridge.PlanComplexity(plan)`.

### `NewRemote(addr string) *RemoteCompiler`
HTTP client to remote compiler service.

### `(c *RemoteCompiler) Compile(dsl, ruleID) (*Result, error)`
**Flow:**
1. POST to `{addr}/compile` with `{dsl, rule_id}`.
2. Read response → unmarshal.
3. Check error field → return error if present.
4. Check status code → return error if not 200.

**Edge Cases:**
- Network error → error.
- Compiler service error → error from response body.
- Non-200 status → error.

### `NewCompileHandler() *CompileHandler`
HTTP handler wrapping `LocalCompiler`.

### `(h *CompileHandler) HandleCompile(w, r)`
POST `/compile` → compile DSL → return plan+complexity.

### `(h *CompileHandler) HandleValidate(w, r)`
POST `/validate` → compile with ruleID="validate" → return valid/complexity/size/error.

---

## internal/cluster/cluster.go (RaftCluster — detailed)

### `NewRaftCluster(nodeID, raftDir, raftBind) *RaftCluster`
Creates Raft cluster config. FSM is NoopFSM (Raft used only for leader election).

### `(rc *RaftCluster) Start() error`
**Flow:**
1. Create raft directory.
2. Open BoltDB stores (log + stable).
3. Create file snapshot store.
4. Create TCP transport.
5. Configure Raft: 1s heartbeat, 1s election, 500ms leader lease, 50ms commit.
6. Start `raft.NewRaft`.
7. Spawn `trackLeadership` goroutine.

### `(rc *RaftCluster) Stop()`
Stops Raft → closes transport → closes stores.

### `(rc *RaftCluster) BootstrapCluster() error`
Initializes cluster with this node as sole voter. Skips if state already exists.

### `(rc *RaftCluster) Join(nodeID, raftAddr) error`
Adds voter. Must be called on leader. Edge cases: not leader → error.

### `(rc *RaftCluster) Leave(nodeID) error`
Removes server. Must be called on leader.

### `(rc *RaftCluster) IsLeader() bool` — Checks `raft.State() == Leader`.
### `(rc *RaftCluster) LeaderAddr() string` — Returns `raft.Leader()`.
### `(rc *RaftCluster) CurrentTerm() uint64` — Parses from `raft.Stats()`.
### `(rc *RaftCluster) ClusterSize() int` — Returns voter count from configuration.
### `(rc *RaftCluster) LastContact() time.Duration` — Time since last leader contact.
### `(rc *RaftCluster) Raft() *raft.Raft` — Returns underlying Raft instance.

### `(rc *RaftCluster) CaptureLeadershipToken() pkgcluster.LeadershipToken`
Captures `{Leader: IsLeader(), Term: CurrentTerm()}`.

### `(rc *RaftCluster) ValidateLeadershipToken(token) bool`
Returns `current.Leader && current.Term == token.Term`.

### `(rc *RaftCluster) SubscribeLeaderChanges(fn func(isLeader bool))`
Registers callback for leadership changes.

### `(rc *RaftCluster) trackLeadership()`
Background goroutine: listens on `raft.LeaderCh()`, updates `isLeader`/`leaderAddr`, fires all subscriber callbacks.

---

## internal/cluster/cluster.go (ClusterNode — detailed)

### `NewClusterNode(nodeID, grpcAddr) *ClusterNode`
Creates node with GRPCBus, peer map, handler map, and Gossiper.

### `(cn *ClusterNode) Start() error`
Starts GRPCBus, subscribes to `_flowrulz_gossip` topic, starts Gossiper.

### `(cn *ClusterNode) AddPeer(id, addr) error`
Creates gRPC client to peer, connects. Edge cases: already exists → no-op.

### `(cn *ClusterNode) RemovePeer(id)`
Closes client, removes from map.

### `(cn *ClusterNode) Publish(topic, key, body) error`
**Flow:**
1. Publish to local GRPCBus.
2. Fan-out to all peers via goroutines (30s timeout each).

**Edge Cases:**
- Peer publish failure → logged, not fatal.
- Concurrent publishes to many peers.

### `(cn *ClusterNode) PublishToPeer(peerID, topic, body) error`
Direct publish to specific peer.

### `(cn *ClusterNode) Subscribe(topic, handler)`
Registers handler on GRPCBus and local handler map.

### `(cn *ClusterNode) Unsubscribe(topic)`
Removes from both GRPCBus and handler map.

### `(cn *ClusterNode) Stop()`
Cancels gossip, stops gossiper, closes all peer clients, stops GRPCBus.

---

## internal/plugins/loader.go

### `LoadDir(pluginDir string) error`
**Flow:**
1. Read directory.
2. For each `.wasm` file → read bytes → `bridge.RegisterPlugin(name, data)`.

**Edge Cases:**
- Directory doesn't exist → log info, return nil (not error).
- Read error → return error.
- Register error → return error.

---

## internal/plandist/distributor.go (PlanDistributor — detailed)

### `New(nodeID string, opts ...Option) *PlanDistributor`
Creates with defaults: `planTopic="_flowrulz_plans"`, `ackTopic="_flowrulz_acks"`.

Options: `WithPlanTopic`, `WithAckTopic`, `WithPlanConsumer`, `WithPlanProducer`, `WithAckConsumer`, `WithAckProducer`, `WithPlanHandler`, `WithAckHandler`, `WithQuorumProvider`, `WithClusterTerm`.

### `(pd *PlanDistributor) Start(ctx) error`
Idempotent. Starts plan/ack consumers in goroutines.

### `(pd *PlanDistributor) Stop() error`
Stops consumers, closes producers, resets started flag.

### `(pd *PlanDistributor) SetTerm(term)` / `CurrentTerm() uint64`
Atomic term management.

### `(pd *PlanDistributor) PublishPlan(ctx, ruleID, version, plan, dsl) error`
Marshals `PlanMessage{Type:"plan"}` → sends via plan producer.

### `(pd *PlanDistributor) ActivatePlan(ctx, ruleID, version) error`
Marshals `PlanMessage{Type:"activate"}` → sends.

### `(pd *PlanDistributor) DeactivatePlan(ctx, ruleID) error`
Marshals `PlanMessage{Type:"deactivate"}` → sends.

### `(pd *PlanDistributor) OnPlan(fn)` / `OnAck(fn)`
Sets handler callbacks.

### `PlanMessageFromBytes(data) (*PlanMessage, error)` — JSON unmarshal.

---

## internal/plandist/ack.go

### `(pd *PlanDistributor) SendAck(ctx, ruleID, version, status) error`
Marshals `AckMessage` → sends via ack producer with key `ruleID:version`.

### `(pd *PlanDistributor) WaitForAcks(ctx, ruleID, version, quorum, timeout) error`

**Flow:**
1. Calculate quorum: `0` → majority of followers `(n-1)/2+1`; `-1` → all followers; `>0` → exact count.
2. Store `pendingAck` with done channel + atomic counter.
3. Block on `select`: done channel or timeout.

**Edge Cases:**
- Single node (no followers) → logs info, returns nil immediately (nothing to wait for).
- No `QuorumProvider` configured → logs warning, defaults to quorum=1 (may timeout if nobody sends acks).
- Timeout → error with received/expected count.
- Quorum reached → return nil.

### `(pd *PlanDistributor) handleAck(ack AckMessage)`
Finds pending ack → increments counter → sends on done channel if quorum reached.

### `(pd *PlanDistributor) RecordAck(msg AckMessage)` — Alias for `handleAck`.
### `AckMessageFromBytes(data) (*AckMessage, error)` — JSON unmarshal.
### `ackKey(ruleID, version) string` — Returns `"ruleID:version"`.

---

## internal/transport/factory.go

### `NewTransportFactory(kind TransportKind) *TransportFactory`
Creates with empty factory maps.

### `(f *TransportFactory) RegisterProducer(kind, factory)` / `RegisterConsumer(kind, factory)`
Registers factory for transport kind.

### `(f *TransportFactory) SetKind(kind)` / `Kind() TransportKind`
Gets/sets active transport kind.

### `(f *TransportFactory) NewProducer(topic) MessageProducer`
Creates producer using active kind's factory. Falls back to `noopProducer`.

### `(f *TransportFactory) NewConsumer(topic, handler) MessageConsumer`
Creates consumer using active kind's factory. Falls back to `noopConsumer`.

### Transport Kinds
| Kind | Description |
|---|---|
| `KindKafka` | Kafka via Sarama |
| `KindCluster` | gRPC cluster transport |
| `KindMemory` | In-memory bus |
| `KindNoop` | Discards all messages |

---

## internal/node/messages.go

### `(n *ProdNode) handleNodeDiscoveryMessage(ctx, msg) ([]byte, error)`
**Flow:**
1. Unmarshal `NodeDiscoveryMessage`.
2. Ignore if from self.
3. `Membership.Heartbeat(nodeID, address)`.
4. If ClusterNode exists → `ClusterNode.AddPeer(nodeID, address)`.

### `(n *ProdNode) handlePlanMessage(ctx, msg) ([]byte, error)`
**Flow:**
1. Unmarshal `PlanMessage`.
2. Reject if `pm.Term < PlanDist.CurrentTerm()` (stale plan).
3. Switch on `pm.Type`:
   - `"plan"` → `Engine.AddVersion` + `PlanDist.SendAck`.
   - `"activate"` → `Engine.Promote`.

### `(n *ProdNode) handleAckMessage(ctx, msg) ([]byte, error)`
Unmarshal → `PlanDist.RecordAck`.

### `(n *ProdNode) handlePartitionMessage(ctx, msg) ([]byte, error)`
`Partitions.HandleAssignmentMessage(msg)`.

---

## pkg/common/ (additional)

### `WriteJSON(path string, v any) error`
Atomic write: marshal → write `.tmp` → rename.

### `ReadJSON(path string, v any) error`
Reads file → unmarshals JSON.

### `LoadDir[T any](dir, ext string, decode func([]byte) (T, error)) ([]T, error)`
**Flow:**
1. Read directory entries.
2. Filter by extension.
3. Read each file → call decode function.
4. Return all decoded items.

### `NewBearerAuth() *BearerAuth`
Creates auth from `FLOWRULZ_CLUSTER_TOKEN` env.

### `(a *BearerAuth) Check(r *http.Request) bool`
Validates `Authorization: Bearer {token}` header.

### `(a *BearerAuth) Require(next http.HandlerFunc) http.HandlerFunc`
Middleware: rejects with 401 if auth fails.

---

## internal/engine/pkgsupport.go (pkg/engine.Engine adapter)

### `(e *Engine) Start(ctx) error` / `Stop() error` — No-ops (engine has no lifecycle).
### `(e *Engine) AddRule(ctx, rule) error` — Delegates to `Deploy(rule.ID, rule.DSL)`.
### `(e *Engine) RemoveRule(ctx, ruleID) error` — Delegates to `Remove(ruleID)`.
### `(e *Engine) GetRule(ctx, ruleID) (*Rule, error)` — Returns rule with active status and lane.
### `(e *Engine) ListRules(ctx) ([]*Rule, error)` — Returns all rules.
### `(e *Engine) Execute(ctx, ruleID, body, opts) (*Result, error)` — Executes active plan directly (single-shot, no scheduler).
### `(e *Engine) CompileRule(ctx, rule) error` — Delegates to `Deploy`.
### `(e *Engine) InvalidateCompilation(ruleID)` — No-op (cache invalidation not implemented).

---

## pkg/cluster/ — Types

### `MemberID` (string) / `ClusterState` (int) / `MemberInfo` struct / `CancelFunc`
### `ClusterMember` interface — `IsLeader`, `LeaderID`, `Start`, `Stop`, `Join`, `Leave`, `CaptureLeadershipToken`, `ValidateLeadershipToken`, `ClusterSize`, `SubscribeLeaderChanges`, `Raft`

---

## pkg/engine/ — Types

### `Engine` interface / `Rule` struct / `ExecuteOptions` struct

---

## pkg/membership/ — Types

### `Membership` interface / `NodeInfo` struct / `CancelFunc`

---

## pkg/node/ — Types

### `ID` (string) / `Node` interface / `ExecuteRequest` / `ExecuteResponse`

---

## pkg/partition/ — Types

### `PartitionID` (uint32) / `Assignment` struct / `Producer` interface / `PartitionManager` interface / `RebalanceNotifier` interface

---

## pkg/plandist/ — Types

### `PlanDistributor` interface / `PlanMessage` / `AckMessage` / `QuorumProvider` interface

---

## pkg/registry/ — Types

### `Registry` interface / `ServiceID` / `LBStrategy` / `ServiceRegistration` / `MethodSpec` / `ServiceInstance` / `EventType` / `RegistryEvent`

---

## pkg/reliability/ — Types

### `CircuitBreaker` interface / `CircuitState` / `Deduplicator` interface / `RateLimiter` interface / `DLQ` interface / `DeadLetterMessage` / `SagaStep` / `SagaStatus` / `SagaOrchestrator` interface

---

## pkg/scheduler/ — Types

### `Scheduler` interface / `Lane` / `LaneConfig` / `ExecutionID` / `State` / `Plan` / `Instruction` / `OpCode` / `ExecutionContext` / `StateChange` / `Result` / `SchedulerSnapshot`

---

## pkg/store/ — Types

### `ExecutionID` (string) / `ExecutionRecord` / `Store` interface

---

## pkg/transport/ — Types

### `EventBus` interface / `Handler` / `MessageType` / `Message` / `MessageHandler` / `Subscription` / `Publisher` / `Subscriber` / `Requester` / `Replier` / `Broadcaster` / `MessageProducer` / `MessageConsumer` / `FullEventBus`

---

## pkg/vm/ — Types

### `PlanCompiler` interface / `VMRunner` interface / `CompileResult` / `StepResult` / `StepCode` / `StepOptions`
