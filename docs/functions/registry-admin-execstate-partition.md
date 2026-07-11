# internal/registry, internal/admin, internal/execstate, internal/partition, internal/plandist, internal/replyrouter, internal/observability — Function Reference

---

## internal/registry/registry.go + lookup.go

### `New() *ServiceRegistry`
Creates with empty endpoint and instance maps.

### `(r *ServiceRegistry) Register(name string, endpoint *Endpoint) error`
**Flow:**
1. If name not in map → create entry with empty instances.
2. Append endpoint to endpoints list.

### `(r *ServiceRegistry) RegisterInstance(inst *ServiceInstance) error`
**Flow:**
1. If name not in map → create entry.
2. Append instance to instances list.
3. If first instance for service → mark in change channel.

### `(r *ServiceRegistry) Unregister(name, nodeID string)`
Removes endpoints matching name+nodeID.

### `(r *ServiceRegistry) UnregisterInstance(name, instanceID string)`
Removes specific instance.

### `(r *ServiceRegistry) Lookup(name string) []*Endpoint`
Returns all endpoints for service.

### `(r *ServiceRegistry) LookupInstance(name, method string) (*ServiceInstance, error)`
**Flow:**
1. Get instances for service.
2. Filter by method match.
3. Round-robin pick from candidates.

**Edge Cases:**
- No instances → error.
- No method match → error.

### `(r *ServiceRegistry) LookupInstanceWithProtocol(name, method string, protocol Protocol) (*ServiceInstance, error)`
Filters by protocol in addition to method.

### `(r *ServiceRegistry) Pick(name string) (*Endpoint, error)`
Default round-robin pick.

### `(r *ServiceRegistry) PickWithStrategy(name string, strategy LBStrategy) (*Endpoint, error)`
Load balancing strategies:
- **RoundRobin**: cycling index.
- **Random**: random pick.
- **LeastConn**: pick instance with fewest active connections.
- **WeightedRandom**: random with weight consideration.

### `(r *ServiceRegistry) ListServices() []string` / `ListEndpoints(name)` / `ListInstances(name)` / `Snapshot()` / `ServiceInfo(name)` / `AllServiceInfo()`
Query methods returning various views of registry state.

### `(r *ServiceRegistry) MarkUnhealthy(name, nodeID string)`
Marks instance as unhealthy (excluded from picks).

---

## internal/registry/pkgsupport.go (pkg-level wrapper)

### `NewRegistry() *Registry`
Creates pkg-level `Registry` wrapping internal `ServiceRegistry`.

### `Register(ctx, svc)`, `Unregister(ctx, name)`, `Lookup(ctx, name)`, `LookupMultiple(ctx, names)`, `ListServices(ctx)`, `HealthCheck(ctx, name)`, `SubscribeChanges(ctx, pattern)`
All delegate to internal `ServiceRegistry` with context propagation.

---

## internal/admin/api.go

### `New(eng *engine.Engine) *Server` / `NewWithCompiler(eng, comp) *Server`
Creates admin HTTP API server. Registers routes on mux.

### `(s *Server) Handler() http.Handler`
Returns HTTP mux with routes:

| Method | Path | Handler |
|---|---|---|
| POST | `/rules` | `deployRule` |
| DELETE | `/rules/{id}` | `removeRule` |
| GET | `/rules` | `listRules` |
| GET | `/rules/{id}` | `getRule` |
| GET | `/rules/{id}/versions` | `listVersions` |
| POST | `/rules/{id}/promote` | `promoteVersion` |
| POST | `/rules/{id}/rollback` | `rollbackVersion` |
| POST | `/validate` | `validateRule` |
| GET | `/lanes` | `listLanes` |
| GET | `/health` | `health` |
| GET | `/metrics` | `metrics` |
| GET | `/debug` | `debug` |
| GET | `/dlq` | `listDLQ` |
| POST | `/dlq/replay` | `replayDLQ` |
| POST | `/dlq/replay-all` | `replayAllDLQ` |
| DELETE | `/dlq` | `clearDLQ` |

### `(s *Server) RegisterDLQ(dlq *reliability.DLQ)`
Registers DLQ for replay endpoints.

### Middleware
- `rateLimit`: 100 req/s per IP.
- `auth`: Bearer token check (if `FLOWRULZ_CLUSTER_TOKEN` env set).

---

## internal/admin/service.go

### `newRuleService(eng, comp) *ruleService`

### `(rs *ruleService) DeployRule(id, dsl string) error`
Compiles DSL → `Engine.Deploy(id, dsl)`.

### `(rs *ruleService) RemoveRule(id string)`
`Engine.Remove(id)`.

### `(rs *ruleService) ListRules() []ruleView`
Returns rule summaries (id, active version, lane, version count).

### `(rs *ruleService) RuleDetail(id string) (map[string]interface{}, bool)`
Returns full rule detail including all versions.

### `(rs *ruleService) RuleVersions(id string) []versionView`
Returns version list for a rule.

### `(rs *ruleService) ValidateDSL(dsl string) (map[string]interface{}, error)`
Compiles DSL without deploying, returns validation result.

### `(rs *ruleService) PromoteVersion(id string, version uint64) error`
`Engine.Promote(id, version)`.

### `(rs *ruleService) Lanes() []map[string]interface{}` / `HealthSnapshot()` / `DebugSnapshot()`
Return system state for admin endpoints.

---

## internal/execstate/filestore.go

### `NewFileStore(dir string) (*FileStore, error)`
Creates directory if needed. Returns sharded file store (16 shards by FNV-32 hash of ID).

### `(fs *FileStore) Create(ctx, state) error`
Creates `{id}.json`. Edge cases: already exists → error.

### `(fs *FileStore) Save(ctx, state) error`
Atomic write: marshal → write `.tmp` → rename. Updates `UpdatedAt`.

### `(fs *FileStore) Load(ctx, id) (*State, error)`
Reads and unmarshals `{id}.json`.

### `(fs *FileStore) ListByStatus(ctx, statuses...) ([]*State, error)`
**Flow:**
1. Look up candidate IDs from in-memory `statusIndex` (status → set of IDs).
2. Read only candidate files (not full directory scan).
3. Sort by `CreatedAt` ascending.

### `(fs *FileStore) Delete(ctx, id) error`
Removes `{id}.json`. No-op if file doesn't exist.

### `(fs *FileStore) Close() error` — No-op.

**Edge Cases:**
- Sharding reduces lock contention (16 independent mutexes).
- Atomic write prevents corruption on crash.
- `ListByStatus` uses in-memory status index — O(k) where k = matching executions, not O(n) total files.
- Status index built once at startup from disk, maintained on Create/Save/Delete.

---

## internal/execstate/execstate.go

### Status Constants

| Status | Value | Description |
|---|---|---|
| `StatusCreated` | 0 | Execution created, not yet started |
| `StatusRunning` | 1 | Actively executing steps |
| `StatusWaitingForService` | 2 | Waiting for external service response |
| `StatusCompleted` | 3 | Successfully completed |
| `StatusFailed` | 4 | Failed (error stored in `Error` field) |

### `(s Status) String() string`
Returns human-readable status name.

---

## internal/execstate/pkgsupport.go

### `NewExecutionStore(dir string) (*ExecutionStore, error)`
Wraps `FileStore` as `pkg/store.Store` interface. Implements `Create`, `Save`, `Load`, `Delete`, `ListByStatus`, `Close`.

---

## internal/partition/manager.go

### `New(numPartitions uint32) *Manager`
Creates partition manager with empty assignments.

### `(m *Manager) Assignments() []string`
Returns current partition-to-node assignments.

### `(m *Manager) NodeForPartition(partition) string`
Returns node owning given partition.

### `(m *Manager) PartitionsForNode(nodeID) []PartitionID`
Returns all partitions owned by node.

### `(m *Manager) PartitionForKey(key) PartitionID`
Hash-based partition selection: `fnv32(key) % numPartitions`.

### `(m *Manager) Rebalance(aliveNodes []string, term uint64) []Assignment`
**Flow:**
1. Sort alive nodes.
2. Consistent hash: assign partitions to nodes in round-robin.
3. Return new assignments.

### `(m *Manager) ApplyAssignments(assignments []Assignment)`
Applies new assignments to internal state.

### `(m *Manager) PublishAssignments(ctx, assignments) error`
Marshals assignments → sends via partition producer.

### `(m *Manager) HandleAssignmentMessage(msg []byte) error`
Parses assignment message → `ApplyAssignments`.

### `(m *Manager) OnLeaderChange(leaderID string)`
Sets leader, triggers rebalance if this node is new leader.

### `(m *Manager) LeaderID() string`
Returns current leader.

### `(m *Manager) NumPartitions() uint32`
Returns partition count.

---

## internal/partition/rebalance.go

### `NewRebalanceNotifier(m, aliveFn, termFn) *RebalanceNotifier`
Creates rebalance notifier with callbacks for alive nodes and current term.

### `(rn *RebalanceNotifier) SetNotify(fn func())`
Sets callback triggered on rebalance.

### `(rn *RebalanceNotifier) CheckAndRebalance() bool`
**Flow:**
1. Get alive nodes and current term.
2. Compare with last known state.
3. If changed → trigger `Manager.Rebalance` → apply assignments → publish → call notify callback → return `true`.
4. If unchanged → return `false`.

**Edge Cases:**
- Uses atomic counter for term comparison.
- Prevents duplicate rebalances via term tracking.

---

## internal/plandist/distributor.go

### `NewPlanDistributor(opts...) *PlanDistributor`
Creates plan distributor for cross-cluster plan deployment.

### Key Methods:
- `CurrentTerm() uint64` — Returns current deployment term.
- `IncDeployTerm() uint64` — Atomically increments and returns term.
- `Stop()` — Stops distributor.

---

## internal/replyrouter/router.go

### `New(opts...) *ReplyRouter`
Default: 1s cleanup interval, 10000 max pending.

Options: `WithCleanupInterval(d)`, `WithMaxPending(n)`.

### `(rr *ReplyRouter) Register(ctx, correlationID, ch, timeout) error`

**Flow:**
1. Validate non-empty correlation ID.
2. Lock → check duplicate → check max pending → store with deadline → unlock.

**Edge Cases:**
- Empty ID → error.
- Duplicate ID → `ErrDuplicateCorrID`.
- Max pending reached → `ErrPendingLimit`.

### `(rr *ReplyRouter) Cancel(correlationID)`
Removes from pending, closes reply channel.

### `(rr *ReplyRouter) Deliver(ctx, correlationID, msg) bool`
**Flow:**
1. Lock → find pending request → remove → try-send on channel → close channel → unlock.

**Edge Cases:**
- Channel full → message dropped (non-blocking send).
- Not found → returns `false`.

### `(rr *ReplyRouter) PendingCount() int`
Returns count under read-lock.

### `(rr *ReplyRouter) StartCleanup(ctx)` / `StopCleanup()`
Background goroutine evicting expired requests.

### `(rr *ReplyRouter) cleanup()`
Iterates pending, closes channels for expired deadlines.

---

## internal/observability/metrics.go

### `NewMetricsCollector() *MetricsCollector`
Creates with empty counter/gauge/histogram maps.

### `(mc *MetricsCollector) Counter(name string) *Counter`
Lazy-creates counter (atomic int64).

### `(mc *MetricsCollector) Gauge(name string) *Gauge`
Lazy-creates gauge (atomic int64).

### `(mc *MetricsCollector) Histogram(name string, buckets []float64) *Histogram`
Lazy-creates histogram with buckets.

### `(mc *MetricsCollector) Snapshot() MetricsSnapshot`
Returns all metric values as JSON-serializable struct.

### Global Shortcuts
- `GetCounter(name) *Counter` — Returns from default collector.
- `GetGauge(name) *Gauge` — Returns from default collector.
- `RecordExec(name)` — Increments `exec.{name}` counter.
- `RecordError(name)` — Increments `error.{name}` counter.

### `SpanExporter`
OpenTelemetry span exporter for distributed tracing. Starts OTel pipeline, exports spans to configured backend.

### `NewSpanExporter(endpoint) *SpanExporter`
Creates OTLP gRPC exporter. Returns nil if endpoint empty.

### `(se *SpanExporter) Start(ctx)`
Runs export loop every 5s. Reads raw span data from bridge FFI, converts to OTel spans.

### `(se *SpanExporter) Stop()`
Signals stop, shuts down OTel provider with 5s timeout.

---

## internal/observability/metrics.go — Counter/Gauge/Histogram

### `Counter` — Atomic int64. Methods: `Inc()`, `Add(n)`, `Value()`, `Name()`, `Reset()`.
### `Gauge` — Atomic int64. Methods: `Set(n)`, `Add(n)`, `Value()`, `Name()`.
### `Histogram` — Bucket-based. Method: `Observe(v float64)`. Counts values per bucket.

---

## internal/common/lifecycle.go

### `Service` interface
```go
type Service interface {
    Start(ctx context.Context) error
    Stop() error
}
```

### `NewLifecycleRegistry() *LifecycleRegistry`
### `(r *LifecycleRegistry) Register(name, svc)`
### `(r *LifecycleRegistry) StartAll(ctx) error` — Starts services in registration order.
### `(r *LifecycleRegistry) StopAll(ctx) error` — Stops in reverse order. **Aggregates all errors** via `errors.Join` (no silent swallow).

---

## internal/execstate/memorystore.go

### `NewMemoryStore() *MemoryStore`
In-memory implementation of the Store interface. Same API as FileStore.

---

## internal/execstate/pkgsupport.go

### `NewExecutionStore(dir) (*ExecutionStore, error)`
Wraps FileStore as `pkg/store.Store` interface. Converts between internal `State` and `pkg/store.ExecutionRecord`.

### Methods: `Create`, `Save`, `Load`, `List`, `ListByPlan`, `Delete`, `Close`

---

## internal/registry/http.go — HTTP Handlers

### `RegisterRequest` / `HeartbeatRequest`
```go
type RegisterRequest struct {
    ID, Name, Version, Address, Protocol, Zone, NodeID string
    Port int
    Methods []MethodInfo
    Capabilities ServiceCapabilities
    Tags []string
    Metadata map[string]string
    Weight int
}
type HeartbeatRequest struct {
    Name, InstanceID string
}
```

### `(r *ServiceRegistry) RegisterHTTPHandler(w, req)` — POST `/register`. Validates bearer token, decodes JSON, registers instance.
### `(r *ServiceRegistry) HeartbeatHTTPHandler(w, req)` — POST `/heartbeat`. Updates heartbeat timestamp.
### `(r *ServiceRegistry) ListServicesHTTPHandler(w, req)` — GET `/services`. Returns all service info as JSON.
### `(r *ServiceRegistry) StartHeartbeatChecker(stopCh)` — Background goroutine: checks expired instances every 15s.

---

## internal/registry/health.go

### `(r *ServiceRegistry) SetHeartbeatTimeout(d)`
### `(r *ServiceRegistry) Heartbeat(name, instanceID) error` — Updates heartbeat timestamp, marks healthy.
### `(r *ServiceRegistry) CheckExpired() []string` — Marks instances as unhealthy if past timeout. Returns expired IDs.
### `(r *ServiceRegistry) MarkUnhealthy(name, nodeID)` / `MarkHealthy(name, nodeID)` — Manual health override.

---

## internal/partition/manager.go

### `(m *Manager) SetProducer(p)` — Sets the partition message producer for rebalance notifications.

---

## internal/execstate/exec_registry.go

### `ExecRegistry` interface — Tracks in-flight executions for cancellation and observability.
```go
type ExecRegistry interface {
    Register(id string, cancel context.CancelFunc, name string)
    Unregister(id string)
    Cancel(id string) bool
    CancelAll()
    List() map[string]time.Time
    Len() int
}
```

### `NewExecRegistry() ExecRegistry`

---

## internal/execstate/execstate.go — Store Interface

### `Store` interface
```go
type Store interface {
    Create(ctx context.Context, s *State) error
    Save(ctx context.Context, s *State) error
    Load(ctx context.Context, id string) (*State, error)
    ListByStatus(ctx context.Context, statuses ...Status) ([]*State, error)
    Delete(ctx context.Context, id string) error
    Close() error
}
```

---

## internal/replyrouter/router.go — Sentinel Errors

### `ErrPendingLimit` — `errors.New("replyrouter: max pending requests reached")`
### `ErrDuplicateCorrID` — `errors.New("replyrouter: duplicate correlation ID")`
