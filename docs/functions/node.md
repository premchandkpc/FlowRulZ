# internal/node — Function Reference

Central struct (`ProdNode`) wiring all modules. Handles execution, service calling, recovery, HTTP, cluster, and lifecycle.

---

## Core Lifecycle

### `NewNode(cfg Config, deps Dependencies) *ProdNode`

**Flow:**
1. Creates `ProdNode` with config, deps, HTTP client (10s timeout), empty consumer/producer slices, exec semaphore (capacity 16).
2. Copies all dependency fields from `deps` into node.
3. Creates `ServiceCaller`: if `cfg.HasTLS()` → `NewServiceCallerWithTLS(cert, key)`, else `NewServiceCaller()`.
4. Creates `ExecRegistry` (thread-safe map of cancel funcs).
5. Sets registry heartbeat timeout from config.
6. Calls `configureEngineHooks()` — wires `AfterDeploy`/`AfterPromote` callbacks on Engine.
7. Loads plugins from `cfg.PluginDir` or `FLOWRULZ_PLUGIN_DIR` env.

**Edge Cases:**
- Plugin load failure → warning only, node still starts.
- TLS cert files missing → `NewServiceCallerWithTLS` stores paths; actual TLS error surfaces on first gRPC call.

---

### `(n *ProdNode) Start(ctx context.Context) error`

**Flow:**
1. **Guard:** if `len(Seeds) > 0` and `RaftCluster == nil` → return error (multi-node requires Raft).
2. `startCluster(ctx)` — initializes Raft if configured, starts ClusterNode gossip.
3. `startConsumers(ctx, handler, kafkaCfg)` — creates plan/ack/partition consumers, starts each.
4. `startSubsystems(ctx)` — starts Membership eviction, PlanDist, Scheduler, Rebalancer, ReplyRouter cleanup, Dedup cleanup, `recoverInFlight`.
5. `startGRPC()` — starts GRPCBus if configured.
6. `startOTel(ctx)` — starts OpenTelemetry exporter.
7. `serveHTTP(ctx)` — starts HTTP server in goroutine.

**Edge Cases:**
- Multi-node without Raft → returns error at startup, refuses to run.
- Fixed startup order; no rollback on partial failure.
- If Raft not configured (single-node), cluster step is a no-op.

---

### `(n *ProdNode) Shutdown(ctx context.Context) error`

**Flow:**
1. `Execs.CancelAll()` — cancels all in-flight executions.
2. Stops all consumers, clears slice.
3. Stops PlanDist, Scheduler, ReplyRouter cleanup.
4. Closes all producers, clears slice.
5. HTTP server shutdown with 5s timeout.
6. Stops ClusterNode, GRPCBus, OtelExporter, RaftCluster.
7. Closes StateStore, ServiceCaller.

**Edge Cases:**
- Continues shutdown even if individual stops fail.
- HTTP shutdown has 5s hard timeout.

---

## Identity & Leadership

### `(n *ProdNode) ID() pkgnode.ID`
Returns the node's string ID as a typed ID.

### `(n *ProdNode) Addr() string`
Returns the HTTP listen address (`host:port`).

### `(n *ProdNode) IsLeader() bool`
- If `RaftCluster != nil` → delegates to `RaftCluster.IsLeader()`.
- If no Raft configured → returns `true` (single-node default).

### `(n *ProdNode) CurrentTerm() uint64`
- If `RaftCluster != nil` → `RaftCluster.CurrentTerm()`.
- Else → `PlanDist.CurrentTerm()`.

### `(n *ProdNode) CaptureLeadershipToken() pkgcluster.LeadershipToken`
**Fencing pattern** for split-brain prevention. Captures current leader state + term atomically.
- No Raft → returns `{Leader: true, Term: 0}` (always valid).

### `(n *ProdNode) ValidateLeadershipToken(token) bool`
Checks if a previously captured token is still current.
- No Raft → always returns `token.Valid()`.

### `(n *ProdNode) LeaderID() pkgnode.ID`
- If Raft configured and self is Raft leader → returns self ID.
- If Raft configured but not leader → returns Raft's `LeaderAddr()` (Raft is authoritative).
- No Raft → falls back to `Membership.LeaderID()` (single-node mode only).
- **WARNING:** Membership uses lowest-ID heuristic, not consensus. Multi-node MUST use Raft.

### `(n *ProdNode) Ready(ctx) error`
Returns `fmt.Errorf("leader not initialized")` if `IsLeader() && PlanDist.CurrentTerm() == 0`.

---

## Execution

### `(n *ProdNode) Execute(ctx, req) (*ExecuteResponse, error)`
Thin wrapper: calls `executeAll(ctx, req.Body)`, returns first result.

---

### `(n *ProdNode) executePlan(ctx, plan, body) ([]byte, error)`

**Flow:**
1. Extracts service name map from plan via `bridge.PlanServices`.
2. Generates `execID` (UUID).
3. Creates cancellable context, registers in `ExecRegistry`.
4. Creates `execstate.State` with `StatusCreated`, saves to `StateStore` (failure = warning).
5. Calls `runSteps(execCtx, execID, plan, names, nil, nil, st)`.
6. On success: saves `StatusCompleted` + output.
7. On failure: saves `StatusFailed` + error.
8. Defers `Execs.Unregister(execID)` + `cancel()`.

**Edge Cases:**
- `StateStore.Create` failure → warning, execution continues.
- `StateStore.Save` failure → warning, execution continues.

---

### `(n *ProdNode) runSteps(ctx, execID, plan, names, startCtx, startResp, st) ([]byte, error)`

**Flow (step loop, max 1000 iterations):**
1. Check `ctx.Done()` → if cancelled, `tryCompensate` + return error.
2. Call `bridge.ExecuteStep(plan, ctxBytes, respBytes, nil)`.
3. Handle `out.Result`:
   - **StepDone**: Record `exec.completed`, clear saga, return output.
   - **StepPending**: Save `StatusWaitingForService` + pending svc/body/ctx. Parse compensation info → register saga step. Call `callService(svcName, method, body, timeoutMs)`. On success: save `StatusRunning`, clear pending, set `respBytes`.
   - **StepContinue**: Clear `respBytes`, save `StatusRunning`.
   - **Default**: `tryCompensate` + return "unexpected step result".
4. On any error from `bridge.ExecuteStep` or `callService`: `tryCompensate` + return error.
5. If loop exhausts: `tryCompensate` + return "exceeded max steps".

**Edge Cases:**
- Context cancellation at any step triggers compensation.
- `callService` failure → compensation before returning error.
- `bridge.ExecuteStep` returning unexpected result code → compensation + error.

---

### `(n *ProdNode) executeAll(ctx, body) ([][]byte, error)`

**Flow:**
1. `Engine.ActivePlanBytes()` — get all active plans.
2. If no plans → return nil.
3. Type-assert Scheduler to `*scheduler.Scheduler`.
4. Create results slice + channel.
5. For each plan: acquire semaphore slot → spawn goroutine → create `scheduler.Task` → `sched.EnqueueAndWait(ctx, task)`.
6. Collect results; first error cancels all others.

**Edge Cases:**
- Semaphore capacity 16 limits total concurrent executions.
- Context cancellation → returns `ctx.Err()`.
- Scheduler type assertion failure → error.

---

### `(n *ProdNode) handleIncomingMessage(ctx, msg) ([]byte, error)`

**Flow:**
1. `RateLimiter.Allow("ingress")` → if denied: record error, DLQ entry, return nil.
2. `Dedup.CheckAndMark(hash)` → if duplicate: record skip, return nil.
3. Create 30s timeout context.
4. `executeAll(execCtx, msg)`.
5. On error: record error, DLQ entry, return error.
6. Return first result.

**Edge Cases:**
- Rate limited → message goes to DLQ, no error returned to caller.
- Duplicate → silently dropped.
- No active plans → nil, nil.

---

### `(n *ProdNode) callService(ctx, svcName, method, body, timeoutMs) ([]byte, error)`

**Flow:**
1. Record `exec.svc_call`.
2. Set timeout: `timeoutMs` if > 0, else 10s default.
3. Get/create circuit breaker per service (threshold=5, recovery=30s).
4. `cb.Allow()` → if open: record error, return error.
5. `Registry.LookupInstance(svcName, method)` → if error: `cb.Failure()`, return error.
6. If `inst == nil` → passthrough (return body unchanged).
7. `serviceCaller.CallService(ctx, inst, method, body, cb, Registry)`.

**Edge Cases:**
- Circuit breaker: 5 failures within 30s → opens for 30s.
- Service not found in registry → passthrough (returns original body).
- Registry lookup failure → circuit breaker records failure.

---

## Service Caller

### `NewServiceCaller() *ServiceCaller`
HTTP client: 30s timeout, 100 max idle conns, 10 per host, 90s idle timeout. Empty gRPC/TCP connection maps.

### `NewServiceCallerWithTLS(certFile, keyFile) *ServiceCaller`
Same HTTP config, plus stores TLS cert/key paths for gRPC connections.

### `(sc *ServiceCaller) CallService(ctx, inst, method, body, cb, reg) ([]byte, error)`

**Flow:**
1. Validate `inst` not nil.
2. `validateServiceName(inst.Name)` — regex `^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$`.
3. `validateMethodName(method)` — regex `^[a-zA-Z0-9][a-zA-Z0-9/_-]{0,255}$`.
4. Check `len(body) <= 10MB`.
5. Switch on `inst.Endpoint.Protocol`:
   - `ProtocolHTTP` → `callHTTP`
   - `ProtocolGRPC` → `callGRPC`
   - `ProtocolTCP` → `callTCP`
   - Default → error "unsupported protocol".

**Edge Cases:**
- Invalid service/method name → immediate error (no network call).
- Body > 10MB → immediate error.
- Nil instance → error.

---

### `(sc *ServiceCaller) callHTTP(ctx, inst, method, body, cb, reg)`

**Flow:**
1. Build URL: `http://addr:port/method`.
2. POST with `Content-Type: application/json`, `X-Service-Name`, `X-Method` headers.
3. If HTTP error → `cb.Failure()`, return error.
4. If status ≥ 500 → drain body, `cb.Failure()`, `reg.MarkUnhealthy`, return error.
5. Read response body, `cb.Success()`, return body.

**Edge Cases:**
- 5xx → marks service instance as unhealthy in registry.
- Connection error → circuit breaker records failure.
- 4xx → treated as success (service logic error, not infrastructure).

---

### `(sc *ServiceCaller) callGRPC(ctx, inst, method, body, cb, reg)`

**Returns an error** if gRPC transport is not implemented. Does NOT silently fall back to HTTP.

**Edge Cases:**
- gRPC connection failure → evicts cached connection, returns error.
- gRPC not implemented → returns error immediately (no HTTP downgrade).

---

### `(sc *ServiceCaller) callTCP(ctx, inst, method, body, cb, reg)`

**Flow:**
1. Get/create TCP connection pool (capacity 5) for `addr:port`.
2. Get connection from pool (creates new if empty).
3. Set deadline from context (or 30s default).
4. Write: `[4-byte BE length][method+body]`.
5. Read: `[4-byte BE length][response body]`.
6. Reset deadline, return connection to pool.

**Edge Cases:**
- Dead connection detected via `isConnAlive` (zero-byte read test) → discarded, new connection created.
- Response > 10MB → connection closed, circuit breaker failure.
- Write/read error → connection closed (not returned to pool), circuit breaker failure.
- Pool full → excess connections are closed.

---

## Recovery

### `(n *ProdNode) tryCompensate(execID string)`
Calls `Saga.Compensate(execID)` if saga tracker exists. Logs error on failure.

### `(n *ProdNode) recoverInFlight(ctx)`
**Flow:**
1. If no `StateStore` → return.
2. `StateStore.ListByStatus(ctx, StatusRunning, StatusWaitingForService)`.
3. If empty → return.
4. Spawn goroutines (max concurrency 8) to `recoverExecution` each.

**Edge Cases:**
- No state store → no-op.
- Recovery goroutines bounded by semaphore of 8.

### `(n *ProdNode) recoverExecution(ctx, st)`
**Flow:**
1. Extract service names from `st.PlanBytes`.
2. If `StatusWaitingForService`:
   - Parse pending service/method from names map.
   - Re-call the pending service.
   - If fail → mark `StatusFailed`, save, return.
   - If success → set `startResp`, mark `StatusRunning`, clear pending.
3. Call `runSteps(ctx, st.ID, st.PlanBytes, names, st.CtxBytes, startResp, st)`.
4. On success → `StateStore.Delete`.
5. On failure → mark `StatusFailed`, save.

**Edge Cases:**
- Recovery uses `context.Background()` (not request-scoped).
- Failed recovery retry → execution marked as failed permanently.

---

## HTTP Handlers

### `(n *ProdNode) handleHealth(w, r)`
Returns `{"status":"ok","node_id":"..."}`.

### `(n *ProdNode) handleReadyz(w, r)`
Checks `RaftCluster.IsLeader()` (or true if no cluster) + `PlanDist != nil`.

### `(n *ProdNode) handleMetrics(w, r)`
Returns `MetricsCollector.Snapshot()` as JSON.

### `(n *ProdNode) handleClusterJoin(w, r)`
POST with `{"node_id":"...","address":"..."}` → `Membership.Add(id, addr)`. Requires cluster auth.

### `(n *ProdNode) handleDeleteExecution(w, r)`
`DELETE /exec/{id}` → cancels execution + `StateStore.Delete`.

### `(n *ProdNode) handleListExecutions(w, r)`
`GET /executions?status=...` → `StateStore.ListByStatus`.

### `(n *ProdNode) handleListPartitions(w, r)`
`GET /partitions` → `Partitions.Assignments()`.

### `(n *ProdNode) handleRebalance(w, r)`
`POST /rebalance` → `Rebalancer.CheckAndRebalance()`.

---

## Config

### `DefaultConfig() *Config`
Environment-based defaults:
- `FLOWRULZ_NODE_ID` → `"node-1"`
- `FLOWRULZ_HTTP_PORT` → `8080`
- `FLOWRULZ_GRPC_PORT` → `9090`
- `FLOWRULZ_DATA_DIR` → `./data`
- `FLOWRULZ_KAFKA_BROKERS` → empty (in-memory transport)
- Various timeout/capacity defaults.

### Config Methods
| Method | Default |
|---|---|
| `ExecDir()` | `{DataDir}/exec` |
| `DLQDir()` | `{DataDir}/dlq` |
| `GRPCListenAddr()` | `{BindHost}:{GRPCPort}` |
| `HTTPListenAddr()` | `{BindHost}:{HTTPPort}` |
| `ReplyRouterCleanupInterval()` | 1s |
| `ReplyRouterMaxPending()` | 10000 |
| `DedupCapacity()` | 10000 |
| `DedupTTL()` | 5m |
| `DLQMaxEntries()` | 10000 |
| `RegistryHeartbeatTimeout()` | 30s |
| `NumPartitions()` | 16 |
| `HasTLS()` | `TLSCertFile != ""` |
| `AdvertiseHost()` | `AdvertiseHost` or `BindHost` |

---

## ExecRegistry

Thread-safe map of `execID → {cancelFunc, timestamp}`.

| Method | Description |
|---|---|
| `NewExecRegistry()` | Creates empty registry. |
| `Register(id, cancel, name)` | Stores cancel func with timestamp. |
| `Unregister(id)` | Removes entry. |
| `Cancel(id) bool` | Calls cancel, removes entry. Returns true if found. |
| `CancelAll()` | Cancels all in-flight. |
| `List() map[string]time.Time` | Snapshot of all entries. |
| `Len() int` | Count of entries. |

---

## Factory

### `DefaultDependencies(cfg) Dependencies`
Creates all default dependencies with sensible defaults:
- `Engine`: persisted to `{DataDir}/rules.json`
- `Scheduler`: 3 lanes (Fast/Normal/Heavy)
- `ReplyRouter`: 1s cleanup, 10k max pending
- `PlanDist`: default plan topic
- `Membership`: default lease
- `Partitions`: 16 partitions
- `Rebalancer`: checks on leader change
- `Registry`: 30s heartbeat timeout
- `DLQ`: 10k entries, disk persistence
- `RateLimiter`: default buckets
- `Dedup`: 10k capacity, 5m TTL
- `Saga`: with disk persistence
- `StateStore`: `{DataDir}/exec`
- `Metrics`: default collector
- `OtelExporter`: from config

---

## Messages

| Handler | Topic | Action |
|---|---|---|
| `handleNodeDiscoveryMessage` | discovery | `Membership.Heartbeat(id, addr)` |
| `handlePlanMessage` | plan | `Engine.AddVersion` or `Engine.Promote` |
| `handleAckMessage` | ack | Logs acknowledgment |
| `handlePartitionMessage` | partition | `Partitions.HandleAssignmentMessage` |

---

## Lifecycle Hooks

### `configureEngineHooks()`
Sets `Engine.AfterDeploy` → `distributePlan` (publishes plan bytes to plan topic).
Sets `Engine.AfterPromote` → `distributeActivate` (publishes activate message to plan topic).

### `distributePlan(id, dsl, plan, version)`
Checks `IsLeader()` → if yes, creates `PlanDist.PlanMessage`, marshals, sends via plan producer.

### `distributeActivate(id, version)`
Checks `IsLeader()` → if yes, creates `PlanDist.ActivateMessage`, sends via plan producer.

---

## Cluster

### `startCluster(ctx)`
If `RaftCluster` configured: initializes Raft, starts `ClusterNode` (gossip), calls `joinRaftCluster`.

### `joinRaftCluster(ctx)`
Auto-joins if `FLOWRULZ_RAFT_JOIN` env is set (format: `nodeID@address`).

### `nextDeployTerm() uint64`
Returns `PlanDist.IncDeployTerm()` — atomic increment for deploy ordering.

### `MakeProducerFromCluster(topic, clusterNode, kafkaCfg) MessageProducer`
Returns Kafka producer if brokers configured, else ClusterProducer (gossip-based), else in-memory no-op.

---

## Types

### `NodeDiscoveryMessage`
```go
type NodeDiscoveryMessage struct {
    NodeID  string `json:"node_id"`
    Address string `json:"address"`
}
```
Published to `_flowrulz_members` topic for gossip-based node discovery.
