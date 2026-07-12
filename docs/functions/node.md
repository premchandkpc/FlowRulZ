# `server/internal/node` — Complete API Reference

All exported and internal-but-critical functions, methods, and types in the `node` package.

---

## Table of Contents

1. [Core Lifecycle](#core-lifecycle)
2. [Identity & Leadership](#identity--leadership)
3. [Execution](#execution)
4. [Service Caller](#service-caller)
5. [Recovery](#recovery)
6. [HTTP Handlers](#http-handlers)
7. [Config](#config)
8. [ExecRegistry](#execregistry)
9. [Factory](#factory)
10. [Messages](#messages)
11. [Lifecycle (Internal)](#lifecycle-internal)
12. [Cluster (Internal)](#cluster-internal)
13. [Internal Helpers](#internal-helpers)

---

## Core Lifecycle

### `NewNode`

```go
func NewNode(cfg Config, deps Dependencies) *ProdNode
```

**Flow:**

1. Allocates `ProdNode` with config, HTTP address, 10s `http.Client`, empty consumer/producer slices, and `execSem` channel sized to `executeAllSemaphore` (16).
2. Copies all `Dependencies` fields into the node.
3. Checks `cfg.HasTLS()` — if true, creates `NewServiceCallerWithTLS`; otherwise `NewServiceCaller`.
4. Creates `ExecRegistry` via `NewExecRegistry()`.
5. Calls `Registry.SetHeartbeatTimeout(cfg.RegistryHeartbeatTimeout())`.
6. Calls `configureEngineHooks()` to wire `Engine.AfterDeploy` and `Engine.AfterPromote`.
7. Loads plugins from `cfg.PluginDir` if set; otherwise from `FLOWRULZ_PLUGIN_DIR` env var.

**Edge Cases:**

- Plugin load failure logs a warning but does not abort construction.
- TLS cert/key must both be present; partial TLS config silently falls back to plaintext `ServiceCaller`.
- `NewNode` does not start any goroutines — safe to construct multiple times.

---

### `ProdNode.Start`

```go
func (n *ProdNode) Start(ctx context.Context) error
```

**Flow:**

1. Builds Kafka config from `n.config`.
2. Calls `startCluster(ctx)` — initializes gossip if `ClusterNode` is non-nil.
3. Calls `startConsumers(ctx, handler, kafkaCfg)` — creates and starts 5 consumers (input, membership, plan, ack, partition).
4. Calls `startSubsystems(ctx)` — starts PlanDist, Membership eviction, Raft, Scheduler, ReplyRouter cleanup, Dedup cleanup, recovery.
5. Calls `startGRPC()` — starts GRPCBus if configured.
6. Calls `startOTel(ctx)` — starts OTel exporter goroutine if configured.
7. Calls `serveHTTP(ctx)` — starts HTTP server in a goroutine.
8. Returns `nil`.

**Edge Cases:**

- Starts in a fixed order with no rollback on partial failure. If one subsystem fails to start, subsequent subsystems still start.
- Always returns `nil` — errors are logged, not propagated.

---

### `ProdNode.Shutdown`

```go
func (n *ProdNode) Shutdown(ctx context.Context) error
```

**Flow:**

1. `n.Execs.CancelAll()` — cancels all in-flight executions.
2. Stops all consumers, nils the slice.
3. `n.PlanDist.Stop()`, `n.Scheduler.Stop()`, `n.ReplyRouter.StopCleanup()`.
4. Closes all producers, nils the slice.
5. HTTP server shutdown with 5-second timeout (via `context.WithTimeout`).
6. Nil-checks and stops: `ClusterNode`, `GRPCBus`, `OtelExporter`, `RaftCluster`, `StateStore`, `serviceCaller`.
7. Logs shutdown complete.

**Edge Cases:**

- Continues shutdown even if individual stops fail. Errors are logged, not returned.
- HTTP shutdown uses a fresh `context.Background()` with 5s timeout — does not inherit from caller's context.
- `RaftCluster.Stop()` receives the caller's `ctx`, so Raft shutdown can respect external deadlines.

---

## Identity & Leadership

### `ProdNode.ID`

```go
func (n *ProdNode) ID() pkgnode.ID
```

Returns the node ID as `pkgnode.ID`. Pure accessor.

---

### `ProdNode.Addr`

```go
func (n *ProdNode) Addr() string
```

Returns the HTTP listen address (e.g. `:8080`). Pure accessor.

---

### `ProdNode.IsLeader`

```go
func (n *ProdNode) IsLeader() bool
```

**Flow:**

1. If `RaftCluster` is non-nil, delegates to `RaftCluster.IsLeader()`.
2. If `RaftCluster` is nil, returns `true` (single-node mode).

---

### `ProdNode.CurrentTerm`

```go
func (n *ProdNode) CurrentTerm() uint64
```

**Flow:**

1. If `RaftCluster` is non-nil, returns `RaftCluster.CurrentTerm()`.
2. Otherwise falls back to `PlanDist.CurrentTerm()`.

---

### `ProdNode.CaptureLeadershipToken`

```go
func (n *ProdNode) CaptureLeadershipToken() pkgcluster.LeadershipToken
```

**Fencing pattern:** Captures the current leader state + term so that later `ValidateLeadershipToken` can detect leadership changes.

**Flow:**

1. If `RaftCluster` is non-nil, delegates to `RaftCluster.CaptureLeadershipToken()`.
2. Otherwise returns `{Leader: true, Term: 0}` — always leader in single-node mode.

**Edge Cases:**

- The returned token is a snapshot; it can become stale if leadership changes between capture and validate.

---

### `ProdNode.ValidateLeadershipToken`

```go
func (n *ProdNode) ValidateLeadershipToken(token pkgcluster.LeadershipToken) bool
```

**Flow:**

1. If `RaftCluster` is non-nil, delegates to `RaftCluster.ValidateLeadershipToken(token)`.
2. Otherwise returns `token.Valid()` — always valid in single-node mode.

---

### `ProdNode.LeaderID`

```go
func (n *ProdNode) LeaderID() pkgnode.ID
```

**Flow:**

1. If `RaftCluster` is non-nil AND `RaftCluster.IsLeader()` is true, returns `pkgnode.ID(n.nodeID)`.
2. Otherwise delegates to `Membership.LeaderID()`.

---

### `ProdNode.Ready`

```go
func (n *ProdNode) Ready(ctx context.Context) error
```

**Flow:**

1. If node is leader AND `PlanDist.CurrentTerm() == 0`, returns `fmt.Errorf("leader not initialized")`.
2. Otherwise returns `nil`.

**Edge Cases:**

- A leader with term 0 means PlanDist has never been promoted — the node cannot serve requests yet.
- Non-leaders always return ready (followers proxy to leader).

---

## Execution

### `ProdNode.Execute`

```go
func (n *ProdNode) Execute(ctx context.Context, req *pkgnode.ExecuteRequest) (*pkgnode.ExecuteResponse, error)
```

**Flow:**

1. Calls `n.executeAll(ctx, req.Body)`.
2. If error, returns `ExecuteResponse{Error: err.Error()}` + error.
3. If no results, returns empty `ExecuteResponse{}`.
4. Otherwise returns `ExecuteResponse{Body: out[0]}` — first plan's result.

**Edge Cases:**

- Only the first plan's result is returned to the caller. Concurrent plan results are discarded in the response (but side effects still execute).

---

### `ProdNode.executePlan`

```go
func (n *ProdNode) executePlan(ctx context.Context, plan []byte, body []byte) ([]byte, error)
```

**Flow:**

1. Calls `bridge.PlanServices(plan)` to extract service name map (`uint16 → string`).
2. Generates UUID `execID`.
3. Creates cancellable context, registers in `ExecRegistry`.
4. Creates `execstate.State` with status `StatusCreated`.
5. If `StateStore` is non-nil, calls `StateStore.Create()` — failure is a warning only.
6. Calls `n.runSteps(...)`.
7. On completion: saves final status (`StatusCompleted` or `StatusFailed`) to `StateStore`.
8. Defers `ExecRegistry.Unregister(execID)` and `cancel()`.

**Edge Cases:**

- State store create failure does not abort execution — only a warning is logged.
- State store save failure on completion is also a warning only.
- The `cancel()` is called both in defer and potentially by the caller via `ExecRegistry.Cancel()` — safe because `context.CancelFunc` is idempotent.

---

### `ProdNode.runSteps`

```go
func (n *ProdNode) runSteps(ctx context.Context, execID string, plan []byte, names map[uint16]string, startCtx, startResp []byte, st *execstate.State) ([]byte, error)
```

**Flow (step loop, max 1000 iterations):**

1. Each iteration: checks `ctx.Done()` → calls `bridge.ExecuteStep(plan, ctxBytes, respBytes, nil)`.
2. On `StepDone`: records metric, clears saga, returns output.
3. On `StepPending`:
   - Saves state to `StateStore` with `StatusWaitingForService`.
   - Resolves service name from `names` map or fallback `svc-{id}`.
   - Parses compensation info via `bridge.ParseCompensation()`.
   - Registers saga step if compensation is available.
   - Calls `n.callService(...)`.
   - On service error: calls `tryCompensate()` then returns error.
   - On success: saves state back to `StatusRunning`, clears pending fields.
4. On `StepContinue`: resets `respBytes` to nil, saves state.
5. On unexpected result: calls `tryCompensate()` then returns error.

**Edge Cases:**

- **Context cancellation:** Detected at the top of each loop iteration. Triggers compensation before returning.
- **Max steps exceeded:** Returns `fmt.Errorf("execution exceeded max steps")` after compensation.
- **Unexpected step result:** Treated as error (ctx truncated or invalid).
- **Service call failure:** Always compensates before returning.
- `startCtx`/`startResp` parameters allow resuming from a checkpoint (used by recovery).

---

### `ProdNode.executeAll`

```go
func (n *ProdNode) executeAll(ctx context.Context, body []byte) ([][]byte, error)
```

**Flow:**

1. Gets all active plan bytes from `Engine.ActivePlanBytes()`.
2. Returns `nil, nil` if no plans.
3. Type-asserts `Scheduler` to concrete `*scheduler.Scheduler`.
4. Creates cancellable context, results slice, and result channel.
5. For each plan: acquires semaphore slot (`n.execSem`), spawns goroutine.
6. Each goroutine creates a `scheduler.Task` with `PriorityNormal`, calls `sched.EnqueueAndWait()`.
7. Collects all results. On first error: cancels all other plans.

**Edge Cases:**

- **No active plans:** Returns `nil, nil` — not an error.
- **Scheduler unavailable:** Returns error if `Scheduler` cannot be type-asserted to `*scheduler.Scheduler`.
- **Concurrency limit:** `execSem` channel (size 16) bounds total concurrent plan executions node-wide.
- **First error cancels:** `cancel()` is called on the first error, propagating to all in-flight plans.
- Semaphore is always released via `defer func() { <-n.execSem }()`.

---

### `ProdNode.handleIncomingMessage`

```go
func (n *ProdNode) handleIncomingMessage(ctx context.Context, msg []byte) ([]byte, error)
```

**Flow:**

1. **Rate limit:** `n.RateLimiter.Allow("ingress")` → if denied: records metric, sends to DLQ with `msgID = common.HashBodyPrefixed("rl", msg)`, returns `nil, nil`.
2. **Dedup:** `n.Dedup.CheckAndMark(msgIDStr)` → if duplicate: records metric, returns `nil, nil`.
3. Creates 30-second timeout context.
4. Calls `n.executeAll(execCtx, msg)`.
5. On error: records metric, sends to DLQ with error, returns error.
6. On no results: returns `nil, nil`.
7. Returns `results[0]`.

**Edge Cases:**

- Rate-limited messages go to DLQ and return `nil, nil` (not an error to the consumer).
- Duplicate messages return `nil, nil` (silently dropped).
- 30-second timeout is hardcoded via `defaultExecTimeout`.

---

### `ProdNode.callService`

```go
func (n *ProdNode) callService(ctx context.Context, svcName, method string, body []byte, timeoutMs uint64) ([]byte, error)
```

**Flow:**

1. Records `svc_call` metric.
2. Sets timeout: `timeoutMs` if > 0, else 10 seconds.
3. Loads-or-stores per-service circuit breaker (threshold=5, recovery=30s) via `sync.Map`.
4. Checks `cb.Allow()` — if circuit open: records metric, logs warning, returns error.
5. Calls `n.Registry.LookupInstance(svcName, method)`.
6. On lookup error: `cb.Failure()`, returns error.
7. If `inst == nil`: logs passthrough, returns `body` unchanged (no service to call).
8. Calls `n.serviceCaller.CallService(...)`.

**Edge Cases:**

- **Circuit open:** Returns error immediately, does not call service.
- **Registry lookup fail:** Calls `cb.Failure()` before returning error.
- **Nil instance (passthrough):** Returns the input body directly — allows pipelines with no registered service to complete without error.
- **Per-service breakers:** Each service gets its own `CircuitBreaker` stored in `sync.Map` (thread-safe).

---

## Service Caller

### `NewServiceCaller`

```go
func NewServiceCaller() *ServiceCaller
```

Creates an HTTP client with 30s timeout, 100 max idle conns, 10 per host, 90s idle timeout. Initializes empty gRPC and TCP connection maps.

---

### `NewServiceCallerWithTLS`

```go
func NewServiceCallerWithTLS(certFile, keyFile string) *ServiceCaller
```

Same as `NewServiceCaller` but stores TLS cert/key paths for gRPC connections.

---

### `ServiceCaller.CallService`

```go
func (sc *ServiceCaller) CallService(ctx context.Context, inst *registry.ServiceInstance, method string, body []byte, cb *reliability.CircuitBreaker, reg *registry.ServiceRegistry) ([]byte, error)
```

**Flow:**

1. Validates `inst` is non-nil.
2. Validates `inst.Name` against `^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$`.
3. Validates `method` against `^[a-zA-Z0-9][a-zA-Z0-9/_-]{0,255}$`.
4. Checks `len(body) <= 10*1024*1024` (10 MB).
5. Dispatches by protocol: `callHTTP`, `callGRPC`, or `callTCP`.

**Edge Cases:**

- Nil instance → `fmt.Errorf("nil service instance")`.
- Invalid service name → error with regex hint.
- Invalid method name → error with regex hint.
- Body > 10 MB → error with byte count.
- Unsupported protocol → error with protocol name.

---

### `ServiceCaller.callHTTP`

```go
func (sc *ServiceCaller) callHTTP(ctx context.Context, inst *registry.ServiceInstance, method string, body []byte, cb *reliability.CircuitBreaker, reg *registry.ServiceRegistry) ([]byte, error)
```

**Flow:**

1. Constructs URL: `http://{addr}:{port}/{method}`.
2. Creates POST request with `Content-Type: application/json`, `X-Service-Name`, `X-Method` headers.
3. Executes request.
4. On connection error: `cb.Failure()`, `reg.MarkUnhealthy()`, returns error.
5. On 5xx status: reads/discard body, `cb.Failure()`, `reg.MarkUnhealthy()`, returns error.
6. On success: reads body, `cb.Success()`, returns body.

**Edge Cases:**

- Connection error: marks instance unhealthy + circuit breaker failure.
- 5xx: marks instance unhealthy + circuit breaker failure.
- 4xx: returns body as-is with `cb.Success()` — client errors are not circuit-breaker failures.
- Response body is read fully into memory.

---

### `ServiceCaller.callGRPC`

```go
func (sc *ServiceCaller) callGRPC(ctx context.Context, inst *registry.ServiceInstance, method string, body []byte, cb *reliability.CircuitBreaker, reg *registry.ServiceRegistry) ([]byte, error)
```

**Flow:**

1. Gets/creates cached gRPC connection via `getGRPCConn(addr)`.
2. On connection failure: `cb.Failure()`, `evictGRPCConn(addr)`, returns error.
3. Logs warning and **falls back to `callHTTP`** — gRPC unary calls are not yet implemented.

**Edge Cases:**

- TLS cert load failure → connection fails.
- Connection failure → evicts cached conn, forcing reconnect on next call.
- Current behavior is HTTP fallback with a warning log.

---

### `ServiceCaller.callTCP`

```go
func (sc *ServiceCaller) callTCP(ctx context.Context, inst *registry.ServiceInstance, method string, body []byte, cb *reliability.CircuitBreaker, reg *registry.ServiceRegistry) ([]byte, error)
```

**Flow:**

1. Gets TCP connection pool for `addr`.
2. Gets a connection from pool (health-checks stale conns, dials new if needed).
3. Sets deadline from context or 30s fallback.
4. Writes length-prefixed message: `[4-byte BE length][method + body]`.
5. Reads length-prefixed response: `[4-byte BE length][response body]`.
6. If `respLen > 10MB`: closes conn, `cb.Failure()`, returns error.
7. Resets deadline, returns conn to pool, `cb.Success()`.

**Edge Cases:**

- **Dial failure:** `cb.Failure()`, marks unhealthy.
- **Write/read error:** Closes connection (does not return to pool), `cb.Failure()`.
- **Response > 10 MB:** Closes connection, `cb.Failure()`.
- **Pool full (5 conns):** Excess connections are closed immediately.
- **Stale connections:** Health-checked via 1-byte read with zero deadline before reuse.

---

### `ServiceCaller.getGRPCConn`

```go
func (sc *ServiceCaller) getGRPCConn(addr string) (*grpc.ClientConn, error)
```

**Flow:**

1. Locks `grpcConnsMu`.
2. Returns cached connection if exists.
3. If TLS configured: loads X509 key pair, creates TLS credentials with min version TLS 1.2.
4. Otherwise: creates insecure credentials.
5. Calls `grpc.NewClient(addr, opts...)`, caches and returns.

**Edge Cases:**

- TLS cert load failure returns error without caching.
- Connections are cached indefinitely until evicted or closed.

---

### `ServiceCaller.evictGRPCConn`

```go
func (sc *ServiceCaller) evictGRPCConn(addr string)
```

Removes and closes a cached gRPC connection. Thread-safe. No-op if address not cached.

---

### `ServiceCaller.Close`

```go
func (sc *ServiceCaller) Close()
```

Closes all cached gRPC connections and all TCP connection pools. Thread-safe.

---

## Recovery

### `ProdNode.tryCompensate`

```go
func (n *ProdNode) tryCompensate(execID string)
```

**Flow:**

1. If `Saga` is nil, returns immediately.
2. Calls `n.Saga.Compensate(execID)`.
3. On error, logs error.

**Edge Cases:**

- Called on every execution error path (step failure, cancellation, max steps).
- No-op if no saga tracker is configured.

---

### `ProdNode.recoverInFlight`

```go
func (n *ProdNode) recoverInFlight(ctx context.Context)
```

**Flow:**

1. If `StateStore` is nil, returns (no-op).
2. Lists all states with `StatusRunning` or `StatusWaitingForService`.
3. If empty, returns.
4. Spawns goroutines with concurrency limit of 8 (`maxRecoveryConcurrency`).
5. Each goroutine calls `recoverExecution(ctx, st)`.
6. Waits for all goroutines to complete.

**Edge Cases:**

- **No StateStore:** Complete no-op — works in ephemeral mode.
- **Empty list:** No-op.
- **Concurrency limit:** 8 simultaneous recoveries max.
- Called during `startSubsystems()` — runs before the node starts accepting new work.

---

### `ProdNode.recoverExecution`

```go
func (n *ProdNode) recoverExecution(ctx context.Context, st *execstate.State)
```

**Flow:**

1. Extracts service names from `st.PlanBytes` via `bridge.PlanServices()`.
2. If `st.Status == StatusWaitingForService`:
   - Resolves pending service name.
   - Calls `bridge.ParseServiceMethod(rawName)`.
   - Re-calls the pending service via `n.callService(...)`.
   - On failure: marks `StatusFailed`, saves, returns.
   - On success: updates state to `StatusRunning`, clears pending, saves.
3. Calls `n.runSteps(...)` with `st.CtxBytes` as resume context.
4. On failure: marks `StatusFailed`, saves.
5. On success: deletes from `StateStore`.

**Edge Cases:**

- **Re-call failure:** Marks execution as failed — does not retry indefinitely.
- **Successful recovery:** Deletes from state store (execution is complete).
- Uses `context.Background()` for recovery operations — does not inherit shutdown context.
- Recovery always uses the original plan bytes from the state store.

---

## HTTP Handlers

### `ProdNode.registerHandlers`

```go
func (n *ProdNode) registerHandlers(mux *http.ServeMux)
```

**Registered Routes:**

| Method | Path | Handler |
|--------|------|---------|
| POST | `/cluster/join` | `requireClusterAuth(handleClusterJoin)` |
| GET | `/health` | `handleHealth` |
| GET | `/readyz` | `handleReadyz` |
| GET | `/metrics` | `handleMetrics` |
| DELETE | `/executions/{id}` | `handleDeleteExecution` |
| GET | `/executions` | `handleListExecutions` |
| GET | `/partitions` | `handleListPartitions` |
| POST | `/partitions/rebalance` | `handleRebalance` |

---

### `ProdNode.requireClusterAuth`

```go
func (n *ProdNode) requireClusterAuth(next http.HandlerFunc) http.HandlerFunc
```

**Flow:**

1. Reads `FLOWRULZ_API_KEY` env var. If empty, returns 401.
2. Reads `Authorization` header.
3. Compares via `crypto/subtle.ConstantTimeCompare` to `"Bearer {apiKey}"`.
4. On mismatch, returns 401.
5. Otherwise, calls `next(w, r)`.

**Edge Cases:**

- API key from env var — no config file lookup.
- Constant-time comparison prevents timing attacks.
- If `FLOWRULZ_API_KEY` is not set, all cluster auth is denied (fail-closed).

---

### `ProdNode.handleClusterJoin`

```go
func (n *ProdNode) handleClusterJoin(w http.ResponseWriter, r *http.Request)
```

**Flow:**

1. Returns 400 if `RaftCluster` is nil.
2. Reads body with 1 MB max.
3. Decodes JSON `{node_id, raft_addr}`.
4. Validates: `node_id` non-empty, ≤ 128 chars; `raft_addr` non-empty, valid `host:port`.
5. Rejects localhost/127.0.0.1/::1 addresses.
6. Calls `n.RaftCluster.Join(...)`.
7. On success, returns `{"status": "joined"}`.

**Edge Cases:**

- Localhost addresses rejected with hint to set `AdvertiseAddr`.
- Port "0" is explicitly rejected.
- Join failure returns 500 (internal error) — no retry.

---

### `ProdNode.handleHealth`

```go
func (n *ProdNode) handleHealth(w http.ResponseWriter, r *http.Request)
```

Returns JSON: `{"status": "ok", "node_id": "...", "is_leader": true/false, "term": N}`. Always 200.

---

### `ProdNode.handleReadyz`

```go
func (n *ProdNode) handleReadyz(w http.ResponseWriter, r *http.Request)
```

**Flow:**

1. If node is leader AND `PlanDist.CurrentTerm() == 0`: returns 503 with `{"status": "not ready", "reason": "leader not initialized"}`.
2. Otherwise returns 200 with `{"status": "ready"}`.

---

### `ProdNode.handleMetrics`

```go
func (n *ProdNode) handleMetrics(w http.ResponseWriter, r *http.Request)
```

**Flow:**

1. Takes snapshot from `MetricsCollector.Snapshot()`.
2. Augments with runtime gauges: `pending_requests`, `dlq_size`, `inflight_execs`.
3. Returns as JSON.

---

### `ProdNode.handleDeleteExecution`

```go
func (n *ProdNode) handleDeleteExecution(w http.ResponseWriter, r *http.Request)
```

**Flow:**

1. Reads `{id}` from path.
2. Calls `n.Execs.Cancel(id)`.
3. If found: 202 with `{"status": "cancelling", "id": "..."}`.
4. If not found: 404.

---

### `ProdNode.handleListExecutions`

```go
func (n *ProdNode) handleListExecutions(w http.ResponseWriter, r *http.Request)
```

Returns `ExecRegistry.List()` as JSON — map of `{exec_id: start_time}`.

---

### `ProdNode.handleListPartitions`

```go
func (n *ProdNode) handleListPartitions(w http.ResponseWriter, r *http.Request)
```

**Flow:**

1. Gets partition assignments from `Partitions.Assignments()`.
2. Builds per-node partition list from `Membership.AliveNodes()`.
3. Returns JSON: `{num_partitions, assignments, node_partitions}`.

---

### `ProdNode.handleRebalance`

```go
func (n *ProdNode) handleRebalance(w http.ResponseWriter, r *http.Request)
```

**Flow:**

1. Captures leadership token → validates → returns 403 if not leader.
2. Calls `Partitions.Rebalance(aliveNodes, token.Term)`.
3. Re-validates leadership before publishing.
4. Publishes assignments with 10s timeout.
5. Returns `{"status": "rebalanced", "assignments": N}`.

**Edge Cases:**

- Leadership lost between capture and validate → returns 409 Conflict.
- Publish failure → 500.

---

## Config

### `DefaultConfig`

```go
func DefaultConfig() *Config
```

Returns `&Config` with:

| Field | Default |
|-------|---------|
| `NodeID` | `"node-1"` |
| `HTTPAddr` | `":8080"` |
| `GRPCAddr` | `":9090"` |
| `TLSCertFile` | `FLOWRULZ_TLS_CERT` env |
| `TLSKeyFile` | `FLOWRULZ_TLS_KEY` env |
| `RaftPort` | `cluster.DefaultRaftPort` |
| `RaftDir` | `$TMPDIR/flowrulz-raft` |
| `RaftBootstrap` | `false` |
| `Topic` | `"flowrulz-input"` |
| `KafkaGroupID` | `"flowrulz"` |

---

### `Config.ExecDir`

```go
func (c *Config) ExecDir() string
```

Returns `c.ExecStateDir` if set, else `$TMPDIR/flowrulz-execstate`.

---

### `Config.DLQDir`

```go
func (c *Config) DLQDir() string
```

Returns `$TMPDIR/flowrulz-dlq`.

---

### `Config.GRPCListenAddr`

```go
func (c *Config) GRPCListenAddr() string
```

Returns `c.GRPCAddr` if set, else `DefaultGRPCAddr` (`:9090`).

---

### `Config.HTTPListenAddr`

```go
func (c *Config) HTTPListenAddr() string
```

Returns `c.HTTPAddr` if set, else `DefaultHTTPAddr` (`:8080`).

---

### `Config.ReplyRouterCleanupInterval`

```go
func (c *Config) ReplyRouterCleanupInterval() time.Duration
```

Returns `1 * time.Second`.

---

### `Config.ReplyRouterMaxPending`

```go
func (c *Config) ReplyRouterMaxPending() int
```

Returns `10000`.

---

### `Config.DedupCapacity`

```go
func (c *Config) DedupCapacity() int
```

Returns `10000`.

---

### `Config.DedupTTL`

```go
func (c *Config) DedupTTL() time.Duration
```

Returns `5 * time.Minute`.

---

### `Config.DLQMaxEntries`

```go
func (c *Config) DLQMaxEntries() int
```

Returns `10000`.

---

### `Config.RegistryHeartbeatTimeout`

```go
func (c *Config) RegistryHeartbeatTimeout() time.Duration
```

Returns `30 * time.Second`.

---

### `Config.NumPartitions`

```go
func (c *Config) NumPartitions() int
```

Returns `64`.

---

### `Config.HasTLS`

```go
func (c *Config) HasTLS() bool
```

Returns `true` if both `TLSCertFile` and `TLSKeyFile` are non-empty.

---

### `Config.AdvertiseHost`

```go
func (c *Config) AdvertiseHost() string
```

**Flow:**

1. If `AdvertiseAddr` is set, extracts and returns host.
2. Otherwise extracts host from `GRPCAddr`.
3. On parse error or empty host, returns `"localhost"`.

---

## ExecRegistry

### `NewExecRegistry`

```go
func NewExecRegistry() *ExecRegistry
```

Returns a thread-safe registry backed by `map[string]*execEntry`.

---

### `ExecRegistry.Register`

```go
func (er *ExecRegistry) Register(id string, cancel context.CancelFunc, name string)
```

Stores the cancel function with timestamp under write lock. Overwrites if ID already exists.

---

### `ExecRegistry.Unregister`

```go
func (er *ExecRegistry) Unregister(id string)
```

Deletes entry under write lock. No-op if ID not found.

---

### `ExecRegistry.Cancel`

```go
func (er *ExecRegistry) Cancel(id string) bool
```

**Flow:**

1. Under write lock: looks up entry, deletes it, calls `cancel()`.
2. Returns `true` if found, `false` otherwise.

**Edge Cases:**

- Delete happens before cancel call — prevents double-cancel races.
- Cancel is called under the lock — safe because `context.CancelFunc` is idempotent.

---

### `ExecRegistry.CancelAll`

```go
func (er *ExecRegistry) CancelAll()
```

Calls `cancel()` on every entry under write lock. Does not delete entries (callers typically nil the map afterwards).

---

### `ExecRegistry.List`

```go
func (er *ExecRegistry) List() map[string]time.Time
```

Returns a snapshot copy of all entries: `{exec_id: start_time}`.

---

### `ExecRegistry.Len`

```go
func (er *ExecRegistry) Len() int
```

Returns entry count under read lock.

---

## Factory

### `DefaultDependencies`

```go
func DefaultDependencies(cfg Config) Dependencies
```

**Flow:**

1. **Engine:** `engine.New(persistPath)` or `engine.NewWithCompiler(persistPath, compiler)` if `CompilerAddr` set.
2. **Metrics:** `observability.NewMetricsCollector()`.
3. **Scheduler:** `scheduler.New(nil)`.
4. **ReplyRouter:** configured with `ReplyRouterCleanupInterval` and `ReplyRouterMaxPending`.
5. **Dedup:** capacity=`DedupCapacity`, TTL=`DedupTTL`.
6. **RateLimiter:** `reliability.NewRateLimiter()`.
7. **ServiceRegistry:** `registry.New()`, heartbeat timeout from config.
8. **ClusterNode:** Only if `KafkaBrokers` is empty — `cluster.NewClusterNode(nodeID, grpcAddr)`.
9. **DLQ:** Creates DLQ dir, makes producer (Kafka/Cluster/memory), configures DLQ with max entries + producer + dir.
10. **Membership:** `membership.New()`.
11. **PlanDist:** With plan/ack producers and membership quorum provider.
12. **Partitions:** `partition.New(64)`, with partition producer.
13. **Rebalancer:** Notifies on alive node changes and term changes.
14. **AdminSrv:** `admin.New(eng)` or `admin.NewWithCompiler(...)`.
15. **StateStore:** `execstate.NewFileStore(execDir)` — init failure is warning only.
16. **Saga:** `reliability.NewSagaTrackerWithDir(noop_fn, execDir)`.
17. **RaftCluster:** If `RaftDir` and `RaftPort > 0` — creates `RaftCluster` and wraps in `ClusterMember`.
18. **GRPCBus:** If `GRPCAddr` set — creates with or without TLS.
19. **OtelExporter:** If `FLOWRULZ_OTEL_ENDPOINT` env set.

**Edge Cases:**

- StateStore init failure logs warning — node works in ephemeral mode.
- ClusterNode only created when no Kafka — avoids dual transport.
- RaftCluster only created with valid `RaftDir` + `RaftPort > 0`.

---

### `MakeProducerFromCluster`

```go
func MakeProducerFromCluster(topic string, clusterNode *cluster.ClusterNode, kc kafkatransport.Config) transport.MessageProducer
```

**Priority:**

1. If Kafka brokers configured → `kafkatransport.NewProducer(topic, kc)`.
2. If `clusterNode` non-nil → `cluster.NewClusterProducer(topic, clusterNode)`.
3. Otherwise → `transport.NewProducer(topic)` (in-memory).

---

## Messages

### `ProdNode.handleNodeDiscoveryMessage`

```go
func (n *ProdNode) handleNodeDiscoveryMessage(ctx context.Context, msg []byte) ([]byte, error)
```

**Flow:**

1. Unmarshals `NodeDiscoveryMessage{NodeID, Address}`.
2. Ignores self (`nd.NodeID == n.nodeID`).
3. Calls `Membership.Heartbeat(nd.NodeID, nd.Address)`.
4. If `ClusterNode` is non-nil and address is non-empty, adds peer.

**Edge Cases:**

- Unmarshal error returns `nil, nil` — does not fail consumer.
- Self-messages silently dropped.

---

### `ProdNode.handlePlanMessage`

```go
func (n *ProdNode) handlePlanMessage(ctx context.Context, msg []byte) ([]byte, error)
```

**Flow:**

1. Unmarshals `plandist.PlanMessage`.
2. Rejects messages with `pm.Term < PlanDist.CurrentTerm()` (stale term).
3. On `"plan"` type: `Engine.AddVersion(...)`, then sends ack.
4. On `"activate"` type: `Engine.Promote(...)`.

**Edge Cases:**

- Stale term messages silently dropped (logged as warning).
- Ack send failure is logged but not returned as error.
- Activate failure is logged but swallowed.

---

### `ProdNode.handleAckMessage`

```go
func (n *ProdNode) handleAckMessage(ctx context.Context, msg []byte) ([]byte, error)
```

Unmarshals `plandist.AckMessage`, calls `PlanDist.RecordAck(...)`. Always returns `nil, nil`.

---

### `ProdNode.handlePartitionMessage`

```go
func (n *ProdNode) handlePartitionMessage(ctx context.Context, msg []byte) ([]byte, error)
```

Calls `Partitions.HandleAssignmentMessage(msg)`. Errors logged. Always returns `nil, nil`.

---

## Lifecycle (Internal)

### `ProdNode.startConsumers`

```go
func (n *ProdNode) startConsumers(ctx context.Context, handler transport.MessageHandler, kc kafkatransport.Config)
```

**Flow:**

1. Creates 5 consumers via `makeConsumer`: input, membership, plan, ack, partition.
2. Appends to `n.consumers` under lock.
3. Starts each consumer in its own goroutine.

**Topics Consumed:**

| Consumer | Topic | Handler |
|----------|-------|---------|
| Input | `n.config.Topic` | `handleIncomingMessage` |
| Membership | `_flowrulz_members` | `handleNodeDiscoveryMessage` |
| Plan | `plandist.DefaultPlanTopic` | `handlePlanMessage` |
| Ack | `plandist.DefaultAckTopic` | `handleAckMessage` |
| Partition | `partition.PartitionTopic` | `handlePartitionMessage` |

---

### `ProdNode.startSubsystems`

```go
func (n *ProdNode) startSubsystems(ctx context.Context)
```

**Flow:**

1. `PlanDist.Start(ctx)`.
2. `Membership.StartEviction(ctx, DefaultHeartbeatTimeout)`.
3. Sets rebalancer notify callback with fencing pattern.
4. If `RaftCluster` non-nil: starts Raft, optionally bootstraps, subscribes to leader changes, optionally joins cluster.
5. `Scheduler.Start(ctx)`.
6. `ReplyRouter.StartCleanup(ctx)`.
7. `Dedup.StartCleanup(ctx, 30s)`.
8. `recoverInFlight(ctx)`.

**Edge Cases:**

- Raft start failure is logged, not fatal.
- Raft bootstrap failure is logged as warning.
- Leader change callback: on promotion, sets term + triggers partition onLeaderChange + rebalance. On step-down, clears partition leader.

---

### `ProdNode.startOTel`

```go
func (n *ProdNode) startOTel(ctx context.Context)
```

No-op if `OtelExporter` is nil. Otherwise starts exporter in a goroutine.

---

### `ProdNode.configureEngineHooks`

```go
func (n *ProdNode) configureEngineHooks()
```

Sets `Engine.AfterDeploy = n.handleEngineDeploy` and `Engine.AfterPromote = n.handleEnginePromote`.

---

### `ProdNode.handleEngineDeploy`

```go
func (n *ProdNode) handleEngineDeploy(id, dsl string, plan []byte, version uint64)
```

**Flow:**

1. Captures leadership token → returns if not leader.
2. Sets `PlanDist.SetTerm(token.Term)`.
3. Re-validates leadership before publishing.
4. Spawns `distributePlan(...)` in goroutine.

**Edge Cases:**

- Leadership lost between capture and validate → discards plan (logged as warning).

---

### `ProdNode.handleEnginePromote`

```go
func (n *ProdNode) handleEnginePromote(id string, version uint64)
```

**Flow:**

1. Captures leadership token → returns if not leader.
2. Re-validates leadership.
3. Spawns `distributeActivate(...)` in goroutine.

---

### `ProdNode.distributePlan`

```go
func (n *ProdNode) distributePlan(id, dsl string, plan []byte, version uint64)
```

**Flow:**

1. Creates 15s timeout context.
2. `PlanDist.PublishPlan(...)`.
3. `PlanDist.WaitForAcks(...)` with 10s timeout.
4. `PlanDist.ActivatePlan(...)`.

**Edge Cases:**

- All errors are logged, not returned (runs in background goroutine).

---

### `ProdNode.distributeActivate`

```go
func (n *ProdNode) distributeActivate(id string, version uint64)
```

Creates 10s timeout context, calls `PlanDist.ActivatePlan(...)`. Error logged.

---

## Cluster (Internal)

### `ProdNode.startCluster`

```go
func (n *ProdNode) startCluster(ctx context.Context)
```

**Flow:**

1. Returns if `ClusterNode` is nil.
2. Starts `ClusterNode`.
3. Registers gossip `OnNodeJoin` callback: calls `Membership.Heartbeat`, auto-adds peer.
4. Spawns discovery goroutine: publishes `NodeDiscoveryMessage` every 3 seconds.
5. Connects to seed nodes (skipping self).

**Edge Cases:**

- Self-address skipped in seed list.
- Discovery goroutine respects `ctx.Done()`.
- Seed connection errors logged, not fatal.

---

### `ProdNode.joinRaftCluster`

```go
func (n *ProdNode) joinRaftCluster(ctx context.Context)
```

**Flow:**

1. Constructs `raftAddr` from `AdvertiseHost()` + `RaftPort`.
2. Marshals `{node_id, raft_addr}`.
3. For each seed: POSTs to `http://{seed}/cluster/join` up to 30 times with 2s sleep.
4. Returns on first 200 response.

**Edge Cases:**

- Up to 30 retry attempts per seed, 2s between attempts.
- Respects `ctx.Done()` between retries.
- Uses advertise address (not bind address) — critical for k8s.
- Logs error if all seeds exhausted.

---

### `ProdNode.nextDeployTerm`

```go
func (n *ProdNode) nextDeployTerm() uint64
```

- If `RaftCluster` non-nil: returns `RaftCluster.CurrentTerm()`.
- Otherwise: returns `PlanDist.CurrentTerm() + 1`.

---

## Internal Helpers

### `ProdNode.makeProducer`

```go
func (n *ProdNode) makeProducer(topic string, kc kafkatransport.Config) transport.MessageProducer
```

**Priority:**

1. Kafka brokers → Kafka producer (appended to `n.producers` under lock).
2. `ClusterNode` non-nil → Cluster producer.
3. Otherwise → in-memory producer.

---

### `ProdNode.makeConsumer`

```go
func (n *ProdNode) makeConsumer(topic string, handler transport.MessageHandler, kc kafkatransport.Config) transport.MessageConsumer
```

**Priority:**

1. Kafka brokers → Kafka consumer.
2. `ClusterNode` non-nil → Cluster consumer.
3. Otherwise → in-memory consumer.

---

### `ProdNode.startGRPC`

```go
func (n *ProdNode) startGRPC()
```

No-op if `GRPCBus` is nil. Otherwise calls `GRPCBus.Start()`. Errors logged.

---

### `ProdNode.serveHTTP`

```go
func (n *ProdNode) serveHTTP(ctx context.Context)
```

**Flow:**

1. Creates `http.ServeMux`, registers:
   - `/admin/` → AdminSrv handler.
   - `/register` → Registry HTTP handler.
   - `/heartbeat` → Registry HTTP handler.
   - `/services` → Registry HTTP handler.
   - All node handlers via `registerHandlers()`.
2. Creates `http.Server` with the mux.
3. Spawns goroutine:
   - If TLS: loads cert, configures `TLSConfig`, calls `ListenAndServeTLS`.
   - TLS load failure: falls back to plaintext.
   - Otherwise: calls `ListenAndServe`.

**Edge Cases:**

- TLS cert load failure → falls back to plaintext (logged as error).
- `http.ErrServerClosed` is suppressed (expected on shutdown).
- HTTP server runs in a goroutine — does not block `Start()`.

---

## Constants

| Name | Value | Description |
|------|-------|-------------|
| `maxExecutionSteps` | `1000` | Maximum step iterations before aborting |
| `executeAllSemaphore` | `16` | Node-wide concurrency limit for `executeAll` |
| `defaultExecTimeout` | `30s` | Default timeout for incoming message execution |
| `maxRecoveryConcurrency` | `8` | Max concurrent recovery goroutines |
| `tcpPoolSize` | `5` | TCP connection pool size per address |
| `DefaultMembersTopic` | `"_flowrulz_members"` | Topic for node discovery messages |
| `DefaultHTTPAddr` | `":8080"` | Default HTTP listen address |
| `DefaultGRPCAddr` | `":9090"` | Default gRPC listen address |
| `DefaultTopic` | `"flowrulz-input"` | Default input topic |
| `DefaultNodeID` | `"node-1"` | Default node identifier |
| `DefaultGroupID` | `"flowrulz"` | Default Kafka consumer group |

---

## Regex Validators

| Name | Pattern | Purpose |
|------|---------|---------|
| `validServiceName` | `^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$` | Service name validation |
| `validMethodName` | `^[a-zA-Z0-9][a-zA-Z0-9/_-]{0,255}$` | Method name validation |
