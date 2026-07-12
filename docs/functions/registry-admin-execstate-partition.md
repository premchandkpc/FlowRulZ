# FlowRulZ Exported Function Reference

> Comprehensive documentation of all exported functions across internal packages, bridge, and pkg.

---

## Table of Contents

- [internal/registry](#internalregistry)
  - [registry.go](#registrygo)
  - [lookup.go](#lookupgo)
  - [health.go](#healthgo)
  - [http.go](#httpgo)
  - [pkgsupport.go](#pkgsupportgo)
- [internal/admin](#internaladmin)
  - [api.go](#apigo)
  - [service.go](#servicego)
- [internal/execstate](#internalexecstate)
  - [execstate.go](#execstatego)
  - [filestore.go](#filestorego)
  - [memorystore.go](#memorystorego)
  - [pkgsupport.go](#pkgsupportgo-1)
- [internal/partition](#internalpartition)
  - [manager.go](#managergo)
  - [rebalance.go](#rebalancego)
- [internal/plandist](#internalplandist)
  - [distributor.go](#distributorgo)
  - [ack.go](#ackgo)
- [internal/replyrouter](#internalreplyrouter)
  - [router.go](#routergo)
- [internal/observability](#internalobservability)
  - [metrics.go](#metricsgo)
  - [tracer.go](#tracergo)
- [internal/flow](#internalflow)
  - [registry.go](#registrygo-1)
- [bridge](#bridge)
  - [compile.go](#compilego)
  - [execute.go](#executego)
  - [bridge.go](#bridgego)
  - [plan.go](#plango)
  - [vm_adapter.go](#vm_adaptergo)
- [pkg](#pkg)
  - [pkg/common](#pkgcommon)
  - [pkg/cluster](#pkgcluster)
  - [pkg/scheduler](#pkgscheduler)
  - [pkg/store](#pkgstore)
  - [pkg/vm](#pkgvm)

---

## internal/registry

### registry.go

#### `New() *ServiceRegistry`

**Signature:**
```go
func New() *ServiceRegistry
```

**Flow:**
1. Allocates `ServiceRegistry` with empty `services`, `instances`, and `roundRobin` maps.
2. Sets default LB strategy to `LBStrategyLeastLoaded`.
3. Sets heartbeat timeout to 30 seconds.
4. Returns initialized `*ServiceRegistry`.

**Edge Cases:**
- Thread-safe for concurrent use (RWMutex protected).
- `roundRobin` counters are lazily initialized per-service on first pick.

---

#### `(r *ServiceRegistry) Register(name string, endpoint *Endpoint) error`

**Signature:**
```go
func (r *ServiceRegistry) Register(name string, endpoint *Endpoint) error
```

**Flow:**
1. Validates `name` is non-empty; returns error if empty.
2. Validates `endpoint` is non-nil; returns error if nil.
3. Defaults `endpoint.Protocol` to `ProtocolHTTP` if unset.
4. Protocol-specific validation:
   - HTTP/gRPC/TCP: requires non-empty `Address` and positive `Port`.
   - Kafka: requires non-empty `Topic`.
5. Returns error for unsupported protocols.
6. Defaults `endpoint.NodeID` to `localNodeID()` if empty.
7. Sets `endpoint.Healthy = true`.
8. Acquires write lock.
9. If endpoint already exists (same `NodeID` + `Address` + `Port`), replaces it.
10. Otherwise, appends to `services[name]`.
11. Initializes round-robin counter if this is the first endpoint for the service.

**Edge Cases:**
- Duplicate registration (same NodeID+Address+Port) silently replaces the existing endpoint.
- Empty name → `"registry: empty service name"` error.
- nil endpoint → `"registry: nil endpoint"` error.
- Empty address for HTTP/gRPC/TCP → error.
- Port ≤ 0 → error.
- Kafka with empty topic → error.

---

#### `(r *ServiceRegistry) RegisterInstance(inst *ServiceInstance) error`

**Signature:**
```go
func (r *ServiceRegistry) RegisterInstance(inst *ServiceInstance) error
```

**Flow:**
1. Validates `inst.Name` is non-empty.
2. Defaults `inst.Endpoint.Protocol` to `ProtocolHTTP` if unset.
3. Protocol-specific validation (same as `Register`).
4. Auto-generates `inst.ID` if empty:
   - Kafka: `"name-topic"`
   - Other: `"name-address-port"`
5. Defaults `inst.Weight` to 100 if ≤ 0.
6. Sets `inst.Healthy = true`, `inst.HeartbeatAt = time.Now()`.
7. Preserves `inst.RegisteredAt` if already set.
8. Acquires write lock.
9. If instance with same ID exists, replaces it.
10. Creates or updates the corresponding `Endpoint` in `services[name]`.
11. Appends new instance if not found.
12. Initializes round-robin counter if needed.

**Edge Cases:**
- Auto-generated ID ensures uniqueness across protocols.
- Weight defaults to 100 (normalized).
- `RegisteredAt` is preserved on re-registration (not overwritten).
- Both `instances` and `services` maps are updated atomically.

---

#### `(r *ServiceRegistry) Unregister(name string, nodeID string)`

**Signature:**
```go
func (r *ServiceRegistry) Unregister(name string, nodeID string)
```

**Flow:**
1. Acquires write lock.
2. Filters out endpoints matching `nodeID` from `services[name]`.
3. If no endpoints remain, deletes `services[name]` and `roundRobin[name]`.
4. Filters out instances matching `nodeID` from `instances[name]`.
5. If no instances remain, deletes `instances[name]`.

**Edge Cases:**
- Empty `nodeID` removes ALL endpoints for the service.
- Non-existent service name is a no-op (no error).
- Cleans up both `services` and `instances` maps.

---

#### `(r *ServiceRegistry) UnregisterInstance(name, instanceID string)`

**Signature:**
```go
func (r *ServiceRegistry) UnregisterInstance(name, instanceID string)
```

**Flow:**
1. Acquires write lock.
2. Filters out instance with matching `instanceID` from `instances[name]`.
3. If no instances remain, deletes `instances[name]`.
4. Does NOT remove corresponding endpoint from `services[name]`.

**Edge Cases:**
- Does not clean up orphaned endpoints in `services[name]`.
- Non-existent instance ID is a no-op.

---

### lookup.go

#### `(r *ServiceRegistry) Lookup(name string) []*Endpoint`

**Signature:**
```go
func (r *ServiceRegistry) Lookup(name string) []*Endpoint
```

**Flow:**
1. Acquires read lock.
2. Looks up `services[name]`.
3. Filters to only healthy endpoints.
4. Returns slice of healthy endpoints (nil if none).

**Edge Cases:**
- Returns nil (not empty slice) if service not found.
- Unhealthy endpoints are excluded from results.
- Returns a new slice (safe for callers to modify).

---

#### `(r *ServiceRegistry) LookupInstance(name, method string) (*ServiceInstance, error)`

**Signature:**
```go
func (r *ServiceRegistry) LookupInstance(name, method string) (*ServiceInstance, error)
```

**Flow:**
1. Delegates to `LookupInstanceWithProtocol(name, method, "")`.

**Edge Cases:**
- Empty `method` matches all instances (no method filtering).

---

#### `(r *ServiceRegistry) LookupInstanceWithProtocol(name, method string, protocol Protocol) (*ServiceInstance, error)`

**Signature:**
```go
func (r *ServiceRegistry) LookupInstanceWithProtocol(name, method string, protocol Protocol) (*ServiceInstance, error)
```

**Flow:**
1. Acquires read lock.
2. Checks if `instances[name]` exists and is non-empty.
3. Builds candidate list filtering by:
   - `inst.Healthy == true`
   - Heartbeat not expired (`time.Since(HeartbeatAt) <= hbTimeout`)
   - Protocol match (if `protocol != ""`)
   - Method match (if `method != ""`, checks `inst.Methods` list)
4. Returns error if no candidates found.
5. Uses `pickInstance(candidates)` for load-balanced selection.

**Edge Cases:**
- Empty `protocol` accepts any protocol (backward compatible).
- Heartbeat-expired instances are excluded even if `Healthy == true`.
- Protocol filtering happens BEFORE load balancing — candidates of different protocols are never mixed.
- Error message includes protocol, service name, and method for debugging.

---

#### `(r *ServiceRegistry) Pick(name string) (*Endpoint, error)`

**Signature:**
```go
func (r *ServiceRegistry) Pick(name string) (*Endpoint, error)
```

**Flow:**
1. Delegates to `PickWithStrategy(name, r.defStrategy)`.

**Edge Cases:**
- Default strategy is `LBStrategyLeastLoaded`.

---

#### `(r *ServiceRegistry) PickWithStrategy(name string, strategy LBStrategy) (*Endpoint, error)`

**Signature:**
```go
func (r *ServiceRegistry) PickWithStrategy(name string, strategy LBStrategy) (*Endpoint, error)
```

**Flow:**
1. Acquires read lock to get endpoints for `name`.
2. Filters to healthy endpoints.
3. Returns error if no healthy endpoints found.
4. Applies load-balancing strategy:
   - `LBStrategyRoundRobin`: Atomic counter modulo healthy count. Acquires write lock for counter init.
   - `LBStrategyLocalPrefer`: Returns first endpoint with matching `NodeID` (or any if none match).
   - `LBStrategyLeastLoaded`: Returns endpoint with minimum `Load` value.
   - Default (unknown/random): Returns random healthy endpoint.

**Edge Cases:**
- Round-robin counter is lazily initialized under write lock.
- LocalPrefer falls back to random if no local endpoint found.
- Unknown strategy defaults to random selection.
- Service not found → `"registry: service %q not found"` error.
- No healthy endpoints → `"registry: no healthy endpoints for %q"` error.

---

#### `(r *ServiceRegistry) ListServices() []string`

**Signature:**
```go
func (r *ServiceRegistry) ListServices() []string
```

**Flow:**
1. Acquires read lock.
2. Collects unique names from both `instances` and `services` maps.
3. Returns deduplicated list of service names.

**Edge Cases:**
- Deduplicates names that appear in both maps.
- Returns nil (not empty slice) if no services registered.
- Order is non-deterministic (map iteration).

---

#### `(r *ServiceRegistry) ListEndpoints(name string) []*Endpoint`

**Signature:**
```go
func (r *ServiceRegistry) ListEndpoints(name string) []*Endpoint
```

**Flow:**
1. Acquires read lock.
2. Returns a copy of `services[name]` (including unhealthy endpoints).

**Edge Cases:**
- Returns nil if service not found.
- Returns ALL endpoints (not filtered by health).
- Returns a defensive copy (safe for callers to modify).

---

#### `(r *ServiceRegistry) ListInstances(name string) []*ServiceInstance`

**Signature:**
```go
func (r *ServiceRegistry) ListInstances(name string) []*ServiceInstance
```

**Flow:**
1. Acquires read lock.
2. Returns a copy of `instances[name]`.

**Edge Cases:**
- Returns nil if service not found.
- Returns ALL instances (not filtered by health).
- Returns a defensive copy.

---

#### `(r *ServiceRegistry) Snapshot() map[string][]*Endpoint`

**Signature:**
```go
func (r *ServiceRegistry) Snapshot() map[string][]*Endpoint
```

**Flow:**
1. Acquires read lock.
2. Deep-copies all service → endpoints mappings.

**Edge Cases:**
- Returns empty map (not nil) if no services.
- Deep copy prevents concurrent modification.

---

#### `(r *ServiceRegistry) ServiceInfo(name string) *ServiceInfo`

**Signature:**
```go
func (r *ServiceRegistry) ServiceInfo(name string) *ServiceInfo
```

**Flow:**
1. Acquires read lock.
2. Returns `ServiceInfo` with name, methods (from first instance), instances, and endpoints.

**Edge Cases:**
- Returns nil if service not found.
- Methods are taken from the FIRST instance only (assumes consistent methods across instances).
- Deep copies instances and endpoints.

---

#### `(r *ServiceRegistry) AllServiceInfo() []*ServiceInfo`

**Signature:**
```go
func (r *ServiceRegistry) AllServiceInfo() []*ServiceInfo
```

**Flow:**
1. Acquires read lock.
2. Iterates all services in `instances` map.
3. Builds `ServiceInfo` for each.

**Edge Cases:**
- Methods come from the first instance of each service.
- Only iterates `instances` map (services-only in `services` map are omitted).

---

### health.go

#### `(r *ServiceRegistry) SetHeartbeatTimeout(d time.Duration)`

**Signature:**
```go
func (r *ServiceRegistry) SetHeartbeatTimeout(d time.Duration)
```

**Flow:**
1. Acquires write lock.
2. Sets `r.hbTimeout` to the given duration.

**Edge Cases:**
- Default timeout is 30 seconds.
- Changing timeout affects future heartbeat checks but does not retroactively expire instances.

---

#### `(r *ServiceRegistry) Heartbeat(name, instanceID string) error`

**Signature:**
```go
func (r *ServiceRegistry) Heartbeat(name, instanceID string) error
```

**Flow:**
1. Acquires write lock.
2. Finds instance by `instanceID` in `instances[name]`.
3. Updates `inst.HeartbeatAt = time.Now()` and `inst.Healthy = true`.
4. Finds corresponding endpoint in `services[name]` by NodeID+Address match.
5. Sets `ep.Healthy = true`.
6. Returns error if instance not found.

**Edge Cases:**
- Returns `"registry: instance %s/%s not found"` if instance doesn't exist.
- Endpoint health is updated based on instance health.
- Concurrent heartbeats are safe (write-locked).

---

#### `(r *ServiceRegistry) CheckExpired() []string`

**Signature:**
```go
func (r *ServiceRegistry) CheckExpired() []string
```

**Flow:**
1. Acquires write lock.
2. Iterates all instances across all services.
3. If `time.Since(inst.HeartbeatAt) > r.hbTimeout`:
   - Sets `inst.Healthy = false`.
   - Sets corresponding endpoint `Healthy = false`.
   - Appends `"name/instanceID"` to expired list.

**Edge Cases:**
- Returns nil if no instances expired.
- Both instance AND endpoint health are set to false.
- Expired IDs formatted as `"name/instanceID"`.

---

#### `(r *ServiceRegistry) MarkUnhealthy(name string, nodeID string)`

**Signature:**
```go
func (r *ServiceRegistry) MarkUnhealthy(name string, nodeID string)
```

**Flow:**
1. Acquires write lock.
2. Sets `Healthy = false` for all endpoints of `name` matching `nodeID`.
3. Sets `Healthy = false` for all instances of `name` matching `nodeID`.

**Edge Cases:**
- No-ops silently if service or nodeID not found.
- Affects all endpoints/instances matching the nodeID (may be multiple).

---

#### `(r *ServiceRegistry) MarkHealthy(name string, nodeID string)`

**Signature:**
```go
func (r *ServiceRegistry) MarkHealthy(name string, nodeID string)
```

**Flow:**
1. Acquires write lock.
2. Sets `Healthy = true` for all endpoints of `name` matching `nodeID`.
3. Sets `Healthy = true` for all instances of `name` matching `nodeID`.

**Edge Cases:**
- Counterpart to `MarkUnhealthy`.
- No-ops silently if service or nodeID not found.

---

### http.go

#### `(r *ServiceRegistry) RegisterHTTPHandler(w http.ResponseWriter, req *http.Request)`

**Signature:**
```go
func (r *ServiceRegistry) RegisterHTTPHandler(w http.ResponseWriter, req *http.Request)
```

**Flow:**
1. Rejects non-POST requests with 405.
2. Checks Bearer auth; returns 401 if unauthorized.
3. Limits request body to 1MB.
4. Decodes `RegisterRequest` JSON from body.
5. Validates name, address, port; defaults protocol to HTTP.
6. Creates `ServiceInstance` and calls `r.RegisterInstance()`.
7. Returns 201 with `{"status":"registered", "name":..., "instance_id":...}`.

**Edge Cases:**
- Missing `FLOWRULZ_API_KEY` env var → auth disabled (open mode).
- Request body > 1MB → 400 error.
- Invalid JSON → 400 error.
- Registration error → 500 error.

---

#### `(r *ServiceRegistry) HeartbeatHTTPHandler(w http.ResponseWriter, req *http.Request)`

**Signature:**
```go
func (r *ServiceRegistry) HeartbeatHTTPHandler(w http.ResponseWriter, req *http.Request)
```

**Flow:**
1. Rejects non-POST with 405.
2. Checks Bearer auth; returns 401 if unauthorized.
3. Limits body to 1MB.
4. Decodes `HeartbeatRequest` (name + instance_id).
5. Calls `r.Heartbeat(name, instanceID)`.
6. Returns 404 if instance not found, 200 on success.

**Edge Cases:**
- Missing name or instance_id → 400 error.

---

#### `(r *ServiceRegistry) StartHeartbeatChecker(stopCh <-chan struct{})`

**Signature:**
```go
func (r *ServiceRegistry) StartHeartbeatChecker(stopCh <-chan struct{})
```

**Flow:**
1. Starts goroutine with 15-second ticker.
2. On each tick, calls `r.CheckExpired()`.
3. Logs warnings for each expired instance.
4. Returns when `stopCh` is closed.

**Edge Cases:**
- Non-blocking (returns immediately).
- Goroutine leaks if `stopCh` is never closed.
- Fixed 15-second check interval (not configurable).

---

#### `(r *ServiceRegistry) ListServicesHTTPHandler(w http.ResponseWriter, req *http.Request)`

**Signature:**
```go
func (r *ServiceRegistry) ListServicesHTTPHandler(w http.ResponseWriter, req *http.Request)
```

**Flow:**
1. Sets Content-Type to `application/json`.
2. Returns `{"services": [...]}` with all service info.

**Edge Cases:**
- Unauthenticated endpoint.
- No method filtering.

---

### pkgsupport.go

#### `NewRegistry() *Registry`

**Signature:**
```go
func NewRegistry() *Registry
```

**Flow:**
1. Creates inner `ServiceRegistry` via `New()`.
2. Initializes subscription map.
3. Returns `*Registry` implementing `pkgregistry.Registry` interface.

**Edge Cases:**
- Compile-time check: `var _ pkgregistry.Registry = (*Registry)(nil)`.
- Wraps internal `ServiceRegistry` with pkg-level interface.

---

#### `(r *Registry) Register(ctx context.Context, svc *pkgregistry.ServiceRegistration) error`

**Signature:**
```go
func (r *Registry) Register(ctx context.Context, svc *pkgregistry.ServiceRegistration) error
```

**Flow:**
1. Creates `Endpoint` from `svc.Address` with `ProtocolHTTP`.
2. Delegates to `r.inner.Register(svc.Name, ep)`.

**Edge Cases:**
- Always uses `ProtocolHTTP` regardless of source protocol.
- Does not propagate method/capability information.

---

#### `(r *Registry) Unregister(ctx context.Context, name string) error`

**Signature:**
```go
func (r *Registry) Unregister(ctx context.Context, name string) error
```

**Flow:**
1. Calls `r.inner.Unregister(name, "")`.
2. Empty nodeID removes ALL endpoints.

**Edge Cases:**
- Always succeeds (returns nil).
- Removes all endpoints for the given name.

---

#### `(r *Registry) Lookup(ctx context.Context, name string) (*pkgregistry.ServiceInstance, error)`

**Signature:**
```go
func (r *Registry) Lookup(ctx context.Context, name string) (*pkgregistry.ServiceInstance, error)
```

**Flow:**
1. Calls `r.inner.LookupInstance(name, "")`.
2. Converts to `pkgregistry.ServiceInstance`.
3. Returns `pkgregistry.ErrServiceNotFound` if not found.

**Edge Cases:**
- Maps internal error to pkg-level sentinel error.
- Address formatted as `"address:port"`.

---

#### `(r *Registry) LookupMultiple(ctx context.Context, names []string) ([]*pkgregistry.ServiceInstance, error)`

**Signature:**
```go
func (r *Registry) LookupMultiple(ctx context.Context, names []string) ([]*pkgregistry.ServiceInstance, error)
```

**Flow:**
1. Iterates names, calls `r.Lookup()` for each.
2. Skips names that return errors.
3. Returns empty slice (not nil) if no instances found.

**Edge Cases:**
- Silently skips not-found services.
- Never returns error (partial results are returned).

---

#### `(r *Registry) ListServices(ctx context.Context) ([]*pkgregistry.ServiceRegistration, error)`

**Signature:**
```go
func (r *Registry) ListServices(ctx context.Context) ([]*pkgregistry.ServiceRegistration, error)
```

**Flow:**
1. Calls `r.inner.ListServices()` for names.
2. For each name, calls `r.inner.ServiceInfo()` to get methods.
3. Converts methods to `pkgregistry.MethodSpec`.

**Edge Cases:**
- Methods are taken from the first instance of each service.
- Always succeeds (returns nil error).

---

#### `(r *Registry) HealthCheck(ctx context.Context, name string) (bool, error)`

**Signature:**
```go
func (r *Registry) HealthCheck(ctx context.Context, name string) (bool, error)
```

**Flow:**
1. Calls `r.inner.LookupInstance(name, "")`.
2. Returns `inst.Healthy` if found.
3. Returns `pkgregistry.ErrServiceNotFound` if not found.

---

#### `(r *Registry) SubscribeChanges(ctx context.Context, pattern string) (<-chan pkgregistry.RegistryEvent, error)`

**Signature:**
```go
func (r *Registry) SubscribeChanges(ctx context.Context, pattern string) (<-chan pkgregistry.RegistryEvent, error)
```

**Flow:**
1. Creates buffered channel (capacity 64).
2. Stores channel in `subs[pattern]`.
3. Starts goroutine that waits for `ctx.Done()` and cleans up.
4. Returns the channel.

**Edge Cases:**
- Channel buffer is 64; events beyond capacity are dropped silently.
- Context cancellation closes the channel and removes from subs map.
- Pattern-based filtering is NOT implemented (all patterns stored independently).
- Only one subscriber per pattern (overwrites previous).

---

## internal/admin

### api.go

#### `New(eng *engine.Engine) *Server`

**Signature:**
```go
func New(eng *engine.Engine) *Server
```

**Flow:**
1. Delegates to `NewWithCompiler(eng, compiler.NewLocal())`.

---

#### `NewWithCompiler(eng *engine.Engine, comp compiler.Compiler) *Server`

**Signature:**
```go
func NewWithCompiler(eng *engine.Engine, comp compiler.Compiler) *Server
```

**Flow:**
1. Defaults compiler to `compiler.NewLocal()` if nil.
2. Reads `FLOWRULZ_API_KEY` from env; logs warning if empty.
3. Creates rate limiter (50 req/s, burst 100) for `"admin-api"` bucket.
4. Registers all HTTP handlers on the mux:
   - `POST /rules` → deployRule
   - `DELETE /rules/{id}` → removeRule
   - `GET /rules` → listRules
   - `GET /rules/{id}` → getRule
   - `GET /rules/{id}/versions` → listVersions
   - `POST /rules/{id}/validate` → validateRule
   - `POST /rules/{id}/promote` → promoteVersion
   - `POST /rules/{id}/rollback` → rollbackVersion
   - `GET /lanes` → listLanes
   - `GET /health` → health (unauthenticated)
   - `GET /metrics` → metrics
   - `GET /debug` → debug

**Edge Cases:**
- `FLOWRULZ_API_KEY` not set → all mutating endpoints reject unauthenticated requests.
- Rate limit: 50 req/s per bucket, burst of 100.
- Health endpoint is unauthenticated.
- Request body limit: 1MB.

---

#### `(s *Server) Handler() http.Handler`

**Signature:**
```go
func (s *Server) Handler() http.Handler
```

**Flow:**
1. Returns `s.mux` as `http.Handler`.

**Edge Cases:**
- Returns nil-safe handler (mux is always initialized).

---

#### `(s *Server) RegisterDLQ(dlq *reliability.DLQ)`

**Signature:**
```go
func (s *Server) RegisterDLQ(dlq *reliability.DLQ)
```

**Flow:**
1. Stores `dlq` reference.
2. Registers DLQ endpoints:
   - `GET /dlq` → listDLQ
   - `POST /dlq/replay/{id}` → replayDLQ
   - `POST /dlq/replay` → replayAllDLQ
   - `DELETE /dlq` → clearDLQ

**Edge Cases:**
- DLQ endpoints are optional; `nil` DLQ returns empty results.
- All DLQ endpoints are authenticated and rate-limited.

---

#### `(s *Server) deployRule(w http.ResponseWriter, r *http.Request)`

**Signature:**
```go
func (s *Server) deployRule(w http.ResponseWriter, r *http.Request)
```

**Flow:**
1. Limits body to 1MB.
2. Decodes JSON: `{"id": "...", "dsl": "..."}`.
3. Validates `id` (1-256 chars) and `dsl` (1-1MB).
4. Calls `s.rules.DeployRule(id, dsl)`.
5. Returns 201 with `{"id": "..."}` on success.

**Edge Cases:**
- Invalid ID length → 400.
- Empty or oversized DSL → 400.
- Deploy failure → 500.

---

#### `(s *Server) removeRule(w http.ResponseWriter, r *http.Request)`

**Signature:**
```go
func (s *Server) removeRule(w http.ResponseWriter, r *http.Request)
```

**Flow:**
1. Extracts `id` from path.
2. Validates ID (1-256 chars).
3. Calls `s.rules.RemoveRule(id)`.
4. Returns 204 No Content.

**Edge Cases:**
- Non-existent ID is a no-op (204 still returned).

---

#### `(s *Server) listRules(w http.ResponseWriter, r *http.Request)`

**Signature:**
```go
func (s *Server) listRules(w http.ResponseWriter, r *http.Request)
```

**Flow:**
1. Calls `s.rules.ListRules()`.
2. Returns JSON array of rule views.

---

#### `(s *Server) getRule(w http.ResponseWriter, r *http.Request)`

**Signature:**
```go
func (s *Server) getRule(w http.ResponseWriter, r *http.Request)
```

**Flow:**
1. Extracts `id` from path.
2. Calls `s.rules.RuleDetail(id)`.
3. Returns 404 if not found.

---

#### `(s *Server) listVersions(w http.ResponseWriter, r *http.Request)`

**Signature:**
```go
func (s *Server) listVersions(w http.ResponseWriter, r *http.Request)
```

**Flow:**
1. Extracts `id` from path.
2. Returns `s.rules.RuleVersions(id)` as JSON.

---

#### `(s *Server) validateRule(w http.ResponseWriter, r *http.Request)`

**Signature:**
```go
func (s *Server) validateRule(w http.ResponseWriter, r *http.Request)
```

**Flow:**
1. Decodes `{"dsl": "..."}` from body.
2. Validates DSL length (1-1MB).
3. Calls `s.rules.ValidateDSL(dsl)`.
4. Returns validation result with `valid`, `complexity_score`, `plan_bytes`.

**Edge Cases:**
- Returns validation error in result body (not HTTP error).

---

#### `(s *Server) promoteVersion(w http.ResponseWriter, r *http.Request)`

**Signature:**
```go
func (s *Server) promoteVersion(w http.ResponseWriter, r *http.Request)
```

**Flow:**
1. Extracts `id` from path, `version` from query param.
2. Parses version as uint64.
3. Calls `s.rules.PromoteVersion(id, version)`.
4. Returns 404 if rule/version not found.

---

#### `(s *Server) rollbackVersion(w http.ResponseWriter, r *http.Request)`

**Signature:**
```go
func (s *Server) rollbackVersion(w http.ResponseWriter, r *http.Request)
```

**Flow:**
1. Delegates to `s.promoteVersion(w, r)`.

**Edge Cases:**
- Rollback is semantically identical to promote (version-based).

---

#### `(s *Server) listLanes(w http.ResponseWriter, r *http.Request)`

**Signature:**
```go
func (s *Server) listLanes(w http.ResponseWriter, r *http.Request)
```

**Flow:**
1. Returns `s.rules.Lanes()` as JSON array with name, batch_size, poll_timeout.

---

#### `(s *Server) health(w http.ResponseWriter, r *http.Request)`

**Signature:**
```go
func (s *Server) health(w http.ResponseWriter, r *http.Request)
```

**Flow:**
1. Returns `s.rules.HealthSnapshot()` as JSON.
2. Unauthenticated endpoint.

---

#### `(s *Server) metrics(w http.ResponseWriter, r *http.Request)`

**Signature:**
```go
func (s *Server) metrics(w http.ResponseWriter, r *http.Request)
```

**Flow:**
1. Reads `runtime.MemStats`.
2. Outputs Prometheus text format with:
   - `flowrulz_goroutines`
   - `flowrulz_alloc_bytes`
   - `flowrulz_heap_objects`
   - `flowrulz_num_rules`
   - `flowrulz_next_gc_bytes`

---

#### `(s *Server) debug(w http.ResponseWriter, r *http.Request)`

**Signature:**
```go
func (s *Server) debug(w http.ResponseWriter, r *http.Request)
```

**Flow:**
1. Returns `s.rules.DebugSnapshot()` with detailed memory stats, GC info, CGo calls.

---

#### `(s *Server) listDLQ(w http.ResponseWriter, r *http.Request)`

**Signature:**
```go
func (s *Server) listDLQ(w http.ResponseWriter, r *http.Request)
```

**Flow:**
1. If DLQ not configured, returns empty array.
2. Calls `s.dlq.List()` and returns entries.

---

#### `(s *Server) replayDLQ(w http.ResponseWriter, r *http.Request)`

**Signature:**
```go
func (s *Server) replayDLQ(w http.ResponseWriter, r *http.Request)
```

**Flow:**
1. Extracts `id` from path.
2. If DLQ not configured → 404.
3. Calls `s.dlq.Replay(r.Context(), id)`.
4. Returns 500 on replay error.

---

#### `(s *Server) replayAllDLQ(w http.ResponseWriter, r *http.Request)`

**Signature:**
```go
func (s *Server) replayAllDLQ(w http.ResponseWriter, r *http.Request)
```

**Flow:**
1. If DLQ not configured → 404.
2. Calls `s.dlq.ReplayAll(r.Context())`.
3. Returns count of replayed entries.

---

#### `(s *Server) clearDLQ(w http.ResponseWriter, r *http.Request)`

**Signature:**
```go
func (s *Server) clearDLQ(w http.ResponseWriter, r *http.Request)
```

**Flow:**
1. If DLQ not configured → 404.
2. Calls `s.dlq.Clear()`.
3. Returns 204 No Content.

---

### service.go

#### `newRuleService(eng *engine.Engine, comp compiler.Compiler) *ruleService`

**Signature:**
```go
func newRuleService(eng *engine.Engine, comp compiler.Compiler) *ruleService
```

**Flow:**
1. Defaults compiler to `compiler.NewLocal()` if nil.
2. Returns `ruleService` with engine and compiler references.

---

#### `(rs *ruleService) DeployRule(id, dsl string) error`

**Signature:**
```go
func (rs *ruleService) DeployRule(id, dsl string) error
```

**Flow:**
1. Checks engine is non-nil.
2. Calls `rs.engine.Deploy(id, dsl)`.

**Edge Cases:**
- Returns `"engine not configured"` if engine is nil.

---

#### `(rs *ruleService) RemoveRule(id string)`

**Signature:**
```go
func (rs *ruleService) RemoveRule(id string)
```

**Flow:**
1. Calls `rs.engine.Remove(id)` if engine is non-nil.

**Edge Cases:**
- No-op if engine is nil.

---

#### `(rs *ruleService) ListRules() []ruleView`

**Signature:**
```go
func (rs *ruleService) ListRules() []ruleView
```

**Flow:**
1. Calls `rs.engine.Rules()`.
2. Converts each rule to `ruleView` with version info.
3. Marks the active version with `Active: true`.

---

#### `(rs *ruleService) RuleDetail(id string) (map[string]interface{}, bool)`

**Signature:**
```go
func (rs *ruleService) RuleDetail(id string) (map[string]interface{}, bool)
```

**Flow:**
1. Searches `rs.engine.Rules()` for matching ID.
2. Returns `{"id": ..., "versions": [...]}` with DSL and lane info.
3. Returns `(nil, false)` if not found.

**Edge Cases:**
- DSL content is included in version views.
- Lane is included in version views.

---

#### `(rs *ruleService) RuleVersions(id string) []versionView`

**Signature:**
```go
func (rs *ruleService) RuleVersions(id string) []versionView
```

**Flow:**
1. Finds rule by ID.
2. Returns version views with DSL, active flag.
3. Returns empty slice if not found.

---

#### `(rs *ruleService) ValidateDSL(dsl string) (map[string]interface{}, error)`

**Signature:**
```go
func (rs *ruleService) ValidateDSL(dsl string) (map[string]interface{}, error)
```

**Flow:**
1. Compiles DSL with `compiler.Compile(dsl, "validate")`.
2. On success: returns `{"valid": true, "complexity_score": ..., "plan_bytes": ...}`.
3. On failure: returns `{"valid": false, "error": "..."}`.

**Edge Cases:**
- Compiler must be non-nil.
- Both validation result and error are returned for caller flexibility.

---

#### `(rs *ruleService) PromoteVersion(id string, version uint64) error`

**Signature:**
```go
func (rs *ruleService) PromoteVersion(id string, version uint64) error
```

**Flow:**
1. Calls `rs.engine.Promote(id, version)`.

---

#### `(rs *ruleService) Lanes() []map[string]interface{}`

**Signature:**
```go
func (rs *ruleService) Lanes() []map[string]interface{}
```

**Flow:**
1. Iterates `engine.DefaultLanes`.
2. Returns lane configs with name, batch_size, poll_timeout.

---

#### `(rs *ruleService) HealthSnapshot() map[string]interface{}`

**Signature:**
```go
func (rs *ruleService) HealthSnapshot() map[string]interface{}
```

**Flow:**
1. Reads `runtime.MemStats`.
2. Returns JSON with status, time, goroutines, alloc_mb, heap_objects, num_rules, go_version.

---

#### `(rs *ruleService) DebugSnapshot() map[string]interface{}`

**Signature:**
```go
func (rs *ruleService) DebugSnapshot() map[string]interface{}
```

**Flow:**
1. Reads `runtime.MemStats`.
2. Returns detailed memory stats: alloc, total_alloc, sys, heap_alloc, heap_sys, heap_objects, gc_cycles, gc_pause_ns.
3. Includes goroutines, cgo_calls, num_rules, go_version.

---

## internal/execstate

### execstate.go

#### Status Type and Constants

```go
type Status int

const (
    StatusCreated          Status = iota  // "created"
    StatusRunning                          // "running"
    StatusWaitingForService               // "waiting_for_service"
    StatusCompleted                       // "completed"
    StatusFailed                          // "failed"
)
```

#### `(s Status) String() string`

**Signature:**
```go
func (s Status) String() string
```

**Flow:**
1. Maps status code to string representation.
2. Returns `"unknown"` for unrecognized values.

**Edge Cases:**
- Unknown status values return `"unknown"` (not panic).

---

### filestore.go

#### `NewFileStore(dir string) (*FileStore, error)`

**Signature:**
```go
func NewFileStore(dir string) (*FileStore, error)
```

**Flow:**
1. Creates directory with `os.MkdirAll(dir, 0755)`.
2. Initializes 16-shard structure with RWMutex per shard.
3. Initializes status index: `map[Status]map[string]bool`.
4. Calls `buildIndex()` to scan existing files.
5. Returns `*FileStore`.

**Edge Cases:**
- Directory creation failure → error.
- `buildIndex()` reads all `.json` files in dir on startup.
- Corrupt/unreadable files are silently skipped during index build.
- 16 shards provide concurrent access for different IDs.

---

#### `(fs *FileStore) Create(_ context.Context, s *State) error`

**Signature:**
```go
func (fs *FileStore) Create(_ context.Context, s *State) error
```

**Flow:**
1. Selects shard via FNV-32a hash of `s.ID`.
2. Acquires shard write lock.
3. Checks if file already exists (`os.Stat`).
4. Writes state to `path(id)` via `writeLocked`.
5. Adds to status index.

**Edge Cases:**
- Already exists → `"execstate: %s already exists"` error.
- `writeLocked` uses atomic write: marshal → `.tmp` → rename.

---

#### `(fs *FileStore) Save(_ context.Context, s *State) error`

**Signature:**
```go
func (fs *FileStore) Save(_ context.Context, s *State) error
```

**Flow:**
1. Selects shard via FNV-32a hash.
2. Acquires shard write lock.
3. Captures old status from index.
4. Sets `s.UpdatedAt` to current UTC time.
5. Writes atomically (tmp + rename).
6. Updates status index (removes old, adds new).

**Edge Cases:**
- Updates `UpdatedAt` automatically.
- Idempotent — can save same state multiple times.
- Atomic write prevents corruption on crash.

---

#### `(fs *FileStore) Load(_ context.Context, id string) (*State, error)`

**Signature:**
```go
func (fs *FileStore) Load(_ context.Context, id string) (*State, error)
```

**Flow:**
1. Selects shard.
2. Acquires shard read lock.
3. Reads and unmarshals from `path(id)`.

**Edge Cases:**
- File not found → wrapped error from `os.ReadFile`.
- Corrupt JSON → unmarshal error.

---

#### `(fs *FileStore) ListByStatus(_ context.Context, statuses ...Status) ([]*State, error)`

**Signature:**
```go
func (fs *FileStore) ListByStatus(_ context.Context, statuses ...Status) ([]*State, error)
```

**Flow:**
1. Acquires index read lock.
2. Collects candidate IDs from index (all if no statuses specified).
3. For each candidate, acquires shard read lock and reads file.
4. Sorts results by `CreatedAt` ascending.
5. Silently skips files that fail to read.

**Edge Cases:**
- Empty `statuses` returns ALL states.
- Corrupt files are silently skipped with warning log.
- Results sorted by creation time.
- Returns nil (not error) for empty results.

---

#### `(fs *FileStore) Delete(_ context.Context, id string) error`

**Signature:**
```go
func (fs *FileStore) Delete(_ context.Context, id string) error
```

**Flow:**
1. Selects shard.
2. Acquires shard write lock.
3. Removes file via `os.Remove`.
4. Removes from all status index entries.

**Edge Cases:**
- Non-existent file → `os.IsNotExist` check, no error returned.
- Removes from ALL status buckets in index.

---

#### `(fs *FileStore) Close() error`

**Signature:**
```go
func (fs *FileStore) Close() error
```

**Flow:**
1. Returns nil (no-op).

**Edge Cases:**
- No resources to release.

---

### memorystore.go

#### `NewMemoryStore() *MemoryStore`

**Signature:**
```go
func NewMemoryStore() *MemoryStore
```

**Flow:**
1. Initializes empty `entries` map.
2. Returns `*MemoryStore` implementing `Store` interface.

---

#### `(ms *MemoryStore) Create(_ context.Context, s *State) error`

**Signature:**
```go
func (ms *MemoryStore) Create(_ context.Context, s *State) error
```

**Flow:**
1. Acquires write lock.
2. Returns error if ID already exists.
3. Stores state in map.

**Edge Cases:**
- `"execstate: %s already exists"` for duplicates.

---

#### `(ms *MemoryStore) Save(_ context.Context, s *State) error`

**Signature:**
```go
func (ms *MemoryStore) Save(_ context.Context, s *State) error
```

**Flow:**
1. Acquires write lock.
2. Sets `UpdatedAt` to current UTC time.
3. Stores/overwrites state in map.

**Edge Cases:**
- Idempotent — overwrites existing entries.

---

#### `(ms *MemoryStore) Load(_ context.Context, id string) (*State, error)`

**Signature:**
```go
func (ms *MemoryStore) Load(_ context.Context, id string) (*State, error)
```

**Flow:**
1. Acquires read lock.
2. Returns state or `"execstate: %s not found"` error.

---

#### `(ms *MemoryStore) ListByStatus(_ context.Context, statuses ...Status) ([]*State, error)`

**Signature:**
```go
func (ms *MemoryStore) ListByStatus(_ context.Context, statuses ...Status) ([]*State, error)
```

**Flow:**
1. Acquires read lock.
2. Filters by status if provided.
3. Sorts by `CreatedAt` ascending.

**Edge Cases:**
- Empty `statuses` returns all entries.

---

#### `(ms *MemoryStore) Delete(_ context.Context, id string) error`

**Signature:**
```go
func (ms *MemoryStore) Delete(_ context.Context, id string) error
```

**Flow:**
1. Acquires write lock.
2. Returns error if not found.
3. Deletes from map.

---

#### `(ms *MemoryStore) Close() error`

**Signature:**
```go
func (ms *MemoryStore) Close() error
```

**Flow:**
1. Returns nil.

---

### pkgsupport.go (execstate)

#### `NewExecutionStore(dir string) (*ExecutionStore, error)`

**Signature:**
```go
func NewExecutionStore(dir string) (*ExecutionStore, error)
```

**Flow:**
1. Creates inner `FileStore` via `NewFileStore(dir)`.
2. Returns `*ExecutionStore` implementing `pkgstore.Store`.

**Edge Cases:**
- Compile-time check: `var _ pkgstore.Store = (*ExecutionStore)(nil)`.
- Wraps internal `FileStore` with pkg-level interface.

---

#### `(s *ExecutionStore) Create(ctx context.Context, record *pkgstore.ExecutionRecord) error`

**Signature:**
```go
func (s *ExecutionStore) Create(ctx context.Context, record *pkgstore.ExecutionRecord) error
```

**Flow:**
1. Converts `ExecutionRecord` to internal `State`.
2. Maps `CompletedAt` to `UpdatedAt`.
3. Delegates to `s.inner.Create()`.

---

#### `(s *ExecutionStore) Save(ctx context.Context, record *pkgstore.ExecutionRecord) error`

**Signature:**
```go
func (s *ExecutionStore) Save(ctx context.Context, record *pkgstore.ExecutionRecord) error
```

**Flow:**
1. Converts record to `State`.
2. Sets `UpdatedAt` to current time.
3. Delegates to `s.inner.Save()`.

---

#### `(s *ExecutionStore) Load(ctx context.Context, id pkgstore.ExecutionID) (*pkgstore.ExecutionRecord, error)`

**Signature:**
```go
func (s *ExecutionStore) Load(ctx context.Context, id pkgstore.ExecutionID) (*pkgstore.ExecutionRecord, error)
```

**Flow:**
1. Delegates to `s.inner.Load()`.
2. Converts `State` to `ExecutionRecord`.

---

#### `(s *ExecutionStore) List(ctx context.Context) ([]*pkgstore.ExecutionRecord, error)`

**Signature:**
```go
func (s *ExecutionStore) List(ctx context.Context) ([]*pkgstore.ExecutionRecord, error)
```

**Flow:**
1. Calls `s.inner.ListByStatus()` with no filter.
2. Converts all states to records.

---

#### `(s *ExecutionStore) ListByPlan(ctx context.Context, planID string) ([]*pkgstore.ExecutionRecord, error)`

**Signature:**
```go
func (s *ExecutionStore) ListByPlan(ctx context.Context, planID string) ([]*pkgstore.ExecutionRecord, error)
```

**Flow:**
1. Calls `s.List()` to get all records.
2. Filters by `PlanID` match.

**Edge Cases:**
- Full scan of all records (no index-based filtering).

---

#### `(s *ExecutionStore) Delete(ctx context.Context, id pkgstore.ExecutionID) error`

**Signature:**
```go
func (s *ExecutionStore) Delete(ctx context.Context, id pkgstore.ExecutionID) error
```

**Flow:**
1. Delegates to `s.inner.Delete()`.

---

#### `(s *ExecutionStore) Close() error`

**Signature:**
```go
func (s *ExecutionStore) Close() error
```

**Flow:**
1. Delegates to `s.inner.Close()`.

---

## internal/partition

### manager.go

#### `New(numPartitions uint32) *Manager`

**Signature:**
```go
func New(numPartitions uint32) *Manager
```

**Flow:**
1. Defaults to `DefaultNumPartitions` (64) if 0.
2. Allocates assignment slices and node-parts map.

**Edge Cases:**
- `numPartitions = 0` → uses default 64.
- No producer assigned by default.

---

#### `(m *Manager) SetProducer(p pkgpartition.Producer)`

**Signature:**
```go
func (m *Manager) SetProducer(p pkgpartition.Producer)
```

**Flow:**
1. Acquires write lock.
2. Stores producer reference.

---

#### `(m *Manager) Assignments() []string`

**Signature:**
```go
func (m *Manager) Assignments() []string
```

**Flow:**
1. Acquires read lock.
2. Returns copy of assignment slice (partition → nodeID).

**Edge Cases:**
- Returns a defensive copy.

---

#### `(m *Manager) NodeForPartition(partition pkgpartition.PartitionID) string`

**Signature:**
```go
func (m *Manager) NodeForPartition(partition pkgpartition.PartitionID) string
```

**Flow:**
1. Acquires read lock.
2. Returns `assignments[partition]`.
3. Returns empty string if out of bounds.

**Edge Cases:**
- Partition out of bounds → empty string (not error).

---

#### `(m *Manager) PartitionsForNode(nodeID string) []pkgpartition.PartitionID`

**Signature:**
```go
func (m *Manager) PartitionsForNode(nodeID string) []pkgpartition.PartitionID
```

**Flow:**
1. Acquires read lock.
2. Returns copy of partition IDs assigned to `nodeID`.

---

#### `(m *Manager) PartitionForKey(key string) pkgpartition.PartitionID`

**Signature:**
```go
func (m *Manager) PartitionForKey(key string) pkgpartition.PartitionID
```

**Flow:**
1. Computes FNV-32a hash of key.
2. Returns `hash % numPartitions`.

**Edge Cases:**
- Deterministic — same key always maps to same partition.
- Distribution depends on key entropy.

---

#### `(m *Manager) NumPartitions() uint32`

**Signature:**
```go
func (m *Manager) NumPartitions() uint32
```

**Flow:**
1. Returns `m.numPartitions`.

---

#### `(m *Manager) LeaderID() string`

**Signature:**
```go
func (m *Manager) LeaderID() string
```

**Flow:**
1. Acquires read lock.
2. Returns current leader ID.

**Edge Cases:**
- Empty string means no leader known.

---

#### `(m *Manager) Rebalance(aliveNodes []string, term uint64) []Assignment`

**Signature:**
```go
func (m *Manager) Rebalance(aliveNodes []string, term uint64) []Assignment
```

**Flow:**
1. Acquires write lock.
2. Sorts `aliveNodes` alphabetically.
3. Assigns partitions round-robin: `partition[i] = aliveNodes[i % len(aliveNodes)]`.
4. Sets leader to first alive node.
5. Returns `[]Assignment` for all partitions.

**Edge Cases:**
- Empty `aliveNodes` → clears all assignments.
- Leader is first node in sorted list.
- Deterministic given same set of nodes.
- Returns assignments even if unchanged.

---

#### `(m *Manager) ApplyAssignments(assignments []Assignment)`

**Signature:**
```go
func (m *Manager) ApplyAssignments(assignments []Assignment)
```

**Flow:**
1. Acquires write lock.
2. Builds new assignment array and node-parts map from provided assignments.
3. Updates term if any assignment has a higher term.

**Edge Cases:**
- Assignments with out-of-bounds partition IDs are silently skipped.
- Updates current term to the maximum across all assignments.

---

#### `(m *Manager) PublishAssignments(ctx context.Context, assignments []Assignment) error`

**Signature:**
```go
func (m *Manager) PublishAssignments(ctx context.Context, assignments []Assignment) error
```

**Flow:**
1. Acquires read lock to get producer, term, leaderID.
2. Creates `PartitionMessage` with type "assign".
3. Marshals to JSON.
4. Publishes via producer.

**Edge Cases:**
- Returns error if no producer configured.
- Release read lock before publishing (producer operations may block).

---

#### `(m *Manager) HandleAssignmentMessage(msg []byte) error`

**Signature:**
```go
func (m *Manager) HandleAssignmentMessage(msg []byte) error
```

**Flow:**
1. Unmarshals `PartitionMessage` from JSON.
2. If type is "assign":
   - Validates leader: rejects if from non-leader.
   - Validates term: rejects if stale.
   - Calls `m.ApplyAssignments()`.
3. Returns error for invalid leader/term.

**Edge Cases:**
- Rejects assignments from non-leader nodes.
- Rejects assignments with stale terms.
- If no leader is known (first assignment), accepts unconditionally.

---

#### `(m *Manager) OnLeaderChange(leaderID string)`

**Signature:**
```go
func (m *Manager) OnLeaderChange(leaderID string)
```

**Flow:**
1. Acquires write lock.
2. Updates `m.leaderID`.

**Edge Cases:**
- Does NOT trigger rebalance — caller must trigger separately.
- Empty string clears leader.

---

### rebalance.go

#### `NewRebalanceNotifier(m *Manager, aliveFn func() []string, termFn func() uint64) *RebalanceNotifier`

**Signature:**
```go
func NewRebalanceNotifier(m *Manager, aliveFn func() []string, termFn func() uint64) *RebalanceNotifier
```

**Flow:**
1. Creates notifier with manager, alive function, and term function references.

**Edge Cases:**
- `aliveFn` and `termFn` are called during `CheckAndRebalance()`.

---

#### `(rn *RebalanceNotifier) SetNotify(fn func())`

**Signature:**
```go
func (rn *RebalanceNotifier) SetNotify(fn func())
```

**Flow:**
1. Acquires mutex.
2. Stores notification callback.

**Edge Cases:**
- Only one callback supported; overwrites previous.

---

#### `(rn *RebalanceNotifier) CheckAndRebalance() bool`

**Signature:**
```go
func (rn *RebalanceNotifier) CheckAndRebalance() bool
```

**Flow:**
1. Acquires mutex.
2. Calls `aliveFn()` to get current alive nodes.
3. Returns false if no nodes.
4. Compares sorted node list with `lastNodes`.
5. If unchanged, returns false.
6. If changed:
   - Updates `lastNodes`.
   - Calls `manager.Rebalance(nodes, term)`.
   - Calls `notifyFn()` if set.
   - Returns true.

**Edge Cases:**
- Returns false if no alive nodes.
- Returns false if node set unchanged.
- Deterministic comparison (sorted lists).
- Notification fires AFTER rebalance completes.

---

## internal/plandist

### distributor.go

#### `New(nodeID string, opts ...Option) *PlanDistributor`

**Signature:**
```go
func New(nodeID string, opts ...Option) *PlanDistributor
```

**Flow:**
1. Creates `PlanDistributor` with defaults:
   - `planTopic`: `"_flowrulz_plans"`
   - `ackTopic`: `"_flowrulz_acks"`
2. Applies functional options.
3. Creates `stopCh` channel.

**Edge Cases:**
- Options can override topics, producers, consumers, handlers.

---

#### `(pd *PlanDistributor) Start(ctx context.Context) error`

**Signature:**
```go
func (pd *PlanDistributor) Start(ctx context.Context) error
```

**Flow:**
1. Acquires mutex; returns nil if already started (idempotent).
2. Sets `started = true`.
3. Starts plan consumer goroutine if configured.
4. Starts ack consumer goroutine if configured.

**Edge Cases:**
- Idempotent — multiple Start calls are safe.
- Returns nil on success.

---

#### `(pd *PlanDistributor) Stop() error`

**Signature:**
```go
func (pd *PlanDistributor) Stop() error
```

**Flow:**
1. Acquires mutex.
2. Closes `stopCh`.
3. Stops plan and ack consumers.
4. Closes plan and ack producers.
5. Sets `started = false`.

**Edge Cases:**
- No-op if not started.
- Closes stop channel once.

---

#### `(pd *PlanDistributor) SetTerm(term uint64)`

**Signature:**
```go
func (pd *PlanDistributor) SetTerm(term uint64)
```

**Flow:**
1. Atomically stores term.

---

#### `(pd *PlanDistributor) CurrentTerm() uint64`

**Signature:**
```go
func (pd *PlanDistributor) CurrentTerm() uint64
```

**Flow:**
1. Atomically loads and returns term.

---

#### `(pd *PlanDistributor) PublishPlan(ctx context.Context, ruleID string, version uint64, plan []byte, dsl string) error`

**Signature:**
```go
func (pd *PlanDistributor) PublishPlan(ctx context.Context, ruleID string, version uint64, plan []byte, dsl string) error
```

**Flow:**
1. Checks plan producer is configured.
2. Creates `PlanMessage` with type "plan".
3. Marshals to JSON.
4. Publishes with `ruleID` as key.

**Edge Cases:**
- Returns error if no plan producer configured.
- Message includes current cluster term.

---

#### `(pd *PlanDistributor) ActivatePlan(ctx context.Context, ruleID string, version uint64) error`

**Signature:**
```go
func (pd *PlanDistributor) ActivatePlan(ctx context.Context, ruleID string, version uint64) error
```

**Flow:**
1. Creates `PlanMessage` with type "activate".
2. No plan body included.
3. Publishes with `ruleID` as key.

---

#### `(pd *PlanDistributor) DeactivatePlan(ctx context.Context, ruleID string) error`

**Signature:**
```go
func (pd *PlanDistributor) DeactivatePlan(ctx context.Context, ruleID string) error
```

**Flow:**
1. Creates `PlanMessage` with type "deactivate".
2. Version set to 0.
3. Publishes with `ruleID` as key.

---

#### `(pd *PlanDistributor) OnPlan(fn func(ctx context.Context, msg PlanMessage) error)`

**Signature:**
```go
func (pd *PlanDistributor) OnPlan(fn func(ctx context.Context, msg PlanMessage) error)
```

**Flow:**
1. Acquires mutex.
2. Sets plan handler callback.

---

#### `(pd *PlanDistributor) OnAck(fn func(ctx context.Context, msg AckMessage))`

**Signature:**
```go
func (pd *PlanDistributor) OnAck(fn func(ctx context.Context, msg AckMessage))
```

**Flow:**
1. Acquires mutex.
2. Sets ack handler callback.

---

#### `PlanMessageFromBytes(data []byte) (*PlanMessage, error)`

**Signature:**
```go
func PlanMessageFromBytes(data []byte) (*PlanMessage, error)
```

**Flow:**
1. Unmarshals `PlanMessage` from JSON bytes.

**Edge Cases:**
- Returns error on invalid JSON.

---

### ack.go

#### `(pd *PlanDistributor) SendAck(ctx context.Context, ruleID string, version uint64, status string) error`

**Signature:**
```go
func (pd *PlanDistributor) SendAck(ctx context.Context, ruleID string, version uint64, status string) error
```

**Flow:**
1. Checks ack producer is configured.
2. Creates `AckMessage` with nodeID, ruleID, version, status.
3. Marshals to JSON.
4. Publishes with key `"ruleID:version"`.

**Edge Cases:**
- Returns error if no ack producer.

---

#### `(pd *PlanDistributor) WaitForAcks(ctx context.Context, ruleID string, version uint64, quorum int, timeout time.Duration) error`

**Signature:**
```go
func (pd *PlanDistributor) WaitForAcks(ctx context.Context, ruleID string, version uint64, quorum int, timeout time.Duration) error
```

**Flow:**
1. Calculates effective quorum:
   - `quorum = 0`: majority of followers `(n-1)/2 + 1`
   - `quorum < 0`: all followers `n-1`
   - `quorum > 0`: exact count
2. If single node (n=1), returns nil immediately.
3. Stores `pendingAck` in `pendingAcks` map.
4. Waits on `done` channel or context timeout.
5. Returns nil if quorum reached, error otherwise.

**Edge Cases:**
- Single-node cluster skips ack wait.
- `quorum = 0` calculates majority automatically.
- `quorum = -1` waits for all followers.
- Timeout returns `"plandist: ack timeout"` error.
- Insufficient acks returns `"plandist: insufficient acks"` error.

---

#### `(pd *PlanDistributor) RecordAck(msg AckMessage)`

**Signature:**
```go
func (pd *PlanDistributor) RecordAck(msg AckMessage)
```

**Flow:**
1. Delegates to `handleAck(msg)`.

---

#### `AckMessageFromBytes(data []byte) (*AckMessage, error)`

**Signature:**
```go
func AckMessageFromBytes(data []byte) (*AckMessage, error)
```

**Flow:**
1. Unmarshals `AckMessage` from JSON bytes.

---

## internal/replyrouter

### router.go

#### `New(opts ...Option) *ReplyRouter`

**Signature:**
```go
func New(opts ...Option) *ReplyRouter
```

**Flow:**
1. Creates `ReplyRouter` with defaults:
   - `cleanupTick`: 1 second
   - `maxPending`: 10,000
2. Applies functional options.
3. Initializes pending map and cleanup stop channel.

**Edge Cases:**
- Default max pending is 10,000.
- Default cleanup interval is 1 second.

---

#### `(rr *ReplyRouter) Register(ctx context.Context, correlationID string, ch chan<- *transport.Message, timeout time.Duration) error`

**Signature:**
```go
func (rr *ReplyRouter) Register(ctx context.Context, correlationID string, ch chan<- *transport.Message, timeout time.Duration) error
```

**Flow:**
1. Returns error if `correlationID` is empty.
2. Calculates `deadline = now + timeout`.
3. Acquires write lock.
4. Returns `ErrDuplicateCorrID` if correlation ID already exists.
5. Returns `ErrPendingLimit` if `maxPending` reached.
6. Stores `PendingRequest` with correlation ID, reply channel, and deadline.

**Edge Cases:**
- Empty correlation ID → `"replyrouter: empty correlation ID"` error.
- Duplicate ID → `ErrDuplicateCorrID` error.
- Max pending exceeded → `ErrPendingLimit` error.
- `maxPending = 0` disables limit check.

---

#### `(rr *ReplyRouter) Cancel(correlationID string)`

**Signature:**
```go
func (rr *ReplyRouter) Cancel(correlationID string)
```

**Flow:**
1. Acquires write lock.
2. Finds and removes pending request.
3. Closes reply channel.

**Edge Cases:**
- No-op if correlation ID not found.
- Closes channel to signal cancellation to receiver.
- Safe to call multiple times (idempotent after first).

---

#### `(rr *ReplyRouter) Deliver(ctx context.Context, correlationID string, msg *transport.Message) bool`

**Signature:**
```go
func (rr *ReplyRouter) Deliver(ctx context.Context, correlationID string, msg *transport.Message) bool
```

**Flow:**
1. Acquires write lock.
2. Finds and removes pending request.
3. Attempts non-blocking send on reply channel.
4. Closes reply channel.
5. Returns `true` if delivered, `false` if not found.

**Edge Cases:**
- Non-blocking send: if receiver isn't ready, message is dropped.
- Channel is always closed (even if send fails).
- Returns false if correlation ID not found.
- Lock released before close to prevent races.

---

#### `(rr *ReplyRouter) PendingCount() int`

**Signature:**
```go
func (rr *ReplyRouter) PendingCount() int
```

**Flow:**
1. Acquires read lock.
2. Returns count of pending requests.

---

#### `(rr *ReplyRouter) StartCleanup(ctx context.Context)`

**Signature:**
```go
func (rr *ReplyRouter) StartCleanup(ctx context.Context)
```

**Flow:**
1. Starts goroutine with ticker at `cleanupTick` interval.
2. On each tick, calls `rr.cleanup()`.
3. Returns when `cleanupStop` is closed.

**Edge Cases:**
- Non-blocking (returns immediately).
- Goroutine leaks if `StopCleanup()` not called.

---

#### `(rr *ReplyRouter) StopCleanup()`

**Signature:**
```go
func (rr *ReplyRouter) StopCleanup()
```

**Flow:**
1. Closes `cleanupStop` channel.

**Edge Cases:**
- Only call once; panic on double-close.
- Blocks until cleanup goroutine exits.

---

## internal/observability

### metrics.go

#### `NewMetricsCollector() *MetricsCollector`

**Signature:**
```go
func NewMetricsCollector() *MetricsCollector
```

**Flow:**
1. Initializes empty maps for counters, gauges, histograms.
2. Returns `*MetricsCollector`.

---

#### `(mc *MetricsCollector) Counter(name string) *Counter`

**Signature:**
```go
func (mc *MetricsCollector) Counter(name string) *Counter
```

**Flow:**
1. Acquires write lock.
2. Returns existing counter if found.
3. Creates and stores new counter.

**Edge Case:**
- Same name always returns same counter instance.

---

#### `(c *Counter) Inc() int64`

**Signature:**
```go
func (c *Counter) Inc() int64
```

**Flow:**
1. Atomically adds 1.
2. Returns new value.

---

#### `(c *Counter) Add(n int64) int64`

**Signature:**
```go
func (c *Counter) Add(n int64) int64
```

**Flow:**
1. Atomically adds `n`.
2. Returns new value.

---

#### `(c *Counter) Value() int64`

**Signature:**
```go
func (c *Counter) Value() int64
```

**Flow:**
1. Atomically loads value.

---

#### `(c *Counter) Reset()`

**Signature:**
```go
func (c *Counter) Reset()
```

**Flow:**
1. Atomically stores 0.

---

#### `(mc *MetricsCollector) Gauge(name string) *Gauge`

**Signature:**
```go
func (mc *MetricsCollector) Gauge(name string) *Gauge
```

**Flow:**
1. Same pattern as `Counter()`.

---

#### `(g *Gauge) Set(n int64)`

**Signature:**
```go
func (g *Gauge) Set(n int64)
```

**Flow:**
1. Atomically stores `n`.

---

#### `(g *Gauge) Add(n int64)`

**Signature:**
```go
func (g *Gauge) Add(n int64)
```

**Flow:**
1. Atomically adds `n`.

---

#### `(mc *MetricsCollector) Histogram(name string, buckets []float64) *Histogram`

**Signature:**
```go
func (mc *MetricsCollector) Histogram(name string, buckets []float64) *Histogram
```

**Flow:**
1. Returns existing histogram if found.
2. Creates histogram with `len(buckets)+1` count buckets.
3. Stores and returns.

**Edge Cases:**
- Last bucket is the `+Inf` bucket (values > last bucket boundary).

---

#### `(h *Histogram) Observe(v float64)`

**Signature:**
```go
func (h *Histogram) Observe(v float64)
```

**Flow:**
1. Increments total count.
2. Finds first bucket where `v <= bucket`.
3. Increments that bucket's count.
4. If no bucket matches, increments the overflow bucket.

---

#### `(mc *MetricsCollector) Snapshot() MetricSnapshot`

**Signature:**
```go
func (mc *MetricsCollector) Snapshot() MetricSnapshot
```

**Flow:**
1. Acquires read lock.
2. Returns `MetricSnapshot` with current counter and gauge values.

**Edge Cases:**
- Snapshots are point-in-time (not atomic across counters).

---

#### `RecordExec(name string)`

**Signature:**
```go
func RecordExec(name string)
```

**Flow:**
1. Increments counter `"exec.<name>"`.

---

#### `RecordError(name string)`

**Signature:**
```go
func RecordError(name string)
```

**Flow:**
1. Increments counter `"error.<name>"`.

---

### tracer.go

#### `NewSpanExporter(endpoint string) *SpanExporter`

**Signature:**
```go
func NewSpanExporter(endpoint string) *SpanExporter
```

**Flow:**
1. Returns nil if endpoint is empty.
2. Creates OTLP gRPC trace exporter.
3. Creates resource with `service.name=flowrulz`.
4. Creates `TracerProvider` with batcher.
5. Sets global `otel.TracerProvider`.
6. Returns `*SpanExporter` with span size from bridge.

**Edge Cases:**
- Returns nil if endpoint empty (disables tracing).
- Returns nil on exporter/resource creation failure.
- Uses insecure gRPC connection.

---

#### `(se *SpanExporter) Start(ctx context.Context)`

**Signature:**
```go
func (se *SpanExporter) Start(ctx context.Context)
```

**Flow:**
1. Returns immediately if nil.
2. Starts 5-second ticker.
3. On each tick, calls `exportSpans()`.
4. Returns on `stopCh` or `ctx.Done()`.

**Edge Cases:**
- Nil-safe (no-op on nil receiver).
- Goroutine leaks if `Stop()` not called.

---

#### `(se *SpanExporter) Stop()`

**Signature:**
```go
func (se *SpanExporter) Stop()
```

**Flow:**
1. Returns immediately if nil.
2. Closes `stopCh`.
3. Shuts down provider with 5-second timeout.

---

## internal/flow

### registry.go

#### `NewRegistry(c cache.Cache) *Registry`

**Signature:**
```go
func NewRegistry(c cache.Cache) *Registry
```

**Flow:**
1. Creates `Registry` with empty flows map.
2. Initializes `Parser`, `Analyzer`, `Compiler`.
3. Sets TTL to 5 minutes.
4. Creates stop channel.

---

#### `(r *Registry) LoadFile(ctx context.Context, path string) error`

**Signature:**
```go
func (r *Registry) LoadFile(ctx context.Context, path string) error
```

**Flow:**
1. Parses `.flow` file into AST via `r.parser.ParseFile()`.
2. Calls `r.Register(ctx, ast)`.

---

#### `(r *Registry) LoadDirectory(ctx context.Context, dir string) error`

**Signature:**
```go
func (r *Registry) LoadDirectory(ctx context.Context, dir string) error
```

**Flow:**
1. Reads directory entries.
2. Filters to `.flow` files.
3. Calls `r.LoadFile()` for each.

**Edge Cases:**
- Stops on first error.
- Skips non-`.flow` files.

---

#### `(r *Registry) Register(ctx context.Context, ast *Flow) error`

**Signature:**
```go
func (r *Registry) Register(ctx context.Context, ast *Flow) error
```

**Flow:**
1. Validates name is non-empty.
2. Runs semantic analysis; stores error state if analysis fails.
3. Compiles AST to IR.
4. Computes SHA-256 hash of AST.
5. Caches IR in cache store.
6. Caches route mapping if trigger topic exists.
7. Stores `FlowState` in registry.

**Edge Cases:**
- Semantic errors stored in `FlowState.Status = "error"`.
- Cache key format: `"flow:<name>:ir"`.
- Route key format: `"flow:route:<topic>"`.

---

#### `(r *Registry) Get(ctx context.Context, name string) (*FlowState, error)`

**Signature:**
```go
func (r *Registry) Get(ctx context.Context, name string) (*FlowState, error)
```

**Flow:**
1. Checks in-memory registry first.
2. Falls back to cache (deserializes IR).
3. Returns error if not found anywhere.

**Edge Cases:**
- Cache fallback provides graceful degradation.

---

#### `(r *Registry) GetByTopic(ctx context.Context, topic string) (*FlowState, error)`

**Signature:**
```go
func (r *Registry) GetByTopic(ctx context.Context, topic string) (*FlowState, error)
```

**Flow:**
1. Checks route cache for topic mapping.
2. Falls back to linear scan of all flows.
3. Returns error if no flow matches topic.

---

#### `(r *Registry) List(ctx context.Context) []*FlowState`

**Signature:**
```go
func (r *Registry) List(ctx context.Context) []*FlowState
```

**Flow:**
1. Returns all registered flows as a slice.

---

#### `(r *Registry) Delete(ctx context.Context, name string) error`

**Signature:**
```go
func (r *Registry) Delete(ctx context.Context, name string) error
```

**Flow:**
1. Removes from in-memory registry.
2. Removes from cache.
3. Returns error if not found.

---

#### `(r *Registry) Format(name string) (string, error)`

**Signature:**
```go
func (r *Registry) Format(name string) (string, error)
```

**Flow:**
1. Finds flow by name.
2. Returns canonical `.flow` representation via `Formatter`.

**Edge Cases:**
- Returns error if flow not found or AST is nil.

---

#### `(r *Registry) Close() error`

**Signature:**
```go
func (r *Registry) Close() error
```

**Flow:**
1. Closes stop channel.

---

## bridge

### compile.go

#### `Compile(dsl string, ruleID string) ([]byte, error)`

**Signature:**
```go
func Compile(dsl string, ruleID string) ([]byte, error)
```

**Flow:**
1. Returns error if DSL is empty.
2. Gets output buffer from pool (256KB).
3. Calls C function `flowrulz_compile()` via CGo.
4. On success, copies output to new byte slice.
5. Returns compiled plan bytes.

**Edge Cases:**
- Empty DSL → `"compile: empty dsl"` error.
- Uses sync.Pool for output buffers to reduce allocations.
- Error buffer is 4KB; errors beyond that are truncated.
- CGo FFI call; must be called from main goroutine or properly initialized thread.

---

#### `Intern(s string) uint16`

**Signature:**
```go
func Intern(s string) uint16
```

**Flow:**
1. Returns 0 if string is empty.
2. Calls C function `flowrulz_intern()`.
3. Returns 16-bit intern ID.

**Edge Cases:**
- Empty string → 0.
- Same string always returns same ID (intern table).

---

#### `InternLookup(id uint16) string`

**Signature:**
```go
func InternLookup(id uint16) string
```

**Flow:**
1. Allocates 256-byte buffer.
2. Calls C function `flowrulz_intern_lookup()`.
3. Returns string from buffer.

**Edge Cases:**
- Buffer overflow if interned string > 256 bytes (silently truncated).
- ID 0 returns empty string.

---

#### `RegisterPlugin(name string, wasmBytes []byte) error`

**Signature:**
```go
func RegisterPlugin(name string, wasmBytes []byte) error
```

**Flow:**
1. Validates name is non-empty.
2. Validates wasmBytes is non-empty.
3. Calls C function `flowrulz_register_plugin()`.

**Edge Cases:**
- Empty name → error.
- Empty WASM bytes → error.
- FFI error → returns error with error code.

---

### execute.go

#### `InitContext(body []byte) ([]byte, error)`

**Signature:**
```go
func InitContext(body []byte) ([]byte, error)
```

**Flow:**
1. Gets output buffer from pool.
2. Calls C function `flowrulz_init_context()`.
3. Returns initialized context bytes.

**Edge Cases:**
- Empty body is allowed (nil pointer).
- Context bytes are opaque to Go.

---

#### `Execute(plan []byte, body []byte, caller ServiceCaller, ctx *ExecContext) ([]byte, error)`

**Signature:**
```go
func Execute(plan []byte, body []byte, caller ServiceCaller, ctx *ExecContext) ([]byte, error)
```

**Flow:**
1. Returns error if plan is empty.
2. Generates unique `ctxID` via atomic counter.
3. Stores `caller` in `callerMap` for CGo callback.
4. Extracts optional `ExecContext` fields (MessageID, CorrelationID, TraceID, Partition, Offset).
5. Gets output buffer from pool.
6. Calls C function `flowrulz_execute()`.
7. Cleans up caller from map on return.

**Edge Cases:**
- Empty plan → error.
- Nil caller is allowed (no service calls).
- Nil `ctx` is allowed (no message metadata).
- `callerMap` uses sync.Map for concurrent access.
- Caller response limited to 65536 bytes.
- panics in caller callback are caught by `defer recover()` in `goServiceCaller`.

---

#### `(o *StepOutput) DelayMs() uint64`

**Signature:**
```go
func (o *StepOutput) DelayMs() uint64
```

**Flow:**
1. If `PendingBody` < 8 bytes, returns 0.
2. Reads first 8 bytes as little-endian uint64.

**Edge Cases:**
- Returns 0 for short bodies.

---

#### `ExecuteStep(plan, ctxBytes, respBytes []byte, caller ServiceCaller) (*StepOutput, error)`

**Signature:**
```go
func ExecuteStep(plan, ctxBytes, respBytes []byte, caller ServiceCaller) (*StepOutput, error)
```

**Flow:**
1. Generates unique `ctxID`.
2. Stores caller in map.
3. Gets output buffer from pool.
4. Calls C function `flowrulz_execute_step()`.
5. Populates `StepOutput` with result, output, pending info, context bytes.
6. Error strings set for specific error codes (-8, -1).

**Edge Cases:**
- `respBytes` can be nil (first step) or empty sentinel.
- `ctxBytes` can be nil (first step).
- `StepResult` codes: 0=Done, 1=Pending, 2=Continue, 3=Delay.

---

### bridge.go

#### `SpanSize() int`

**Signature:**
```go
func SpanSize() int
```

**Flow:**
1. Returns size of a single span from C runtime.

---

#### `GetSpans() []byte`

**Signature:**
```go
func GetSpans() []byte
```

**Flow:**
1. Allocates 4096-byte buffer.
2. Calls C function `flowrulz_get_spans()`.
3. Returns buffer truncated to actual span count.

**Edge Cases:**
- Fixed 4096-byte buffer; may truncate if many spans.

---

#### `ParseServiceMethod(s string) (service, method string)`

**Signature:**
```go
func ParseServiceMethod(s string) (service, method string)
```

**Flow:**
1. Splits on first `.`.
2. Returns `(before, after)`.
3. If no `.`, returns `(s, "")`.

**Edge Cases:**
- `"user.Create"` → `("user", "Create")`
- `"user"` → `("user", "")`

---

#### `ParseCompensation(s string) (service, method, compensator, compMethod string)`

**Signature:**
```go
func ParseCompensation(s string) (service, method, compensator, compMethod string)
```

**Flow:**
1. Splits on `:`.
2. If no `:`, parses as service.method only.
3. Before colon: parsed as service.method.
4. After colon: parsed as compensator.method or service.compMethod.

**Edge Cases:**
- `"user.Create:rollback"` → `("user", "Create", "user", "rollback")`
- `"user.Create:saga.Undo"` → `("user", "Create", "saga", "Undo")`
- `"user.Create"` → `("user", "Create", "", "")`

---

#### `MsgAlloc(size int) unsafe.Pointer`

**Signature:**
```go
func MsgAlloc(size int) unsafe.Pointer
```

**Flow:**
1. Allocates memory in C runtime.

---

#### `MsgRelease(ptr unsafe.Pointer)`

**Signature:**
```go
func MsgRelease(ptr unsafe.Pointer)
```

**Flow:**
1. Releases memory in C runtime.

---

### plan.go

#### `PlanServices(plan []byte) ([]ServiceEntry, error)`

**Signature:**
```go
func PlanServices(plan []byte) ([]ServiceEntry, error)
```

**Flow:**
1. Returns error if plan is empty.
2. Calls C function `flowrulz_plan_services()`.
3. Unmarshals JSON array of `ServiceEntry` (ID + Name).

**Edge Cases:**
- Empty plan → error.
- FFI error → returns error code.
- Returns list of service references in the plan.

---

#### `PlanComplexity(plan []byte) uint32`

**Signature:**
```go
func PlanComplexity(plan []byte) uint32
```

**Flow:**
1. Returns 0 if plan is empty.
2. Calls C function `flowrulz_plan_complexity()`.

**Edge Cases:**
- Empty plan → 0.

---

### vm_adapter.go

#### `NewBridgeVM() *BridgeVM`

**Signature:**
```go
func NewBridgeVM() *BridgeVM
```

**Flow:**
1. Returns empty `BridgeVM` implementing `vm.PlanCompiler` and `vm.VMRunner`.

**Edge Cases:**
- Compile-time checks: `var _ vm.PlanCompiler = (*BridgeVM)(nil)` and `var _ vm.VMRunner = (*BridgeVM)(nil)`.

---

#### `(b *BridgeVM) Compile(ctx context.Context, dsl string, ruleID string) (*vm.CompileResult, error)`

**Signature:**
```go
func (b *BridgeVM) Compile(ctx context.Context, dsl string, ruleID string) (*vm.CompileResult, error)
```

**Flow:**
1. Calls `Compile(dsl, ruleID)`.
2. Extracts services via `PlanServices()`.
3. Gets complexity via `PlanComplexity()`.
4. Returns `CompileResult` with plan bytes, services, complexity.

**Edge Cases:**
- Wraps errors with `vm.ErrCompileFailed`.

---

#### `(b *BridgeVM) CompileAndCache(ctx context.Context, dsl string, ruleID string) (*vm.CompileResult, error)`

**Signature:**
```go
func (b *BridgeVM) CompileAndCache(ctx context.Context, dsl string, ruleID string) (*vm.CompileResult, error)
```

**Flow:**
1. Delegates to `b.Compile()`.

**Edge Cases:**
- No caching implemented (forward-compatible stub).

---

#### `(b *BridgeVM) InvalidateCache(ruleID string)`

**Signature:**
```go
func (b *BridgeVM) InvalidateCache(ruleID string)
```

**Flow:**
1. No-op (cache invalidation not implemented).

---

#### `(b *BridgeVM) InitContext(ctx context.Context, body []byte) ([]byte, error)`

**Signature:**
```go
func (b *BridgeVM) InitContext(ctx context.Context, body []byte) ([]byte, error)
```

**Flow:**
1. Delegates to `InitContext(body)`.

---

#### `(b *BridgeVM) ExecuteStep(ctx context.Context, plan, ctxBytes, respBytes []byte, opts *vm.StepOptions) (*vm.StepResult, error)`

**Signature:**
```go
func (b *BridgeVM) ExecuteStep(ctx context.Context, plan, ctxBytes, respBytes []byte, opts *vm.StepOptions) (*vm.StepResult, error)
```

**Flow:**
1. Extracts `ServiceCallback` from opts.
2. Calls `ExecuteStep(plan, ctxBytes, respBytes, caller)`.
3. Converts `StepOutput` to `vm.StepResult`.
4. Maps bridge step codes to vm step codes.

**Edge Cases:**
- `opts` can be nil.
- Nil callback → nil caller → no service calls.
- Maps unknown step codes to `vm.StepFailed`.

---

#### `(b *BridgeVM) ParseServiceMethod(raw string) (string, string)`

**Signature:**
```go
func (b *BridgeVM) ParseServiceMethod(raw string) (string, string)
```

**Flow:**
1. Delegates to `ParseServiceMethod(raw)`.

---

## pkg

### pkg/common

#### `HashBody(body []byte) string`

**Signature:**
```go
func HashBody(body []byte) string
```

**Flow:**
1. Computes FNV-128a hash of body.
2. Returns hex-encoded hash string.

**Edge Cases:**
- Empty body produces valid hash.
- Used for deduplication and rate-limit message IDs.

---

#### `HashBodyPrefixed(prefix string, body []byte) string`

**Signature:**
```go
func HashBodyPrefixed(prefix string, body []byte) string
```

**Flow:**
1. Computes `HashBody(body)`.
2. Returns `"<prefix>-<hash>"`.

**Edge Cases:**
- Empty prefix is allowed (produces `-hash`).
- Used for namespacing different hash domains.

---

#### `NewBearerAuth() *BearerAuth`

**Signature:**
```go
func NewBearerAuth() *BearerAuth
```

**Flow:**
1. Returns `BearerAuth` that lazily loads `FLOWRULZ_API_KEY` from env.

**Edge Cases:**
- Key is loaded once via `sync.Once`.
- Empty env var → auth disabled (open mode).

---

#### `(a *BearerAuth) Check(r *http.Request) bool`

**Signature:**
```go
func (a *BearerAuth) Check(r *http.Request) bool
```

**Flow:**
1. Loads API key (once).
2. If key is empty, returns true (open mode).
3. Compares `Authorization` header with `"Bearer <key>"` using constant-time comparison.

**Edge Cases:**
- Open mode (no key configured) → always true.
- Constant-time comparison prevents timing attacks.

---

#### `(a *BearerAuth) Require(next http.HandlerFunc) http.HandlerFunc`

**Signature:**
```go
func (a *BearerAuth) Require(next http.HandlerFunc) http.HandlerFunc
```

**Flow:**
1. Checks auth.
2. Returns 401 if unauthorized.
3. Calls `next` if authorized.

---

#### `WriteJSON(path string, v any) error`

**Signature:**
```go
func WriteJSON(path string, v any) error
```

**Flow:**
1. Marshals `v` to JSON.
2. Creates parent directory with `MkdirAll`.
3. Writes to `.tmp` file with `0600` permissions.
4. Renames `.tmp` to final path (atomic).

**Edge Cases:**
- Atomic write prevents corruption on crash.
- Directory created automatically.
- File permissions: `0600` (owner read/write only).

---

#### `ReadJSON(path string, v any) error`

**Signature:**
```go
func ReadJSON(path string, v any) error
```

**Flow:**
1. Reads file.
2. Unmarshals JSON into `v`.

---

#### `LoadDir[T any](dir, ext string, decode func([]byte) (T, error)) ([]T, error)`

**Signature:**
```go
func LoadDir[T any](dir, ext string, decode func([]byte) (T, error)) ([]T, error)
```

**Flow:**
1. Reads directory entries.
2. Filters by extension.
3. Reads and decodes each file.
4. Skips files that fail to read or decode.

**Edge Cases:**
- Generic function; works with any type.
- Silently skips unreadable/undecodable files.
- Returns nil (not error) for empty directories.

---

### pkg/cluster

#### `LeadershipToken.Valid() bool`

**Signature:**
```go
func (lt LeadershipToken) Valid() bool
```

**Flow:**
1. Returns `lt.Leader && lt.Term > 0`.

**Edge Cases:**
- Term must be > 0 (prevents stale tokens from term 0).
- Used for split-brain fencing.

---

### pkg/scheduler

#### `DefaultLaneConfigs() map[Lane]LaneConfig`

**Signature:**
```go
func DefaultLaneConfigs() map[Lane]LaneConfig
```

**Flow:**
1. Returns default lane configurations:
   - `LaneFast`: Concurrency=50, QueueSize=5000
   - `LaneNormal`: Concurrency=20, QueueSize=2000
   - `LaneHeavy`: Concurrency=5, QueueSize=500

**Edge Cases:**
- Fast lane: highest concurrency, largest queue.
- Heavy lane: lowest concurrency, smallest queue.
