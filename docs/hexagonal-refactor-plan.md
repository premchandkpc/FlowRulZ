# Hexagonal Architecture Refactoring Plan

> Goal: Separate concerns properly with ports & adapters, single composition root,
> loose coupling. Incremental — not a rewrite.

---

## Problem Statement

The `node` package is a god-package (17 internal imports, 22-field Dependencies struct,
all lifecycle orchestration). Interfaces live inside the core instead of at boundaries.
Global singletons (`observability.RecordExec`) prevent testability. The composition
root (`DefaultDependencies`) constructs everything but offers no override capability.

---

## Architecture After Refactoring

```
cmd/flowrulz/main.go          ← thin entry point, env→Config only
  │
  └─ internal/bootstrap/       ← COMPOSITION ROOT: wires adapters → ports
       │
       ├─ internal/core/        ← DOMAIN LOGIC (zero imports from adapters/)
       │    ├─ domain/          ← domain types: Rule, Plan, ExecutionID, etc.
       │    ├─ execution/       ← execute rules, orchestrate service calls
       │    ├─ distribution/    ← plan distribution, ack protocol
       │    ├─ clustering/      ← leadership, fencing, term management
       │    └─ partitioning/    ← shard assignment, rebalance logic
       │
       ├─ internal/ports/       ← INTERFACES the core needs (inbound + outbound)
       │
       └─ internal/adapters/    ← IMPLEMENTATIONS of ports
            ├─ transport/       ← Kafka, gRPC Cluster Bus
            ├─ persistence/     ← engine file store, exec state, cache
            ├─ reliability/     ← DLQ, circuit breaker, dedup, rate limiter, saga
            ├─ observability/   ← metrics, OTel tracing
            ├─ registry/        ← service discovery, HTTP registration
            ├─ admin/           ← HTTP admin API
            ├─ compiler/        ← DSL → bytecode (local FFI or remote)
            ├─ plugins/         ← WASM loader
            └─ flow/            ← flow DSL parser, analyzer, compiler
```

**Key principle:** `core/` never imports `adapters/`. `adapters/` import `ports/`.
`bootstrap/` imports both. `cmd/` imports only `bootstrap/`.

---

## Phase 1: Extract Port Interfaces (foundation)

Move all port interfaces from `node/interfaces.go` and scattered locations into
`internal/ports/`. Each port file is small, pure Go, zero imports from `internal/`.

### New files: `internal/ports/`

```go
// ports/execution.go — outbound ports the execution engine needs
type RuleEngine interface {
    ActivePlanBytes() [][]byte
    AddVersion(id, dsl string, plan []byte, version uint64) error
    Promote(id string, version uint64) error
    SetAfterDeploy(fn func(id, dsl string, plan []byte, version uint64))
    SetAfterPromote(fn func(id string, version uint64))
}

type ServiceInvoker interface {
    Invoke(ctx context.Context, serviceName, method string, body []byte) ([]byte, error)
}

type StateStore interface {
    Create(ctx context.Context, record *ExecutionRecord) error
    Save(ctx context.Context, record *ExecutionRecord) error
    Load(ctx context.Context, id ExecutionID) (*ExecutionRecord, error)
    List(ctx context.Context) ([]*ExecutionRecord, error)
    Delete(ctx context.Context, id ExecutionID) error
    Close() error
}

// ports/transport.go — outbound transport
type MessageProducer interface {
    SendMessage(ctx context.Context, key string, value []byte) error
    Close() error
}

type MessageConsumer interface {
    Start(ctx context.Context) error
    Stop() error
}

type TransportFactory interface {
    NewProducer(topic string) MessageProducer
    NewConsumer(topic string, handler MessageHandler) MessageConsumer
}

// ports/cluster.go — outbound cluster operations
type ClusterMember interface {
    IsLeader() bool
    CurrentTerm() uint64
    LeaderID() string
    LeaderAddr() string
    Start(ctx context.Context) error
    Stop(ctx context.Context) error
    SubscribeLeaderChanges(fn func(isLeader bool))
    CaptureLeadershipToken() LeadershipToken
    ValidateLeadershipToken(token LeadershipToken) bool
}

// ports/registry.go — outbound service discovery
type ServiceRegistry interface {
    Lookup(serviceName, method string) (*ServiceInstance, error)
    MarkUnhealthy(serviceName, nodeID string)
    RegisterHTTPHandler(w http.ResponseWriter, r *http.Request)
    HeartbeatHTTPHandler(w http.ResponseWriter, r *http.Request)
    ListServicesHTTPHandler(w http.ResponseWriter, r *http.Request)
}

// ports/metrics.go — outbound observability
type MetricsCollector interface {
    RecordExec(name string)
    RecordError(name string)
    Snapshot() MetricSnapshot
}

type Tracer interface {
    Start(ctx context.Context)
    Stop()
}

// ports/reliability.go — outbound resilience
type RateLimiter interface {
    Allow(key string) bool
}

type Deduplicator interface {
    CheckAndMark(key string) bool
    StartCleanup(ctx context.Context, interval time.Duration)
}

type CircuitBreaker interface {
    Allow() bool
    Success()
    Failure()
}

type DeadLetterQueue interface {
    Send(entry *DeadLetterEntry) error
    Len() int
}

type SagaTracker interface {
    RegisterStep(execID string, step SagaStep)
    Compensate(execID string) error
    Clear(execID string)
}

// ports/config.go — configuration value object
type Config struct { ... }  // moved from node/config.go
```

### Domain types: `internal/core/domain/`

Move pure data types out of adapter packages into domain:

```go
// domain/types.go
type Rule struct { ID, DSL string; Active bool; Lane string; ... }
type Plan struct { ID string; Version uint64; Bytes []byte }
type ExecutionID string
type ExecutionRecord struct { ID ExecutionID; PlanID string; State string; ... }
type ServiceInstance struct { ID, Name, Address string; Healthy bool; Methods []MethodSpec }
type DeadLetterEntry struct { ... }
type SagaStep struct { ... }
type MetricSnapshot struct { ... }
type LeadershipToken struct { Term uint64; LeaderID string; Valid bool }
type AckMessage struct { ... }
```

**This breaks the leak** where `node/interfaces.go` imports `registry.ServiceInstance`
(concrete adapter type) in a port interface.

---

## Phase 2: Break the God-Package (`node/`)

Split `internal/node/` into focused domain packages under `internal/core/`:

### `internal/core/execution/` (from `node/execution_engine.go`)
- `ExecutionEngine` struct — orchestrates rule execution
- Imports only `ports/` interfaces
- No knowledge of transport, cluster, or admin

### `internal/core/distribution/` (from `node/lifecycle.go` distributePlan, distributeActivate)
- `PlanDistributor` — publish plans, wait for acks, activate
- Imports only `ports/` interfaces

### `internal/core/clustering/` (from `node/leadership.go`, `node/cluster.go`)
- `RaftLeadershipStrategy`, `SingleLeaderStrategy`
- Fencing token logic
- Imports only `ports/` interfaces

### `internal/core/partitioning/` (from `node/lifecycle.go` rebalance logic)
- Rebalance orchestration
- Imports only `ports/` interfaces

### `internal/node/prod.go` — becomes thin orchestrator

After extraction, `ProdNode` shrinks to:

```go
type ProdNode struct {
    config     ports.Config
    execution  *coreexecution.Engine
    cluster    *coreclustering.Manager
    distrib    *coredistribution.PlanDistributor
    parts      *corepartitioning.Manager
    // ... remaining deps via ports
}

func (n *ProdNode) Start(ctx context.Context) error {
    n.cluster.Start(ctx)
    n.distrib.Start(ctx)
    n.parts.Start(ctx)
    n.execution.Start(ctx)
    // ... start adapters
}

func (n *ProdNode) Shutdown(ctx context.Context) error {
    // graceful shutdown in reverse order
}
```

`ProdNode` becomes a **lifecycle orchestrator** — it starts/stops subsystems but
contains no business logic itself.

---

## Phase 3: Eliminate Global Singletons

### `observability/metrics.go`

Before:
```go
var defaultCollector = NewMetricsCollector()
func RecordExec(name string)  { GetCounter("exec." + name).Inc() }
```

After: `MetricsCollector` is a port. `observability` package implements it as an
adapter. All callers receive it via dependency injection.

```go
// ports/metrics.go
type MetricsCollector interface {
    RecordExec(name string)
    RecordError(name string)
    Snapshot() MetricSnapshot
}

// adapters/observability/prometheus.go
type PrometheusCollector struct { ... }
func (c *PrometheusCollector) RecordExec(name string) { ... }
```

---

## Phase 4: Proper Composition Root

### `internal/bootstrap/wire.go` (renamed from `builder.go`)

```go
func Wire(cfg Config) *node.ProdNode {
    // --- Adapters ---
    metrics     := observability.NewPrometheusCollector()
    tracer      := observability.NewOTelTracer(cfg.OTELEndpoint)
    store       := persistence.NewMemoryStore()
    engine      := engine.New(cfg.PersistPath)
    sched       := scheduler.New(nil)
    replyRouter := replyrouter.New(...)
    dedup       := reliability.NewDedupTracker(...)
    rateLimiter := reliability.NewRateLimiter()
    dlq         := reliability.NewDLQ(...)
    saga        := reliability.NewSagaTracker(...)
    registry    := registry.New()
    transport   := transport.NewFactory(...)
    cluster     := cluster.NewRaftCluster(...)
    grpcBus     := grpc.NewBus(cfg.GRPCAddr)
    adminSrv    := admin.New(engine)
    flowReg     := flow.NewRegistry(cache.NewMemoryCache())
    invoker     := NewProductionInvoker(registry)

    // --- Core ---
    exec := coreexecution.New(engine, sched, store, saga, invoker, metrics)
    distrib := coredistribution.New(cluster, transport, ...)
    clustering := coreclustering.New(cluster, ...)
    parts := corepartitioning.New(transport, ...)

    // --- Node (thin orchestrator) ---
    return node.New(node.Config{...}, node.Layers{
        Execution:  exec,
        Cluster:    clustering,
        Distrib:    distrib,
        Parts:      parts,
        Metrics:    metrics,
        // ...
    })
}
```

**Override capability:** Callers can replace any adapter before calling `Wire()`:

```go
func main() {
    cfg := loadConfig()
    // Custom metrics collector
    metrics := mycustom.NewMetricsCollector()
    // Wire with override
    n := bootstrap.WireWith(cfg, bootstrap.WithMetrics(metrics))
    n.Start(ctx)
}
```

---

## Phase 5: Clean Up `pkg/` Interfaces

Now that `internal/ports/` defines the real port surfaces, audit `pkg/`:

| Package | Action | Reason |
|---|---|---|
| `pkg/cluster` | **Keep** | Used as DI type in `ProdNode` fields |
| `pkg/membership` | **Keep** | Used as DI type |
| `pkg/node` | **Keep types only** | `ID`, `ExecuteRequest`, `ExecuteResponse` |
| `pkg/partition` | **Keep** | Used as DI type |
| `pkg/plandist` | **Keep** | Used as DI type |
| `pkg/replyrouter` | **Keep** | Used as DI type |
| `pkg/scheduler` | **Keep** | Used as DI type |
| `pkg/transport` | **Keep** | Used as DI type |

All `pkg/` interfaces should be thin — just the interface + domain types.
The rich implementations stay in `internal/adapters/`.

---

## File Move Summary

| From | To | Action |
|---|---|---|
| `node/interfaces.go` | `internal/ports/*.go` | Move interfaces, remove internal imports |
| `node/execution_engine.go` | `internal/core/execution/` | Move struct + logic |
| `node/lifecycle.go` (distribute*) | `internal/core/distribution/` | Move plan distribution logic |
| `node/leadership.go` | `internal/core/clustering/` | Move leadership strategies |
| `node/lifecycle.go` (rebalance) | `internal/core/partitioning/` | Move rebalance orchestration |
| `node/service_caller.go` | `internal/adapters/invoker/` | Protocol-aware dispatch |
| `node/production_invoker.go` | `internal/adapters/invoker/` | Service lookup + dispatch |
| `node/admin_http.go` | `internal/adapters/admin/` | HTTP server wiring |
| `node/cluster_adapter.go` | `internal/adapters/cluster/` | Cluster transport adapter |
| `node/message_router.go` | `internal/adapters/transport/` | Message routing |
| `node/ingress_pipeline.go` | `internal/core/execution/` | Ingress pipeline (part of execution) |
| `node/recovery.go` | `internal/core/execution/` | Recovery logic |
| `node/exec_registry.go` | `internal/ports/` or `core/execution/` | Execution tracking |
| `node/config.go` | `internal/ports/config.go` | Config becomes a port value object |
| `node/factory.go` | `internal/bootstrap/wire.go` | Single composition root |
| `bootstrap/builder.go` | `internal/bootstrap/wire.go` | Merge into composition root |
| `observability/metrics.go` (globals) | `adapters/observability/` | Remove global singletons |

---

## What `node/` Looks Like After

```
internal/node/
├── prod.go          — ProdNode: lifecycle orchestrator, thin
├── config.go        — Config struct (or move to ports/)
└── node_test.go     — integration tests
```

~50 lines in `prod.go`, down from 295+ across 12 files.

---

## Migration Strategy (incremental, not big-bang)

### Step 1: Create `internal/ports/` with interfaces
- Copy interfaces from `node/interfaces.go` → `ports/*.go`
- Remove `internal/` imports from port interfaces (use domain types)
- Update `node/interfaces.go` to alias `ports/` types
- **Build + test must pass** — no behavioral change

### Step 2: Create `internal/core/domain/` with types
- Move `ServiceInstance`, `ExecutionRecord`, `DeadLetterEntry`, etc. to domain
- Update all imports
- **Build + test must pass**

### Step 3: Extract `internal/core/execution/`
- Move `ExecutionEngine` + ingress pipeline + recovery
- Wire through ports
- **Build + test must pass**

### Step 4: Extract `internal/core/distribution/`
- Move plan distribution logic
- **Build + test must pass**

### Step 5: Extract `internal/core/clustering/`
- Move leadership strategies
- **Build + test must pass**

### Step 6: Extract `internal/core/partitioning/`
- Move rebalance orchestration
- **Build + test must pass**

### Step 7: Move adapters to `internal/adapters/`
- Move `service_caller.go`, `admin_http.go`, `cluster_adapter.go`, `message_router.go`
- **Build + test must pass**

### Step 8: Eliminate observability globals
- Replace `RecordExec()`/`RecordError()` with injected `MetricsCollector`
- Update all call sites
- **Build + test must pass**

### Step 9: Clean up bootstrap/composition root
- Merge `builder.go` into `wire.go`
- Add `WireWith()` + functional options for overrides
- **Build + test must pass**

### Step 10: Clean up `pkg/` interfaces
- Verify all `pkg/` interfaces are still needed
- Remove any that are now redundant with `ports/`
- **Build + test must pass**

---

## What This Achieves

| Before | After |
|---|---|
| `node` imports 17 internal packages | `core/` imports only `ports/` |
| 22-field Dependencies struct | Typed layer structs, each < 6 fields |
| Global `RecordExec()` | Injected `MetricsCollector` |
| No testability at domain level | Core logic testable with mock ports |
| `bootstrap.NodeBuilder` is vestigial | `WireWith()` functional options |
| `ProdNode` owns all business logic | `ProdNode` is lifecycle orchestrator only |
| Interfaces leak adapter types | Domain types in `core/domain/` |
| One 295-line god file | 10 focused files, each < 100 lines |
