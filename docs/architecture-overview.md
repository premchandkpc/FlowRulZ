# Go Server Architecture

> Last updated: 2026-07-06. Replaces the archived `restructure-plan-ARCHIVED.md`.

## Package Hierarchy (Hexagonal)

```
cmd/flowrulz/              ← thin entry point, env→Config only
  │
  └─ internal/bootstrap/   ← COMPOSITION ROOT: wires adapters → ports
       │
       ├─ internal/core/           ← DOMAIN LOGIC (zero adapter imports)
       │    ├─ domain/             ← domain types (ExecutionID, ServiceInstance, etc.)
       │    ├─ execution/          ← ExecutionEngine + IngressPipeline
       │    ├─ clustering/         ← RaftLeadershipStrategy, SingleLeaderStrategy
       │    └─ distribution/       ← PlanOrchestrator (publish → ack → activate)
       │
       ├─ internal/ports/          ← INTERFACES the core needs (zero internal imports)
       │
       ├─ internal/adapters/       ← IMPLEMENTATIONS of ports
       │    ├─ invoker/            ← ProductionInvoker (HTTP/gRPC/TCP dispatch)
       │    ├─ observability/      ← MetricsAdapter (wraps global singletons)
       │    └─ cluster/            ← TransportAdapter (wraps ClusterNode)
       │
       └─ internal/node/           ← ProdNode: thin lifecycle orchestrator
```

**Golden rule:** `core/` never imports `adapters/`. `adapters/` import `ports/`.
`bootstrap/` imports both. `cmd/` imports only `bootstrap/`.

---

## `server/pkg/` — 8 Live Interface Packages

| Package | Purpose | Key Types |
|---|---|---|
| `cluster` | Cluster membership & leadership | `ClusterMember`, `MemberID`, `LeadershipToken` |
| `membership` | Node membership tracking | `Membership`, `MemberInfo` |
| `node` | Node types only (no interface) | `ID`, `ExecuteRequest`, `ExecuteResponse` |
| `partition` | Key-space shard management | `Partition`, `Partitioner` |
| `plandist` | Plan distribution protocol | `PlanDistributor`, `PlanMessage` |
| `replyrouter` | Correlation ID routing | `ReplyRouter` |
| `scheduler` | Priority lanes + scheduling | `Scheduler`, `Lane`, `Result` |
| `transport` | Event bus abstraction | `EventBus`, `Publisher`, `Subscriber` |

**Deleted (2026-07-06):** `engine/`, `registry/`, `store/`, `reliability/`, `vm/` — Potemkin abstractions declared but never used as DI types. `node/Node` interface — dead (only referenced by a compile-time shim).

---

## `server/internal/` — Implementation Packages

### Core Node
- **`node/`** — `ProdNode` (central struct), `Dependencies` (DI fields), `Interfaces` (16 real DI contracts: `NodeEngine`, `NodeDLQ`, etc.), leadership fencing logic
- **`bootstrap/`** — `NodeBuilder` composition root, wires `ProdNode` via `WithDefaults()`

### Execution
- **`engine/`** — Rule lifecycle, versioning, lane routing, persistence
- **`scheduler/`** — Priority lanes (fast/heavy/normal), work stealing
- **`pipeline/`** — Ingress pipeline, enrichment, Gate/Map execution
- **`flowengine/`** — Flow orchestration state machine

### Cluster & Distribution
- **`cluster/`** — gRPC Cluster Bus (peer-to-peer), `RaftCluster`, `ClusterMember` adapter
- **`membership/`** — Gossip protocol, leader lease, heartbeat eviction
- **`partition/`** — FNV-32a key hashing, shard assignment, rebalancing
- **`plandist/`** — Plan distribution + ack protocol
- **`replyrouter/`** — Correlation ID → pending request channel

### Transport
- **`transport/`** — Adapter layer over Kafka (legacy) and gRPC
- **`transport/grpc/`** — gRPC transport implementation
- **`transport/kafka/`** — Kafka transport (legacy fallback, only active when `FLOWRULZ_KAFKA_BROKERS` set)

### Infrastructure
- **`policy/`** — 9-level policy resolver (Platform → Runtime), deep-merge semantics, O(1) cached lookup
- **`registry/`** — Service registry via HTTP heartbeat, `ServiceRegistry`, `Endpoint`
- **`execstate/`** — In-memory execution state (`MemoryStore`), JSON file persistence (`FileStore`)
- **`reliability/`** — DLQ, saga tracker, circuit breaker, dedup, rate limiter
- **`compiler/`** — DSL compiler abstraction (local/remote)
- **`plugins/`** — WASM plugin loader (wasmtime)
- **`observability/`** — OTel tracing, Prometheus metrics
- **`cache/`** — In-memory cache with TTL
- **`flow/`** — Flow DSL parser, analyzer, compiler, formatter

### Remaining Adapter (live)
- **`cluster/pkgsupport.go`** — `ClusterMember` adapter wrapping `RaftCluster` → `pkgcluster.ClusterMember`
- **`scheduler/pkgsupport.go`** — Scheduler adapter wrapping `internal/scheduler` → `pkgscheduler.Scheduler`

---

## Bridge (CGo FFI Seam)

- **`bridge/bridge.go`** — Go↔Rust FFI: `Compile()`, `Execute()`, `InitContext()`, `ExecuteStep()`
- **`bridge/caller_bridge.c`** — C trampoline for service dispatch callbacks
- **`bridge/bridge_test.go`** — Bridge tests including `TestExecuteStepMultiCall`

The step API inverts control: Go drives the VM loop, resolving service calls between instructions.

---

## `server/cmd/` — Entry Points

- **`cmd/flowrulz/`** — Production binary via `bootstrap.NodeBuilder`

---

## Deleted (Audit Trail)

| Deleted | Date | Reason |
|---|---|---|
| `internal/adapters/` (old) | 2026-07-06 | Zero imports, never wired |
| `internal/ports/` (old) | 2026-07-06 | Zero imports, never used |
| `bridge/vm_adapter.go` | 2026-07-06 | `NewBridgeVM` never called |
| `pkg/engine/` | 2026-07-06 | Interface never used as DI type |
| `pkg/registry/` | 2026-07-06 | Interface never used as DI type |
| `pkg/store/` | 2026-07-06 | Interface never used as DI type |
| `pkg/reliability/` | 2026-07-06 | Interfaces never used as DI types |
| `pkg/vm/` | 2026-07-06 | Interface never used as DI type |
| `pkg/node/Node` | 2026-07-06 | Interface dead; types kept |
| `internal/engine/pkgsupport.go` | 2026-07-06 | Adapter for dead interface |
| `internal/registry/pkgsupport.go` | 2026-07-06 | Adapter for dead interface |
| `internal/execstate/pkgsupport.go` | 2026-07-06 | Adapter for dead interface |
| `internal/reliability/pkgsupport.go` | 2026-07-06 | Adapter for dead interface |

**Re-created (2026-07-06):** `internal/ports/`, `internal/core/`, `internal/adapters/` — new hexagonal packages with clean interfaces, zero internal imports, fully wired to existing code via adapters.

---

## Cluster Model

Single-leader, **no Raft/Paxos** for cluster state (per `cluster-model.md`). Leader elected by lowest-ID ordering of alive nodes. gRPC Cluster Bus for peer-to-peer communication. Kafka is a legacy fallback only.

Fencing token pattern: capture token → do work → re-validate token → publish. Skipping re-validation opens split-brain.

---

## Hexagonal Architecture (New — incremental migration)

New packages provide clean hexagonal architecture. Existing code continues to work.

### New packages (`internal/`)

| Package | File | Lines | Purpose |
|---|---|---|---|
| `ports/` | `ports.go` | 258 | 20+ interfaces, zero internal imports |
| `core/domain/` | `types.go` | 68 | Pure domain types |
| `core/execution/` | `engine.go` | 205 | VM execution engine |
| `core/execution/` | `ingress.go` | 68 | Rate limit → dedup → exec → DLQ |
| `core/clustering/` | `leadership.go` | 97 | Raft + SingleLeader strategies |
| `core/distribution/` | `plan_orchestrator.go` | 52 | Publish → ack → activate |
| `adapters/invoker/` | `invoker.go` | 234 | HTTP/gRPC/TCP dispatch |
| `adapters/observability/` | `metrics.go` | 52 | Injectable metrics adapter |
| `adapters/cluster/` | `transport.go` | 42 | Cluster transport adapter |

### Existing code (still working, not deleted)

| File | Status | Notes |
|---|---|---|
| `node/execution_engine.go` | ✅ **Replaced** | Delegates to `core/execution.Engine` via adapters |
| `node/adapters.go` | **NEW** | Bridges `execstate.Store`, `reliability.SagaTracker`, etc. → `ports.*` |
| `node/leadership.go` | Kept as-is | Uses `pkgcluster.LeadershipToken` — migration deferred |
| `node/production_invoker.go` | Kept as-is | Uses `registry.ServiceInstance` — migration deferred |
| `node/service_caller.go` | Kept as-is | Protocol dispatch — migration deferred |
| `node/cluster_adapter.go` | Kept as-is | Wraps `ClusterNode` — migration deferred |

### Migration path (incremental, per-file)

1. **`node/leadership.go`** → `core/clustering/leadership.go`: Replace `pkgcluster.LeadershipToken` with `ports.LeadershipToken` across `prod.go`, `leadership.go`, `lifecycle.go`. Update `pkgcluster.ClusterMember` references to `ports.ClusterMember`.

2. **`node/production_invoker.go`** + `service_caller.go` → `adapters/invoker/invoker.go`: Replace `registry.ServiceInstance` with `ports.ServiceInstance` across `registry/` package. Update `ServiceLookup` → `ports.ServiceLookup`.

3. **`node/cluster_adapter.go`** → `adapters/cluster/transport.go`: Replace `ClusterTransport` interface with `ports.ClusterTransport`.

4. **`node/prod.go`**: After all adapters migrated, remove old adapter code from `node/` and have `prod.go` import from `core/` + `adapters/` directly.

### When to migrate each file

- When touching a file for a feature/bug fix, migrate it then
- Don't do a big-bang rewrite — incremental is safer
- The new packages are available for any new code that imports from `ports/`

---

## Testing

- **Go:** `CGO_ENABLED=1 go test -count=1 ./server/...` (requires Rust cdylib built first)
- **Rust:** `cd runtime && cargo test` (401 tests)
- **E2E:** `make e2e` (3-node docker-compose cluster)
- **Pre-existing:** `internal/flow.TestFlowRegistryIntegration` had a nil-pointer bug (fixed 2026-07-06)
