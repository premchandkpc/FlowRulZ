# Go Server Architecture

> Last updated: 2026-07-12. Replaces the archived `restructure-plan-ARCHIVED.md`.

## Package Hierarchy

```
cmd/flowrulz/              ← thin entry point, env→Config only
  │
  └─ internal/bootstrap/   ← COMPOSITION ROOT: NodeBuilder.WithDefaults()
       │
       ├─ internal/node/           ← ProdNode: central orchestrator
       ├─ internal/engine/         ← Rule lifecycle, versioning
       ├─ internal/scheduler/      ← Priority lanes, work stealing
       ├─ internal/cluster/        ← gRPC Cluster Bus + RaftCluster
       ├─ internal/membership/     ← Gossip, leader lease, heartbeat
       ├─ internal/partition/      ← FNV-32a hashing, shard assignment
       ├─ internal/plandist/       ← Plan distribution + ack protocol
       ├─ internal/reliability/    ← DLQ, saga, circuit breaker, dedup
       ├─ internal/registry/       ← Service registry via HTTP heartbeat
       ├─ internal/execstate/      ← Execution state (MemoryStore, FileStore)
       ├─ internal/compiler/       ← DSL compiler (local/remote)
       ├─ internal/flow/           ← Flow DSL parser, analyzer, formatter
       ├─ internal/transport/      ← Adapter layer (Kafka legacy + gRPC)
       ├─ internal/observability/  ← OTel tracing, Prometheus metrics
       └─ internal/plugins/        ← WASM plugin loader (wasmtime)
```

---

## `server/pkg/` — 14 Interface Packages (DI contracts)

| Package | Purpose | Key Types |
|---|---|---|
| `cluster` | Cluster membership & leadership | `ClusterMember`, `MemberID`, `LeadershipToken` |
| `common` | Shared utilities (TLS, auth middleware) | `TLSCipherSuites`, `BearerAuth` |
| `engine` | Engine interface | `Engine` |
| `membership` | Node membership tracking | `Membership`, `MemberInfo` |
| `node` | Node types only (no interface) | `ID`, `ExecuteRequest`, `ExecuteResponse` |
| `partition` | Key-space shard management | `Partition`, `Partitioner` |
| `plandist` | Plan distribution protocol | `PlanDistributor`, `PlanMessage` |
| `registry` | Service registry interface | `ServiceRegistry`, `Endpoint` |
| `reliability` | DLQ, saga, circuit breaker, dedup, rate limit | `DLQ`, `SagaOrchestrator`, `RateLimiter` |
| `replyrouter` | Correlation ID routing | `ReplyRouter` |
| `scheduler` | Priority lanes + scheduling | `Scheduler`, `Lane`, `Result` |
| `store` | Execution state store | `Store`, `ExecutionState` |
| `transport` | Event bus abstraction | `EventBus`, `Publisher`, `Subscriber` |
| `vm` | VM interface | `VM`, `Plan` |

---

## `server/internal/` — Implementation Packages

### Core Node
- **`node/`** — `ProdNode` (central struct), `Dependencies` (DI fields), `Interfaces` (16 real DI contracts: `NodeEngine`, `NodeDLQ`, etc.), leadership fencing logic
- **`bootstrap/`** — `NodeBuilder` composition root, wires `ProdNode` via `WithDefaults()`

### Execution
- **`engine/`** — Rule lifecycle, versioning, lane routing, persistence
- **`scheduler/`** — Priority lanes (fast/heavy/normal), work stealing

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
| `pkg/node/Node` | 2026-07-06 | Interface dead; types kept |
| `internal/pipeline/` | pre-2026-07-06 | Removed, ingress logic moved to engine |
| `internal/flowengine/` | pre-2026-07-06 | Removed, flow orchestration moved to flow/ |
| `internal/policy/` | pre-2026-07-06 | Removed, policy resolver moved to flow/ |

---

## Cluster Model

Single-leader, **Raft consensus for leader election** (`cluster/raft.go`, NoopFSM — no state replication through the Raft log). Leader elected by Raft; control-plane state distributed via gRPC Cluster Bus (default) or Kafka pub/sub (legacy fallback when `FLOWRULZ_KAFKA_BROKERS` set).

Fencing token pattern: capture Raft term → do work → re-validate term → publish. Skipping re-validation opens split-brain.

---

## Testing

- **Go:** `CGO_ENABLED=1 go test -count=1 ./server/...` (requires Rust cdylib built first)
- **Rust:** `cd runtime && cargo test` (401 tests)
- **E2E:** `make e2e` (3-node docker-compose cluster)
- **Pre-existing:** `internal/flow.TestFlowRegistryIntegration` had a nil-pointer bug (fixed 2026-07-06)
