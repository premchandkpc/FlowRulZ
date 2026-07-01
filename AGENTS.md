# FlowRulZ Agent Configuration

## AI Agent Setup (opencode)

The repository uses **opencode** as the AI coding assistant. It reads `CLAUDE.md` (project context + conventions) and `AGENTS.md` (this file, agent config) on each conversation start.

### Available Skills

| Skill | Trigger | Purpose |
|-------|---------|---------|
| `caveman` | `/caveman`, "caveman mode" | Ultra-compressed communication (lite/full/ultra/wenyan variants) |
| `caveman-commit` | `/commit`, staging changes | Ultra-compressed Conventional Commits (subject <=50 chars) |
| `caveman-compress` | `/caveman:compress <file>` | Compress memory files into caveman format |
| `caveman-help` | `/caveman-help` | Quick-ref for all caveman modes/commands |
| `caveman-review` | `/review`, PR review | Ultra-compressed code review comments |
| `customize-opencode` | Editing opencode config | Editing `opencode.json`, `.opencode/`, skills, MCP, permissions |
| `find-skills` | "find a skill for X" | Discover and install new agent skills |
| `frontend-design` | Building new UI | Aesthetic direction, typography, intentional visual design |
| `skill-creator` | Create/edit skills | Create, modify, benchmark, and optimize agent skills |

### Agent Tools

The AI agent has access to:

- **`bash`** — Shell execution (git, npm, docker, build commands)
- **`read`** — Read files/directories
- **`write`** — Write files
- **`edit`** — Exact string replacement in files
- **`glob`** — File pattern matching
- **`grep`** — Content search with regex
- **`question`** — Ask user for preferences/decisions
- **`task`** — Delegate complex multistep work to sub-agents
- **`todowrite`** — Structured task list management
- **`webfetch`** — Fetch URL content
- **`websearch`** — Real-time web search

### Sub-Agent Types

| Type | Use Case | Tools |
|------|----------|-------|
| `explore` | Fast codebase exploration, file/pattern searches | read, glob, grep, bash |
| `general` | Complex multi-step research and execution tasks | All tools |

### Conventions

1. Agent reads `docs/` dir on each conversation start
2. After any code change, relevant `.md` files in `docs/` must be updated
3. Never let docs go stale
4. Agent runs `make test` / `go vet` to verify changes before signaling completion
5. Only commit when explicitly asked by the user

---

## Current State (SOLID Restructure — Phase 3b complete)

### Completed
- **13 interface packages** in `go/pkg/` — all concerns separated (node, transport, cluster, scheduler, store, vm, registry, plandist, membership, reliability, replyrouter, partition, engine)
- **`go/internal/node/`** — ProdNode DI assembler (implements `pkg/node.Node`, wraps concrete internal types) — 10 files
- **`go/cmd/flowrulz/main.go`** — updated to use ProdNode
- **`go/bridge/vm_adapter.go`** — BridgeVM adapter (implements `vm.PlanCompiler` + `vm.VMRunner`)
- **`go/internal/transport/memory/`** — In-memory FullEventBus for tests/simulator
- **All previous file-splitting steps 12-19 completed** (execnode_http, plandist, membership, manager, bus, dashboard, scheduler, admin)
- **File renames** (same-package): `raft_cluster.go→raft.go`, `plan.go→distributor.go`, `scheduler.go→prod.go`, `timerwheel.go→worker.go`, `server.go→api.go`
- **Kafka subdirectory**: `kafka.go` → `kafka/config.go+consumer.go+producer.go` (package `kafka`, implements `transport.MessageConsumer/Producer`)

### Building
- `go build ./go/...` passes (only pre-existing CGo linker warnings)
- `go vet ./go/...` passes
- `go test ./go/...` passes (only pre-existing `TestExecuteStepMultiCall` failure in bridge)
- Full project: `go build ./...` passes (includes simulator)

### Phase 3 — Adapter layer complete (July 2026)

**New adapter files implementing `pkg/` interfaces from `internal/`:**

| Package | File | Adapter type(s) | Interface(s) |
|---------|------|----------------|--------------|
| `internal/execstate` | `pkgsupport.go` | `ExecutionStore` wraps `FileStore` | `pkg/store.Store` |
| `internal/scheduler` | `pkgsupport.go` | Inline on `*Scheduler` + `EnqueueTask` rename | `pkg/scheduler.Scheduler` |
| `internal/registry` | `pkgsupport.go` | `Registry` wraps `*ServiceRegistry` | `pkg/registry.Registry` |
| `internal/cluster` | `pkgsupport.go` | `ClusterMember` wraps `*RaftCluster` + `GossiperAdapter` wraps `*Gossiper` | `pkg/cluster.ClusterMember`, `pkg/cluster.Gossiper` |
| `internal/engine` | `pkgsupport.go` | Inline on `*Engine` | `pkg/engine.Engine` |
| `internal/reliability` | `pkgsupport.go` | `RateLimiterAdapter`, `DLQAdapter`, `SagaTrackerAdapter`, inline on `CircuitBreaker` + `DedupTracker` | All 5 `pkg/reliability` interfaces |

**Compile-time assertions added (6 new):**
- ✅ `pkg/store.Store` → `internal/execstate.ExecutionStore`
- ✅ `pkg/scheduler.Scheduler` → `internal/scheduler.Scheduler`
- ✅ `pkg/registry.Registry` → `internal/registry.Registry`
- ✅ `pkg/cluster.ClusterMember` + `Gossiper` → `internal/cluster.ClusterMember` + `GossiperAdapter`
- ✅ `pkg/engine.Engine` → `internal/engine.Engine`
- ✅ All 5 `pkg/reliability` interfaces → `internal/reliability.*Adapter` types

**Side-effect changes:**
- `internal/execstate.Storer.List` → `ListByStatus` (avoid collision with `pkg/store.Store.List`)
- `internal/scheduler.Scheduler.Start() error` (was void)
- `internal/scheduler.Scheduler.Stop() error` (was void)
- `internal/scheduler.Enqueue` → `EnqueueTask` (to add `Enqueue(*ExecutionContext)` for interface)

**Phase 3b — ProdNode DI wiring (partial)**
- Created `Dependencies` struct (`go/internal/node/prod.go:36`) with interface + concrete fields
- Created `NewNode(cfg Config, deps Dependencies)` — pure DI constructor
- Created `DefaultDependencies(cfg Config) Dependencies` in `factory.go` — production wiring
- Changed 5 ProdNode fields to `pkg/` interface types:
  - `Scheduler` → `scheduler.Scheduler`
  - `Membership` → `membership.Membership`
  - `Partitions` → `partition.PartitionManager`
  - `Rebalancer` → `partition.RebalanceNotifier`
  - `ReplyRouter` → `replyrouter.ReplyRouter`
- `NewProdNode(cfg)` kept as backward-compat wrapper (calls `NewNode(cfg, DefaultDependencies(*cfg))`)
- `main.go` updated to use `NewNode` + `DefaultDependencies` directly

**Still concrete in ProdNode (deferred):**
- Engine, Registry, DLQ, RateLimiter, Dedup, Saga, StateStore, RaftCluster, ClusterNode, GRPCBus — internal methods not yet on `pkg/` interfaces
- `go/internal/execnode/execnode.go` — still on old pattern (concrete fields, no DI)
- `go/internal/admin/api.go` — uses concrete `*engine.Engine` + `*reliability.DLQ`
- See `go/internal/node/prod.go` for current state
