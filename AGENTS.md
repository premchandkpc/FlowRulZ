# FlowRulZ Project Context

## Overview
Event-driven DAG execution engine. Compiles DSL rules → bytecode plans, distributes across Raft cluster, executes via Rust VM with work-stealing scheduler.

## Repo Structure
```
server/           Go control plane (cluster, scheduler, transport, DI)
  internal/
    node/         ProdNode — central struct wiring all modules
    bootstrap/    NodeBuilder — DI composition root
    cluster/      Raft (hashicorp/raft), peer mgmt, FSM
    scheduler/    Priority lanes + work stealing (dequeueOrSteal)
    transport/    Kafka, gRPC bus, cluster transport
    reliability/  DLQ, Saga, Circuit Breaker, Dedup, Rate Limiter
    engine/       Rule lifecycle (deploy, compile, compile)
    plandist/     Plan distribution + ack protocol
    partition/    Key-space shard mgmt + rebalancing
    membership/   Gossip, leader lease, heartbeat eviction
    execstate/    FileStore — JSON execution records
    registry/     Service registry via HTTP heartbeat
    admin/        Admin HTTP API
    observability/ OTel tracing, Prometheus metrics
  bridge/         CGo FFI → Rust runtime
  pkg/            Public interfaces (interfaces for DI/testability)

runtime/          Rust bytecode VM
  src/
    executor/     MAP, GATE, SERVICE_CALL step handlers
    bytecode/     Opcodes, plan/schema types
    memory/       Arena allocator (bumpalo)
    tracing/      Span ring buffer
    ffi/          C FFI exports for Go bridge

sdk/
  flow/           Go client library (Publish, Request, Execute, Stream)
  java/           Java SDK (Maven, com.flowrulz)
  python/         Python SDK (pip, flowrulz)
  javascript/     JS/TS SDK (npm, flowrulz)
  rust/           Rust SDK (cargo, flowrulz-sdk)
simulator/        Load gen + timeline scenario testing
docs/obsidian-vault/  Obsidian vault (arch map, 26 notes, 1 canvas)
```

## Architecture
- **Raft consensus** for leader election + log replication (hashicorp/raft)
- **Priority lanes**: Fast (50 workers) > Normal (20) > Heavy (5)
- **Work stealing**: idle workers steal from Heavy→Normal→Fast lanes
- **Execution**: Go scheduler → CGo bridge → Rust VM → HTTP service calls
- **Persistence**: JSON FileStore for execution records + DLQ replay
- **DI**: Manual constructor injection via NodeBuilder.WithDefaults()

## Refactoring Gaps (completed)
1. **Structured logging**: `log.Printf`→`slog` (64 call sites in 18 Go files); `eprintln!`→`log::warn!` (Rust)
2. **Split execnode God object**: deleted `server/internal/execnode/` (11 files dead code); exported `MakeProducerFromCluster`/`MakeConsumerFromCluster` to transport pkg
3. **ExecuteAll bypasses scheduler**: routes through `scheduler.EnqueueAndWait`
4. **Execution history**: completed states saved as `StatusCompleted` + output (not deleted)
5. **Work stealing**: `slotWorker.dequeueOrSteal()` steals from Heavy→Normal→Fast when idle
6. **DI migration**: `NodeBuilder.WithDefaults()` delegates to `DefaultDependencies()` factory

## Tests
- Go: `cd server && go test ./internal/... ./bridge/...` (19 packages, 274 tests, all pass)
- Rust: `cd runtime && cargo test` (401 tests, all pass)
- **Bridge `TestExecuteStepMultiCall`**: Fixed — root cause was `sync.Pool` buffer aliasing in `Compile`, `Execute`, `InitContext` returning pool slices directly (`outBuf[:outLen]`). `ExecuteStep` reused the same pool buffer, overwriting plan data before Rust deserialized it. Fixed by `make+copy` in all 3 functions. 10/10 passes.
- **Scheduler `TestPriorityOrdering`**: Fixed — root cause was data race on `execOrder` slice appended from multiple goroutines without synchronization. Added `sync.Mutex`. 50/50 passes.

## Obsidian Vault
`docs/obsidian-vault/` — 26 notes + 1 `.canvas` map. Architecture, modules, concepts, all linked via wikilinks.
