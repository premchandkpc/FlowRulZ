# FlowRulZ

Event-driven DAG execution engine. DSL → bytecode → Raft cluster → Rust VM (work-stealing scheduler).

## Structure
```
server/       Go control plane (node, bootstrap, cluster, scheduler, transport, reliability, engine, plandist, partition, membership, execstate, registry, admin, observability, bridge→CGo FFI, pkg→interfaces)
runtime/      Rust VM (executor, bytecode, memory, tracing, ffi)
sdk/          Go, Java, Python, JS/TS, Rust
simulator/    40+ services, 8 modes, 50+ scenarios
docs/         Architecture guides + Obsidian vault (26 notes)
```

## Key Facts
- Raft for leader election; priority lanes: Fast(50) > Normal(20) > Heavy(5); work stealing
- Execution: Go scheduler → CGo bridge → Rust VM → HTTP service calls
- DI: manual via NodeBuilder.WithDefaults(); 13 pkg/ interfaces for testability
- Hexagonal gaps: mixed DI types by design (pkg interfaces for external consumers, concrete types for internal use)
- pkg/reliability adapters: wrapper types in `pkgsupport.go` bridge internal→pkg interfaces (RateLimiterPkgAdapter, DLQPkgAdapter, SagaPkgAdapter)
- Compile-time checks: `var _ pkg.Type = (*InternalType)(nil)` in pkgsupport.go for all 5 reliability pkg interfaces

## Tests
- `cd server && go test -race ./internal/... ./bridge/...` (286+)
- `cd runtime && cargo test` (411+)
- `go test ./simulator/...` (-race)

## Gotchas
- ExecutionContext: use State()/SetVariable()/Variable() accessors (sync.Mutex)
- TimerWheel: Stop() waits for callbacks (sync.WaitGroup); Start() is idempotent (sync.Once)
- ReplyRouter: uses PendingRequest.closeOnce() to prevent double-close across Cancel/Deliver/cleanup
- SpanRingBuffer: drain_global_buffer() before emitting in tests
- Leader election: RaftCluster is authoritative when configured; Membership.LeaderID() is single-node heuristic only (lowest-ID, no consensus)
- execTask: has `defer recover()` — panics write error to ResultCh, callers never hang
- callGRPC/ConnectWithTLS: fail loudly, no silent HTTP/insecure downgrade
- ProdNode.Start(): refuses to start if Seeds configured without RaftCluster
- Scheduler.Stop(): releases mutex before wg.Wait() to prevent deadlock if tasks call Snapshot()
- TCP conn pool: uses closed flag + closeMu to prevent panic on send to closed channel
- DLQ: captures replayFn under lock before executing outside lock
- CircuitBreaker.State(): acquires mutex (was previously racy)
- ClusterNode.Start(): sets started=true only after bus.Start() succeeds

## Docs
`flow-architecture.md` `vm-architecture.md` `bytecode-format.md` `dsl-syntax.md` `memory-management.md` `ffi-api.md` `cluster-model.md` `flows.md` `file-index.md` `obsidian-vault/`
