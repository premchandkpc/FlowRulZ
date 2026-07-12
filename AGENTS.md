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
- `cd server && go test -race ./internal/... ./bridge/...` (315+)
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
- CallServiceWithRetry: exponential backoff (100ms→5s), defaults to 0 retries (backward compatible)
- Scheduler metrics: opt-in via NewWithMetrics(); New() unchanged
- Saga compensator: wired post-construction via SetCompensator() (serviceCaller created after SagaTracker)
- Admin extended: RegisterExtended() for scheduler snapshot + recovery trigger (avoids circular deps)
- DLQ.LoadFromMessages: idempotent rebuild from raw JSON, deduplicates by entry ID
- Tracing: ContextWithTraceID/TraceIDFromContext for request-scoped trace IDs, propagated via HTTP X-Trace-ID and gRPC metadata
- DLQ entry IDs: validated against `^[a-zA-Z0-9_\-]+$` to prevent path traversal on disk persistence
- goServiceCaller (CGo): has `defer recover()` — panics in callback don't crash the process
- GetSpans/InternLookup: bounds-check FFI return values against buffer capacity before slicing
- Node handlers: /executions, /partitions, /rebalance wrapped with requireClusterAuth
- All data persistence files written with 0600 (not 0644)
- ServiceCaller.grpcConns: uses sync.Map for lock-free reads on connection cache
- Scheduler.mu: sync.RWMutex — Snapshot() uses RLock to allow concurrent EnqueueTask
- Health endpoint: unauthenticated, returns only {status:ok} — detailed stats behind /metrics (authed)
- TLS cipher suites: explicit allowlist (ECDHE+AES-GCM only), no CBC/3DES
- Cluster TLS: GRPCClient.ConnectWithTLS implemented; ClusterNode uses TLS when configured; AddPeer auto-selects TLS/plaintext

## Docs
`flow-architecture.md` `vm-architecture.md` `bytecode-format.md` `dsl-syntax.md` `memory-management.md` `ffi-api.md` `cluster-model.md` `flows.md` `file-index.md` `obsidian-vault/`
