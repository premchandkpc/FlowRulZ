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
- Hexagonal gaps: mixed DI types, orphaned interfaces, pkg/reliability API mismatch

## Tests
- `cd server && go test -race ./internal/... ./bridge/...` (286+)
- `cd runtime && cargo test` (411+)
- `go test ./simulator/...` (-race)

## Gotchas
- ExecutionContext: use State()/SetVariable()/Variable() accessors (sync.Mutex)
- TimerWheel: Stop() waits for callbacks (sync.WaitGroup)
- ReplyRouter: channel closes under lock (prevents double-close)
- SpanRingBuffer: drain_global_buffer() before emitting in tests

## Docs
`flow-architecture.md` `vm-architecture.md` `bytecode-format.md` `dsl-syntax.md` `memory-management.md` `ffi-api.md` `cluster-model.md` `flows.md` `file-index.md` `obsidian-vault/`
