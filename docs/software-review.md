# Software Review: FlowRulZ

Comprehensive multi-layer review conducted July 2026.

## Scores

| Area | /10 |
|------|-----|
| Vision | 9.8 |
| Ambition | 10 |
| Architecture | 8.7 |
| Code Organization | 8.8 |
| Runtime Design | 8.5 |
| Compiler Design | 9.0 |
| Documentation | 8.5 |
| Production Readiness | 7.2 |
| Scalability Potential | 9.3 |
| Extensibility | 9.5 |
| Maintainability | 8.2 |
| Enterprise Readiness | 7.5 |

## Architecture

```
Client → Gateway → Planner → Scheduler → Runtime → Workers → Storage
```

Strong Rust/Go language split: Rust owns the hot path (compiler, VM, expression engine, memory), Go owns I/O (networking, cluster, scheduler, observability).

## Key Strengths

- **Platform architecture, not application**: DSL → Bytecode → VM → Runtime separates every concern.
- **Rust/Go split**: Each language does what it does best.
- **Bytecode compilation**: DSL compiles to versionable, serializable, cacheable bytecode.
- **ExecutionContext model**: Services enrich context instead of replacing it — enables stateful workflows.
- **DAG sub-language**: Complex orchestration expressed declaratively, validated at compile time.
- **Documentation**: 12 docs files (DSL spec, bytecode format, VM architecture, cluster model, flows, etc.) plus interactive HTML visual guide.
- **Testing**: 111 Rust tests, Go unit tests, 8 e2e cluster tests (leader failover, partition rebalance, plan distribution).
- **K8s deployment**: Both kustomize and Helm charts, kind config for local testing.
- **WASM plugin system**: Sandboxed plugins via wasmtime, though surface area is narrow.

## Architecture Concerns

### 1. `execnode.go` is a God object (1049 lines)
Wires 15+ subsystems in one constructor + orchestration file. Highest refactoring priority — needs control plane / data plane split.

### 2. Single-leader bottleneck
Lowest-ID leader election. No formal consensus (no Raft/Paxos). Leader does all compilation, plan distribution, partition assignment.

### 3. Fixed 64 partitions
Cannot change partition count without data reshuffle. No consistent hashing with virtual nodes.

### 4. No transactional message ingestion
Crash between consume and state persist loses the message. At-most-once delivery with best-effort dedup.

### 5. 8 opcodes are no-ops at VM level
`Retry`, `Timeout`, `Pipe`, `Key`, `Split`, `SvcArg`, `RetryData`, `JumpOffset` are parsed and compiled but the VM dispatches them as `Ok(())`. By design — these are compile-time metadata (Retry/Timeout hoisted to flags, Pipe/Key/Split resolved by optimizer, SvcArg/RetryData/JumpOffset are bytecode metadata, Label is a jump target). Could be stripped from bytecode pre-runtime in a future optimization pass.

## Performance Concerns

- `ExecutionContext` serialized/deserialized via bincode every step (O(ctx) per step). Could be reduced with dirty-flag tracking or partial serialization, but requires FFI refactoring.

## Security

- Admin API key auth but no TLS.
- FFI has no sandbox for service callers.
- Service registry open registration at `/register` with no auth.
- No dependency scanning or SBOM.

## Operations

| Endpoint | Purpose |
|----------|---------|
| `/health` | node_id, is_leader, term |
| `/readyz` | 503 if leader uninitialized |
| `/metrics` | counters, gauges, pending, DLQ, inflight |
| `/admin/` | rule CRUD, validate, promote, rollback |

Missing: runbooks, DR procedures, structured logging, alert config.

## Recommended Priorities

1. **Refactor `execnode.go`** — split control plane from data plane
2. **Formalize compiler↔planner↔scheduler↔runtime contracts** before adding features
3. **Document operational runbooks** — DR, backup, incident response
4. **Add structured logging** — replace `log.Printf` with leveled, JSON-capable logger
