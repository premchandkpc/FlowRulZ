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

## Critical Findings

### ~~Bug: `flowrulz_msg_release` undefined behavior (`rust/src/ffi.rs:248`)~~ FIXED
Added null check + proper layout tracking via header-size pattern. See `ffi.rs:flowrulz_msg_alloc`/`flowrulz_msg_release`.

### ~~Bug: No panic boundary on `OpCode::Jmp` (`executor/mod.rs`)~~ FIXED
Added `if target < plan.instructions.len()` bound check before assigning to `self.ctx.ip`.

### Issue: `respBytesPtr` sentinel undocumented (`go/bridge/bridge.go`)
`[1]byte` sentinel distinguishes nil from empty response. Works but is subtle and easy to misuse.

### ~~Issue: `scheduler.go` goroutine unbounded (`go/internal/scheduler/scheduler.go`)~~ FIXED
Replaced per-task `go execTask` with N pre-spawned `slotWorker` goroutines (one per `MaxConcurrent`). Removed semaphore channel.

## Architecture Concerns

### 1. `execnode.go` is a God object (1049 lines)
Wires 15+ subsystems in one constructor + orchestration file. Highest refactoring priority — needs control plane / data plane split.

### ~~2. Bytecode versioning~~ FIXED
`ExecutionPlan` has `version: u64` field. Added `BYTECODE_VERSION = 1` constant + `check_plan_version()` on every deserialization path. `FfiError::VersionMismatch` (-10) on mismatch.

### 3. Single-leader bottleneck
Lowest-ID leader election. No formal consensus (no Raft/Paxos). Leader does all compilation, plan distribution, partition assignment.

### ~~4. Sequential `executeAll`~~ FIXED
Fanned out with `context.WithCancel` + bounded semaphore (max 10 concurrent plan executions). First error cancels remaining in-flight.

### 5. Fixed 64 partitions
Cannot change partition count without data reshuffle. No consistent hashing with virtual nodes.

### 6. No transactional message ingestion
Crash between consume and state persist loses the message. At-most-once delivery with best-effort dedup.

### ~~7. `gate.rs` skips 2 instructions on false~~ FIXED
Replaced hardcoded `skip=2` with `skip_count()` that scans forward to next Gate/Label instruction.

### 8. 8 opcodes are no-ops at VM level
`Retry`, `Timeout`, `Pipe`, `Key`, `Split`, `SvcArg`, `RetryData`, `JumpOffset` are parsed and compiled but the VM dispatches them as `Ok(())`.

## Performance Concerns

- 256KB buffer allocated per `bridge.Execute` call regardless of response size.
- `ExecutionContext` serialized/deserialized via bincode every step (O(ctx) per step).
- `ExecutionPlan` deserialized from bytes on every step (O(plan) per step).
- No plan caching on Rust side.
- Goroutine unbounded in scheduler.

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
2. **Add bytecode version header** — prevent silent cluster breakage
3. **Fix undefined behavior in `flowrulz_msg_release`** — UB is a hard blocker
4. **Formalize compiler↔planner↔scheduler↔runtime contracts** before adding features
5. **Parallelize `executeAll`** — goroutine fan-out per active plan
6. **Add panic boundary for Jmp out-of-bounds** — defense in depth
7. **Introduce plan caching on Rust side** — avoid O(plan) deserialization per execution step
8. **Pool output buffers in Go bridge** — stop allocating 256KB per call
9. **Document operational runbooks** — DR, backup, incident response
10. **Add structured logging** — replace `log.Printf` with leveled, JSON-capable logger
