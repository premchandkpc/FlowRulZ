# Architecture Overview

Event-driven DAG execution engine. DSL → bytecode → Raft cluster → Rust VM.

## System Components

```
                    ┌─────────────┐
                    │   SDKs      │
                    │ Go/Py/JS/   │
                    │ Java/Rust   │
                    └──────┬──────┘
                           │ HTTP/gRPC
                    ┌──────▼──────┐
                    │   Node      │
                    │  (Go)       │
                    ├─────────────┤
                    │  Transport  │◄── Kafka / gRPC / Memory
                    │  Scheduler  │
                    │  Engine     │
                    │  Bridge     │──── CGo FFI
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │  Rust VM    │
                    │  Executor   │
                    │  Bytecode   │
                    │  Memory     │
                    │  Tracing    │
                    └─────────────┘
```

## Core Components

### Go Control Plane (`server/`)

| Package | Responsibility |
|---------|---------------|
| `cmd/flowrulz` | Main entrypoint — node startup, signal handling |
| `internal/node` | HTTP handlers, service calling, plan execution orchestration |
| `internal/scheduler` | Work-stealing scheduler with priority lanes |
| `internal/engine` | Rule management, plan compilation, execution state |
| `internal/cluster` | Raft consensus, gossip protocol, peer management |
| `internal/transport` | Message bus abstraction (Kafka, gRPC, memory) |
| `internal/plandist` | Plan distribution across cluster nodes |
| `internal/registry` | Service discovery and health checking |
| `internal/execstate` | Execution state persistence (memory/file) |
| `internal/reliability` | Circuit breaker, rate limiter, DLQ, saga, dedup |
| `internal/admin` | Admin HTTP API |
| `internal/observability` | Prometheus metrics, OpenTelemetry tracing |
| `internal/flow` | Flow DSL parser, compiler, code generation |
| `internal/compiler` | DSL → bytecode compilation (local or remote) |
| `bridge` | CGo FFI bridge to Rust VM |
| `pkg/` | Public interfaces for external consumers |

### Rust VM (`runtime/`)

| Module | Responsibility |
|--------|---------------|
| `dsl/` | Lexer, parser, optimizer, compiler for bytecode DSL |
| `bytecode/` | Instruction set, execution plan, constant pool |
| `executor/` | VM execution engine with work-stealing |
| `memory/` | Arena allocator, string interning |
| `ffi/` | C FFI interface for Go bridge |
| `tracing/` | Distributed tracing spans |

### SDKs (`sdk/`)

| Language | Status |
|----------|--------|
| Go | Client library |
| Python | Client library with async support |
| JavaScript/TypeScript | Client library |
| Java | Client library |
| Rust | Client library |

## Data Flow

### 1. DSL Compilation

```
DSL string
  → Lexer (tokenize)
  → Parser (AST)
  → Optimizer (constant folding, dead code elimination)
  → Compiler (ExecutionPlan)
  → Bincode serialization
```

### 2. Plan Distribution

```
Leader node compiles DSL
  → PlanDistributor.PublishPlan()
  → Kafka/gRPC broadcast
  → All nodes receive plan
  → Each node caches plan locally
```

### 3. Event Processing

```
Event arrives (HTTP/gRPC/Kafka)
  → Node receives event
  → Looks up rule by topic
  → Retrieves cached ExecutionPlan
  → Creates ExecutionContext
  → Submits to Scheduler
```

### 4. VM Execution

```
Scheduler assigns to lane (Fast/Normal/Heavy)
  → Worker picks up ExecutionContext
  → VM interprets bytecode instructions
  → Service calls → StepPending → Go calls service → response fed back
  → Gate/Branch → skip or continue
  → Parallel → fan-out → Collect → merge
  → Execution complete → StepDone
```

## Execution Model

### Step Results

| Result | Meaning |
|--------|---------|
| `StepDone` | Execution complete |
| `StepPending` | Needs service call (svc_id + body) |
| `StepContinue` | Move to next instruction |
| `StepFailed` | Execution failed |

### Service Call Flow

```
VM instruction: Next(service_id=3, timeout=5000)
  → StepPending { svc_id: 3, body: [...] }
  → Go bridge receives pending
  → Looks up service by ID in ServiceTable
  → Makes HTTP/gRPC call
  → Receives response
  → Feeds response back into VM
  → VM continues to next instruction
```

### Parallel Execution

```
VM instruction: Parallel(count=3, first_svc=0)
  → StepPending for svc 0, 1, 2
  → Go bridge calls all three concurrently
  → Collect results as they arrive
  → VM instruction: Collect
  → Merge results into array
  → Continue
```

## Scheduler

### Priority Lanes

| Lane | Score Range | Concurrency | Use Case |
|------|-------------|-------------|----------|
| Fast | < 10 | 50 | Gates, maps, simple operations |
| Normal | 10-50 | 20 | Sequential chains, emits |
| Heavy | > 50 | 5 | Parallel, DAG, chunk operations |

### Work Stealing

When a lane's queue is empty, workers steal from other lanes' queues. This prevents starvation and improves utilization.

### Complexity Scoring

| Operation | Score |
|-----------|-------|
| Next, Async | 10 |
| Parallel, DAG | 20 |
| Chunk | 25 |
| Gate | 5 |
| Map | 3 |
| Emit | 8 |
| Buffer | 15 |

## Cluster Architecture

### Raft Consensus

- Leader election via Raft
- Leader distributes plans
- Followers replicate state
- Automatic failover on leader loss

### Gossip Protocol

- Peer discovery
- Health monitoring
- Membership updates

### Plan Distribution

- Leader compiles and distributes plans
- Plans broadcast via Kafka/gRPC
- Each node maintains local plan cache
- Version-based deduplication

## Reliability Features

| Feature | Description |
|---------|-------------|
| Circuit Breaker | Prevents cascading failures |
| Rate Limiter | Token bucket rate limiting |
| DLQ | Dead letter queue for failed events |
| Saga | Compensating transactions |
| Dedup | Duplicate detection with TTL |
| Retry | Configurable retry policies |
| Timeout | Per-step and flow-wide timeouts |

## Observability

### Metrics (Prometheus)

- `flowrulz_events_total` — total events processed
- `flowrulz_event_duration_seconds` — processing latency
- `flowrulz_errors_total` — error count by type
- `flowrulz_scheduler_queue_depth` — queue depth per lane
- `flowrulz_circuit_breaker_state` — breaker state

### Tracing (OpenTelemetry)

- Request-scoped trace IDs
- Propagated via HTTP `X-Trace-ID` and gRPC metadata
- Span recording for each VM step

### Logging (slog)

- Structured JSON logging
- Request-scoped context
- Configurable levels

## Edge Cases & Gotchas

### ExecutionContext Thread Safety
`ExecutionContext` uses `sync.Mutex` internally. Always use accessors:
```go
// CORRECT
val := ctx.Variable("my_var")
ctx.SetVariable("my_val", newValue)

// WRONG — data race
val := ctx.Variables["my_var"]
```

### TimerWheel Stop Behavior
`TimerWheel.Stop()` waits for all callbacks to complete (`sync.WaitGroup`):
```go
tw.Stop() // blocks until all timer callbacks finish
// Don't call this in a goroutine that timers might callback into
```

### ReplyRouter Double-Close
`ReplyRouter` uses `PendingRequest.closeOnce()` to prevent double-close:
```go
// Safe — closeOnce prevents panic
req.Cancel()    // first close
req.Deliver()   // second close — no-op
```

### SpanRingBuffer Draining
Always drain the global span buffer before emitting in tests:
```go
spans := tracing.DrainGlobalBuffer()
// Now safe to assert on spans
```

### Leader Election
- `Membership.LeaderID()` is single-node heuristic (lowest ID)
- Raft cluster is authoritative when configured
- Don't rely on `LeaderID()` for multi-node consensus

### execTask Panic Recovery
`execTask` has `defer recover()` — panics write error to `ResultCh`:
```go
// This won't crash the process
go func() {
    result := execTask(ctx) // panic inside → caught by recover
    resultCh <- result
}()
```

### Service Caller CGo Safety
`goServiceCaller` has `defer recover()` — panics in callbacks don't crash:
```go
func (sc *goServiceCaller) CallService(svcID uint16, body []byte) ([]byte, error) {
    defer recover() // catches panics from callback
    return sc.callback(svcID, body)
}
```

### TCP Connection Pool
Uses `closed` flag + `closeMu` to prevent panic on send to closed channel:
```go
pool.mu.Lock()
if pool.closed {
    pool.mu.Unlock()
    return ErrClosed
}
pool.mu.Unlock()
```

### DLQ Lock Discipline
DLQ captures `replayFn` under lock before executing outside lock:
```go
dlq.mu.Lock()
fn := dlq.replayFns[entryID]
dlq.mu.Unlock()
fn() // execute outside lock to prevent deadlock
```

### Data Persistence Permissions
All data files written with `0600` (not `0644`):
```go
os.WriteFile(path, data, 0600) // owner read/write only
```

### TLS Cipher Suites
Explicit allowlist — no CBC/3DES:
```
TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256
TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384
TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256
TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384
```

### CallServiceWithRetry
Exponential backoff (100ms→5s), defaults to 0 retries:
```go
// Must explicitly set retries
result, err := CallServiceWithRetry(ctx, svc, body, 3)
```

### Scheduler Stop Deadlock
`Scheduler.Stop()` releases mutex before `wg.Wait()`:
```go
func (s *Scheduler) Stop() {
    s.mu.Lock()
    s.stopped = true
    s.mu.Unlock() // release BEFORE Wait
    s.wg.Wait()   // tasks might call Snapshot() which needs mu
}
```

## Deployment

### Single Node

```bash
go run server/cmd/flowrulz/main.go
```

### Cluster

```bash
# Node 1 (leader)
go run server/cmd/flowrulz/main.go -node-id node-1 -seeds node-2:7946,node-3:7946

# Node 2
go run server/cmd/flowrulz/main.go -node-id node-2 -seeds node-1:7946,node-3:7946

# Node 3
go run server/cmd/flowrulz/main.go -node-id node-3 -seeds node-1:7946,node-2:7946
```

**ProdNode.Start() refuses to start if Seeds configured without RaftCluster.**

### Kubernetes

See `k8s/` directory for Helm charts and Kustomize manifests.

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `FLOWRULZ_LOG` | Log level (debug/info/warn/error) | info |
| `FLOWRULZ_LIB_PATH` | Path to `libflowrulz_core.so` | `./runtime/target/release/` |
| `FLOWRULZ_PLUGINS_DIR` | WASM plugins directory | `./plugins/` |

### Health Check

Unauthenticated endpoint at `/health`:
```json
{"status": "ok"}
```

Detailed stats behind `/metrics` (authenticated).

### Admin API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check (unauthenticated) |
| `/metrics` | GET | Prometheus metrics (authenticated) |
| `/rules` | GET | List registered rules |
| `/rules` | POST | Register new rule |
| `/rules/{id}` | DELETE | Remove rule |
| `/executions` | GET | List active executions |
| `/partitions` | GET | List partitions |
| `/rebalance` | POST | Trigger partition rebalance |

### Node Handlers Auth

`/executions`, `/partitions`, `/rebalance` wrapped with `requireClusterAuth` — internal cluster calls only.
