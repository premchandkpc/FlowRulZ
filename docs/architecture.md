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

### Kubernetes

See `k8s/` directory for Helm charts and Kustomize manifests.
