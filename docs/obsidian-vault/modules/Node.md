---
title: Node
tags:
  - module
  - core
  - go
aliases:
  - ProdNode
---

# Node

> [!info] Production node — the main struct
> Path: `server/internal/node/`

`ProdNode` is the central struct that wires together all server modules and implements the public `Node` interface. It is the refactored successor to the deleted `execnode/` package.

## Implements

```go
type Node interface {
    Start(ctx context.Context) error
    Stop(ctx context.Context) error
    ExecuteAll(ctx context.Context, msg *Message) (*ExecuteResult, error)
    Cluster() cluster.RaftNode
    Scheduler() scheduler.Scheduler
}
```

## ExecuteAll

> [!important] ExecuteAll refactored (Gap #3)
> `ExecuteAll` now routes through `Scheduler.EnqueueAndWait`. Incoming messages are: rate-limited → dedup'd → queued on a priority lane → dispatched to a slot worker → `executePlan()` → `Bridge(runSteps)` → Rust VM.

## Construction

Built exclusively through [[Bootstrap]]:

```go
deps := bootstrap.DefaultDependencies(config)
node := node.NewNode(config, deps)
```

## Dependencies

- [[Cluster]]
- [[Scheduler]]
- [[Transport]]
- [[Engine]]
- [[PlanDist]]
- [[Reliability]] (DLQ, Saga, Circuit Breaker, Dedup, Rate Limiter)
- [[ExecState]]
- [[Registry]]
- [[Membership]]
- [[Plugins]]
- [[Observability]]

## Key Flow

```mermaid
sequenceDiagram
    participant C as Client/SDK
    participant N as ProdNode
    participant S as Scheduler
    participant VM as Rust VM (Bridge)
    participant R as Reliability
    participant Svc as Target Service

    C->>N: ExecuteAll(msg)
    N->>R: RateLimit
    N->>R: Dedup
    N->>S: EnqueueAndWait(plan)
    S->>N: dequeueOrSteal() → slotWorker
    N->>VM: runSteps(steps)
    VM->>Svc: HTTP call (SERVICE_CALL)
    Svc-->>VM: response
    VM-->>N: result
    Note over N: if step fails → DLQ or execute compensators
    N-->>C: ExecuteResult
```
