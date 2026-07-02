---
title: Bootstrap
tags:
  - module
  - di
  - go
---

# Bootstrap

> [!info] Dependency injection composition root
> Path: `server/internal/bootstrap/`

Wires all dependencies into a `NodeBuilder` that constructs the production [[Node]].

## Key Types

| Type | File | Purpose |
|------|------|---------|
| `NodeBuilder` | `builder.go` | Fluent builder with With* methods |
| `Dependencies` | `deps.go` | All external dependencies bundled |
| `DefaultDependencies` | `deps.go` | Factory for production defaults |

## Builder Pattern

```go
node, err := bootstrap.NewNodeBuilder().
    WithDefaults(config).
    WithScheduler(customSched).   // optional override
    WithLogger(customLogger).     // optional override
    Build()
```

> [!info] DI strategy (Gap #6)
> `WithDefaults()` delegates to `DefaultDependencies()` factory, which constructs the full dependency graph. There is no abstraction over DI — just clean constructor injection with a builder facade.

## Dependencies Created

- [[Cluster]] — Raft node, peer manager, FSM
- [[Scheduler]] — priority lanes + work stealing
- [[Transport]] — Kafka/bus consumers/producers
- [[Engine]] — rule lifecycle manager
- [[PlanDist]] — plan distribution
- [[Reliability]] — DLQ, Saga, Circuit Breaker, Dedup, Rate Limiter
- [[ExecState]] — FileStore for execution records
- [[Registry]] — service registry
- [[Membership]] — heartbeater, leader lease
- [[Plugins]] — WASM plugin loader
