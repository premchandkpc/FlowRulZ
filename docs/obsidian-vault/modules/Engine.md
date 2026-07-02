---
title: Engine
tags:
  - module
  - compilation
  - go
---

# Engine

> [!info] Rule compilation and deployment
> Path: `server/internal/engine/`

Manages rule lifecycle: deploy, promote, version, archive. Compiles DSL → bytecode and stores in registry.

## Key Types

| Type | File | Purpose |
|------|------|---------|
| `Engine` | `engine.go` | Rule management, plan deployment |
| `Rule` | `rule.go` | Rule definition (ID, DSL, version) |
| `Compiler` | `compiler.go` | DSL to bytecode plan compilation |

## Rule Lifecycle

1. **Deploy** — DSL submitted via admin API or SDK
2. **Compile** — engine compiles DSL → `plan.Plan` (bytecode steps)
3. **Distribute** — [[PlanDist]] sends plan to all cluster nodes
4. **Execute** — [[Scheduler]] dispatches when messages arrive
5. **Archive** — old versions retained for rollback

## Dependencies

- [[Cluster]] — co-located on every node
- [[PlanDist]] — plan distribution protocol
- [[Scheduler]] — plan dispatch
- [[Compiler|Compiler (remote)]] — optional standalone compile service at `server/cmd/flowrulz-compiler/`
