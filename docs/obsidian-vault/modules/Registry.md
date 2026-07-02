---
title: Registry
tags:
  - module
  - discovery
  - go
---

# Registry

> [!info] Service registry via HTTP heartbeat
> Path: `server/internal/registry/`

Services register with FlowRulZ via periodic HTTP heartbeats. The registry tracks availability and routes service calls.

## Key Types

| Type | File | Purpose |
|------|------|---------|
| `Registry` | `registry.go` | Service registration, lookup, heartbeat |
| `Service` | `registry.go` | Registered service (ID, address, metadata) |

## Dependencies

- [[Transport]] — services are called via HTTP transport
- [[Node]] — registered services invoked during plan execution (SERVICE_CALL steps)
