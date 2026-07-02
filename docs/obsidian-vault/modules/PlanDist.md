---
title: Plan Distributor
tags:
  - module
  - distribution
  - go
aliases:
  - plandist
---

# Plan Distributor

> [!info] Plan distribution protocol
> Path: `server/internal/plandist/`

After the [[Engine]] compiles a rule, `PlanDist` distributes the resulting plan to all nodes in the cluster and awaits acks.

## Key Types

| Type | File | Purpose |
|------|------|---------|
| `Distributor` | `distributor.go` | Core distribution logic |
| `Acker` | `ack.go` | Tracks per-node acknowledgement |

## Protocol

1. Leader compiles plan → sends to all followers
2. Each follower compiles locally (or accepts serialized plan)
3. Follower sends ACK
4. Leader marks plan as deployed once quorum acks received
5. If any node NACKs, plan deployment is rolled back

## Dependencies

- [[Transport]] — uses cluster transport for distribution
- [[Cluster]] — knows node membership for target list
- [[Engine]] — source of compiled plans
