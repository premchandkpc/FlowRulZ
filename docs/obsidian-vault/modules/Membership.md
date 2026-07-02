---
title: Membership
tags:
  - module
  - gossip
  - go
---

# Membership

> [!info] Cluster membership and leader lease
> Path: `server/internal/membership/`

Provides heartbeat-based peer discovery, leader lease enforcement, and stale-node eviction.

## Key Types

| Type | File | Purpose |
|------|------|---------|
| `Gossiper` | `gossip.go` | P2P membership exchange |
| `LeaderLease` | `lease.go` | Leader lease with expiry |
| `Heartbeater` | `heartbeat.go` | Periodic heartbeat to peers |

## Leader Lease

> [!important] Lease timeout
> LeaderHeartbeatTimeout = 500ms. If a follower stops receiving heartbeats, it triggers a new leader election.

## Dependencies

- [[Cluster]] — feeds peer updates to Raft
- [[Transport]] — heartbeat messages via cluster transport
