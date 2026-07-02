---
title: Cluster
tags:
  - module
  - raft
  - go
---

# Cluster

> [!info] Raft-based cluster coordination
> Path: `server/internal/cluster/`

Manages node lifecycle, [[Raft Consensus|Raft]] state machine, peer connections, and leader/follower roles.

## Key Types

| Type | File | Purpose |
|------|------|---------|
| `Node` | `node.go` | Wraps hashicorp/raft, provides IsLeader, Address, Raft state |
| `PeerManager` | `peers.go` | Peer discovery via static config or [[Membership\|gossip]] |
| `State` | `node.go` | Observable state (Leader, Candidate, Follower) |
| `FSM` | `fsm.go` | Raft FSM: Apply, Snapshot, Restore for plan/member state |

## Dependencies

- [[Membership]] — gossip-based peer discovery
- [[Partition]] — partition ownership via Raft leadership
- [[Transport]] — cluster peer communication
- [[Registry]] — service discovery

## Key Interfaces

```go
// RaftNode abstracts hashicorp/raft for testability
type RaftNode interface {
    IsLeader() bool
    State() raft.RaftState
    Leader() ServerAddress
    Apply(cmd []byte, timeout time.Duration) raft.ApplyFuture
    AddVoter(...) Future
    RemoveServer(...) Future
    Shutdown() Future
}
```
