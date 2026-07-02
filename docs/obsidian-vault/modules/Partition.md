---
title: Partition
tags:
  - module
  - sharding
  - go
---

# Partition

> [!info] Key-space partition management
> Path: `server/internal/partition/`

Manages partition ownership across the cluster. The leader owns writable partitions; followers own read replicas.

## Key Types

| Type | File | Purpose |
|------|------|---------|
| `Manager` | `manager.go` | Partition ownership, rebalancing trigger |
| `Partition` | `partition.go` | Single partition with ID (string) and state |
| `Rebalancer` | `rebalancer.go` | Rebalance logic when nodes join/leave |
| `Store` | `store.go` | Partition state persistence |

## Rebalancing

When nodes join or leave the cluster, the leader triggers rebalancing to redistribute partitions evenly. Each partition maps to a Raft log entry for linearizability.

## Dependencies

- [[Cluster]] — leader drives rebalancing decisions
- [[Raft Consensus|Raft]] — partition state replicated via FSM
