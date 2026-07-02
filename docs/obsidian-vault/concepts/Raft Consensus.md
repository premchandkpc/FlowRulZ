---
title: Raft Consensus
tags:
  - concept
  - raft
---

# Raft Consensus

FlowRulZ uses hashicorp/raft for cluster coordination. Raft provides leader election, log replication, and fault tolerance.

## Cluster Roles

| Role | Description |
|------|-------------|
| **Leader** | Handles all client requests, plan distribution, partition rebalancing |
| **Follower** | Replicates log, accepts distributed plans, executes work |
| **Candidate** | Transient state during leader election |

## Raft Log Entries

The FSM processes:
- `ApplyPlanOp` — deploy/update a plan
- `RemovePlanOp` — delete a plan
- `PartitionOp` — partition ownership changes
- `MembershipOp` — node join/leave

## Leader Election

1. Follower detects heartbeat timeout (500ms)
2. Becomes Candidate, requests votes
3. Majority (N/2 + 1) grants → new Leader
4. New leader replays all committed log entries

## Integration

> [!info] RaftNode interface
> See [[Cluster#Key Interfaces]] for the testable `RaftNode` abstraction over hashicorp/raft.

## Dependencies

- [[Cluster]] — RaftNode wrapper
- [[Membership]] — heartbeat-based leader lease enforcement
