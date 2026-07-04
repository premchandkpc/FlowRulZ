# Replication and Acknowledgment Design

## Overview

FlowRulZ has three distinct classes of state, each with different replication requirements and trade-offs. This document defines the target semantics for each class.

## Data Classes

### 1. Cluster Control-Plane State

**What:** Partition assignments, plan versions, term/leader info, membership changes.

**Current mechanism:** Distributed via Kafka pub/sub topics (`_flowrulz_plans`, `_flowrulz_partitions`, `_flowrulz_members`). Raft is used only for leader election (NoopFSM — no state goes through the Raft log).

**Target consistency:** Eventual consistency is acceptable here.

**Why Kafka + eventual consistency is acceptable:**
- Partition assignments are idempotent (applying the same assignment twice is safe)
- Plan versions are monotonic (only higher versions are accepted)
- The consistency window is bounded by Kafka replication lag (~100ms in production)
- The fencing token pattern prevents split-brain: stale leaders cannot publish

**Consistency window:** < 200ms (Kafka replication lag + consumer poll interval).

**Failure mode:** If the leader dies mid-publish, the new leader will re-rebalance after detecting the membership change. In-flight publishes from the old leader are harmless because:
1. Fencing tokens prevent stale publishes (term mismatch)
2. Assignments are idempotent
3. Plans are version-stamped (only higher versions accepted)

**Future improvement:** Move partition assignments and plan distribution through the Raft log instead of Kafka for stronger consistency. This would:
- Eliminate the Kafka dependency for control-plane operations
- Provide linearizable reads for leadership-gated operations
- Reduce the consistency window to 0

**Trade-off:** Raft log replication adds latency (~50ms per round-trip) and requires all nodes to be reachable. Kafka provides better availability under network partitions.

### 2. Per-Execution State (execstate.Store)

**What:** In-flight execution records, saga compensation logs.

**Current mechanism:** Local disk per node (`execstate.FileStore` — JSON files).

**Target replication:** None (local-disk-only) for now.

**Why local-disk is acceptable:**
- Executions are ephemeral (complete in seconds)
- Each execution is owned by exactly one node (partitioned by key)
- If a node dies, in-flight executions are lost anyway (no checkpointing mid-execution)
- Saga compensation can replay from the last checkpoint on restart

**Failure mode:** If a node dies:
- In-flight executions are lost (no durability guarantee)
- Pending saga compensations are lost (must be re-triggered manually)
- New executions will be routed to surviving nodes after rebalance

**Future improvement:** Add durable execution state via:
- Option A: Replicated state machine (Raft log) for execution records
- Option B: Distributed store (etcd/Postgres) for execution state
- Option C: Kafka consumer offset commits only after execution state is durably saved

**Trade-off:** Durable execution state adds latency (~50ms per checkpoint) and requires a replicated store. Local disk is simpler and faster but loses work on node failure.

### 3. Message Processing Acknowledgment

**What:** Kafka consumer offset commits.

**Current mechanism:** Manual offset commits (AutoCommit is disabled). Offsets are committed after execution completes.

**Target semantics:** At-least-once delivery with deduplication.

**Why at-least-once is acceptable:**
- The `DedupTracker` prevents duplicate processing (FNV-128a hash of message body)
- Duplicates are detected and dropped within the TTL window (5 minutes)
- The cost of exactly-once (transactional Kafka) is too high for most use cases

**Failure mode:** If the process dies between execution and offset commit:
- The message will be re-delivered on restart
- The `DedupTracker` will detect the duplicate and drop it
- No double-processing occurs

**Consistency guarantee:** Offset commits happen AFTER execution state is durably saved. This is enforced by the execution pipeline:
1. Execute the plan (VM step-by-step)
2. Save execution state to FileStore
3. Commit Kafka offset

**Future improvement:** Use Kafka transactions for exactly-once delivery:
```go
// Transactional offset commit
txn := producer.BeginTxn()
txn.SendOffsetsToTransaction(offsets, groupID)
txn.Commit()
```

**Trade-off:** Kafka transactions add latency (~100ms) and require transactional IDs. Deduplication is simpler and sufficient for most use cases.

## Summary Table

| Data Class | Replication | Consistency | Failure Mode | Future |
|---|---|---|---|---|
| Control-plane (partitions, plans) | Kafka pub/sub | Eventual (< 200ms) | Re-rebalance after leader election | Raft log |
| Execution state | Local disk | None (ephemeral) | Lose in-flight work | Replicated store |
| Message acknowledgment | Kafka offset commits | At-least-once | Replay + dedup | Kafka transactions |

## Liveness Model

**Gossip proposes, Raft-confirmed-leader disposes:**

1. **Gossip (SWIM)** detects failures in < 1s
2. **Rebalancer** proposes new assignments based on gossip membership
3. **Fencing token** captures Raft term at decision time
4. **Raft-confirmed-leader** validates term before publishing
5. **Stale leaders** skip publishes (term mismatch)

This gives us fast detection (< 1s) with consistent decisions (only Raft leader acts).

## Fencing Token Pattern

Every leadership-gated operation must follow this pattern:

```go
// 1. Capture leadership state before deciding to act
token := node.CaptureLeadershipToken()
if !token.Valid() {
    return // not leader
}

// 2. Do work (rebalance, compile, etc.)
assignments := partMgr.Rebalance(aliveNodes, token.Term)

// 3. Re-validate before the side-effecting publish
if !node.ValidateLeadershipToken(token) {
    return // leadership changed, discard
}

// 4. Publish (safe — term is valid)
partMgr.PublishAssignments(ctx, assignments)
```

This prevents split-brain: if leadership changed between step 1 and step 3, the token is invalid and the publish is skipped.
