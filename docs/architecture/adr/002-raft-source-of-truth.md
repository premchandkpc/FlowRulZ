# ADR-002: Make Raft the Source of Truth for Control Plane State

## Status

Proposed

## Context

FlowRulZ currently uses Raft for leader election only (`NoopFSM`). Application state — rules, plans, partitions, membership — is maintained through separate local/Kafka mechanisms. This creates several correctness issues:

1. **Election ≠ authority**: A new leader is elected without possessing the authoritative flow state. It can serve stale or incomplete data.
2. **Deploy before cluster commit**: `Engine.Deploy()` mutates local state before `AfterDeploy` publishes asynchronously. Quorum failure is logged but the plan is still activated locally.
3. **Promotion not persisted**: `Engine.Promote()` sets `ActiveVersion` but doesn't call `saveRules()`. Rollbacks and canary promotions are lost on restart.
4. **Partition authority mismatch**: `PartitionManager.Rebalance()` sets `leaderID` to `aliveNodes[0]` (alphabetical sort), not the Raft leader. Legitimate Raft leader assignments can be rejected.
5. **Kafka commits on error**: `Consumer.ConsumeClaim()` calls `MarkMessage`/`Commit` even when the handler returns an error. Failed messages are permanently lost.
6. **ACK quorum spoofable**: `PlanDistributor` counts ACKs without deduplicating by `NodeID`, validating term, or authenticating senders. One node can satisfy multiple quorum slots.
7. **Recovery re-executes**: `recoverInFlight()` re-calls pending services without idempotency keys. A crash after service success but before state update causes duplicate side effects.

## Decision

Replace `NoopFSM` with a deterministic Raft FSM that replicates all control-plane commands. The FSM owns:

- **Flow definitions** and immutable versioned plans
- **Activation/promote/rollback** commands
- **Membership** and **partition epochs**
- **Deployment status**

### Specific Changes

1. **Raft FSM**: Implement `fsm.Apply()` to process commands:
   - `DeployFlow{id, dsl, plan_bytes, version}`
   - `PromoteVersion{id, version}`
   - `RollbackVersion{id, version}`
   - `RemoveFlow{id}`
   - `UpdateMembership{node_id, action}`
   - `RebalancePartitions{epoch, assignments}`

2. **Snapshot support**: `fsm.Snapshot()` serializes current state; `fsm.Restore()` rebuilds from snapshot. Nodes rebuild control-plane state from the committed log after restart.

3. **Content-addressed plans**: Plans stored as immutable artifacts:
   - `flow_id + version + plan_hash + compiler_version`
   - Plans are never mutated; new versions create new artifacts

4. **Epoch-based partitions**: Partition assignments carry Raft term/epoch. Every execution message validates the epoch. Stale assignments are rejected.

5. **Idempotent recovery**: `recoverInFlight()` uses durable idempotency keys. Service calls include the key; external services deduplicate.

6. **Kafka at-least-once**: Consumer only commits on handler success. Failed messages are retried. Add retry topics and DLQ for poison messages.

7. **ACK validation**: `PlanDistributor` validates:
   - ACK from expected `NodeID`
   - ACK term matches current term
   - No duplicate node IDs counting toward quorum

### Implementation Order

| Phase | Scope | Effort |
|-------|-------|--------|
| 1 | Fix promotion persistence, Kafka commit, partition leader | Done (this PR) |
| 2 | Raft FSM for rules + plans | Medium |
| 3 | Snapshot + restore | Medium |
| 4 | Epoch-based partitions | Low |
| 5 | Idempotent recovery + outbox | High |
| 6 | ACK validation + term fencing | Low |
| 7 | gRPC bus durability (optional) | Medium |

## Consequences

### Positive
- Single source of truth for all control-plane state
- Leader always has authoritative data
- Rollbacks and canary promotions survive restarts
- No split-brain between partition authority and Raft
- Failed messages are retried, not silently lost

### Negative
- Raft log becomes the bottleneck for rule deployments
- Snapshot/restore adds complexity
- FSM must be deterministic (no side effects in Apply)
- Kafka handler latency increases (no premature commit)

### Risks
- FSM bugs can corrupt cluster state
- Snapshot size grows with rule count (mitigate with incremental snapshots)
- Recovery latency increases (must replay log from snapshot)

## Alternatives Considered

1. **Keep NoopFSM + fix local persistence**: Simpler but doesn't solve split-brain or election-vs-authority.
2. **Use etcd/Consul instead of Raft**: Adds operational complexity; Raft is already embedded.
3. **Event-sourcing with Kafka only**: Kafka is not a consensus system; can't guarantee leader authority.
