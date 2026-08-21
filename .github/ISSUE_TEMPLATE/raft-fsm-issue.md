# Raft election-only: make FSM the source of truth for control plane state

## Problem

Raft currently uses `NoopFSM` — it elects a leader but does not replicate application state. Rules, plans, partitions, and membership are maintained through separate local/Kafka mechanisms. This creates correctness issues:

1. **Leader has no authoritative state** — new leader elected without possessing flow state
2. **Deploy before cluster commit** — `Engine.Deploy()` mutates local state before async cluster publish
3. **Promotion lost on restart** — `Engine.Promote()` doesn't call `saveRules()` _(fixed in this PR)_
4. **Partition leader ≠ Raft leader** — `aliveNodes[0]` after sort, not Raft leader _(fixed in this PR)_
5. **Kafka commits on error** — failed messages permanently lost _(fixed in this PR)_
6. **ACK quorum spoofable** — no node-ID dedup, no term validation
7. **Recovery re-executes** — no idempotency keys on service calls

## This PR Fixes

- [x] `engine.Promote()` now persists `ActiveVersion` via `saveRules()`
- [x] `partition.Rebalance()` no longer overrides `leaderID` from sorted nodes
- [x] `kafka.ConsumeClaim()` only commits offsets on handler success

## Remaining Work (see ADR-002)

- [ ] Replace `NoopFSM` with deterministic Raft FSM
- [ ] FSM commands: DeployFlow, PromoteVersion, RollbackVersion, RemoveFlow
- [ ] Snapshot + restore for FSM state
- [ ] Content-addressed immutable plan storage
- [ ] Epoch-based partition assignments
- [ ] Idempotent recovery with durable keys
- [ ] ACK validation (node-ID dedup, term check)
- [ ] gRPC bus durability (optional)

## References

- `docs/architecture/adr/002-raft-source-of-truth.md`
- `server/internal/cluster/raft.go` — NoopFSM
- `server/internal/engine/engine.go` — Promote (fixed)
- `server/internal/partition/manager.go` — Rebalance (fixed)
- `server/internal/transport/kafka/consumer.go` — ConsumeClaim (fixed)
