# Cluster Model

Raft consensus, plan distribution, membership, gossip protocol.

## Overview

```
Node 1 (Leader) <--> Node 2 (Follower) <--> Node 3 (Follower)
       |                   |                   |
       +----------+--------+-------------------+
                  |
           Kafka (Transport)
```

## Raft Consensus

### Leader Election

- Nodes elect a leader via Raft protocol
- Leader is authoritative for plan distribution
- Automatic failover on leader loss
- Priority: lowest ID wins ties

### State Replication

- Leader replicates state to followers
- Followers acknowledge writes
- Consensus required for plan activation
- Read-your-writes consistency

### Term Management

- Each election increments term
- Stale leaders are detected by term
- Plans include term for deduplication
- Term changes trigger plan redistribution

## Gossip Protocol

### Peer Discovery

- Nodes broadcast presence via gossip
- Automatic peer discovery on startup
- Manual seeds for bootstrap

### Health Monitoring

- Heartbeat-based health checks
- Failure detection via gossip
- Automatic peer removal on failure

### Membership Updates

- Join/leave events propagated via gossip
- Consistent membership across cluster
- Automatic rebalance on membership change

## Plan Distribution

### Flow

1. Leader compiles DSL to ExecutionPlan
2. Leader publishes plan via PlanDistributor
3. Plan broadcast to all nodes via Kafka/gRPC
4. Each node caches plan locally
5. Version-based deduplication

### Plan Message

```json
{
  "type": "plan",
  "rule_id": "order-processing",
  "version": 5,
  "term": 3,
  "plan": "<bincode bytes>",
  "dsl": "schema:{...} n:validate | n:process",
  "node_id": "node-1"
}
```

### Message Types

| Type | Description |
|------|-------------|
| `plan` | New/updated execution plan |
| `activate` | Activate a plan version |
| `deactivate` | Deactivate a plan |

### Acknowledgment

```
Plan broadcast -> All nodes receive -> Each sends ACK
-> Leader collects ACKs -> Quorum reached -> Plan activated
```

## Membership

### Node States

| State | Description |
|-------|-------------|
| `Alive` | Node is healthy and responsive |
| `Suspect` | Node may be failing |
| `Dead` | Node is confirmed down |

### Leader Detection

- `Membership.LeaderID()` - single-node heuristic (lowest ID)
- Raft cluster is authoritative when configured
- No consensus for single-node deployments

### Lease Management

Leases expire after TTL, renewed via heartbeat. Expired leases trigger failover.

## Transport

### Kafka

- Primary transport for plan distribution
- Topics: `_flowrulz_plans`, `_flowrulz_acks`
- Consumer groups for load balancing
- Exactly-once semantics with dedup

### gRPC

- Direct node-to-node communication
- Used for service calls
- TLS support with mutual authentication
- Connection pooling with `sync.Map`

### Memory

- In-process bus for single-node deployments
- Useful for testing
- Zero network overhead

## Cluster Configuration

### Single Node

```bash
flowrulz -node-id node-1
```

### Multi-Node Cluster

```bash
# Node 1
flowrulz -node-id node-1 -seeds node-2:7946,node-3:7946

# Node 2
flowrulz -node-id node-2 -seeds node-1:7946,node-3:7946

# Node 3
flowrulz -node-id node-3 -seeds node-1:7946,node-2:7946
```

### TLS

```bash
flowrulz -node-id node-1 \
  -tls-cert /path/to/cert.pem \
  -tls-key /path/to/key.pem \
  -tls-ca /path/to/ca.pem
```

## Failure Scenarios

### Leader Failure

1. Follower nodes detect leader missing via gossip
2. Raft election triggered
3. New leader elected
4. Plans redistributed from new leader
5. In-flight executions continue on current node

### Network Partition

1. Partitioned nodes cannot reach leader
2. Leader continues serving if quorum maintained
3. Partitioned nodes enter suspect state
4. On rejoin, state is reconciled

### Node Rejoin

1. Node rejoins cluster via gossip
2. State synchronization occurs
3. Plans are redistributed
4. Node returns to alive state

## Scaling

| Nodes | Use Case |
|-------|----------|
| 1 | Development, single-server |
| 3 | Small production (1 failure tolerance) |
| 5 | Medium production (2 failure tolerance) |
| 7+ | Large production (consider sharding) |
