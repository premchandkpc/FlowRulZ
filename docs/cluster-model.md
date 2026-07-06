# Cluster Model

FlowRulZ operates as a **single-leader cluster** using Raft (`hashicorp/raft`) for leader election and term management, with a `NoopFSM` — no application state is replicated through the Raft log. The gRPC-based Cluster Bus provides peer-to-peer messaging for state distribution; no Kafka, no ZK required.

## Node Roles

| Role | Responsibility | Component |
|------|---------------|-----------|
| **Leader** | Rule compilation, plan distribution, service registry aggregator, partition assignment | Control Plane |
| **Follower** | Execute plans for owned partitions, register local services, reply to health pings | Data Plane |
| **Worker** | Same as Follower. All non-leader nodes are Workers. | Data Plane |

A node starts as Follower. If no leader exists, it transitions to Leader. Exactly one leader at any time.

```
┌─────────────────────────────────────────────────────┐
│                   Cluster                           │
│                                                      │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐        │
│  │  Leader  │   │ Follower │   │ Follower │        │
│  │          │   │          │   │          │        │
│  │ Compiler │   │ VM       │   │ VM       │        │
│  │ Registry │   │ Worker   │   │ Worker   │        │
│  │ Sched    │   │ SrvReg   │   │ SrvReg   │        │
│  └────┬─────┘   └────┬─────┘   └────┬─────┘        │
│       │              │              │               │
│       └──────────────┴──────────────┘               │
│                        │                            │
│                   ┌────▼────┐                       │
│                   │ Cluster │                       │
│                   │   Bus   │                       │
│                   └─────────┘                       │
└─────────────────────────────────────────────────────┘
```

## Transport: Cluster Bus

The **Cluster Bus** (`server/internal/cluster/`) is a gRPC-based peer-to-peer overlay:

- **ClusterNode**: manages Publish/Subscribe, peer membership, topic handlers
- **ClusterProducer** / **ClusterConsumer**: adapters implementing `transport.MessageProducer` / `transport.MessageConsumer`
- **Topics**: in-memory per-node routing tables — messages published to a topic are delivered to all subscribers across the cluster via gRPC streams
- **No external deps for messaging**: no Kafka, no ZK required — pure gRPC p2p Cluster Bus

### Transport Factory

The `TransportFactory` (`server/internal/transport/factory.go`) abstracts all transport backends behind a single interface. At startup:

1. Always registers in-memory backend as fallback
2. If `FLOWRULZ_KAFKA_BROKERS` is set → registers Kafka, selects `KindKafka`
3. If no Kafka brokers → creates `ClusterNode`, registers cluster, selects `KindCluster`
4. If neither → stays at `KindMemory`

The `MessageRouter` creates all 5 consumers (members, plans, acks, partitions, user topic) through the factory. Producers for DLQ, plans, acks, and partitions also use the factory.

`cluster.RegisterClusterTransport(factory, node)` is the adapter that plugs the cluster's gRPC producer/consumer into the factory.

See `docs/transport-factory.md` for details.

Kafka (`server/internal/transport/kafka/`) remains as a legacy fallback when `FLOWRULZ_KAFKA_BROKERS` is explicitly set.

## Node Identity

```go
type NodeID struct {
    ID        string // "node-1" — human-readable, must be unique
    RPCAddr   string // gRPC address for cluster bus
    HTTPAddr  string // admin API address
    Partitions []int32 // owned partitions
}
```

Node ID is set via `FLOWRULZ_NODE_ID` env var (default `node-1`). Used for leader ordering (lowest ID wins).

## Membership Discovery

### Seed-Based Discovery

Nodes are configured with a list of seed peers via `FLOWRULZ_SEEDS` env var (comma-separated `host:port`):

```
1. Node starts, configures HTTP + gRPC listeners
2. Node connects to seed peers via gRPC
3. Node announces itself on `_flowrulz_members` topic
4. All nodes maintain local member list from topic
5. Leader sends current rule set as catch-up to joining node
```

### Heartbeat Protocol

Every node broadcasts heartbeat on `_flowrulz_members` topic at 3s interval:

```json
{
  "node_id": "node-a",
  "state": "alive",
  "term": 3,
  "partitions": [0, 1, 2],
  "services": ["payment", "inventory"],
  "load": {
    "active_execs": 42,
    "queue_depth": 7
  }
}
```

Leader tracks last heartbeat timestamp per node. Nodes with no heartbeat for `3 * interval` are marked Dead.

### Gossip Protocol

Cluster Bus uses an epidemic gossip protocol (`cluster.Gossiper`) for membership propagation:

- **Push**: Every 2s, each node sends its membership state to 2 random peers on `_flowrulz_gossip`
- **Pull (anti-entropy)**: Every 10s, each node sends its epoch map to a random peer; the peer responds with any states the requester is missing
- **Conflict resolution**: Higher epoch wins; same epoch → higher term wins
- **GossipState**: `{node_id, address, term, epoch}` — epoch is incremented on every local state change

This converges membership state across the cluster faster than heartbeat-only detection.

## Leader Election

**Raft consensus (`hashicorp/raft`) with `NoopFSM` — leader election and term management only, no state replication.**

```
Algorithm (via Raft):
1. Nodes join Raft cluster via seed node's /cluster/join endpoint
2. Raft elects leader via standard Raft protocol (log-based heartbeat)
3. Leader status tracked via RaftCluster.LeaderCh()
4. NoopFSM: Apply/Snapshot/Restore are all no-ops — no state goes through Raft log
5. Application state (plans, partitions) distributed via gRPC Cluster Bus
```

**Fallback**: When Raft is not configured (`deps.Cluster == nil`), `SingleLeaderStrategy` assumes the node is always leader (single-node mode). The `membership.go` lowest-ID logic exists only in this fallback path.

**Epoch-based fencing**: Each leader election increments a monotonic `term` number. The leader embeds its term in every `PlanMessage`. Followers reject plan activation from any term lower than their known current term. `PlanDistributor.SetTerm()` / `CurrentTerm()` manage the term atomically.

**Term persistence**: Term and current leader ID are persisted to `cluster-term.json` in the exec state directory (`TermStore`). On restart, the node restores its known term to avoid accepting stale plans from a previous term.

**Lease-based detection**: A `LeaderLease` (default 8s, ~2.5× heartbeat interval) triggers re-election if the leader's heartbeat hasn't been seen. The `Membership.StartLeaderLeaseChecker` goroutine runs every heartbeat interval and marks the leader dead if its last seen exceeds the lease. A callback notifies `ProdNode.runLeaderElection()` which promotes the next candidate.

**Fencing on heartbeat receive**: When a non-leader heartbeat carries a higher term, the current leader steps down immediately (`handleMembershipMessage` compares `hb.Term > CurrentTerm()`).

```
t=0  Node A starts → no leader → claims leadership → leader=A
t=1  Node B starts → leader=A → follower=B  
t=2  Node C starts → leader=A → follower=C
t=5  Node A crashes → heartbeats stop
t=8  Node B detects A dead → B is now lowest alive → claims leadership → leader=B, term++
t=9  Node C detects A dead → sees leader=B → stays follower
t=10 Node A restarts → sees leader=B → stays follower
```

## Partition Ownership

Partition ownership is managed by `partition.Manager` (`server/internal/partition/`):

- **Fixed N partitions**: Default 64 (configurable via `FLOWRULZ_NUM_PARTITIONS`)
- **Round-robin assignment**: Leader assigns partitions across alive nodes
- **Key-based routing**: `PartitionForKey(key)` uses FNV-32a to map keys to partitions
- **Rebalancing**: Triggered on node join/leave or leader election

### Partition Lifecycle

```
1. Node joins → membership detects → RebalanceNotifier.CheckAndRebalance()
2. Leader calls PartitionManager.Rebalance(aliveNodes, term) → round-robin assignment
3. Leader publishes PartitionMessage to `_flowrulz_partitions` topic
4. All followers apply assignments via HandleAssignmentMessage()
5. Routers use partition assignment for PublishToPartition and reply routing
```

### Rebalance Triggers

| Event | Trigger | Action |
|-------|---------|--------|
| Node joins | Membership heartbeat detects new node | `CheckAndRebalance()` → publish new assignments |
| Node leaves | Lease expiry or stale eviction | `CheckAndRebalance()` → publish new assignments |
| Leader elected | `runLeaderElection()` | `CheckAndRebalance()` on promotion |
| Manual | `POST /partitions/rebalance` | Force rebalance |

### HTTP Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/partitions` | List partition assignments per node |
| `POST` | `/partitions/rebalance` | Force rebalance (leader only) |

### Topics

| Topic | Description |
|-------|-------------|
| `_flowrulz_partitions` | Partition assignment messages |

## Plan Distribution

```
1. Admin API receives deploy/promote request on leader
2. Leader compiles DSL → ExecutionPlan  
3. Leader publishes plan to `_flowrulz_plans` topic (keyed by rule_id):
   { "type": "plan", "rule_id": "order-flow", "version": 17, "term": 3, "plan": ..., "dsl": ... }
4. All followers consume `_flowrulz_plans`
5. Each follower stores the plan (inactive), verifies term ≥ known term
6. Each follower publishes ACK to `_flowrulz_acks`:
   { "node_id": "node-b", "rule_id": "order-flow", "version": 17, "status": "received" }
7. Leader waits for ACKs from a quorum of followers (majority: `(n-1)/2+1` excluding leader; `n=1` skips ack wait)
8. Leader publishes activation command to `_flowrulz_plans`:
   { "type": "activate", "rule_id": "order-flow", "version": 17, "term": 3 }
9. Followers receive activation → mark version active
10. Old version continues active executions, new executions use new version
```

### Internal Topics (Cluster Bus)

| Topic | Description |
|-------|-------------|
| `_flowrulz_members` | Cluster membership + heartbeats |
| `_flowrulz_plans` | Compiled plans + activation commands |
| `_flowrulz_acks` | Acknowledgement records |
| `_flowrulz_replies` | Cross-node reply routing |
| `_flowrulz_dlq` | Dead-letter entries |
| `_flowrulz_gossip` | Gossip protocol push/pull messages |
| `_flowrulz_partitions` | Partition assignment messages |

## Service Registry

Services self-register via HTTP (`POST /register`). Every node runs its own `ServiceRegistry` instance. The leader aggregates and publishes the combined registry.

See `server/internal/registry/` and `docs/flow-architecture.md` for full details.

## Reply Router

Per-node component (`server/internal/replyrouter/`). Tracks pending request/reply by correlation_id. Replies route via cluster bus topic to origin node.

## Node Lifecycle

### Join
1. Node starts with config
2. Connects to seed peers via gRPC
3. Announces on `_flowrulz_members`
4. Receives catch-up rules from leader
5. Begins consuming + executing

### Drain
1. Node receives SIGTERM
2. Leader rebalances partitions away from leaving node
3. Node waits for active executions to complete
4. Node announces departure

### Crash Recovery
1. Node restarts with same NodeID
2. Reconnects to cluster
3. Leader sends missed rule versions
4. Node resumes normal operation

## Failure Detection

| Failure | Detection | Recovery |
|---------|-----------|----------|
| Node crash | Missing 3 heartbeats | Leader marks Dead, partitions reassigned |
| Leader crash | Raft heartbeat timeout | Raft re-elects new leader; next-lowest-ID in Raft cluster |
| Service unhealthy | Active health check fails | Removed from rotation |
| Network partition | Heartbeats lost from subset | On heal, rejoining nodes catch up via plans topic |

## Configuration

```go
type ClusterConfig struct {
    NodeID      string   // unique node identifier
    GRPCAddr    string   // gRPC listen address
    HTTPAddr    string   // admin API address
    Seeds       []string // seed peer addresses
    HeartbeatMS int      // default: 3000
}
```

## Security

- Inter-node gRPC uses mutual TLS (mTLS) when configured
- Admin API uses Bearer token auth (`FLOWRULZ_API_KEY`)
- Internal topics not exposed to client applications

## Future

- Multi-az failover
- Geo-partitioning
- Canary deployments
- Load-aware rebalancing (not just round-robin)
- Partition migration with data transfer
