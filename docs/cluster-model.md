# Cluster Model

FlowRulZ operates as a **single-leader cluster** with no Raft. Kafka provides durability; the cluster provides coordination, plan distribution, and service routing. Nodes are ephemeral — any node can fail and restart without data loss.

## Node Roles

| Role | Responsibility | Component |
|------|---------------|-----------|
| **Leader** | Rule compilation, plan distribution, service registry authority, partition assignment, scheduler | Control Plane |
| **Follower** | Execute plans for owned partitions, register local services, reply to health pings | Data Plane |
| **Worker** | Same as Follower (data plane). All non-leader nodes are Workers. | Data Plane |

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
│                   │  Kafka  │                       │
│                   └─────────┘                       │
└─────────────────────────────────────────────────────┘
```

## Node Identity

Each node has a unique identity:

```go
type NodeID struct {
    ID        string // "hostname:port" — human-readable, must be unique
    RPCAddr   string // address for inter-node RPC
    HTTPAddr  string // admin API address
    Partitions []int32 // owned Kafka partitions
}

type NodeState int
const (
    StateJoining   NodeState = 0
    StateAlive     NodeState = 1  
    StateLeaving   NodeState = 2
    StateDead      NodeState = 3
)
```

Node ID is derived from `HOSTNAME` env var (default `os.Hostname()`) + persisted config path. Used for leader ordering.

## Membership Discovery

### Seed-Based Discovery (v1)

Nodes are configured with a list of seed peers:

```go
type ClusterConfig struct {
    NodeID      string   // unique node identifier
    Seeds       []string // seed peer addresses (host:port)
    RPCAddr     string   // listen address for inter-node RPC
    HTTPAddr    string   // admin API address
    HeartbeatMS int      // heartbeat interval (default 1000ms)
}
```

Join flow:

```
1. Node starts, configures HTTP + RPC listeners
2. Node contacts seeds to announce itself
3. Leader (or first responder) adds node to member list
4. Leader publishes updated member list to internal Kafka topic `_flowrulz_members`
5. All nodes consume `_flowrulz_members` to maintain local member list
6. Leader sends current rule set as catch-up to joining node
```

### Heartbeat Protocol

Every node sends a heartbeat to the leader at a configurable interval (default 1s). Heartbeat payload:

```json
{
  "node_id": "node-a:9091",
  "state": "alive",
  "partitions": [0, 1, 2],
  "services": ["payment", "inventory"],
  "load": {
    "active_execs": 42,
    "queue_depth": 7,
    "cpu_pct": 0.45
  }
}
```

Leader tracks last heartbeat timestamp per node. Nodes with no heartbeat for `3 * HeartbeatMS` are marked `Dead`.

### Kafka-Based Member List

An internal Kafka topic `_flowrulz_members` (1 partition, compacted) stores the authoritative member list. Every node:

1. Publishes its membership record on startup and on state change
2. Consumes the topic to maintain a local view of the cluster
3. Uses key = `node_id` so the compacted topic retains only the latest state per node

### v2 Upgrade Path

Replace seed-based discovery with gossip protocol (memberlist or similar) for:
- Automatic discovery without seed configuration
- Faster failure detection via suspicion timeouts
- No single point of failure for membership

## Leader Election

**Simple ordering — no Raft, no Paxos, no external dependency.**

```
Algorithm:
1. Every node consumes `_flowrulz_members` 
2. Sort alive nodes by (ID, ascending)
3. Lowest-ID node is leader
4. If leader stops heartbeating → nodes detect absence (3x timeout)
5. Next-lowest-ID node promotes itself to leader
6. New leader publishes its leadership claim to `_flowrulz_members`
```

The leader publishes a leadership claim record to `_flowrulz_members` with key `_leader`. Only the leader updates this record (heartbeat + term number). Followers check: if `_leader` record is stale (no update for N seconds), or the claiming node is not the lowest-ID alive node, the next-lowest node claims leadership.

**Epoch-based fencing for split-brain prevention**: Each leader election increments a monotonic `term` number. The leader embeds its current term in every `PlanMessage` (plan and activate). Followers track the highest-known term and reject plan activation from any term lower than their known current term. This prevents a stale leader (from a partitioned minority) from activating plans upon rejoin.

The `term` is stored in `_flowrulz_members` as part of the `_leader` record. A newly elected leader increments the term by 1. `PlanDistributor.SetTerm()` / `CurrentTerm()` manage the term atomically.

```
Typical timeline:

t=0  Node A starts → no leader → claims leadership → leader=A
t=1  Node B starts → leader=A → follower=B  
t=2  Node C starts → leader=A → follower=C
t=5  Node A crashes → heartbeats stop
t=8  Node B detects A dead → B is now lowest alive → claims leadership → leader=B
t=9  Node C detects A dead → sees leader=B → stays follower
t=10 Node A restarts → sees leader=B → stays follower
```

## Partition Ownership

FlowRulZ uses Kafka's consumer group protocol for partition assignment. The leader:

1. Creates a Kafka consumer group per lane (fast/normal/heavy)
2. Kafka's group coordinator handles partition rebalancing
3. Each node's consumer reports the partitions it owns
4. Leader tracks `partition → node_id` mapping for routing decisions

```
┌──────────────┐
│   Kafka      │
│  ┌─────┬──┐  │
│  │ P0  │P1 │  │  Partition 0 → Node A
│  ├─────┼──┤  │  Partition 1 → Node B  
│  │ P2  │P3 │  │  Partition 2 → Node A
│  └─────┴──┘  │  Partition 3 → Node C
└──────────────┘
```

Partition ownership is used for:
- **Direct publish**: Producer can target a specific partition for ordering guarantees
- **Reply routing**: Reply messages are routed to the node that owns the reply topic's partition
- **State affinity**: Related events hash to the same partition → same node → no distributed state needed

## Plan Distribution

When a rule is deployed or promoted on the leader:

```
1. Admin API receives deploy/promote request on leader
2. Leader compiles DSL → ExecutionPlan  
3. Leader publishes plan to internal topic `_flowrulz_plans` (keyed by rule_id), including leader's epoch term:
   { "type": "plan", "rule_id": "order-flow", "version": 17, "term": 3, "plan": ..., "dsl": ... }
4. All followers consume `_flowrulz_plans`
5. Each follower stores the plan in its local Engine (inactive), verifies term ≥ known term
6. Each follower publishes ACK to `_flowrulz_acks`:
   { "node_id": "node-b", "rule_id": "order-flow", "version": 17, "status": "received" }
7. Leader waits for ACKs from a quorum of followers (configurable quorum, default=majority ⌊N/2⌋+1)
8. Leader publishes activation command to `_flowrulz_plans`:
   { "type": "activate", "rule_id": "order-flow", "version": 17, "term": 3 }
9. Followers receive activation → mark version active
10. Old version continues active executions, new executions use new version
```

```
Time    Leader              Follower A          Follower B
│       │                   │                   │
│       │-- plan v17 -------│------------------→│
│       │                   │                   │
│       │←── ack ----------│                   │
│       │←── ack ------------------------------│
│       │                   │                   │
│       │-- activate v17 ---│------------------→│
│       │                   │                   │
│       │                   │-- swap active ----│
│       │                   │-- drain old -------│
▼       ▼                   ▼                   ▼
```

### Internal Topics

| Topic | Partitions | Retention | Description |
|-------|-----------|-----------|-------------|
| `_flowrulz_members` | 1 | Compacted | Cluster membership + heartbeats |
| `_flowrulz_plans` | 1 | Compacted | Compiled plans + activation commands |
| `_flowrulz_acks` | 1 | 1 hour | Acknowledgement records |
| `_flowrulz_replies` | N | 1 hour | Cross-node reply routing |
| `_flowrulz_dlq` | 1 | Compacted | Dead-letter entries |

## Service Registry

The Service Registry maps service names to healthy endpoints across the cluster. It is part of the Control Plane but replicated to all Data Plane nodes. Every node runs its own `ServiceRegistry` instance with HTTP registration endpoints.

### Service Model

Services register as `ServiceInstance` with full metadata:

```go
type ServiceInstance struct {
    ID             string              // auto: "{name}-{address}-{port}"
    Name           string              // service name ("payment")
    Version        string              // semver
    Methods        []MethodInfo        // supported methods
    Capabilities   ServiceCapabilities
    Endpoint       Endpoint            // protocol, address, port
    Zone           string              // deployment zone
    Weight         int                 // LB weight (default 100)
    Tags           map[string]string
    Metadata       map[string]any
    HeartbeatAt    time.Time
    RegisteredAt   time.Time
}

type MethodInfo struct {
    Name       string
    TimeoutMS  int
    Idempotent bool
}
```

### Registration

Services register themselves via HTTP:

| Endpoint | Method | Payload | Description |
|----------|--------|---------|-------------|
| `/register` | POST | `RegisterRequest` | Register or update instance |
| `/heartbeat` | POST | `{name, instance_id}` | Keep instance alive (default 30s TTL) |
| `/services` | GET | — | List all services with instances |

`RegisterRequest` fields: `id`, `name`, `version`, `methods`, `capabilities`, `address`, `port`, `protocol`, `zone`, `weight`, `tags`, `metadata`.

Heartbeat expiry: instances without a heartbeat within 30s are marked unhealthy. A background goroutine checks every 15s.

### Method Syntax in Rules

Rules reference services by name with optional method suffix:

```
n:payment              # calls any method on payment
n:payment.authorize    # calls only the authorize method on payment
```

The method is embedded in the service name string. The Rust DSL lexer captures everything after `n:` including the dot — no Rust changes needed. On the Go side, `bridge.ParseServiceMethod("payment.authorize")` splits it into `("payment", "authorize")`.

### Service Resolution

When the VM executes `n:payment.authorize`:

```
1. VM executes Next opcode with svcID=12
2. FFI callback receives (ctxID, 12, body, ...)
3. Bridge resolves svcID → raw name via PlanServices(plan) map
4. Bridge.ParseServiceMethod("payment.authorize") → ("payment", "authorize")
5. ServiceRegistry.LookupInstance("payment", "authorize") returns healthy instance supporting the method
6. If endpoint is local → call registered handler in-process
7. If endpoint is remote → make HTTP/gRPC call to remote node
8. If call was a Request (expects reply):
   a. Remote node executes the call
   b. Remote node publishes reply to `_flowrulz_replies` with correlation_id
   c. ReplyRouter on origin node picks up the reply → delivers to waiting goroutine
9. Response is returned to the VM
```

Note: `bridge.InternLookup(svcID)` is **broken** for plan-local service IDs (the global intern table has pre-filled strings at IDs 0-6). Use `bridge.PlanServices(plan)` → `map[uint16]string` instead.

### Health Checking

The Service Registry supports two health check modes:

| Mode | Mechanism | Rate |
|------|-----------|------|
| **Passive** | Mark unhealthy when node heartbeat fails | Node-level, 1s |
| **Active** | Periodically probe endpoint `/health` | Service-level, configurable |
| **Heartbeat expiry** | `StartHeartbeatChecker()` goroutine, 15s interval marks instances with no heartbeat >30s | 15s |

Endpoints with consecutive failures exceeding a threshold are marked unhealthy and removed from rotation.

### Load Balancing

When multiple endpoints exist for a service, the registry picks one via `LookupInstance(name, method)`:

| Strategy | Description |
|----------|-------------|
| RoundRobin | Cycle through healthy endpoints |
| Random | Pick randomly (default) |
| LeastLoaded | Pick node with lowest active_execs from heartbeat |
| LocalPrefer | Pick local endpoint if healthy, else round-robin |

Strategy is configurable per service. Method-aware selection: only instances declaring the requested method in their `Methods` list are candidates.

## Reply Router

The Reply Router handles cross-node request/reply correlation. It is a per-node component.

```
Request flow:

1. Client calls Request("payment", payload, timeout)
2. Origin node generates correlation_id, creates PendingRequest
3. Origin node publishes event with Mode=Request, correlation_id in header
4. Destination node receives event, executes VM
5. VM produces reply → destination node publishes reply to `_flowrulz_replies`
   with key = correlation_id
6. Origin node's ReplyRouter consumes `_flowrulz_replies`
7. ReplyRouter matches correlation_id → delivers to PendingRequest channel
8. Waiting goroutine receives reply → returns to client
```

### Pending Request Lifecycle

```go
type PendingRequest struct {
    CorrelationID string
    ReplyCh       chan []byte  // buffered(1)
    Deadline      time.Time
    CreatedAt     time.Time
    SourceNode    string       // node that will produce the reply
}
```

- `Send(corrID) → <-chan []byte` — registers pending request, returns channel
- `Route(corrID, response)` — delivers response to channel, closes it
- Expired requests are evicted by periodic cleanup goroutine
- Maximum pending requests is bounded (configurable, default 10000)

### Reply Partitioning

To ensure the reply consumer and the original request handler are on the same node, reply topic partitions:

- `hash(correlation_id) % N` → partition owned by origin node
- This guarantees the reply is consumed by the same node that sent the request

### v2: Direct Reply

For lower latency, nodes can reply directly over gRPC instead of routing through Kafka. The ReplyRouter still holds pending requests; the transport layer routes the reply via gRPC stream. Kafka remains the fallback.

## Node Lifecycle

### Join

```
1. Node starts with config (Seeds, NodeID, RPCAddr, HTTPAddr)
2. Node initializes local Engine (load persisted rules)
3. Node opens HTTP listener (admin API)
4. Node opens RPC listener (inter-node communication)
5. Node contacts seeds → announces join
6. Leader adds node to member list
7. Leader publishes updated member list to `_flowrulz_members`
8. Leader sends full rule set (all compiled plans) to joining node
9. Node consumes `_flowrulz_members` → acquires member list
10. Node joins Kafka consumer groups → assigned partitions
11. Node begins consuming + executing
12. Node publishes its initial heartbeat (with services, partitions, load)
```

### Drain

Graceful shutdown for maintenance:

```
1. Admin API receives /drain on target node (or SIGTERM)
2. Node sets state=Leaving in heartbeat
3. Leader rebalances partitions away from leaving node (via Kafka rebalance)
4. Node waits for active executions to complete (ActiveExec.Wait())
5. Node publishes final heartbeat with state=Dead
6. Node closes consumers, producer, RPC, HTTP listeners
7. Leader removes node from member list
```

### Crash Recovery

When a node crashes and restarts:

```
1. Node restarts with same NodeID
2. Node loads locally persisted rules
3. Node announces join
4. Leader recognizes node as rejoining (same NodeID)
5. Leader sends any rule versions the node missed (based on version number)
6. Kafka consumer group rebalance reassigns partitions
7. Node resumes normal operation
```

## Failure Detection

| Failure | Detection | Recovery |
|---------|-----------|----------|
| Node crash | Missing 3 heartbeats | Leader marks Dead, partitions reassigned |
| Leader crash | Missing 3 heartbeats from leader | Next-lowest-ID follower becomes leader |
| Service unhealthy | Active health check fails | Removed from rotation, retried periodically |
| Network partition | Heartbeats lost from subset | Split: minority marks majority as dead, majority continues. On heal, rejoining nodes catch up via `_flowrulz_plans` |
| Kafka unavailable | Consumer/producer errors | Node enters read-only mode. No new rule deploys, existing rules continue on local Engine. |

## Configuration

```go
type ClusterConfig struct {
    // Identity
    NodeID      string  
    RPCAddr     string  
    HTTPAddr    string  

    // Discovery
    Seeds       []string // seed peer list

    // Timing  
    HeartbeatMS int     // default: 1000
    TimeoutMS   int     // default: 3000 (3x heartbeat)

    // Plan distribution
    AckQuorum   int     // default: 0 = majority (⌊N/2⌋ + 1); -1 = all alive nodes
    PlanTimeoutMS int   // default: 10000

    // Service registry  
    HealthCheckMS int   // default: 5000
    LBStrategy   string // "random" | "roundrobin" | "localprefer"

    // Reply router
    MaxPendingRequests int  // default: 10000
    ReplyCleanupMS     int  // default: 1000

    // Internal topics
    MemberTopic  string // default: "_flowrulz_members"
    PlanTopic    string // default: "_flowrulz_plans"
    AckTopic     string // default: "_flowrulz_acks"
    ReplyTopic   string // default: "_flowrulz_replies"
}
```

## Security

- Inter-node RPC uses mutual TLS (mTLS) when enabled
- Admin API uses Bearer token auth (existing `FLOWRULZ_API_KEY`)
- Internal topics are not exposed to client applications
- Service registry requires authentication for registration
- Reply router validates that only the originating node can correlate replies

## Not Yet Designed

These are deferred to future iterations:

- **Multi-az failover**: Cross-datacenter replication of internal topics
- **Geo-partitioning**: Route events by geographic region
- **Rate-limited plan distribution**: Throttle plan rollouts to avoid thundering herd
- **Canary deployments**: Route a subset of events to a new rule version before full rollout
- **Cluster-wide metrics aggregation**: Centralized view of all node metrics
- **Automatic rebalancing**: Intelligent partition assignment based on load
