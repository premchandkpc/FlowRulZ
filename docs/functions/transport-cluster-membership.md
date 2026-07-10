# internal/transport, internal/cluster, internal/membership — Function Reference

---

## internal/transport/ (Core Types)

### `NewProducer(topic string) *Producer`
In-memory no-op producer. `Send` and `Close` are no-ops.

### `RegisterMemory(factory *TransportFactory)`
Registers in-memory producer/consumer factories with a `TransportFactory`.

---

## internal/transport/memory/bus.go

### `New() *Bus`
Creates in-memory message bus with topic→handlers map.

### `(b *Bus) Publish(ctx, topic, msg) error`
Dispatches message to all registered handlers for the topic. Handlers called synchronously.

### `(b *Bus) PublishToPartition(ctx, topic, key, msg) error`
Same as `Publish` (partition key ignored in memory backend).

### `(b *Bus) Subscribe(ctx, topic, handler) (*Subscription, error)`
Registers handler for topic. Returns subscription with unique ID.

### `(b *Bus) Unsubscribe(ctx, sub) error`
Removes handler by subscription ID.

### `(b *Bus) Request(ctx, topic, msg, timeout) (*Message, error)`
**Flow:**
1. Generate correlation ID, set on message.
2. Create reply channel, publish message.
3. Wait on reply channel or timeout.

**Edge Cases:** Timeout → error. No reply → timeout.

### `(b *Bus) Reply(ctx, correlationID, msg) error`
Delivers message to pending request's reply channel.

### `(b *Bus) Broadcast(ctx, topic, msg) error` — Same as `Publish`.
### `(b *Bus) TopicStats() map[string]int` — Handler count per topic.
### `(b *Bus) Close() error` — No-op.

---

## internal/transport/grpc/bus.go

### `NewGRPCBus(addr string) *GRPCBus`
Creates gRPC EventBus server with insecure credentials.

### `NewGRPCBusWithTLS(addr, certFile, keyFile string) *GRPCBus`
With TLS credentials.

### `(b *GRPCBus) Start() error`
Starts gRPC server in goroutine.

### `(b *GRPCBus) Publish(ctx, req) (*PublishResponse, error)`
Delivers to registered topic handler.

### `(b *GRPCBus) Request(ctx, req) (*RequestResponse, error)`
Publishes with correlation ID, waits for reply (5s timeout).

### `(b *GRPCBus) Reply(ctx, req) (*ReplyResponse, error)`
Delivers reply to pending request.

### `(b *GRPCBus) Broadcast(ctx, req) (*BroadcastResponse, error)`
Broadcasts to all topic handlers.

### `(b *GRPCBus) Subscribe(req, stream) error`
Server-streaming subscription. Sends messages on the gRPC stream.

### `(b *GRPCBus) Stop()`
Graceful shutdown of gRPC server.

---

## internal/transport/grpc/client.go

### `NewGRPCClient(addr string) *GRPCClient`
Creates with insecure credentials, empty subscription map.

### `(c *GRPCClient) Connect() error`
Connects with insecure credentials.

### `(c *GRPCClient) ConnectWithTLS(certFile, keyFile, caFile) error`
**Returns an error** if TLS is not implemented. Does NOT silently fall back to insecure credentials.

### `(c *GRPCClient) Publish(topic, msg) error`
Marshals message → `PublishRaw` → returns.

### `(c *GRPCClient) Subscribe(topic, handler) *Subscription`
Spawns goroutine reading from gRPC stream, calling handler per message.

### `(c *GRPCClient) Request(topic, msg, timeout) (*Message, error)`
Sends `RequestRequest`, waits for `RequestResponse` with timeout.

### `(c *GRPCClient) Reply(topic, reqID, msg) error`
Sends `ReplyRequest` with correlation ID.

### `(c *GRPCClient) Broadcast(topic, msg) error`
Sends `BroadcastRequest`.

### `(c *GRPCClient) Close()` — Closes gRPC connection.

---

## internal/transport/kafka/producer.go

### `NewProducer(topic string, cfg Config) *Producer`
Creates Kafka producer. Lazy-initializes Sarama producer on first `Send`.

### `(kp *Producer) Send(ctx, key, value) error`
**Flow:**
1. If Sarama producer nil → `initProducer()` (lazy init).
2. Produce message with key/value.

**Edge Cases:**
- Init failure → error returned.
- Produce failure → error returned.
- Idempotent mode enabled via config.

### `(kp *Producer) Close()` — Closes Sarama producer.

---

## internal/transport/kafka/consumer.go

### `NewConsumer(topic string, handler MessageHandler, cfg Config) *Consumer`
Creates Kafka consumer. Supports both Sarama consumer group and in-memory channel mode.

### `(kc *Consumer) Start(ctx)`
Starts consuming in goroutine. If `channel` mode → reads from channel. Else → Sarama consumer group.

### `(kc *Consumer) Stop()`
Signals stop, waits for group/channel to exit.

### `(kc *Consumer) Inject(msg []byte)`
Injects message into in-memory channel (for testing).

### `(kc *Consumer) ConsumeClaim(sess, claim)`
Reads messages from Sarama claim, calls handler for each.

---

## internal/transport/kafka/registry.go

### `RegisterKafka(factory, cfg)`
Registers Kafka producer/consumer factories with a `TransportFactory`.

---

## internal/cluster/transport.go

### `NewClusterProducer(topic string, node *ClusterNode) *ClusterProducer`
Routes messages through cluster gossip.

### `(p *ClusterProducer) Send(ctx, key, value) error`
Publishes via ClusterNode's topic handler.

### `NewClusterConsumer(topic string, handler MessageHandler, node *ClusterNode) *ClusterConsumer`
Subscribes to cluster topic.

### `(c *ClusterConsumer) Start(ctx)` — Registers handler on ClusterNode.
### `(c *ClusterConsumer) Stop()` — Unregisters handler.

---

## internal/cluster/gossip.go

### `NewGossiper(nodeID, grpcAddr string, node *ClusterNode) *Gossiper`
Creates gossip protocol handler for cluster state dissemination.

### `(g *Gossiper) Start(ctx)`
Starts push/sync goroutines (periodic gossip).

### `(g *Gossiper) Stop()`
Signals stop to both goroutines.

### `(g *Gossiper) UpdateState(nodeID, state)`
Updates local state and triggers gossip push to random peers.

### `(g *Gossiper) GetState(nodeID) (GossipState, bool)`
Returns state for specific node.

### `(g *Gossiper) AllStates() []GossipState`
Returns all known node states.

### `(g *Gossiper) HandleGossipMessage(ctx, topic, body)`
Processes incoming gossip message, merges states.

### `(g *Gossiper) GetMyState() GossipState`
Returns this node's own state.

---

## internal/cluster/pkgsupport.go (ClusterMember)

Wraps `RaftCluster` as `pkgcluster.ClusterMember` interface.

| Method | Description |
|---|---|
| `ID()` | Node ID |
| `Addr()` | gRPC address |
| `Start(ctx)` | Starts Raft |
| `Stop(ctx)` | Stops Raft |
| `State()` | Returns `ClusterState` (Follower/Candidate/Leader) |
| `IsLeader()` | Leader check |
| `CurrentTerm()` | Raft term |
| `LeaderID()` / `LeaderAddr()` | Current leader info |
| `SubscribeLeaderChanges(fn)` | Leader change callback |
| `SubscribeTermChanges(fn)` | Term change callback |
| `Join(id, addr)` | Add peer to cluster |
| `Remove(id)` | Remove peer |
| `BootstrapCluster()` | Bootstrap single-node cluster |
| `CaptureLeadershipToken()` | Fencing token capture |
| `ValidateLeadershipToken(token)` | Fencing token validation |

---

## internal/membership/membership.go

### `New() *Membership`
Creates with empty node map, no leader.

### `(m *Membership) Add(id, address)`
**Flow:**
1. Add node to map with `lastSeen=now`.
2. If no leader set → set this node as leader candidate.
3. Start heartbeat timer for node.

**Edge Cases:**
- Re-adding existing node → updates address and lastSeen.

### `(m *Membership) Remove(id)`
Removes node. If removed node was leader → triggers re-election.

### `(m *Membership) MarkDead(id)` / `MarkAlive(id)`
Marks node status. Dead nodes excluded from `AliveNodes()`.

### `(m *Membership) Heartbeat(id, address)`
Updates `lastSeen` time. Re-adds dead nodes if they heartbeat.

### `(m *Membership) AliveCount() int` / `AliveNodes() []string`
Returns count/list of alive nodes.

### `(m *Membership) LeaderID() string`
Returns lexicographically smallest alive node ID. **Single-node heuristic only** — does NOT provide consensus. Use `RaftCluster` for multi-node leader election.

### `(m *Membership) Snapshot() []NodeInfo`
Returns all node info (alive + dead).

### `(m *Membership) Lookup(id) *NodeInfo`
Returns info for specific node.

### `(m *Membership) SetLeaderLease(d time.Duration)`
Sets lease duration for leader heartbeat check.

### `(m *Membership) OnLeaseExpiry(cb func(leaderID string)) pkgmembership.CancelFunc`
Registers callback fired when leader lease expires. Returns cancel func.

---

## internal/membership/lease.go

### `(m *Membership) evictStale()`
Removes nodes that haven't heartbeated within lease duration.

### `(m *Membership) LeaderLastSeen() time.Time`
Returns last heartbeat time from current leader.

### `(m *Membership) StartLeaderLeaseChecker(ctx, interval)`
Background goroutine: if leader hasn't heartbeat within lease → triggers `OnLeaseExpiry` callback.

### `(m *Membership) StartEviction(ctx, interval)`
Background goroutine: calls `evictStale()` periodically.

---

## internal/cluster/node.go

### `NewClusterNode(nodeID, grpcAddr) *ClusterNode`
Creates node with embedded GRPCBus, empty peer map, and Gossiper.

### `(cn *ClusterNode) Gossiper() *Gossiper`
Returns the gossiper instance for registering callbacks.

### `(cn *ClusterNode) Start() error`
Starts GRPCBus, subscribes to `_flowrulz_gossip` topic, starts gossiper background loop.

### `(cn *ClusterNode) AddPeer(id, addr) error`
Connects to peer via gRPC. No-op if already connected.

### `(cn *ClusterNode) RemovePeer(id)`
Disconnects and removes peer.

### `(cn *ClusterNode) Publish(topic, key, body) error`
Publishes to local bus AND fans out to all connected peers concurrently.

### `(cn *ClusterNode) PublishToPeer(peerID, topic, body) error`
Publishes to a specific peer only.

### `(cn *ClusterNode) Subscribe(topic, handler)` / `Unsubscribe(topic)`
Registers/removes handler for topic on local bus.

### `(cn *ClusterNode) Stop()`
Stops gossiper, disconnects all peers, stops bus.

---

## internal/cluster/gossip.go

### `GossipState` / `GossipMessage`
```go
type GossipState struct {
    NodeID  string `json:"node_id"`
    Address string `json:"address"`
    Term    uint64 `json:"term"`
    Epoch   uint64 `json:"epoch"`
}
type GossipMessage struct {
    Type   string            `json:"type"`   // "push", "pull_req", "pull_resp"
    Sender string            `json:"sender"`
    States []GossipState     `json:"states,omitempty"`
    Epochs map[string]uint64 `json:"epochs,omitempty"`
}
```

### `NewGossiper(nodeID, grpcAddr, node) *Gossiper`
Fanout=2, push every 2s, sync every 10s.

### `(g *Gossiper) OnNodeJoin(fn)` — Callback when new node discovered.
### `(g *Gossiper) SetState(term)` — Updates local state with incremented epoch.
### `(g *Gossiper) Start(ctx)` — Runs push/sync loops.
### `(g *Gossiper) Stop()` — Stops background loops.
### `(g *Gossiper) HandleGossipMessage(ctx, topic, body)` — Processes push/pull_req/pull_resp messages.

**Gossip protocol:** Push sends full state to random peers. Pull sends epoch map, peer responds with missing states. Anti-entropy via epoch comparison.

---

## internal/cluster/transport.go

### `NewClusterProducer(topic, node) *ClusterProducer`
Publishes via `ClusterNode.Publish`. `Close()` is no-op.

### `NewClusterConsumer(topic, handler, node) *ClusterConsumer`
Subscribes via `ClusterNode.Subscribe`. Unsubscribes on `Stop()` or context cancel.

### `(c *ClusterConsumer) Topic() string` — Returns subscribed topic.

---

## internal/cluster/raft.go

### `RaftCluster` — Raft consensus for leader election (NoopFSM, no state replication).
See `supplementary.md` for full Raft API.
