# internal/transport, internal/cluster, internal/membership — Function Reference

---

## Table of Contents

- [internal/transport/ (Core Types)](#internaltransport-core-types)
- [internal/transport/memory/bus.go](#internaltransportmemorybusgo)
- [internal/transport/grpc/bus.go](#internaltransportgrpcbusgo)
- [internal/transport/grpc/client.go](#internaltransportgrpcclientgo)
- [internal/transport/kafka/producer.go](#internaltransportkafkaproducergo)
- [internal/transport/kafka/consumer.go](#internaltransportkafkaconsumergo)
- [internal/transport/kafka/registry.go](#internaltransportkafkaregistrygo)
- [internal/cluster/transport.go](#internalclustertransportgo)
- [internal/cluster/gossip.go](#internalclustergossipgo)
- [internal/cluster/pkgsupport.go](#internalclusterpkgsupportgo)
- [internal/membership/membership.go](#internalmembershipmembershipgo)
- [internal/membership/lease.go](#internalmembershiplleasego)

---

## internal/transport/ (Core Types)

### `NewProducer(topic string) *Producer`

**Signature:**
```go
func NewProducer(topic string) *Producer
```

**Flow:**
1. Allocates a `Producer` struct with the given `topic`.
2. Returns a pointer to the new producer.

**Edge Cases:**
- The returned producer is a no-op; `Send` logs and returns `nil`, `Close` is a no-op.
- Used as the default/fallback producer when no real backend is configured.

---

### `(*Producer) Send(ctx context.Context, key, value []byte) error`

**Signature:**
```go
func (p *Producer) Send(ctx context.Context, key, value []byte) error
```

**Flow:**
1. Logs a debug message with topic, key, and byte count.
2. Returns `nil` (no-op).

**Edge Cases:**
- Never returns an error; purely a placeholder implementation.

---

### `(*Producer) Close()`

**Signature:**
```go
func (p *Producer) Close()
```

**Flow:**
1. No-op. Does nothing.

**Edge Cases:**
- Safe to call multiple times.

---

### `RegisterMemory(factory *TransportFactory)`

**Signature:**
```go
func RegisterMemory(factory *TransportFactory)
```

**Flow:**
1. Calls `factory.RegisterProducer(KindMemory, ...)` with a closure that creates a `NewProducer(topic)`.
2. Calls `factory.RegisterConsumer(KindMemory, ...)` with a closure that creates a `NewConsumer(topic, handler)`.

**Edge Cases:**
- If `factory` is `nil`, this panics.
- Can be called multiple times; subsequent calls overwrite the previous factory for `KindMemory`.

---

### Core Types Summary

```go
type MessageHandler func(ctx context.Context, msg []byte) ([]byte, error)

type MessageConsumer interface {
    Topic() string
    Start(ctx context.Context)
    Stop()
}

type MessageProducer interface {
    Send(ctx context.Context, key, value []byte) error
    Close()
}

type TransportKind string
const (
    KindKafka   TransportKind = "kafka"
    KindCluster TransportKind = "cluster"
    KindMemory  TransportKind = "memory"
    KindNoop    TransportKind = "noop"
)

type TransportFactory struct { ... }

func NewTransportFactory(kind TransportKind) *TransportFactory
func (f *TransportFactory) RegisterProducer(kind TransportKind, factory ProducerFactory)
func (f *TransportFactory) RegisterConsumer(kind TransportKind, factory ConsumerFactory)
func (f *TransportFactory) SetKind(kind TransportKind)
func (f *TransportFactory) Kind() TransportKind
func (f *TransportFactory) NewProducer(topic string) MessageProducer
func (f *TransportFactory) NewConsumer(topic string, handler MessageHandler) MessageConsumer
```

`NewProducer` and `NewConsumer` on the factory fall back to noop implementations when no factory is registered for the active kind.

---

## internal/transport/memory/bus.go

### `New() *Bus`

**Signature:**
```go
func New() *Bus
```

**Flow:**
1. Allocates a `Bus` struct.
2. Initializes `topics` as an empty `map[string]map[string]transport.MessageHandler`.
3. Initializes `subs` as an empty `map[string]*subscription`.
4. Initializes `corrMap` as an empty `map[string]string`.
5. Returns a pointer to the new bus.

**Edge Cases:**
- The bus implements `FullEventBus`, `Publisher`, `Subscriber`, `Requester`, `Replier`, and `Broadcaster` interfaces.

---

### `(*Bus) Publish(ctx context.Context, topic string, msg *transport.Message) error`

**Signature:**
```go
func (b *Bus) Publish(ctx context.Context, topic string, msg *transport.Message) error
```

**Flow:**
1. Check if bus is closed; if so, return `"bus closed"` error.
2. If `msg.ID` is empty, generate a monotonically increasing ID (`msg-N`).
3. If `msg.CreatedAt` is zero, set to `time.Now()`.
4. Set `msg.Topic` to the provided topic.
5. Set `msg.Type` to `TypePublish`.
6. RLock `b.mu` and look up handlers for the topic.
7. If no handlers exist, return `nil` (message silently dropped).
8. Call `b.dispatch(ctx, msg, handlers)` to fan out to all handlers concurrently.
9. Return `nil`.

**Edge Cases:**
- Publishing to a topic with no subscribers is a silent no-op (not an error).
- Handlers are called in separate goroutines; errors from handlers are not propagated back.
- The `msg` is mutated in-place (ID, CreatedAt, Topic, Type fields are set).

---

### `(*Bus) PublishToPartition(ctx context.Context, topic, key string, msg *transport.Message) error`

**Signature:**
```go
func (b *Bus) PublishToPartition(ctx context.Context, topic, key string, msg *transport.Message) error
```

**Flow:**
1. Set `msg.PartitionKey` to `key`.
2. Delegate to `b.Publish(ctx, topic, msg)`.

**Edge Cases:**
- Partition key is stored on the message but has no effect in the memory backend (no partitioning logic).

---

### `(*Bus) Subscribe(ctx context.Context, topic string, handler transport.MessageHandler) (*transport.Subscription, error)`

**Signature:**
```go
func (b *Bus) Subscribe(ctx context.Context, topic string, handler transport.MessageHandler) (*transport.Subscription, error)
```

**Flow:**
1. Lock `b.mu`.
2. Generate subscription ID as `sub-<topic>-<count>`.
3. If `b.topics[topic]` is nil, create a new handler map for that topic.
4. Store `handler` in `b.topics[topic][id]`.
5. Create an internal `subscription` record with a `done` channel.
6. Store in `b.subs[id]`.
7. Unlock and return `&transport.Subscription{ID: id, Topic: topic}`.

**Edge Cases:**
- Multiple handlers can subscribe to the same topic; they all receive messages.
- Subscription IDs are deterministic based on the current handler count (not globally unique across unsubscribes).

---

### `(*Bus) Unsubscribe(ctx context.Context, sub *transport.Subscription) error`

**Signature:**
```go
func (b *Bus) Unsubscribe(ctx context.Context, sub *transport.Subscription) error
```

**Flow:**
1. Lock `b.mu`.
2. Iterate all topics to find the handler matching `sub.ID`.
3. Delete the handler from the topic's handler map.
4. Close the subscription's `done` channel.
5. Delete from `b.subs`.
6. If the topic's handler map is now empty, delete the topic entry entirely.
7. Unlock.

**Edge Cases:**
- If the subscription ID doesn't exist, this is a silent no-op (returns `nil`).
- The `done` channel is closed exactly once per valid subscription.
- Concurrent calls with the same subscription are safe (second call is a no-op).

---

### `(*Bus) Request(ctx context.Context, topic string, msg *transport.Message, timeout time.Duration) (*transport.Message, error)`

**Signature:**
```go
func (b *Bus) Request(ctx context.Context, topic string, msg *transport.Message, timeout time.Duration) (*transport.Message, error)
```

**Flow:**
1. Check if bus is closed; if so, return `"bus closed"` error.
2. Generate a correlation ID (`msg-N`) and set on the message.
3. Set `msg.Type` to `TypeRequest`.
4. Store the correlation ID → topic mapping in `b.corrMap`.
5. Create a buffered reply channel (`chan *transport.Message`, capacity 1).
6. Subscribe to `<topic>.reply` with a handler that filters by correlation ID and sends to the reply channel.
7. Publish the original message to the topic.
8. Create a context with the specified timeout.
9. Block waiting for a reply on the reply channel or context cancellation.
10. On reply, unsubscribe from the reply topic and return the reply.
11. On timeout, unsubscribe and return the context error.

**Edge Cases:**
- **Timeout:** Returns `context.DeadlineExceeded` error. The reply subscription is cleaned up via `defer`.
- **No reply:** Always times out; the reply handler is filtered by correlation ID so stale replies are ignored.
- **Bus closed:** Returns error before setting up the request.
- **Publish failure:** Returns the publish error; subscription is still cleaned up.
- The reply channel has capacity 1; if a duplicate reply arrives, it's silently dropped.

---

### `(*Bus) Reply(ctx context.Context, correlationID string, msg *transport.Message) error`

**Signature:**
```go
func (b *Bus) Reply(ctx context.Context, correlationID string, msg *transport.Message) error
```

**Flow:**
1. Set `msg.CorrelationID` to the provided correlation ID.
2. Set `msg.Type` to `TypeReply`.
3. RLock `b.corrMu` and look up the original topic from `b.corrMap`.
4. If the correlation ID is not found, return `"unknown correlation id"` error.
5. Publish the reply message to `<topic>.reply`.

**Edge Cases:**
- **Unknown correlation ID:** Returns error. This happens if the original request timed out and was cleaned up, or if the correlation ID is invalid.
- The correlation map is append-only (entries are never cleaned up), which is a potential memory leak under sustained request/reply workloads.

---

### `(*Bus) Broadcast(ctx context.Context, topic string, msg *transport.Message) error`

**Signature:**
```go
func (b *Bus) Broadcast(ctx context.Context, topic string, msg *transport.Message) error
```

**Flow:**
1. Set `msg.Type` to `TypeBroadcast`.
2. Delegate to `b.Publish(ctx, topic, msg)`.

**Edge Cases:**
- Identical to `Publish` except the message type is `TypeBroadcast`. This distinction is useful for message inspection/filtering.

---

### `(*Bus) TopicStats() map[string]int`

**Signature:**
```go
func (b *Bus) TopicStats() map[string]int
```

**Flow:**
1. RLock `b.mu`.
2. Iterate all topics, counting handlers per topic.
3. Unlock and return the stats map.

**Edge Cases:**
- Returns an empty map if no topics have subscribers.
- The returned map is a snapshot; it may be stale immediately after return.

---

### `(*Bus) Close() error`

**Signature:**
```go
func (b *Bus) Close() error
```

**Flow:**
1. Set `b.closed` to `true` (atomic). All subsequent `Publish`/`Request` calls fail.
2. Wait for all in-flight handler goroutines to complete (`b.wg.Wait()`).
3. Lock `b.mu` and reset `topics` and `subs` maps to empty.
4. Return `nil`.

**Edge Cases:**
- Blocks until all in-flight `dispatch` goroutines finish. If a handler is slow, `Close` blocks.
- After `Close`, the bus is unusable but safe to call `Close` again (idempotent).
- Does not close the `corrMap`; correlation IDs from prior requests remain but are unreachable.

---

## internal/transport/grpc/bus.go

### `NewGRPCBus(addr string) *GRPCBus`

**Signature:**
```go
func NewGRPCBus(addr string) *GRPCBus
```

**Flow:**
1. Allocates a `GRPCBus` with the given address.
2. Initializes empty maps for `subscribers` and `handlers`.
3. Creates a `stopCh` channel for shutdown signaling.
4. Returns the bus.

**Edge Cases:**
- The server is not started until `Start()` is called.

---

### `NewGRPCBusWithTLS(addr, certFile, keyFile string) *GRPCBus`

**Signature:**
```go
func NewGRPCBusWithTLS(addr, certFile, keyFile string) *GRPCBus
```

**Flow:**
1. Same as `NewGRPCBus`, but stores TLS cert/key file paths.
2. TLS is configured when `Start()` is called.

**Edge Cases:**
- If cert/key files don't exist or are invalid, `Start()` returns an error.

---

### `(*GRPCBus) AddTopicHandler(topic string, handler TopicHandler)`

**Signature:**
```go
func (b *GRPCBus) AddTopicHandler(topic string, handler TopicHandler)
```

**Flow:**
1. Lock `b.mu`.
2. Store `handler` in `b.handlers[topic]`.
3. Unlock.

**Edge Cases:**
- Only one `TopicHandler` per topic is supported. Calling again overwrites the previous handler.
- Topic handlers are invoked in addition to streaming subscribers.

---

### `(*GRPCBus) RemoveTopicHandler(topic string)`

**Signature:**
```go
func (b *GRPCBus) RemoveTopicHandler(topic string)
```

**Flow:**
1. Lock `b.mu`.
2. Delete `b.handlers[topic]`.
3. Unlock.

**Edge Cases:**
- No-op if the topic doesn't have a handler.

---

### `(*GRPCBus) Start() error`

**Signature:**
```go
func (b *GRPCBus) Start() error
```

**Flow:**
1. Lock `b.mu`. If already started, return `nil` (idempotent).
2. Open a TCP listener on `b.addr`.
3. If TLS cert/key files are configured, load the X.509 key pair and configure TLS credentials with `MinVersion: TLS 1.2`.
4. Create a new `grpc.Server` with the configured options.
5. Register the `EventBusServer` implementation.
6. Set `b.started = true`.
7. Launch `b.server.Serve(lis)` in a goroutine.
8. Unlock and return `nil`.

**Edge Cases:**
- **Address in use:** Returns `"grpc listen"` error.
- **Invalid TLS cert:** Returns `"grpc TLS cert"` error.
- **Double start:** Returns `nil` silently (guarded by `b.started`).
- The `Serve` goroutine logs errors but doesn't propagate them.

---

### `(*GRPCBus) Publish(ctx context.Context, req *PublishRequest) (*PublishResponse, error)`

**Signature:**
```go
func (b *GRPCBus) Publish(ctx context.Context, req *PublishRequest) (*PublishResponse, error)
```

**Flow:**
1. RLock `b.mu`.
2. Look up streaming subscribers for `req.Topic`. Send the message to each subscriber's channel (non-blocking; dropped if full).
3. Look up the `TopicHandler` for the topic. If present, invoke it.
4. RUnlock.
5. Return an empty `PublishResponse`.

**Edge Cases:**
- If no subscribers or handler exist, the message is silently dropped.
- Subscriber channel sends are non-blocking (use `select/default`); slow subscribers miss messages.
- The `TopicHandler` is called synchronously within the RPC handler; if it blocks, the RPC is delayed.
- Always returns `nil` error (no delivery guarantees).

---

### `(*GRPCBus) Request(ctx context.Context, req *RequestRequest) (*RequestResponse, error)`

**Signature:**
```go
func (b *GRPCBus) Request(ctx context.Context, req *RequestRequest) (*RequestResponse, error)
```

**Flow:**
1. Create a buffered reply channel (capacity 1).
2. Derive the reply topic as `__reply_<correlationId>`.
3. Generate a unique subscription ID.
4. Lock `b.mu`:
   - Send the request message to all subscribers of `req.Topic` (non-blocking).
   - Register the reply channel under the reply topic.
5. Unlock.
6. Set timeout (default 30s if `TimeoutMs <= 0`).
7. Start a timer and block waiting for a reply or timer expiry.
8. On reply, return `&RequestResponse{Msg: resp}`.
9. On timeout, return `"request timeout"` error.
10. Deferred: clean up the reply subscription from `b.subscribers`.

**Edge Cases:**
- **Timeout:** Returns `"request timeout"` error. The reply subscription is cleaned up.
- **No subscribers:** The request message is sent to an empty map (no-op). No reply will come, so this always times out.
- **Default timeout:** If `TimeoutMs` is 0 or negative, defaults to 30 seconds.
- **Stale replies:** Only messages matching the correlation ID reach the reply channel.

---

### `(*GRPCBus) Reply(ctx context.Context, req *ReplyRequest) (*ReplyResponse, error)`

**Signature:**
```go
func (b *GRPCBus) Reply(ctx context.Context, req *ReplyRequest) (*ReplyResponse, error)
```

**Flow:**
1. Call `b.deliverToTopic(ctx, "__reply_<correlationId>", req.Msg)`.
2. Return an empty `ReplyResponse`.

**Edge Cases:**
- If no pending request is waiting for this correlation ID, the message is silently dropped (no subscriber on the reply topic).

---

### `(*GRPCBus) Broadcast(ctx context.Context, req *BroadcastRequest) (*BroadcastResponse, error)`

**Signature:**
```go
func (b *GRPCBus) Broadcast(ctx context.Context, req *BroadcastRequest) (*BroadcastResponse, error)
```

**Flow:**
1. RLock `b.mu`.
2. Iterate all subscriber topics. For each topic matching `req.Topic`:
   - Invoke the `TopicHandler` if registered.
   - Send the message to all streaming subscribers (non-blocking).
3. RUnlock.
4. Return an empty `BroadcastResponse`.

**Edge Cases:**
- Iterates all subscriber maps even though it filters by topic; this is O(total_topics) not O(1).
- Always returns `nil` error.

---

### `(*GRPCBus) Subscribe(req *SubscribeRequest, stream EventBus_SubscribeServer) error`

**Signature:**
```go
func (b *GRPCBus) Subscribe(req *SubscribeRequest, stream EventBus_SubscribeServer) error
```

**Flow:**
1. Lock `b.mu`. Create a buffered channel (capacity 100) for the subscriber.
2. Store the channel under `b.subscribers[req.Topic][req.SubId]`.
3. Unlock.
4. Enter a blocking loop:
   - Read from the channel; send the message on the gRPC stream.
   - If `stream.Context().Done()`, return `nil` (client disconnected).
   - If `stream.Send` fails (e.g., client gone), return the error.
5. Deferred: clean up the subscriber entry from `b.subscribers`.

**Edge Cases:**
- **Client disconnect:** Returns `nil` when `stream.Context()` is cancelled.
- **Slow client:** The channel has capacity 100; if full, new messages are dropped (non-blocking send from `Publish`/`deliverToTopic`).
- **Multiple subscribes:** Each call creates a new channel; old entries with the same `SubId` are overwritten.

---

### `(*GRPCBus) Stop()`

**Signature:**
```go
func (b *GRPCBus) Stop()
```

**Flow:**
1. Lock `b.mu`. Extract and nil-out `b.server`.
2. Unlock.
3. If server was non-nil, call `srv.GracefulStop()` (waits for in-flight RPCs).
4. Close `b.stopCh` exactly once (guarded by `sync.Once`).

**Edge Cases:**
- **Double stop:** Safe; `GracefulStop` is called at most once due to nil check and `sync.Once`.
- **GracefulStop blocks:** If RPCs are in-flight and take long, this blocks.

---

## internal/transport/grpc/client.go

### `NewGRPCClient(addr string) *GRPCClient`

**Signature:**
```go
func NewGRPCClient(addr string) *GRPCClient
```

**Flow:**
1. Allocates a `GRPCClient` with the address and an empty subscription map.
2. Returns the client.

**Edge Cases:**
- Connection is not established until `Connect()` or `ConnectWithTLS()` is called.

---

### `(*GRPCClient) Connect() error`

**Signature:**
```go
func (c *GRPCClient) Connect() error
```

**Flow:**
1. Call `c.connectWithCredentials(insecure.NewCredentials())`.
2. Create a `grpc.ClientConn` with insecure transport credentials.
3. Create an `EventBusClient` from the connection.
4. Return `nil` on success.

**Edge Cases:**
- **Connection failure:** Returns `"grpc connect"` wrapped error.
- The connection is non-blocking (gRPC uses lazy connection).

---

### `(*GRPCClient) ConnectWithTLS(certFile, keyFile, caFile string) error`

**Signature:**
```go
func (c *GRPCClient) ConnectWithTLS(certFile, keyFile, caFile string) error
```

**Flow:**
1. Log a warning that TLS is not fully implemented.
2. Fall back to `c.Connect()` (insecure).

**Edge Cases:**
- **NOT TLS-safe.** This method currently falls back to insecure credentials. Do not use in production for TLS-required connections.
- The `caFile` parameter is accepted but ignored.

---

### `(*GRPCClient) Publish(topic string, msg *transport.Message) error`

**Signature:**
```go
func (c *GRPCClient) Publish(topic string, msg *transport.Message) error
```

**Flow:**
1. Convert `msg` to protobuf `BusMessage` via `toProtoMessage`.
2. Call `c.client.Publish` with a `PublishRequest` containing the topic and message.
3. Return any error.

**Edge Cases:**
- Uses `context.Background()` (no cancellation).
- Returns gRPC transport errors directly.

---

### `(*GRPCClient) Subscribe(topic string, handler transport.Handler) *transport.Subscription`

**Signature:**
```go
func (c *GRPCClient) Subscribe(topic string, handler transport.Handler) *transport.Subscription
```

**Flow:**
1. Generate a subscription ID (`sub-<nanotime>`).
2. Create a cancellable context for the stream.
3. Call `c.client.Subscribe` to open a server-streaming RPC.
4. If the stream fails, log the error and return a subscription with the generated ID (no stream).
5. Store the `streamCancel` function in `c.subs`.
6. Launch a goroutine that reads from the stream and calls `handler` for each message.
7. Return the subscription.

**Edge Cases:**
- **Stream error:** Returns a subscription with no active stream. The caller won't receive messages.
- **Handler panic:** Panics in the handler will crash the goroutine (no recovery).
- **Connection closed:** The goroutine exits silently when `stream.Recv()` returns an error.

---

### `(*GRPCClient) Unsubscribe(subID string)`

**Signature:**
```go
func (c *GRPCClient) Unsubscribe(subID string)
```

**Flow:**
1. Lock `c.subsMu`. Look up and delete the cancel function for `subID`.
2. Unlock.
3. If found, call `cancel()` to close the stream context.
4. Sleep 50ms to allow the stream goroutine to exit.

**Edge Cases:**
- **Unknown subID:** No-op.
- **Race with Subscribe:** The 50ms sleep is a best-effort wait; not guaranteed.

---

### `(*GRPCClient) Request(topic string, msg *transport.Message, timeout time.Duration) (*transport.Message, error)`

**Signature:**
```go
func (c *GRPCClient) Request(topic string, msg *transport.Message, timeout time.Duration) (*transport.Message, error)
```

**Flow:**
1. Convert `msg` to protobuf format.
2. Call `c.client.Request` with `TimeoutMs` converted from the duration.
3. Convert the response back to `transport.Message`.
4. Return any error.

**Edge Cases:**
- Uses `context.Background()` (no cancellation).
- Timeout is server-side (the server enforces it); the client also blocks for the same duration.
- If the server times out, the client receives a gRPC error.

---

### `(*GRPCClient) Reply(topic string, reqID string, msg *transport.Message) error`

**Signature:**
```go
func (c *GRPCClient) Reply(topic string, reqID string, msg *transport.Message) error
```

**Flow:**
1. Convert `msg` to protobuf format.
2. Call `c.client.Reply` with the topic, correlation ID, and message.
3. Return any error.

**Edge Cases:**
- The `topic` parameter is included in the `ReplyRequest` but the server routes by correlation ID.
- Uses `context.Background()`.

---

### `(*GRPCClient) Broadcast(topic string, msg *transport.Message) error`

**Signature:**
```go
func (c *GRPCClient) Broadcast(topic string, msg *transport.Message) error
```

**Flow:**
1. Convert `msg` to protobuf format.
2. Call `c.client.Broadcast` with the topic and message.
3. Return any error.

**Edge Cases:**
- Uses `context.Background()`.

---

### `(*GRPCClient) Close()`

**Signature:**
```go
func (c *GRPCClient) Close()
```

**Flow:**
1. If `c.conn` is non-nil, close the gRPC connection.

**Edge Cases:**
- Safe to call multiple times; the nil check prevents double-close.
- Does not cancel active subscriptions (callers should `Unsubscribe` first).

---

## internal/transport/kafka/producer.go

### `NewProducer(topic string, cfg Config) *Producer`

**Signature:**
```go
func NewProducer(topic string, cfg Config) *Producer
```

**Flow:**
1. Store the topic and config. The Sarama producer is `nil` (lazy-initialized).
2. Return the producer.

**Edge Cases:**
- The Sarama producer is created on the first `Send` call, not at construction time.

---

### `(*Producer) Send(ctx context.Context, key, value []byte) error`

**Signature:**
```go
func (kp *Producer) Send(ctx context.Context, key, value []byte) error
```

**Flow:**
1. Lock `kp.mu`.
2. If `kp.closed` is true, unlock and return `"kafka producer <topic>: closed"` error.
3. If `kp.producer` is nil, call `kp.initProducer()`. If init fails, unlock and return the error.
4. Unlock.
5. Create a `sarama.ProducerMessage` with topic, key, and value.
6. Call `kp.producer.SendMessage(msg)`.
7. Log the partition and offset on success.
8. Return any error.

**Edge Cases:**
- **Closed producer:** Returns error immediately.
- **Init failure (no brokers):** Enters log-only mode; `initProducer` returns `nil` and the producer is not created. `SendMessage` will then panic because `kp.producer` is nil.
- **Init failure (connection error):** Returns the wrapped error.
- **Idempotent mode:** If `cfg.Idempotent` is true, forces `acks=all` and sets `MaxOpenRequests=1`.
- **Produce failure:** Returns `"kafka produce <topic>"` wrapped error.
- The mutex is released before calling `SendMessage`, so concurrent sends are possible.

---

### `(*Producer) Close()`

**Signature:**
```go
func (kp *Producer) Close()
```

**Flow:**
1. Lock `kp.mu`.
2. If already closed, unlock and return.
3. Set `kp.closed = true`.
4. If the Sarama producer is non-nil, call `kp.producer.Close()`.
5. Unlock.

**Edge Cases:**
- **Double close:** Safe; the `kp.closed` guard prevents it.
- **No-op producer:** If `kp.producer` is nil (log-only mode), nothing is closed.

---

## internal/transport/kafka/consumer.go

### `NewConsumer(topic string, handler transport.MessageHandler, cfg Config) *Consumer`

**Signature:**
```go
func NewConsumer(topic string, handler transport.MessageHandler, cfg Config) *Consumer
```

**Flow:**
1. If `cfg.ConsumerCh` is nil, create a buffered channel (capacity 1000).
2. Store the topic, handler, config, message channel, and stop channel.
3. Set `manualCommit` to `true` if brokers are configured (real Kafka mode).
4. Return the consumer.

**Edge Cases:**
- The consumer implements `sarama.ConsumerGroupHandler` interface (`Setup`, `Cleanup`, `ConsumeClaim`).
- `manualCommit` controls whether offsets are committed after each message (for at-least-once delivery).

---

### `(*Consumer) Start(ctx context.Context)`

**Signature:**
```go
func (kc *Consumer) Start(ctx context.Context)
```

**Flow:**
1. Lock `kc.mu`. If already started, unlock and return.
2. Set `kc.started = true`. Unlock.
3. Log startup details (topic, brokers, group, manual_commit).
4. **If no brokers configured (in-memory mode):** Call `kc.runChannel(ctx)` which blocks reading from `kc.msgCh`.
5. **If brokers configured (Kafka mode):**
   a. Create a Sarama config with `RoundRobin` rebalance strategy, `OffsetNewest`, 500ms max processing time.
   b. If `manualCommit` is true, disable auto-commit.
   c. Create a `sarama.ConsumerGroup`.
   d. If group creation fails, fall back to `runChannel`.
   e. Launch a goroutine that calls `client.Consume` in a loop. On error, wait 1 second and retry.
   f. The `Consume` loop continues until `stopCh` is closed.

**Edge Cases:**
- **Double start:** Safe; returns immediately.
- **Consumer group failure:** Falls back to in-memory channel mode (testing-friendly).
- **Context cancellation:** The `Consume` loop checks `stopCh` and returns.
- **Consume error:** Retries after 1 second delay.
- `runChannel` blocks until `stopCh` or `ctx.Done()`. This means `Start` does not return in channel mode; it blocks the calling goroutine.

---

### `(*Consumer) Stop()`

**Signature:**
```go
func (kc *Consumer) Stop()
```

**Flow:**
1. Lock `kc.mu`. If not started, unlock and return.
2. Close `kc.stopCh` to signal all goroutines to stop.
3. If the Sarama consumer group client is non-nil, close it.
4. Wait for the consume goroutine to exit (`kc.wg.Wait()`).
5. Set `kc.started = false`. Unlock.

**Edge Cases:**
- **Double stop:** Safe; the `started` guard prevents it.
- **Channel mode:** Closing `stopCh` causes `runChannel` to return.
- **Blocks:** Waits for the Sarama consumer goroutine to exit; can block if `client.Close` is slow.

---

### `(*Consumer) Inject(msg []byte)`

**Signature:**
```go
func (kc *Consumer) Inject(msg []byte)
```

**Flow:**
1. Try to send `msg` to `kc.msgCh` via non-blocking select.
2. If the channel is full, log a warning and drop the message.

**Edge Cases:**
- **Buffer full:** Messages are silently dropped with a log warning.
- **Testing use only:** This is the primary way to inject messages in the in-memory channel mode.

---

### `(*Consumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error`

**Signature:**
```go
func (kc *Consumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error
```

**Flow:**
1. Iterate over `claim.Messages()` channel.
2. For each message, call `kc.handler(sess.Context(), msg.Value)`.
3. If `manualCommit` is true: mark the message and commit the offset.
4. If `manualCommit` is false: mark the message only (auto-commit handles it).
5. Return `nil` when the channel closes (claim is rebalanced or partition revoked).

**Edge Cases:**
- **Handler error:** Logged but not retried. The message is still marked/committed.
- **Manual commit per message:** This is expensive for high-throughput; consider batching.
- **Partition rebalance:** The channel closes and the method returns; Sarama handles reassignment.

---

## internal/transport/kafka/registry.go

### `RegisterKafka(factory *transport.TransportFactory, cfg RegistrationConfig)`

**Signature:**
```go
func RegisterKafka(factory *transport.TransportFactory, cfg RegistrationConfig)
```

**Flow:**
1. If `cfg.Brokers` is empty, return immediately (no-op).
2. Convert `RegistrationConfig` to internal `Config` (parsing `AcksLevel` from string).
3. Register a producer factory for `KindKafka` that creates a `kafka.NewProducer(topic, kafkaCfg)`.
4. Register a consumer factory for `KindKafka` that creates a `kafka.NewConsumer(topic, handler, kafkaCfg)`.
5. Log the registration.

**Edge Cases:**
- **No brokers:** Returns immediately; no factory is registered. `TransportFactory` will fall back to noop.
- **Idempotent flag:** Passed through to the config; if `AcksAll` is not set, `Send` will override it.

---

### Kafka Config Types

```go
type AcksLevel int
const (
    AcksNone AcksLevel = 0   // sarama.NoResponse
    AcksOne  AcksLevel = 1   // sarama.WaitForLocal (default)
    AcksAll  AcksLevel = -1  // sarama.WaitForAll
)

func AcksLevelFromString(s string) AcksLevel
// "0"     → AcksNone
// "all"   → AcksAll
// "-1"    → AcksAll
// default → AcksOne

type Config struct {
    Brokers    []string
    GroupID    string
    ConsumerCh chan []byte   // nil → auto-created buffer of 1000
    Acks       AcksLevel
    Idempotent bool
}
```

---

## internal/cluster/transport.go

### `NewClusterProducer(topic string, node *ClusterNode) *ClusterProducer`

**Signature:**
```go
func NewClusterProducer(topic string, node *ClusterNode) *ClusterProducer
```

**Flow:**
1. Allocate a `ClusterProducer` with the topic and node reference.
2. Return the producer.

**Edge Cases:**
- The producer does not hold a connection; it delegates to the `ClusterNode` on each `Send`.

---

### `(*ClusterProducer) Send(ctx context.Context, key, value []byte) error`

**Signature:**
```go
func (p *ClusterProducer) Send(ctx context.Context, key, value []byte) error
```

**Flow:**
1. Call `p.node.Publish(p.topic, string(key), value)`.
2. Return the error.

**Edge Cases:**
- The `ClusterNode.Publish` method fans out to all connected peers and the local bus.
- If the node is not started, the behavior depends on the `ClusterNode` implementation.

---

### `(*ClusterProducer) Close()`

**Signature:**
```go
func (p *ClusterProducer) Close()
```

**Flow:**
1. No-op.

**Edge Cases:**
- Safe to call multiple times.

---

### `NewClusterConsumer(topic string, handler transport.MessageHandler, node *ClusterNode) *ClusterConsumer`

**Signature:**
```go
func NewClusterConsumer(topic string, handler transport.MessageHandler, node *ClusterNode) *ClusterConsumer
```

**Flow:**
1. Allocate a `ClusterConsumer` with the topic, handler, and node reference.
2. Return the consumer.

**Edge Cases:**
- The consumer uses an `atomic.Bool` to track started state.

---

### `(*ClusterConsumer) Topic() string`

**Signature:**
```go
func (c *ClusterConsumer) Topic() string
```

**Flow:**
1. Return `c.topic`.

---

### `(*ClusterConsumer) Start(ctx context.Context)`

**Signature:**
```go
func (c *ClusterConsumer) Start(ctx context.Context)
```

**Flow:**
1. If already started (atomic load), return immediately.
2. Set `started` to `true`.
3. Call `c.node.Subscribe(c.topic, handler)` with a wrapper that calls `c.handler(ctx, body)`.
4. Launch a goroutine that waits for `ctx.Done()` and then calls `c.node.Unsubscribe(c.topic)` and sets `started = false`.

**Edge Cases:**
- **Double start:** Safe; returns immediately.
- **Context cancellation:** The goroutine auto-unsubscribes when the context is cancelled.
- **Handler errors:** Logged but not propagated.
- The `Subscribe` call registers on the `ClusterNode`'s local bus; messages are delivered when the node's `Publish` is called.

---

### `(*ClusterConsumer) Stop()`

**Signature:**
```go
func (c *ClusterConsumer) Stop()
```

**Flow:**
1. Call `c.node.Unsubscribe(c.topic)`.
2. Set `started` to `false`.

**Edge Cases:**
- **Double stop:** Safe; `Unsubscribe` is idempotent.
- Does NOT cancel the context goroutine; it relies on `ctx.Done()` for that. If context is still alive, the goroutine remains but `Unsubscribe` is a no-op.

---

## internal/cluster/gossip.go

### Types

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

---

### `NewGossiper(nodeID, grpcAddr string, node *ClusterNode) *Gossiper`

**Signature:**
```go
func NewGossiper(nodeID, grpcAddr string, node *ClusterNode) *Gossiper
```

**Flow:**
1. Create a `Gossiper` with default settings:
   - `fanout`: 2 (push to 2 random peers).
   - `pushInterval`: 2 seconds.
   - `syncInterval`: 10 seconds.
2. Initialize `myState` with the node's ID and address.
3. Create the `stopCh` channel.
4. Return the gossiper.

**Edge Cases:**
- Background goroutines are NOT started until `Start(ctx)` is called.
- `onNodeJoin` callback is nil by default; must be set via `OnNodeJoin`.

---

### `(*Gossiper) OnNodeJoin(fn func(nodeID, address string))`

**Signature:**
```go
func (g *Gossiper) OnNodeJoin(fn func(nodeID, address string))
```

**Flow:**
1. Store the callback in `g.onNodeJoin`.

**Edge Cases:**
- Only the most recent callback is stored; previous callbacks are overwritten.

---

### `(*Gossiper) SetState(term uint64)`

**Signature:**
```go
func (g *Gossiper) SetState(term uint64)
```

**Flow:**
1. Lock `g.statesMu`.
2. Increment `g.myState.Epoch` by 1.
3. Set `g.myState.Term` to the provided term.
4. Unlock.

**Edge Cases:**
- Epoch is monotonically increasing; this ensures the new state is always "newer" than any previous state.
- Does NOT trigger a push; relies on the next push interval.

---

### `(*Gossiper) UpdateState(nodeID string, state GossipState)`

**Signature:**
```go
func (g *Gossiper) UpdateState(nodeID string, state GossipState)
```

**Flow:**
1. Lock `g.statesMu`.
2. Look up existing state for `nodeID`.
3. If the node is new OR the incoming state has a higher epoch (or same epoch but higher term), update the state.
4. If the node is new and `onNodeJoin` is set, invoke the callback (after unlocking).
5. Unlock.

**Edge Cases:**
- **Stale state:** If the incoming state has a lower or equal epoch/term, it's silently ignored.
- **New node detection:** The `onNodeJoin` callback fires only on first discovery, not on re-discovery.
- **Callback under lock:** The callback is invoked AFTER unlocking to prevent deadlocks.

---

### `(*Gossiper) GetState(nodeID string) (GossipState, bool)`

**Signature:**
```go
func (g *Gossiper) GetState(nodeID string) (GossipState, bool)
```

**Flow:**
1. RLock `g.statesMu`.
2. Look up `nodeID` in `g.states`.
3. RUnlock and return the state and whether it was found.

**Edge Cases:**
- Returns a copy of the state (value type, not pointer).

---

### `(*Gossiper) AllStates() []GossipState`

**Signature:**
```go
func (g *Gossiper) AllStates() []GossipState
```

**Flow:**
1. RLock `g.statesMu`.
2. Allocate a slice with capacity `len(g.states) + 1`.
3. Append `g.myState` first.
4. Append all other known states.
5. RUnlock and return the slice.

**Edge Cases:**
- The local node's state is always included.
- Returns a snapshot; may be stale immediately after return.
- Order is non-deterministic (map iteration).

---

### `(*Gossiper) GetMyState() GossipState`

**Signature:**
```go
func (g *Gossiper) GetMyState() GossipState
```

**Flow:**
1. RLock `g.statesMu`.
2. Return `g.myState`.
3. RUnlock.

---

### `(*Gossiper) Start(ctx context.Context)`

**Signature:**
```go
func (g *Gossiper) Start(ctx context.Context)
```

**Flow:**
1. Create a push ticker at `g.pushInterval` (2s).
2. Create a sync ticker at `g.syncInterval` (10s).
3. Enter a select loop:
   - On push tick: call `g.doPush()`.
   - On sync tick: call `g.doSync()`.
   - On `ctx.Done()`: return.
   - On `g.stopCh`: return.

**Edge Cases:**
- **Blocks the calling goroutine.** Must be run in a goroutine or after other setup is complete.
- **Idempotent start:** If called multiple times, multiple loops run concurrently (no guard).
- `doPush` sends full state to 2 random peers.
- `doSync` sends epoch map to 1 random peer (anti-entropy pull).

---

### `(*Gossiper) Stop()`

**Signature:**
```go
func (g *Gossiper) Stop()
```

**Flow:**
1. Non-blocking check if `stopCh` is already closed.
2. If not, close `stopCh`.

**Edge Cases:**
- Safe to call multiple times (uses `select/default` pattern).
- Does not wait for goroutines to exit; relies on context/stopCh to signal.

---

### `(*Gossiper) HandleGossipMessage(ctx context.Context, topic string, body []byte)`

**Signature:**
```go
func (g *Gossiper) HandleGossipMessage(ctx context.Context, topic string, body []byte)
```

**Flow:**
1. Unmarshal `body` into a `GossipMessage`.
2. If unmarshal fails, log the error and return.
3. Switch on `msg.Type`:
   - **`"push"`:** Iterate `msg.States`. For each state (excluding self), call `g.UpdateState`.
   - **`"pull_req"`:** Compare epoch map from the sender. Collect states where the sender's epoch is older. Send a `pull_resp` back to the sender.
   - **`"pull_resp"`:** Iterate `msg.States` (excluding self) and call `g.UpdateState` for each.

**Edge Cases:**
- **Unknown message type:** Silently ignored (no default case in switch).
- **Self-state excluded:** The node never updates its own state from incoming gossip.
- **Pull request efficiency:** Only sends states that are newer than what the sender knows (epoch comparison).
- **Marshall failure:** Logged but not retried.

---

### `(*Gossiper) doPush()` (private)

**Flow:**
1. Select `g.fanout` (2) random peers from the node's peer list.
2. If no peers, return.
3. Collect all states (self + known) into a `GossipMessage` with type `"push"`.
4. Marshal to JSON.
5. For each peer, publish the gossip message to `_flowrulz_gossip` topic.

**Edge Cases:**
- If the peer list is empty, this is a no-op.
- JSON marshal failure is logged and skipped.

---

### `(*Gossiper) doSync()` (private)

**Flow:**
1. Select 1 random peer.
2. If no peers, return.
3. Build an epoch map of all known states.
4. Send a `pull_req` message to the selected peer.

**Edge Cases:**
- Only one peer is contacted per sync cycle (anti-entropy).
- The peer compares epochs and responds with missing states.

---

## internal/cluster/pkgsupport.go

### `NewClusterMember(rc *RaftCluster) *ClusterMember`

**Signature:**
```go
func NewClusterMember(rc *RaftCluster) *ClusterMember
```

**Flow:**
1. Wrap the `RaftCluster` in a `ClusterMember`.
2. Return the adapter.

**Edge Cases:**
- This is an adapter that implements `pkgcluster.ClusterMember` interface by delegating to `RaftCluster` methods.

---

### `(*ClusterMember) ID() pkgcluster.MemberID`

**Signature:**
```go
func (cm *ClusterMember) ID() pkgcluster.MemberID
```

**Flow:**
1. Return the node ID as a `MemberID`.

---

### `(*ClusterMember) Addr() string`

**Signature:**
```go
func (cm *ClusterMember) Addr() string
```

**Flow:**
1. Return the Raft bind address.

---

### `(*ClusterMember) Start(ctx context.Context) error`

**Signature:**
```go
func (cm *ClusterMember) Start(ctx context.Context) error
```

**Flow:**
1. Delegate to `cm.inner.Start()`.

**Edge Cases:**
- The `ctx` parameter is accepted but not passed to `RaftCluster.Start()`. Raft manages its own lifecycle.

---

### `(*ClusterMember) Stop(ctx context.Context) error`

**Signature:**
```go
func (cm *ClusterMember) Stop(ctx context.Context) error
```

**Flow:**
1. Call `cm.inner.Stop()`.
2. Return `nil`.

**Edge Cases:**
- Always returns `nil`. Raft shutdown errors are not propagated.

---

### `(*ClusterMember) State() pkgcluster.ClusterState`

**Signature:**
```go
func (cm *ClusterMember) State() pkgcluster.ClusterState
```

**Flow:**
1. If Raft is nil, return `Follower`.
2. Map `raft.Leader` → `Leader`, `raft.Candidate` → `Candidate`, default → `Follower`.

---

### `(*ClusterMember) IsLeader() bool`

**Signature:**
```go
func (cm *ClusterMember) IsLeader() bool
```

**Flow:**
1. Delegate to `cm.inner.IsLeader()`.

---

### `(*ClusterMember) CurrentTerm() uint64`

**Signature:**
```go
func (cm *ClusterMember) CurrentTerm() uint64
```

**Flow:**
1. Delegate to `cm.inner.CurrentTerm()`.

---

### `(*ClusterMember) LeaderID() pkgcluster.MemberID`

**Signature:**
```go
func (cm *ClusterMember) LeaderID() pkgcluster.MemberID
```

**Flow:**
1. If Raft is nil or this node is not the leader, return empty `MemberID`.
2. Otherwise, return this node's ID.

**Edge Cases:**
- Only returns a value if this node IS the leader. Does not return the actual leader's ID if this node is a follower.

---

### `(*ClusterMember) LeaderAddr() string`

**Signature:**
```go
func (cm *ClusterMember) LeaderAddr() string
```

**Flow:**
1. Delegate to `cm.inner.LeaderAddr()`.

---

### `(*ClusterMember) SubscribeLeaderChanges(fn func(isLeader bool)) pkgcluster.CancelFunc`

**Signature:**
```go
func (cm *ClusterMember) SubscribeLeaderChanges(fn func(isLeader bool)) pkgcluster.CancelFunc
```

**Flow:**
1. Delegate to `cm.inner.SubscribeLeaderChanges(fn)`.
2. Return a no-op cancel function.

**Edge Cases:**
- The cancel function does NOT actually unsubscribe. This is a limitation of the adapter.

---

### `(*ClusterMember) SubscribeTermChanges(fn func(term uint64)) pkgcluster.CancelFunc`

**Signature:**
```go
func (cm *ClusterMember) SubscribeTermChanges(fn func(term uint64)) pkgcluster.CancelFunc
```

**Flow:**
1. Return a no-op cancel function.

**Edge Cases:**
- **Not implemented.** Term change subscription is a no-op. The callback is never invoked.

---

### `(*ClusterMember) Join(memberID pkgcluster.MemberID, addr string) error`

**Signature:**
```go
func (cm *ClusterMember) Join(memberID pkgcluster.MemberID, addr string) error
```

**Flow:**
1. Delegate to `cm.inner.Join(string(memberID), addr)`.

---

### `(*ClusterMember) Remove(memberID pkgcluster.MemberID) error`

**Signature:**
```go
func (cm *ClusterMember) Remove(memberID pkgcluster.MemberID) error
```

**Flow:**
1. Delegate to `cm.inner.Leave(string(memberID))`.

---

### `(*ClusterMember) BootstrapCluster() error`

**Signature:**
```go
func (cm *ClusterMember) BootstrapCluster() error
```

**Flow:**
1. Delegate to `cm.inner.BootstrapCluster()`.

---

### `(*ClusterMember) CaptureLeadershipToken() pkgcluster.LeadershipToken`

**Signature:**
```go
func (cm *ClusterMember) CaptureLeadershipToken() pkgcluster.LeadershipToken
```

**Flow:**
1. Delegate to `cm.inner.CaptureLeadershipToken()`.

**Edge Cases:**
- Returns a `LeadershipToken` with `Leader` and `Term` fields. Use `token.Valid()` to check if leadership is still valid.

---

### `(*ClusterMember) ValidateLeadershipToken(token pkgcluster.LeadershipToken) bool`

**Signature:**
```go
func (cm *ClusterMember) ValidateLeadershipToken(token pkgcluster.LeadershipToken) bool
```

**Flow:**
1. Delegate to `cm.inner.ValidateLeadershipToken(token)`.

**Edge Cases:**
- Returns `false` if leadership has changed (term mismatch or no longer leader).

---

## internal/membership/membership.go

### Constants

```go
const (
    DefaultHeartbeatInterval = 3 * time.Second
    DefaultHeartbeatTimeout  = 10 * time.Second
    DefaultLeaderLease       = 8 * time.Second
)
```

---

### `New() *Membership`

**Signature:**
```go
func New() *Membership
```

**Flow:**
1. Create a `Membership` with an empty node map.
2. Set `heartbeatTimeout` to `DefaultHeartbeatTimeout` (10s).
3. Set `leaderLease` to `DefaultLeaderLease` (8s).
4. Return the membership.

**Edge Cases:**
- No leader is elected initially. `LeaderID()` returns `""`.

---

### `(*Membership) Add(id, address string)`

**Signature:**
```go
func (m *Membership) Add(id, address string)
```

**Flow:**
1. Lock `m.mu`.
2. Create a `NodeInfo` with `IsAlive: true`, `LastSeen: time.Now()`.
3. Store in `m.nodes[id]`.
4. Unlock.

**Edge Cases:**
- **Re-adding:** Overwrites the existing entry. Any previous alive/dead state is lost.
- **No leader election:** This method does NOT trigger leader election. Leader is determined by `LeaderID()`.

---

### `(*Membership) Remove(id string)`

**Signature:**
```go
func (m *Membership) Remove(id string)
```

**Flow:**
1. Lock `m.mu`.
2. Delete `m.nodes[id]`.
3. Unlock.

**Edge Cases:**
- No error if the node doesn't exist.
- If the removed node was the leader, `LeaderID()` will return a different node on next call.

---

### `(*Membership) MarkDead(id string)`

**Signature:**
```go
func (m *Membership) MarkDead(id string)
```

**Flow:**
1. Lock `m.mu`.
2. If the node exists, set `IsAlive = false`.
3. Unlock.

**Edge Cases:**
- No-op if the node doesn't exist.
- Dead nodes are excluded from `AliveNodes()` and `AliveCount()`.

---

### `(*Membership) MarkAlive(id string)`

**Signature:**
```go
func (m *Membership) MarkAlive(id string)
```

**Flow:**
1. Lock `m.mu`.
2. If the node exists, set `IsAlive = true` and `LastSeen = time.Now()`.
3. Unlock.

**Edge Cases:**
- No-op if the node doesn't exist.

---

### `(*Membership) Heartbeat(id, address string)`

**Signature:**
```go
func (m *Membership) Heartbeat(id, address string)
```

**Flow:**
1. Lock `m.mu`.
2. If the node exists:
   a. Set `IsAlive = true`.
   b. Set `LastSeen = time.Now()`.
   c. If `address` is non-empty, update the address.
3. If the node doesn't exist:
   a. Create a new `NodeInfo` with `IsAlive: true`, `LastSeen: now`.
4. Unlock.

**Edge Cases:**
- **Re-adds dead nodes:** A heartbeat from a dead node brings it back to alive.
- **Address update:** If address is empty, the existing address is preserved.
- This is the primary mechanism for nodes to stay alive in the cluster.

---

### `(*Membership) AliveCount() int`

**Signature:**
```go
func (m *Membership) AliveCount() int
```

**Flow:**
1. RLock `m.mu`.
2. Iterate all nodes, counting those with `IsAlive == true`.
3. RUnlock and return the count.

---

### `(*Membership) AliveNodes() []string`

**Signature:**
```go
func (m *Membership) AliveNodes() []string
```

**Flow:**
1. RLock `m.mu`.
2. Collect IDs of all nodes with `IsAlive == true`.
3. Sort the IDs lexicographically.
4. RUnlock and return the sorted list.

**Edge Cases:**
- Returns a sorted slice (lexicographic order).
- Used by `LeaderID()` to determine the leader (first element).

---

### `(*Membership) LeaderID() string`

**Signature:**
```go
func (m *Membership) LeaderID() string
```

**Flow:**
1. Call `m.AliveNodes()`.
2. If the list is empty, return `""`.
3. Return `nodes[0]` (lexicographically smallest alive node ID).

**Edge Cases:**
- **Single-node heuristic only.** This is NOT consensus-based leader election. The leader is simply the lexicographically smallest alive node.
- For multi-node leader election, use `RaftCluster` instead.
- If all nodes are dead, returns `""`.

---

### `(*Membership) Snapshot() []pkgmembership.NodeInfo`

**Signature:**
```go
func (m *Membership) Snapshot() []pkgmembership.NodeInfo
```

**Flow:**
1. RLock `m.mu`.
2. Copy all `NodeInfo` structs into a new slice.
3. RUnlock and return the slice.

**Edge Cases:**
- Returns copies (value types), not pointers.
- Includes both alive and dead nodes.

---

### `(*Membership) Lookup(id string) *pkgmembership.NodeInfo`

**Signature:**
```go
func (m *Membership) Lookup(id string) *pkgmembership.NodeInfo
```

**Flow:**
1. RLock `m.mu`.
2. Look up `id` in `m.nodes`.
3. If found, return a copy (not a pointer to the original).
4. If not found, return `nil`.
5. RUnlock.

**Edge Cases:**
- Returns a defensive copy to prevent external mutation.
- Returns `nil` for unknown IDs.

---

### `(*Membership) SetLeaderLease(d time.Duration)`

**Signature:**
```go
func (m *Membership) SetLeaderLease(d time.Duration)
```

**Flow:**
1. Lock `m.mu`.
2. Set `m.leaderLease` to `d`.
3. Unlock.

**Edge Cases:**
- Affects `StartLeaderLeaseChecker` behavior for subsequent lease checks.
- Setting to 0 means the leader lease expires immediately on any check.

---

### `(*Membership) OnLeaseExpiry(cb func(leaderID string)) pkgmembership.CancelFunc`

**Signature:**
```go
func (m *Membership) OnLeaseExpiry(cb func(leaderID string)) pkgmembership.CancelFunc
```

**Flow:**
1. Lock `m.mu`.
2. Store the callback in `m.leaseCallback`.
3. Return a cancel function that clears the callback.
4. Unlock.

**Edge Cases:**
- Only one callback is supported; subsequent calls overwrite the previous callback.
- The cancel function is safe to call; it sets `leaseCallback = nil`.
- The callback is invoked by `evictStale()` and `StartLeaderLeaseChecker` when the leader's heartbeat times out.

---

## internal/membership/lease.go

### `(*Membership) evictStale()` (private)

**Signature:**
```go
func (m *Membership) evictStale()
```

**Flow:**
1. Lock `m.mu`.
2. Record the current leader ID (before changes).
3. Iterate all nodes. For each alive node:
   a. If `now - LastSeen > heartbeatTimeout`, mark it dead.
   b. If the timed-out node was the leader, record it.
4. Unlock.
5. If the leader was evicted and a `leaseCallback` is set, invoke it with the expired leader's ID.

**Edge Cases:**
- **Double notification:** If the same leader times out across multiple eviction cycles, the callback fires each time (no deduplication here; `StartLeaderLeaseChecker` handles dedup).
- **Lock scope:** The callback is invoked AFTER unlocking to prevent deadlocks.
- **No callback:** If `leaseCallback` is nil, the eviction silently proceeds without notification.

---

### `(*Membership) LeaderLastSeen() time.Time`

**Signature:**
```go
func (m *Membership) LeaderLastSeen() time.Time
```

**Flow:**
1. RLock `m.mu`.
2. Determine the leader ID (lexicographically smallest alive node).
3. If no leader, return zero `time.Time`.
4. Return the leader's `LastSeen` time.
5. RUnlock.

**Edge Cases:**
- Returns zero time if no leader exists.

---

### `(*Membership) StartLeaderLeaseChecker(ctx context.Context, interval time.Duration)`

**Signature:**
```go
func (m *Membership) StartLeaderLeaseChecker(ctx context.Context, interval time.Duration)
```

**Flow:**
1. Launch a goroutine that:
   a. Creates a ticker at the specified interval.
   b. On each tick:
      i. Lock `m.mu`.
      ii. Determine the leader ID.
      iii. If no leader, reset `lastNotified` and continue.
      iv. If the leader hasn't heartbeated within `m.leaderLease` duration:
         - Mark the leader as dead.
         - Unlock.
         - If `leaseCallback` is set AND the leader hasn't been notified yet (`lastNotified != leaderID`):
           - Set `lastNotified = leaderID`.
           - Invoke the callback.
      v. If the leader is within the lease window, reset `lastNotified` and continue.
   c. On `ctx.Done()`, return.

**Edge Cases:**
- **Deduplication:** The `lastNotified` variable prevents the callback from firing repeatedly for the same expired leader. It resets when a new leader appears or the leader recovers.
- **Blocks the goroutine:** Runs until context is cancelled.
- **Leader lease vs heartbeat timeout:** The lease checker uses `leaderLease` (default 8s), which is different from `heartbeatTimeout` (default 10s). The lease is typically shorter to detect leader failure faster.

---

### `(*Membership) StartEviction(ctx context.Context, interval time.Duration)`

**Signature:**
```go
func (m *Membership) StartEviction(ctx context.Context, interval time.Duration)
```

**Flow:**
1. Launch a goroutine that:
   a. Creates a ticker at the specified interval.
   b. On each tick, call `m.evictStale()`.
   c. On `ctx.Done()`, return.

**Edge Cases:**
- **Blocks the goroutine:** Runs until context is cancelled.
- **Separate from leader lease checker:** This handles general node eviction; the leader lease checker handles leader-specific logic.

---

## Summary: Interface Implementations

| Concrete Type | Implements |
|---|---|
| `transport.Producer` | `transport.MessageProducer` |
| `transport.Consumer` | `transport.MessageConsumer` |
| `memory.Bus` | `transport.FullEventBus`, `Publisher`, `Subscriber`, `Requester`, `Replier`, `Broadcaster` |
| `grpctransport.GRPCBus` | `EventBusServer` (gRPC) |
| `grpctransport.GRPCClient` | `transport.EventBus` |
| `kafka.Producer` | `transport.MessageProducer` (implicit via `Send`/`Close`) |
| `kafka.Consumer` | `transport.MessageConsumer` + `sarama.ConsumerGroupHandler` |
| `cluster.ClusterProducer` | `transport.MessageProducer` |
| `cluster.ClusterConsumer` | `transport.MessageConsumer` |
| `cluster.ClusterMember` | `pkgcluster.ClusterMember` |
| `membership.Membership` | `pkgmembership.Membership` |
