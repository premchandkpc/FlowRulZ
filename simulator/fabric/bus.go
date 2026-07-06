package fabric

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/premchandkpc/FlowRulZ/server/pkg/transport"
)

// Bus implements transport.FullEventBus with fabric-aware latency and loss.
// Each node gets its own Bus instance, but they all share the same Fabric.
type Bus struct {
	fabric  *Fabric
	nodeID  string
	mu      sync.RWMutex
	topics  map[string]map[string]transport.MessageHandler
	subs    map[string]*subscription
	msgID   atomic.Uint64
	subID   atomic.Uint64
	closed  atomic.Bool
	wg      sync.WaitGroup
}

type subscription struct {
	id    string
	topic string
	done  chan struct{}
}

// NewBus creates a new fabric-aware EventBus for a specific node.
func NewBus(fabric *Fabric, nodeID string) *Bus {
	b := &Bus{
		fabric: fabric,
		nodeID: nodeID,
		topics: make(map[string]map[string]transport.MessageHandler),
		subs:   make(map[string]*subscription),
	}
	// Register this bus with the fabric for cross-node delivery.
	fabric.registerBus(nodeID, b)
	return b
}

// Publish sends a message through the fabric to all subscribers.
// Messages are subject to latency, jitter, and packet loss.
func (b *Bus) Publish(ctx context.Context, topic string, msg *transport.Message) error {
	if b.closed.Load() {
		return fmt.Errorf("bus closed")
	}
	if msg.ID == "" {
		msg.ID = b.nextMsgID()
	}
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = time.Now()
	}
	msg.Topic = topic
	msg.Type = transport.TypePublish

	b.fabric.stats.MessagesSent.Add(1)

	// Dispatch to local handlers (in-process, no fabric latency).
	b.mu.RLock()
	localHandlers := make(map[string]transport.MessageHandler)
	for id, h := range b.topics[topic] {
		localHandlers[id] = h
	}
	b.mu.RUnlock()

	for _, h := range localHandlers {
		b.wg.Add(1)
		go func(h transport.MessageHandler) {
			defer b.wg.Done()
			h(ctx, msg)
		}(h)
	}

	// Dispatch to remote buses through the fabric.
	b.fabric.dispatchRemote(b.nodeID, topic, msg)

	return nil
}

// PublishToPartition sends a message with a partition key.
func (b *Bus) PublishToPartition(ctx context.Context, topic, key string, msg *transport.Message) error {
	msg.PartitionKey = key
	return b.Publish(ctx, topic, msg)
}

// Subscribe registers a handler for a topic.
func (b *Bus) Subscribe(ctx context.Context, topic string, handler transport.MessageHandler) (*transport.Subscription, error) {
	if b.closed.Load() {
		return nil, fmt.Errorf("bus closed")
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	id := fmt.Sprintf("sub-%d", b.subID.Add(1))
	if b.topics[topic] == nil {
		b.topics[topic] = make(map[string]transport.MessageHandler)
	}
	b.topics[topic][id] = handler

	sub := &subscription{
		id:    id,
		topic: topic,
		done:  make(chan struct{}),
	}
	b.subs[id] = sub

	return &transport.Subscription{ID: id, Topic: topic}, nil
}

// Unsubscribe removes a handler.
func (b *Bus) Unsubscribe(ctx context.Context, sub *transport.Subscription) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if handlers, ok := b.topics[sub.Topic]; ok {
		delete(handlers, sub.ID)
		if len(handlers) == 0 {
			delete(b.topics, sub.Topic)
		}
	}
	if s, ok := b.subs[sub.ID]; ok {
		close(s.done)
		delete(b.subs, sub.ID)
	}
	return nil
}

// Request sends a request and waits for a reply.
func (b *Bus) Request(ctx context.Context, topic string, msg *transport.Message, timeout time.Duration) (*transport.Message, error) {
	if b.closed.Load() {
		return nil, fmt.Errorf("bus closed")
	}

	replyCh := make(chan *transport.Message, 1)
	correlationID := b.nextMsgID()
	msg.CorrelationID = correlationID
	msg.Type = transport.TypeRequest

	replyTopic := "__reply_" + correlationID
	sub, err := b.Subscribe(ctx, replyTopic, func(ctx context.Context, reply *transport.Message) {
		if reply.CorrelationID == correlationID {
			select {
			case replyCh <- reply:
			default:
			}
		}
	})
	if err != nil {
		return nil, err
	}
	defer b.Unsubscribe(ctx, sub)

	if err := b.Publish(ctx, topic, msg); err != nil {
		return nil, err
	}

	select {
	case reply := <-replyCh:
		return reply, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(timeout):
		return nil, fmt.Errorf("request timeout")
	}
}

// Reply sends a reply to a correlation ID.
func (b *Bus) Reply(ctx context.Context, correlationID string, msg *transport.Message) error {
	msg.CorrelationID = correlationID
	msg.Type = transport.TypeReply
	return b.Publish(ctx, "__reply_"+correlationID, msg)
}

// Broadcast sends a message to all subscribers.
func (b *Bus) Broadcast(ctx context.Context, topic string, msg *transport.Message) error {
	msg.Type = transport.TypeBroadcast
	return b.Publish(ctx, topic, msg)
}

// Close shuts down the bus.
func (b *Bus) Close() error {
	b.closed.Store(true)
	b.wg.Wait()
	b.mu.Lock()
	b.topics = make(map[string]map[string]transport.MessageHandler)
	b.subs = make(map[string]*subscription)
	b.mu.Unlock()
	b.fabric.unregisterBus(b.nodeID)
	return nil
}

// Reset resets the bus state for restart.
func (b *Bus) Reset() {
	b.closed.Store(false)
	b.mu.Lock()
	b.topics = make(map[string]map[string]transport.MessageHandler)
	b.subs = make(map[string]*subscription)
	b.mu.Unlock()
	b.fabric.registerBus(b.nodeID, b)
}

// TopicStats returns subscriber counts per topic.
func (b *Bus) TopicStats() map[string]int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	stats := make(map[string]int)
	for topic, handlers := range b.topics {
		stats[topic] = len(handlers)
	}
	return stats
}

func (b *Bus) nextMsgID() string {
	return fmt.Sprintf("msg-%d", b.msgID.Add(1))
}

// deliver delivers a message to this bus's handlers (called by the fabric).
func (b *Bus) deliver(ctx context.Context, msg *transport.Message) {
	if b.closed.Load() {
		return
	}

	b.mu.RLock()
	handlers := make(map[string]transport.MessageHandler)
	for id, h := range b.topics[msg.Topic] {
		handlers[id] = h
	}
	b.mu.RUnlock()

	for _, h := range handlers {
		b.wg.Add(1)
		go func(h transport.MessageHandler) {
			defer b.wg.Done()
			h(ctx, msg)
		}(h)
	}
}
