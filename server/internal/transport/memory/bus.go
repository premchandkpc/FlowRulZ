package memory

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/premchandkpc/FlowRulZ/server/pkg/transport"
)

type subscription struct {
	id    string
	topic string
	done  chan struct{}
}

type Bus struct {
	mu     sync.RWMutex
	topics map[string]map[string]transport.MessageHandler
	subs   map[string]*subscription
	msgID  atomic.Uint64
	closed atomic.Bool

	corrMap map[string]string
	corrMu  sync.RWMutex
	wg      sync.WaitGroup
}

var _ transport.FullEventBus = (*Bus)(nil)
var _ transport.Publisher = (*Bus)(nil)
var _ transport.Subscriber = (*Bus)(nil)
var _ transport.Requester = (*Bus)(nil)
var _ transport.Replier = (*Bus)(nil)
var _ transport.Broadcaster = (*Bus)(nil)

func New() *Bus {
	return &Bus{
		topics:  make(map[string]map[string]transport.MessageHandler),
		subs:    make(map[string]*subscription),
		corrMap: make(map[string]string),
	}
}

func (b *Bus) nextID() string {
	return fmt.Sprintf("msg-%d", b.msgID.Add(1))
}

func (b *Bus) Publish(ctx context.Context, topic string, msg *transport.Message) error {
	if b.closed.Load() {
		return fmt.Errorf("bus closed")
	}
	if msg.ID == "" {
		msg.ID = b.nextID()
	}
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = time.Now()
	}
	msg.Topic = topic
	msg.Type = transport.TypePublish

	b.mu.RLock()
	handlers, ok := b.topics[topic]
	b.mu.RUnlock()

	if !ok {
		return nil
	}

	b.dispatch(ctx, msg, handlers)
	return nil
}

func (b *Bus) PublishToPartition(ctx context.Context, topic, key string, msg *transport.Message) error {
	msg.PartitionKey = key
	return b.Publish(ctx, topic, msg)
}

func (b *Bus) Subscribe(ctx context.Context, topic string, handler transport.MessageHandler) (*transport.Subscription, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := fmt.Sprintf("sub-%s-%d", topic, len(b.topics[topic]))
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

func (b *Bus) Unsubscribe(ctx context.Context, sub *transport.Subscription) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for topic, handlers := range b.topics {
		if _, ok := handlers[sub.ID]; ok {
			delete(handlers, sub.ID)
			if s, ok2 := b.subs[sub.ID]; ok2 {
				close(s.done)
				delete(b.subs, sub.ID)
			}
			if len(handlers) == 0 {
				delete(b.topics, topic)
			}
			return nil
		}
	}
	return nil
}

func (b *Bus) Request(ctx context.Context, topic string, msg *transport.Message, timeout time.Duration) (*transport.Message, error) {
	if b.closed.Load() {
		return nil, fmt.Errorf("bus closed")
	}

	replyCh := make(chan *transport.Message, 1)
	correlationID := b.nextID()
	msg.CorrelationID = correlationID
	msg.Type = transport.TypeRequest

	b.corrMu.Lock()
	b.corrMap[correlationID] = topic
	b.corrMu.Unlock()

	replyTopic := topic + ".reply"
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
	defer func() { _ = b.Unsubscribe(ctx, sub) }()

	if err := b.Publish(ctx, topic, msg); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	select {
	case reply := <-replyCh:
		return reply, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (b *Bus) Reply(ctx context.Context, correlationID string, msg *transport.Message) error {
	msg.CorrelationID = correlationID
	msg.Type = transport.TypeReply

	b.corrMu.RLock()
	topic, ok := b.corrMap[correlationID]
	b.corrMu.RUnlock()
	if !ok {
		return fmt.Errorf("unknown correlation id: %s", correlationID)
	}

	return b.Publish(ctx, topic+".reply", msg)
}

func (b *Bus) Broadcast(ctx context.Context, topic string, msg *transport.Message) error {
	msg.Type = transport.TypeBroadcast
	return b.Publish(ctx, topic, msg)
}

func (b *Bus) TopicStats() map[string]int {
	b.mu.RLock()
	stats := make(map[string]int)
	for topic, handlers := range b.topics {
		stats[topic] = len(handlers)
	}
	b.mu.RUnlock()
	return stats
}

func (b *Bus) Close() error {
	b.closed.Store(true)
	b.wg.Wait()
	b.mu.Lock()
	b.topics = make(map[string]map[string]transport.MessageHandler)
	b.subs = make(map[string]*subscription)
	b.mu.Unlock()
	return nil
}

func (b *Bus) dispatch(ctx context.Context, msg *transport.Message, handlers map[string]transport.MessageHandler) {
	for _, h := range handlers {
		h := h
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			h(ctx, msg)
		}()
	}
}
