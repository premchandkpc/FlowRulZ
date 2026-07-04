package eventbus

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

type EventBus struct {
	mu     sync.RWMutex
	topics map[string]map[string]transport.Handler
	subs   map[string]*subscription
	msgID  atomic.Uint64
	subID  atomic.Uint64
	closed atomic.Bool

	wg sync.WaitGroup
}

func New(bufferSize int) *EventBus {
	return &EventBus{
		topics: make(map[string]map[string]transport.Handler),
		subs:   make(map[string]*subscription),
	}
}

func (b *EventBus) nextID() string {
	return fmt.Sprintf("msg-%d", b.msgID.Add(1))
}

func (b *EventBus) handlersFor(topic string) map[string]transport.Handler {
	b.mu.RLock()
	handlers, ok := b.topics[topic]
	if !ok {
		b.mu.RUnlock()
		return nil
	}
	cpy := make(map[string]transport.Handler, len(handlers))
	for k, v := range handlers {
		cpy[k] = v
	}
	b.mu.RUnlock()
	return cpy
}

func (b *EventBus) Publish(topic string, msg *transport.Message) error {
	if b.closed.Load() {
		return fmt.Errorf("eventbus closed")
	}
	if msg.ID == "" {
		msg.ID = b.nextID()
	}
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = time.Now()
	}
	msg.Topic = topic
	msg.Type = transport.TypePublish

	handlers := b.handlersFor(topic)
	if handlers == nil {
		return nil
	}

	if msg.Delay > 0 {
		time.AfterFunc(msg.Delay, func() {
			b.dispatch(msg, handlers)
		})
		return nil
	}

	b.dispatch(msg, handlers)
	return nil
}

func (b *EventBus) PublishToPartition(topic, key string, msg *transport.Message) error {
	msg.PartitionKey = key
	return b.Publish(topic, msg)
}

func (b *EventBus) Subscribe(topic string, handler transport.Handler) *transport.Subscription {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := fmt.Sprintf("sub-%d", b.subID.Add(1))
	if b.topics[topic] == nil {
		b.topics[topic] = make(map[string]transport.Handler)
	}
	b.topics[topic][id] = handler

	sub := &subscription{
		id:    id,
		topic: topic,
		done:  make(chan struct{}),
	}
	b.subs[id] = sub
	return &transport.Subscription{ID: id, Topic: topic}
}

func (b *EventBus) Request(topic string, msg *transport.Message, timeout time.Duration) (*transport.Message, error) {
	if b.closed.Load() {
		return nil, fmt.Errorf("eventbus closed")
	}

	replyCh := make(chan *transport.Message, 1)
	correlationID := b.nextID()
	msg.CorrelationID = correlationID
	msg.Type = transport.TypeRequest

	replyTopic := topic + ".reply"
	sub := b.Subscribe(replyTopic, func(ctx context.Context, reply *transport.Message) {
		if reply.CorrelationID == correlationID {
			select {
			case replyCh <- reply:
			default:
			}
		}
	})
	defer b.Unsubscribe(sub.ID)

	if err := b.Publish(topic, msg); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	select {
	case reply := <-replyCh:
		return reply, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (b *EventBus) Reply(topic string, reqID string, msg *transport.Message) error {
	msg.CorrelationID = reqID
	msg.Type = transport.TypeReply
	return b.Publish(topic+".reply", msg)
}

func (b *EventBus) Broadcast(topic string, msg *transport.Message) error {
	msg.Type = transport.TypeBroadcast
	return b.Publish(topic, msg)
}

func (b *EventBus) Unsubscribe(subID string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for topic, handlers := range b.topics {
		if _, ok := handlers[subID]; ok {
			delete(handlers, subID)
			if sub, ok2 := b.subs[subID]; ok2 {
				close(sub.done)
				delete(b.subs, subID)
			}
			if len(handlers) == 0 {
				delete(b.topics, topic)
			}
			return
		}
	}
}

func (b *EventBus) dispatch(msg *transport.Message, handlers map[string]transport.Handler) {
	for _, h := range handlers {
		h := h
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			h(context.Background(), msg)
		}()
	}
}

func (b *EventBus) Close() {
	b.closed.Store(true)
	b.wg.Wait()
	b.mu.Lock()
	b.topics = make(map[string]map[string]transport.Handler)
	b.subs = make(map[string]*subscription)
	b.mu.Unlock()
}

func (b *EventBus) TopicStats() map[string]int {
	b.mu.RLock()
	stats := make(map[string]int)
	for topic, handlers := range b.topics {
		stats[topic] = len(handlers)
	}
	b.mu.RUnlock()
	return stats
}
