package memadapter

import (
	"context"
	"sync"
	"time"

	ports "github.com/premchandkpc/FlowRulZ/server/internal/ports/messaging"
	membus "github.com/premchandkpc/FlowRulZ/server/internal/transport/memory"
	pkgt "github.com/premchandkpc/FlowRulZ/server/pkg/transport"
)

type BusAdapter struct {
	inner *membus.Bus
	mu    sync.Mutex
}

var _ ports.Bus = (*BusAdapter)(nil)

func NewBus(cfg ports.Config) ports.Bus {
	return &BusAdapter{inner: membus.New()}
}

func (a *BusAdapter) Publish(ctx context.Context, msg *ports.Message) error {
	return a.inner.Publish(ctx, msg.Topic, toPkgMsg(msg))
}

func (a *BusAdapter) PublishToPartition(ctx context.Context, topic, key string, msg *ports.Message) error {
	return a.inner.PublishToPartition(ctx, topic, key, toPkgMsg(msg))
}

func (a *BusAdapter) Subscribe(ctx context.Context, topic string, handler ports.Handler) (*ports.Subscription, error) {
	sub, err := a.inner.Subscribe(ctx, topic, func(ctx context.Context, msg *pkgt.Message) {
		handler(ctx, fromPkgMsg(msg))
	})
	if err != nil {
		return nil, err
	}
	return &ports.Subscription{ID: sub.ID, Topic: sub.Topic}, nil
}

func (a *BusAdapter) Unsubscribe(ctx context.Context, sub *ports.Subscription) error {
	return a.inner.Unsubscribe(ctx, &pkgt.Subscription{ID: sub.ID, Topic: sub.Topic})
}

func (a *BusAdapter) Request(ctx context.Context, topic string, msg *ports.Message, timeout time.Duration) (*ports.Message, error) {
	reply, err := a.inner.Request(ctx, topic, toPkgMsg(msg), timeout)
	if err != nil {
		return nil, err
	}
	return fromPkgMsg(reply), nil
}

func (a *BusAdapter) Reply(ctx context.Context, correlationID string, msg *ports.Message) error {
	return a.inner.Reply(ctx, correlationID, toPkgMsg(msg))
}

func (a *BusAdapter) Broadcast(ctx context.Context, topic string, msg *ports.Message) error {
	return a.inner.Broadcast(ctx, topic, toPkgMsg(msg))
}

func (a *BusAdapter) Close() error {
	return a.inner.Close()
}

func (a *BusAdapter) TopicStats() map[string]int {
	return a.inner.TopicStats()
}

// -- conversion helpers --

func toPkgMsg(m *ports.Message) *pkgt.Message {
	if m == nil {
		return nil
	}
	return &pkgt.Message{
		ID:            m.ID,
		Topic:         m.Topic,
		Body:          m.Body,
		Headers:       m.Headers,
		CorrelationID: m.CorrelationID,
		ReplyTo:       m.ReplyTo,
		PartitionKey:  m.PartitionKey,
		CreatedAt:     m.CreatedAt,
		Delay:         m.Delay,
		Metadata:      m.Metadata,
	}
}

func fromPkgMsg(m *pkgt.Message) *ports.Message {
	if m == nil {
		return nil
	}
	return &ports.Message{
		ID:            m.ID,
		Topic:         m.Topic,
		Body:          m.Body,
		Headers:       m.Headers,
		CorrelationID: m.CorrelationID,
		ReplyTo:       m.ReplyTo,
		PartitionKey:  m.PartitionKey,
		CreatedAt:     m.CreatedAt,
		Delay:         m.Delay,
		Metadata:      m.Metadata,
	}
}
