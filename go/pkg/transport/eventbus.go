package transport

import (
	"context"
	"time"
)

type EventBus interface {
	Publish(topic string, msg *Message) error
	PublishToPartition(topic, key string, msg *Message) error
	Subscribe(topic string, handler Handler) *Subscription
	Request(topic string, msg *Message, timeout time.Duration) (*Message, error)
	Reply(topic string, reqID string, msg *Message) error
	Broadcast(topic string, msg *Message) error
	Unsubscribe(subID string)
	TopicStats() map[string]int
	Close()
}

type Handler func(ctx context.Context, msg *Message)
