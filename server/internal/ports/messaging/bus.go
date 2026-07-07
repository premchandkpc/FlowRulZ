package messaging

import (
	"context"
	"time"
)

type Bus interface {
	Publisher
	Subscriber
	Request(ctx context.Context, topic string, msg *Message, timeout time.Duration) (*Message, error)
	Reply(ctx context.Context, correlationID string, msg *Message) error
	Broadcast(ctx context.Context, topic string, msg *Message) error
	Close() error
	TopicStats() map[string]int
}
