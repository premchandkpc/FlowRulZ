package transport

import (
	"context"
	"time"
)

type MessageType int

const (
	TypePublish   MessageType = iota
	TypeRequest
	TypeReply
	TypeBroadcast
	TypeExecution
)

type Message struct {
	ID            string
	Type          MessageType
	Topic         string
	Body          []byte
	Headers       map[string]string
	CorrelationID string
	ReplyTo       string
	CreatedAt     time.Time
	Delay         time.Duration
	PartitionKey  string
	Metadata      map[string]interface{}
}

type Handler func(ctx context.Context, msg *Message)

type Subscription struct {
	ID    string
	Topic string
}

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
