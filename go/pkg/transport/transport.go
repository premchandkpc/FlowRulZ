package transport

import (
	"context"
	"time"
)

type MessageType int32

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
	PartitionKey  string
	CreatedAt     time.Time
	Delay         time.Duration
	Metadata      map[string]any
}

type MessageHandler func(ctx context.Context, msg *Message)

type Subscription struct {
	ID    string
	Topic string
}

type Publisher interface {
	Publish(ctx context.Context, topic string, msg *Message) error
	PublishToPartition(ctx context.Context, topic, key string, msg *Message) error
}

type Subscriber interface {
	Subscribe(ctx context.Context, topic string, handler MessageHandler) (*Subscription, error)
	Unsubscribe(ctx context.Context, sub *Subscription) error
}

type Requester interface {
	Request(ctx context.Context, topic string, msg *Message, timeout time.Duration) (*Message, error)
}

type Replier interface {
	Reply(ctx context.Context, correlationID string, msg *Message) error
}

type Broadcaster interface {
	Broadcast(ctx context.Context, topic string, msg *Message) error
}

type MessageProducer interface {
	Send(ctx context.Context, key []byte, msg []byte) error
	Close() error
}

type MessageProducerFactory interface {
	CreateProducer(ctx context.Context, topic string) (MessageProducer, error)
}

type MessageConsumer interface {
	Topic() string
	Start(ctx context.Context) error
	Stop() error
}

type FullEventBus interface {
	Publisher
	Subscriber
	Requester
	Replier
	Broadcaster
	Close() error
	TopicStats() map[string]int
}
