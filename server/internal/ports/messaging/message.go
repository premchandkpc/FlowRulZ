package messaging

import (
	"context"
	"time"
)

type MessageType int32

const (
	TypePublish MessageType = iota
	TypeRequest
	TypeReply
	TypeBroadcast
	TypeExecution
)

type Message struct {
	ID            string
	Type          MessageType
	Topic         string
	Key           []byte
	Body          []byte
	Headers       map[string]string
	CorrelationID string
	ReplyTo       string
	PartitionKey  string
	CreatedAt     time.Time
	Delay         time.Duration
	Metadata      map[string]any
}

type Handler func(ctx context.Context, msg *Message) error

type Subscription struct {
	ID    string
	Topic string
}
