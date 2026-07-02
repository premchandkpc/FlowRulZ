package reliability

import (
	"context"
	"errors"
	"time"
)

type DeadLetterMessage struct {
	OriginalTopic string
	OriginalKey   []byte
	Body          []byte
	Headers       map[string]string
	FailCount     int
	LastError     string
	FailedAt      time.Time
}

type DLQ interface {
	Push(ctx context.Context, msg *DeadLetterMessage) error
	Pop(ctx context.Context) (*DeadLetterMessage, error)
	Peek(ctx context.Context) (*DeadLetterMessage, error)
	Len() int
	Clear(ctx context.Context) error
}

var ErrDLQFull = errors.New("dead letter queue is full")
