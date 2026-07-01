package reliability

import (
	"context"
	"errors"
	"time"
)

type CircuitState int

const (
	CircuitClosed    CircuitState = iota
	CircuitHalfOpen
	CircuitOpen
)

type CircuitBreaker interface {
	Execute(ctx context.Context, name string, fn func(context.Context) error) error
	State(name string) CircuitState
	Reset(name string)
}

type RateLimiter interface {
	Allow(ctx context.Context, key string) bool
	Wait(ctx context.Context, key string) error
	SetRate(key string, rate float64, burst int)
}

type Deduplicator interface {
	IsDuplicate(ctx context.Context, id string) bool
	MarkSeen(ctx context.Context, id string) error
	StartCleanup(ctx context.Context, interval time.Duration)
	StopCleanup()
}

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

type SagaStep struct {
	Name       string
	Execute    func(context.Context) error
	Compensate func(context.Context) error
	Timeout    time.Duration
}

type SagaStatus struct {
	SagaID    string
	State     string
	Completed []string
	Failed    string
	Error     string
}

type SagaOrchestrator interface {
	Begin(ctx context.Context, sagaID string, steps []SagaStep) error
	ExecuteStep(ctx context.Context, sagaID string, stepName string) error
	Compensate(ctx context.Context, sagaID string) error
	Status(ctx context.Context, sagaID string) (*SagaStatus, error)
}

var (
	ErrCircuitOpen = errors.New("circuit breaker is open")
	ErrRateLimited = errors.New("rate limit exceeded")
	ErrDuplicate   = errors.New("duplicate request")
	ErrDLQFull     = errors.New("dead letter queue is full")
	ErrSagaFailed  = errors.New("saga execution failed")
)
