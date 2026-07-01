package store

import (
	"context"
	"errors"
	"time"
)

type ExecutionID string

type ExecutionRecord struct {
	ID          ExecutionID
	PlanID      string
	State       string
	Body        []byte
	Output      []byte
	Error       string
	CreatedAt   time.Time
	CompletedAt time.Time
	NodeID      string
}

type Store interface {
	Create(ctx context.Context, record *ExecutionRecord) error
	Save(ctx context.Context, record *ExecutionRecord) error
	Load(ctx context.Context, id ExecutionID) (*ExecutionRecord, error)
	List(ctx context.Context) ([]*ExecutionRecord, error)
	ListByPlan(ctx context.Context, planID string) ([]*ExecutionRecord, error)
	Delete(ctx context.Context, id ExecutionID) error
	Close() error
}

var ErrNotFound = errors.New("execution record not found")
