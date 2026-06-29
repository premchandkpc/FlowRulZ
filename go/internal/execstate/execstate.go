package execstate

import (
	"context"
	"time"
)

type Status int

const (
	StatusCreated Status = iota
	StatusRunning
	StatusWaitingForService
	StatusCompleted
	StatusFailed
)

func (s Status) String() string {
	switch s {
	case StatusCreated:
		return "created"
	case StatusRunning:
		return "running"
	case StatusWaitingForService:
		return "waiting_for_service"
	case StatusCompleted:
		return "completed"
	case StatusFailed:
		return "failed"
	default:
		return "unknown"
	}
}

type State struct {
	ID          string    `json:"id"`
	RuleID      string    `json:"rule_id"`
	Version     uint64    `json:"version"`
	PlanBytes   []byte    `json:"plan_bytes"`
	CtxBytes    []byte    `json:"ctx_bytes"`
	Status      Status    `json:"status"`
	PendingSvc  uint16    `json:"pending_svc,omitempty"`
	PendingBody []byte    `json:"pending_body,omitempty"`
	Error       string    `json:"error,omitempty"`
	Output      []byte    `json:"output,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type Store interface {
	Create(ctx context.Context, s *State) error
	Save(ctx context.Context, s *State) error
	Load(ctx context.Context, id string) (*State, error)
	List(ctx context.Context, statuses ...Status) ([]*State, error)
	Delete(ctx context.Context, id string) error
	Close() error
}
