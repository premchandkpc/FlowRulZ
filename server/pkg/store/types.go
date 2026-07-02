package store

import "time"

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
