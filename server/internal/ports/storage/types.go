package storage

import "time"

type ExecutionRecord struct {
	ID          string
	PlanID      string
	State       string
	Body        []byte
	Output      []byte
	Error       string
	CreatedAt   time.Time
	CompletedAt time.Time
	NodeID      string
}
