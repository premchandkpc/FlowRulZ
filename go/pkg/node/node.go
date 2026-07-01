package node

import "context"

type ID string

type Node interface {
	ID() ID
	Addr() string
	Start(ctx context.Context) error
	Shutdown(ctx context.Context) error
	Execute(ctx context.Context, req *ExecuteRequest) (*ExecuteResponse, error)
	IsLeader() bool
	CurrentTerm() uint64
	LeaderID() ID
	Ready(ctx context.Context) error
}

type ExecuteRequest struct {
	RuleID   string
	Body     []byte
	Timeout  int64
	Metadata map[string]string
}

type ExecuteResponse struct {
	Body     []byte
	Duration int64
	RuleID   string
	Error    string
}
