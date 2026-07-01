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
