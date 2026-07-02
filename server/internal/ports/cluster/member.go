package cluster

import "context"

type Member interface {
	ID() string
	Addr() string
	IsLeader() bool
	LeaderID() string
	Ready(ctx context.Context) error
}

type Membership interface {
	AliveNodes() []string
	StartEviction(ctx context.Context, timeout int64)
}

type Transport interface {
	Send(ctx context.Context, target string, msg []byte) ([]byte, error)
	Broadcast(ctx context.Context, msg []byte) error
}
