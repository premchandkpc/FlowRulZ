package node

import "errors"

var (
	ErrNotLeader   = errors.New("not the leader")
	ErrNodeStopped = errors.New("node is stopped")
	ErrTimeout     = errors.New("request timeout")
)
