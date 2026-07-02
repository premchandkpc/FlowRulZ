package membership

import (
	"context"
	"time"
)

type Membership interface {
	Add(id, address string)
	Remove(id string)
	Heartbeat(id, address string)
	MarkDead(id string)
	MarkAlive(id string)
	AliveCount() int
	AliveNodes() []string
	LeaderID() string
	Snapshot() []NodeInfo
	Lookup(id string) *NodeInfo
	LeaderLastSeen() time.Time
	SetLeaderLease(d time.Duration)
	OnLeaseExpiry(cb func(leaderID string)) CancelFunc
	StartEviction(ctx context.Context, interval time.Duration)
	StartLeaderLeaseChecker(ctx context.Context, interval time.Duration)
}
