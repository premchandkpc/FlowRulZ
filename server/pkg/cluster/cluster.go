package cluster

import "context"

// LeadershipToken captures leadership state at a point in time.
// Use this to fence against split-brain: capture the token when deciding
// to act, then verify it's still valid before publishing.
type LeadershipToken struct {
	Leader bool
	Term   uint64
}

// Valid returns true if this token still represents valid leadership.
func (lt LeadershipToken) Valid() bool {
	return lt.Leader && lt.Term > 0
}

type ClusterMember interface {
	ID() MemberID
	Addr() string
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	State() ClusterState
	IsLeader() bool
	CurrentTerm() uint64
	LeaderID() MemberID
	LeaderAddr() string
	SubscribeLeaderChanges(fn func(isLeader bool)) CancelFunc
	SubscribeTermChanges(fn func(term uint64)) CancelFunc
	Join(memberID MemberID, addr string) error
	Remove(memberID MemberID) error
	BootstrapCluster() error
	// CaptureLeadershipToken captures leadership state for fencing.
	// See LeadershipToken documentation for usage pattern.
	CaptureLeadershipToken() LeadershipToken
	// ValidateLeadershipToken checks if a previously captured token is still valid.
	ValidateLeadershipToken(token LeadershipToken) bool
}
