package cluster

import "context"

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
}
