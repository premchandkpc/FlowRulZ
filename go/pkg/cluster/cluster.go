package cluster

import "context"

type MemberID string

type ClusterState int

const (
	Follower  ClusterState = iota
	Candidate
	Leader
)

type MemberInfo struct {
	ID       MemberID
	Address  string
	RaftAddr string
	IsLeader bool
	IsAlive  bool
	LastSeen int64
}

type CancelFunc func()

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

type Gossiper interface {
	Start(ctx context.Context) error
	Stop() error
	OnNodeJoin(fn func(nodeID, addr string)) CancelFunc
	OnNodeLeave(fn func(nodeID string)) CancelFunc
	Publish(topic string, key string, data []byte) error
	AddPeer(id, addr string) error
	RemovePeer(id string) error
}
