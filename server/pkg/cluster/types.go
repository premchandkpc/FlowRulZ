package cluster

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
