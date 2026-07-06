// Package clustering implements leadership strategies.
// Depends only on ports — no imports from adapters/.
package clustering

import (
	"github.com/premchandkpc/FlowRulZ/server/internal/ports"
)

// LeadershipStrategy abstracts leadership queries.
type LeadershipStrategy interface {
	IsLeader() bool
	CurrentTerm() uint64
	CaptureLeadershipToken() ports.LeadershipToken
	ValidateLeadershipToken(token ports.LeadershipToken) bool
	LeaderID(selfNodeID string) string
}

// RaftLeadershipStrategy delegates to a Raft cluster for leadership.
type RaftLeadershipStrategy struct {
	cluster ports.ClusterMember
}

func NewRaftLeadershipStrategy(cluster ports.ClusterMember) *RaftLeadershipStrategy {
	return &RaftLeadershipStrategy{cluster: cluster}
}

func (r *RaftLeadershipStrategy) IsLeader() bool {
	return r.cluster.IsLeader()
}

func (r *RaftLeadershipStrategy) CurrentTerm() uint64 {
	return r.cluster.CurrentTerm()
}

func (r *RaftLeadershipStrategy) CaptureLeadershipToken() ports.LeadershipToken {
	return r.cluster.CaptureLeadershipToken()
}

func (r *RaftLeadershipStrategy) ValidateLeadershipToken(token ports.LeadershipToken) bool {
	return r.cluster.ValidateLeadershipToken(token)
}

func (r *RaftLeadershipStrategy) LeaderID(selfNodeID string) string {
	if r.cluster.IsLeader() {
		return selfNodeID
	}
	return string(r.cluster.LeaderID())
}

// SingleLeaderStrategy is the fallback when no Raft is configured.
// It assumes this node is always the leader.
type SingleLeaderStrategy struct {
	currentTerm uint64
	leaderID    string
}

func NewSingleLeaderStrategy() *SingleLeaderStrategy {
	return &SingleLeaderStrategy{}
}

func (s *SingleLeaderStrategy) SetTerm(term uint64) {
	s.currentTerm = term
}

func (s *SingleLeaderStrategy) SetLeaderID(id string) {
	s.leaderID = id
}

func (s *SingleLeaderStrategy) IsLeader() bool {
	return true
}

func (s *SingleLeaderStrategy) CurrentTerm() uint64 {
	return s.currentTerm
}

func (s *SingleLeaderStrategy) CaptureLeadershipToken() ports.LeadershipToken {
	return ports.LeadershipToken{Term: s.currentTerm, Valid: true}
}

func (s *SingleLeaderStrategy) ValidateLeadershipToken(token ports.LeadershipToken) bool {
	return token.Valid
}

func (s *SingleLeaderStrategy) LeaderID(selfNodeID string) string {
	if s.leaderID != "" {
		return s.leaderID
	}
	return selfNodeID
}
