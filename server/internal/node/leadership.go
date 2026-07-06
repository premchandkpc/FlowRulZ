package node

import (
	"github.com/premchandkpc/FlowRulZ/server/internal/ports"
	pkgcluster "github.com/premchandkpc/FlowRulZ/server/pkg/cluster"
	pkgmembership "github.com/premchandkpc/FlowRulZ/server/pkg/membership"
)

// RaftLeadershipStrategy delegates to a Raft cluster for leadership.
type RaftLeadershipStrategy struct {
	cluster pkgcluster.ClusterMember
}

func NewRaftLeadershipStrategy(cluster pkgcluster.ClusterMember) *RaftLeadershipStrategy {
	return &RaftLeadershipStrategy{cluster: cluster}
}

func (r *RaftLeadershipStrategy) IsLeader() bool {
	return r.cluster.IsLeader()
}

func (r *RaftLeadershipStrategy) CurrentTerm() uint64 {
	return r.cluster.CurrentTerm()
}

func (r *RaftLeadershipStrategy) CaptureLeadershipToken() ports.LeadershipToken {
	return leadershipTokenToPort(r.cluster.CaptureLeadershipToken())
}

func (r *RaftLeadershipStrategy) ValidateLeadershipToken(token ports.LeadershipToken) bool {
	return r.cluster.ValidateLeadershipToken(leadershipTokenFromPort(token))
}

func (r *RaftLeadershipStrategy) LeaderID(selfNodeID string) string {
	if r.cluster.IsLeader() {
		return selfNodeID
	}
	return string(r.cluster.LeaderID())
}

// SingleLeaderStrategy is the fallback when no Raft is configured.
// It assumes this node is always the leader and delegates term to PlanDistancer.
type SingleLeaderStrategy struct {
	planDist   PlanDistancer
	membership pkgmembership.Membership
}

func NewSingleLeaderStrategy(planDist PlanDistancer) *SingleLeaderStrategy {
	return &SingleLeaderStrategy{planDist: planDist}
}

func (s *SingleLeaderStrategy) SetMembership(m pkgmembership.Membership) {
	s.membership = m
}

func (s *SingleLeaderStrategy) IsLeader() bool {
	return true
}

func (s *SingleLeaderStrategy) CurrentTerm() uint64 {
	if s.planDist != nil {
		return s.planDist.CurrentTerm()
	}
	return 0
}

func (s *SingleLeaderStrategy) CaptureLeadershipToken() ports.LeadershipToken {
	return ports.LeadershipToken{Term: 0, Valid: true}
}

func (s *SingleLeaderStrategy) ValidateLeadershipToken(token ports.LeadershipToken) bool {
	return token.Valid
}

func (s *SingleLeaderStrategy) LeaderID(selfNodeID string) string {
	if s.membership != nil {
		return s.membership.LeaderID()
	}
	return selfNodeID
}

// Compile-time interface compliance checks
var _ LeadershipStrategy = (*RaftLeadershipStrategy)(nil)
var _ LeadershipStrategy = (*SingleLeaderStrategy)(nil)
