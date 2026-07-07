package node

import (
	"github.com/premchandkpc/FlowRulZ/server/internal/cluster"
	"github.com/premchandkpc/FlowRulZ/server/internal/ports"
	pkgcluster "github.com/premchandkpc/FlowRulZ/server/pkg/cluster"
	pkgmembership "github.com/premchandkpc/FlowRulZ/server/pkg/membership"
)

// RaftLeadershipStrategy delegates to cluster.RaftLeadershipStrategy.
type RaftLeadershipStrategy struct {
	inner *cluster.RaftLeadershipStrategy
}

func NewRaftLeadershipStrategy(c pkgcluster.ClusterMember) *RaftLeadershipStrategy {
	return &RaftLeadershipStrategy{inner: cluster.NewRaftLeadershipStrategy(c)}
}

func (r *RaftLeadershipStrategy) IsLeader() bool                                    { return r.inner.IsLeader() }
func (r *RaftLeadershipStrategy) CurrentTerm() uint64                               { return r.inner.CurrentTerm() }
func (r *RaftLeadershipStrategy) CaptureLeadershipToken() ports.LeadershipToken     { return r.inner.CaptureLeadershipToken() }
func (r *RaftLeadershipStrategy) ValidateLeadershipToken(t ports.LeadershipToken) bool { return r.inner.ValidateLeadershipToken(t) }
func (r *RaftLeadershipStrategy) LeaderID(selfNodeID string) string                 { return r.inner.LeaderID(selfNodeID) }

// SingleLeaderStrategy delegates to cluster.SingleLeaderStrategy.
type SingleLeaderStrategy struct {
	inner *cluster.SingleLeaderStrategy
}

func NewSingleLeaderStrategy(planDist cluster.TermProvider) *SingleLeaderStrategy {
	return &SingleLeaderStrategy{inner: cluster.NewSingleLeaderStrategy(planDist)}
}

func (s *SingleLeaderStrategy) SetMembership(m pkgmembership.Membership) { s.inner.SetMembership(m) }
func (s *SingleLeaderStrategy) IsLeader() bool                           { return s.inner.IsLeader() }
func (s *SingleLeaderStrategy) CurrentTerm() uint64                      { return s.inner.CurrentTerm() }
func (s *SingleLeaderStrategy) CaptureLeadershipToken() ports.LeadershipToken { return s.inner.CaptureLeadershipToken() }
func (s *SingleLeaderStrategy) ValidateLeadershipToken(t ports.LeadershipToken) bool { return s.inner.ValidateLeadershipToken(t) }
func (s *SingleLeaderStrategy) LeaderID(selfNodeID string) string        { return s.inner.LeaderID(selfNodeID) }

// Compile-time interface compliance checks
var _ LeadershipStrategy = (*RaftLeadershipStrategy)(nil)
var _ LeadershipStrategy = (*SingleLeaderStrategy)(nil)
