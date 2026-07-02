package cluster

import (
	"context"
	"log/slog"

	"github.com/hashicorp/raft"

	pkgcluster "github.com/premchandkpc/FlowRulZ/server/pkg/cluster"
)

var (
	_ pkgcluster.ClusterMember = (*ClusterMember)(nil)
	_ pkgcluster.Gossiper     = (*GossiperAdapter)(nil)
)

type ClusterMember struct {
	inner *RaftCluster
}

func NewClusterMember(rc *RaftCluster) *ClusterMember {
	return &ClusterMember{inner: rc}
}

func (cm *ClusterMember) ID() pkgcluster.MemberID {
	return pkgcluster.MemberID(cm.inner.nodeID)
}

func (cm *ClusterMember) Addr() string {
	return cm.inner.raftBind
}

func (cm *ClusterMember) Start(ctx context.Context) error {
	return cm.inner.Start()
}

func (cm *ClusterMember) Stop(ctx context.Context) error {
	cm.inner.Stop()
	return nil
}

func (cm *ClusterMember) State() pkgcluster.ClusterState {
	if cm.inner.raft == nil {
		return pkgcluster.Follower
	}
	switch cm.inner.raft.State() {
	case raft.Leader:
		return pkgcluster.Leader
	case raft.Candidate:
		return pkgcluster.Candidate
	default:
		return pkgcluster.Follower
	}
}

func (cm *ClusterMember) IsLeader() bool {
	return cm.inner.IsLeader()
}

func (cm *ClusterMember) CurrentTerm() uint64 {
	return cm.inner.CurrentTerm()
}

func (cm *ClusterMember) LeaderID() pkgcluster.MemberID {
	if cm.inner.raft == nil || !cm.inner.IsLeader() {
		return ""
	}
	return pkgcluster.MemberID(cm.inner.nodeID)
}

func (cm *ClusterMember) LeaderAddr() string {
	return cm.inner.LeaderAddr()
}

func (cm *ClusterMember) SubscribeLeaderChanges(fn func(isLeader bool)) pkgcluster.CancelFunc {
	cm.inner.SubscribeLeaderChanges(fn)
	return func() {}
}

func (cm *ClusterMember) SubscribeTermChanges(fn func(term uint64)) pkgcluster.CancelFunc {
	return func() {}
}

func (cm *ClusterMember) Join(memberID pkgcluster.MemberID, addr string) error {
	return cm.inner.Join(string(memberID), addr)
}

func (cm *ClusterMember) Remove(memberID pkgcluster.MemberID) error {
	return cm.inner.Leave(string(memberID))
}

func (cm *ClusterMember) BootstrapCluster() error {
	return cm.inner.BootstrapCluster()
}

type GossiperAdapter struct {
	inner *Gossiper
}

func NewGossiperAdapter(g *Gossiper) *GossiperAdapter {
	return &GossiperAdapter{inner: g}
}

func (ga *GossiperAdapter) Start(ctx context.Context) error {
	return nil
}

func (ga *GossiperAdapter) Stop() error {
	ga.inner.Stop()
	return nil
}

func (ga *GossiperAdapter) OnNodeJoin(fn func(nodeID, addr string)) pkgcluster.CancelFunc {
	ga.inner.OnNodeJoin(fn)
	return func() {}
}

func (ga *GossiperAdapter) OnNodeLeave(fn func(nodeID string)) pkgcluster.CancelFunc {
	return func() {}
}

func (ga *GossiperAdapter) Publish(topic string, key string, data []byte) error {
	if ga.inner.node == nil {
		return nil
	}
	ga.inner.node.Publish(topic, key, data)
	return nil
}

func (ga *GossiperAdapter) AddPeer(id, addr string) error {
	if ga.inner.node == nil {
		slog.Warn("gossiper: AddPeer called but no cluster node")
		return nil
	}
	return ga.inner.node.AddPeer(id, addr)
}

func (ga *GossiperAdapter) RemovePeer(id string) error {
	if ga.inner.node == nil {
		return nil
	}
	ga.inner.node.RemovePeer(id)
	return nil
}
