package node

import (
	"context"
	"log/slog"
	"time"

	"github.com/premchandkpc/FlowRulZ/server/internal/membership"
)

const DefaultMembersTopic = "_flowrulz_members"

type NodeDiscoveryMessage struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address"`
}

func (n *ProdNode) startSubsystems(ctx context.Context) {
	n.part.PlanDist.Start(ctx)
	n.cluster.Membership.StartEviction(ctx, membership.DefaultHeartbeatTimeout)

	n.part.Rebalancer.SetNotify(func() {
		token := n.CaptureLeadershipToken()
		if !token.Valid() {
			return
		}
		assignments := n.part.Partitions.Rebalance(n.cluster.Membership.AliveNodes(), token.Term)
		if len(assignments) > 0 {
			if !n.ValidateLeadershipToken(token) {
				slog.Warn("partition: leadership lost during rebalance, discarding assignments")
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := n.part.Partitions.PublishAssignments(ctx, assignments); err != nil {
				slog.Error("partition: publish assignments error", "error", err)
			}
		}
	})

	if n.cluster.RaftCluster != nil {
		if err := n.cluster.RaftCluster.Start(ctx); err != nil {
			slog.Error("raft: start error", "error", err)
		}
		if n.config.RaftBootstrap {
			if err := n.cluster.RaftCluster.BootstrapCluster(); err != nil {
				slog.Warn("raft: bootstrap", "error", err)
			}
		}
		n.cluster.RaftCluster.SubscribeLeaderChanges(func(isLeader bool) {
			if isLeader {
				term := n.cluster.RaftCluster.CurrentTerm()
				n.part.PlanDist.SetTerm(term)
				n.part.Partitions.OnLeaderChange(n.config.NodeID)
				n.part.Rebalancer.CheckAndRebalance()
				slog.Info("raft: node became leader", "node_id", n.config.NodeID, "term", term)
			} else {
				leaderAddr := n.cluster.RaftCluster.LeaderAddr()
				slog.Info("raft: node stepped down", "node_id", n.config.NodeID, "leader_addr", leaderAddr)
				n.part.Partitions.OnLeaderChange("")
			}
		})
		if !n.config.RaftBootstrap && len(n.config.Seeds) > 0 {
			go n.joinRaftCluster(ctx)
		}
	}

	n.exec.Scheduler.Start(ctx)
	n.api.ReplyRouter.StartCleanup(ctx)
	n.reliab.Dedup.StartCleanup(ctx, 30*time.Second)
	n.recoverInFlight(ctx)
}

func (n *ProdNode) startOTel(ctx context.Context) {
	if n.api.OtelExporter == nil {
		return
	}
	go n.api.OtelExporter.Start(ctx)
}

func (n *ProdNode) configureEngineHooks() {
	n.exec.Engine.SetAfterDeploy(n.handleEngineDeploy)
	n.exec.Engine.SetAfterPromote(n.handleEnginePromote)
}

func (n *ProdNode) handleEngineDeploy(id, dsl string, plan []byte, version uint64) {
	token := n.CaptureLeadershipToken()
	if !token.Valid() {
		return
	}
	n.part.PlanDist.SetTerm(token.Term)
	if !n.ValidateLeadershipToken(token) {
		slog.Warn("plandist: leadership lost during deploy, discarding plan", "id", id)
		return
	}
	go n.distributePlan(id, dsl, plan, version)
}

func (n *ProdNode) handleEnginePromote(id string, version uint64) {
	token := n.CaptureLeadershipToken()
	if !token.Valid() {
		return
	}
	if !n.ValidateLeadershipToken(token) {
		slog.Warn("plandist: leadership lost during promote, discarding activation", "id", id)
		return
	}
	go n.distributeActivate(id, version)
}

func (n *ProdNode) distributePlan(id, dsl string, plan []byte, version uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := n.part.PlanDist.PublishPlan(ctx, id, version, plan, dsl); err != nil {
		slog.Error("plandist: publish plan error", "id", id, "version", version, "error", err)
		return
	}

	if err := n.part.PlanDist.WaitForAcks(ctx, id, version, 0, 10*time.Second); err != nil {
		slog.Error("plandist: ack wait error", "id", id, "version", version, "error", err)
	}

	if err := n.part.PlanDist.ActivatePlan(ctx, id, version); err != nil {
		slog.Error("plandist: activate error", "id", id, "version", version, "error", err)
	}
}

func (n *ProdNode) distributeActivate(id string, version uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := n.part.PlanDist.ActivatePlan(ctx, id, version); err != nil {
		slog.Error("plandist: activate error during promote", "id", id, "version", version, "error", err)
	}
}
