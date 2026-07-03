package node

import (
	"context"
	"log/slog"
	"time"

	"github.com/premchandkpc/FlowRulZ/server/internal/membership"
	"github.com/premchandkpc/FlowRulZ/server/internal/partition"
	"github.com/premchandkpc/FlowRulZ/server/internal/plandist"
	"github.com/premchandkpc/FlowRulZ/server/internal/transport"
	kafkatransport "github.com/premchandkpc/FlowRulZ/server/internal/transport/kafka"
)

const DefaultMembersTopic = "_flowrulz_members"

type NodeDiscoveryMessage struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address"`
}

func (n *ProdNode) startConsumers(ctx context.Context, handler transport.MessageHandler, kc kafkatransport.Config) {
	inputConsumer := n.makeConsumer(n.config.Topic, handler, kc)
	membersConsumer := n.makeConsumer(DefaultMembersTopic, n.handleNodeDiscoveryMessage, kc)
	planConsumer := n.makeConsumer(plandist.DefaultPlanTopic, n.handlePlanMessage, kc)
	ackConsumer := n.makeConsumer(plandist.DefaultAckTopic, n.handleAckMessage, kc)
	partConsumer := n.makeConsumer(partition.PartitionTopic, n.handlePartitionMessage, kc)

	n.mu.Lock()
	n.consumers = append(n.consumers, inputConsumer, membersConsumer, planConsumer, ackConsumer, partConsumer)
	n.mu.Unlock()

	go inputConsumer.Start(ctx)
	go membersConsumer.Start(ctx)
	go planConsumer.Start(ctx)
	go ackConsumer.Start(ctx)
	go partConsumer.Start(ctx)
}

func (n *ProdNode) startSubsystems(ctx context.Context) {
	n.PlanDist.Start(ctx)
	n.Membership.StartEviction(ctx, membership.DefaultHeartbeatTimeout)

	n.Rebalancer.SetNotify(func() {
		if !n.IsLeader() {
			return
		}
		assignments := n.Partitions.Rebalance(n.Membership.AliveNodes(), n.PlanDist.CurrentTerm())
		if len(assignments) > 0 {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := n.Partitions.PublishAssignments(ctx, assignments); err != nil {
				slog.Error("partition: publish assignments error", "error", err)
			}
		}
	})

	if n.RaftCluster != nil {
		if err := n.RaftCluster.Start(ctx); err != nil {
			slog.Error("raft: start error", "error", err)
		}
		if n.config.RaftBootstrap {
			if err := n.RaftCluster.BootstrapCluster(); err != nil {
				slog.Warn("raft: bootstrap", "error", err)
			}
		}
		n.RaftCluster.SubscribeLeaderChanges(func(isLeader bool) {
			if isLeader {
				term := n.RaftCluster.CurrentTerm()
				n.PlanDist.SetTerm(term)
				n.Partitions.OnLeaderChange(n.nodeID)
				n.Rebalancer.CheckAndRebalance()
				slog.Info("raft: node became leader", "node_id", n.nodeID, "term", term)
			} else {
				leaderAddr := n.RaftCluster.LeaderAddr()
				slog.Info("raft: node stepped down", "node_id", n.nodeID, "leader_addr", leaderAddr)
				n.Partitions.OnLeaderChange("")
			}
		})
		if !n.config.RaftBootstrap && len(n.config.Seeds) > 0 {
			go n.joinRaftCluster(ctx)
		}
	}

	n.Scheduler.Start(ctx)
	n.ReplyRouter.StartCleanup(ctx)
	n.Dedup.StartCleanup(ctx, 30*time.Second)
	n.recoverInFlight(ctx)
}

func (n *ProdNode) startOTel(ctx context.Context) {
	if n.OtelExporter == nil {
		return
	}
	go n.OtelExporter.Start(ctx)
}

func (n *ProdNode) configureEngineHooks() {
	n.Engine.AfterDeploy = n.handleEngineDeploy
	n.Engine.AfterPromote = n.handleEnginePromote
}

func (n *ProdNode) handleEngineDeploy(id, dsl string, plan []byte, version uint64) {
	if !n.IsLeader() {
		return
	}
	n.PlanDist.SetTerm(n.nextDeployTerm())
	go n.distributePlan(id, dsl, plan, version)
}

func (n *ProdNode) handleEnginePromote(id string, version uint64) {
	if !n.IsLeader() {
		return
	}
	go n.distributeActivate(id, version)
}

func (n *ProdNode) distributePlan(id, dsl string, plan []byte, version uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := n.PlanDist.PublishPlan(ctx, id, version, plan, dsl); err != nil {
		slog.Error("plandist: publish plan error", "id", id, "version", version, "error", err)
		return
	}

	if err := n.PlanDist.WaitForAcks(ctx, id, version, 0, 10*time.Second); err != nil {
		slog.Error("plandist: ack wait error", "id", id, "version", version, "error", err)
	}

	if err := n.PlanDist.ActivatePlan(ctx, id, version); err != nil {
		slog.Error("plandist: activate error", "id", id, "version", version, "error", err)
	}
}

func (n *ProdNode) distributeActivate(id string, version uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := n.PlanDist.ActivatePlan(ctx, id, version); err != nil {
		slog.Error("plandist: activate error during promote", "id", id, "version", version, "error", err)
	}
}
