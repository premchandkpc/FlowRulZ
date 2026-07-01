package node

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/premchandkpc/FlowRulZ/go/internal/plandist"
)

func (n *ProdNode) handleNodeDiscoveryMessage(ctx context.Context, msg []byte) ([]byte, error) {
	var nd NodeDiscoveryMessage
	if err := json.Unmarshal(msg, &nd); err != nil {
		slog.Error("discovery: unmarshal error", "error", err)
		return nil, nil
	}
	if nd.NodeID == n.nodeID {
		return nil, nil
	}
	n.Membership.Heartbeat(nd.NodeID, nd.Address)
	if n.ClusterNode != nil && nd.Address != "" {
		if err := n.ClusterNode.AddPeer(nd.NodeID, nd.Address); err != nil {
			slog.Debug("cluster: auto-add peer from discovery", "peer", nd.NodeID, "addr", nd.Address, "error", err)
		}
	}
	return nil, nil
}

func (n *ProdNode) handlePlanMessage(ctx context.Context, msg []byte) ([]byte, error) {
	pm, err := plandist.PlanMessageFromBytes(msg)
	if err != nil {
		return nil, fmt.Errorf("plandist: unmarshal plan: %w", err)
	}

	if pm.Term < n.PlanDist.CurrentTerm() {
		slog.Warn("plandist: rejected plan from older term", "plan_term", pm.Term, "current_term", n.PlanDist.CurrentTerm())
		return nil, nil
	}

	switch pm.Type {
	case "plan":
		if err := n.Engine.AddVersion(pm.RuleID, pm.DSL, pm.Plan, pm.Version); err != nil {
			return nil, err
		}
		if err := n.PlanDist.SendAck(ctx, pm.RuleID, pm.Version, "ok"); err != nil {
			slog.Error("plandist: ack send error", "error", err)
		}
	case "activate":
		if err := n.Engine.Promote(pm.RuleID, pm.Version); err != nil {
			slog.Error("plandist: activate error", "error", err)
		}
	}
	return nil, nil
}

func (n *ProdNode) handleAckMessage(ctx context.Context, msg []byte) ([]byte, error) {
	am, err := plandist.AckMessageFromBytes(msg)
	if err != nil {
		return nil, fmt.Errorf("plandist: unmarshal ack: %w", err)
	}
	n.PlanDist.RecordAck(*am)
	return nil, nil
}

func (n *ProdNode) handlePartitionMessage(ctx context.Context, msg []byte) ([]byte, error) {
	if err := n.Partitions.HandleAssignmentMessage(msg); err != nil {
		slog.Error("partition: handle message error", "error", err)
	}
	return nil, nil
}
