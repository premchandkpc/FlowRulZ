package execnode

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/premchandkpc/FlowRulZ/go/internal/plandist"
)

func (en *ExecutionNode) handleNodeDiscoveryMessage(ctx context.Context, msg []byte) ([]byte, error) {
	var nd NodeDiscoveryMessage
	if err := json.Unmarshal(msg, &nd); err != nil {
		slog.Error("discovery: unmarshal error", "error", err)
		return nil, nil
	}
	if nd.NodeID == en.nodeID {
		return nil, nil
	}
	en.Membership.Heartbeat(nd.NodeID, nd.Address)
	return nil, nil
}

func (en *ExecutionNode) handlePlanMessage(ctx context.Context, msg []byte) ([]byte, error) {
	pm, err := plandist.PlanMessageFromBytes(msg)
	if err != nil {
		return nil, fmt.Errorf("plandist: unmarshal plan: %w", err)
	}

	// Reject plans from older terms
	if pm.Term < en.PlanDist.CurrentTerm() {
		slog.Warn("plandist: rejected plan from older term", "plan_term", pm.Term, "current_term", en.PlanDist.CurrentTerm())
		return nil, nil
	}

	switch pm.Type {
	case "plan":
		if err := en.Engine.AddVersion(pm.RuleID, pm.DSL, pm.Plan, pm.Version); err != nil {
			return nil, err
		}
		if err := en.PlanDist.SendAck(ctx, pm.RuleID, pm.Version, "ok"); err != nil {
			slog.Error("plandist: ack send error", "error", err)
		}
	case "activate":
		if err := en.Engine.Promote(pm.RuleID, pm.Version); err != nil {
			slog.Error("plandist: activate error", "error", err)
		}
	}
	return nil, nil
}

func (en *ExecutionNode) handleAckMessage(ctx context.Context, msg []byte) ([]byte, error) {
	am, err := plandist.AckMessageFromBytes(msg)
	if err != nil {
		return nil, fmt.Errorf("plandist: unmarshal ack: %w", err)
	}
	en.PlanDist.RecordAck(*am)
	return nil, nil
}

func (en *ExecutionNode) handlePartitionMessage(ctx context.Context, msg []byte) ([]byte, error) {
	if err := en.Partitions.HandleAssignmentMessage(msg); err != nil {
		slog.Error("partition: handle message error", "error", err)
	}
	return nil, nil
}
