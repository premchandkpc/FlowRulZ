package node

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/premchandkpc/FlowRulZ/server/internal/partition"
	"github.com/premchandkpc/FlowRulZ/server/internal/plandist"
	"github.com/premchandkpc/FlowRulZ/server/internal/transport"
	pkgpartition "github.com/premchandkpc/FlowRulZ/server/pkg/partition"
)

// MessageRouter handles transport message demux and routing.
type MessageRouter struct {
	nodeID      string
	topic       string
	factory     TransportFactory
	membership  NodeDiscovery
	clusterNode ClusterTransport
	engine      NodeEngine
	planDist    PlanDistributor
	partitions  pkgpartition.PartitionManager

	consumers []transport.MessageConsumer
	mu        sync.Mutex
}

// NodeDiscovery handles node heartbeat and discovery.
type NodeDiscovery interface {
	Heartbeat(nodeID, address string)
}

// NewMessageRouter creates a MessageRouter with the given dependencies.
func NewMessageRouter(
	nodeID string,
	topic string,
	factory TransportFactory,
	membership NodeDiscovery,
	clusterNode ClusterTransport,
	engine NodeEngine,
	planDist PlanDistributor,
	partitions pkgpartition.PartitionManager,
) *MessageRouter {
	return &MessageRouter{
		nodeID:      nodeID,
		topic:       topic,
		factory:     factory,
		membership:  membership,
		clusterNode: clusterNode,
		engine:      engine,
		planDist:    planDist,
		partitions:  partitions,
		consumers:   make([]transport.MessageConsumer, 0),
	}
}

// StartConsumers sets up transport consumers and routes messages.
func (r *MessageRouter) StartConsumers(ctx context.Context, handler transport.MessageHandler) {
	membersConsumer := r.factory.NewConsumer("_flowrulz_members", r.handleNodeDiscoveryMessage)
	planConsumer := r.factory.NewConsumer(plandist.DefaultPlanTopic, r.handlePlanMessage)
	ackConsumer := r.factory.NewConsumer(plandist.DefaultAckTopic, r.handleAckMessage)
	partConsumer := r.factory.NewConsumer(partition.PartitionTopic, r.handlePartitionMessage)
	inputConsumer := r.factory.NewConsumer(r.topic, handler)

	r.mu.Lock()
	r.consumers = append(r.consumers, inputConsumer, membersConsumer, planConsumer, ackConsumer, partConsumer)
	r.mu.Unlock()

	go inputConsumer.Start(ctx)
	go membersConsumer.Start(ctx)
	go planConsumer.Start(ctx)
	go ackConsumer.Start(ctx)
	go partConsumer.Start(ctx)
}

// StopConsumers stops all transport consumers.
func (r *MessageRouter) StopConsumers() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range r.consumers {
		c.Stop()
	}
	r.consumers = nil
}

func (r *MessageRouter) handleNodeDiscoveryMessage(ctx context.Context, msg []byte) ([]byte, error) {
	var nd NodeDiscoveryMessage
	if err := json.Unmarshal(msg, &nd); err != nil {
		slog.Error("discovery: unmarshal error", "error", err)
		return nil, nil
	}
	if nd.NodeID == r.nodeID {
		return nil, nil
	}
	r.membership.Heartbeat(nd.NodeID, nd.Address)
	if r.clusterNode != nil && nd.Address != "" {
		if err := r.clusterNode.AddPeer(nd.NodeID, nd.Address); err != nil {
			slog.Debug("cluster: auto-add peer from discovery", "peer", nd.NodeID, "addr", nd.Address, "error", err)
		}
	}
	return nil, nil
}

func (r *MessageRouter) handlePlanMessage(ctx context.Context, msg []byte) ([]byte, error) {
	pm, err := plandist.PlanMessageFromBytes(msg)
	if err != nil {
		return nil, fmt.Errorf("plandist: unmarshal plan: %w", err)
	}

	if pm.Term < r.planDist.CurrentTerm() {
		slog.Warn("plandist: rejected plan from older term", "plan_term", pm.Term, "current_term", r.planDist.CurrentTerm())
		return nil, nil
	}

	switch pm.Type {
	case "plan":
		if err := r.engine.AddVersion(pm.RuleID, pm.DSL, pm.Plan, pm.Version); err != nil {
			return nil, err
		}
		if err := r.planDist.SendAck(ctx, pm.RuleID, pm.Version, "ok"); err != nil {
			slog.Error("plandist: ack send error", "error", err)
		}
	case "activate":
		if err := r.engine.Promote(pm.RuleID, pm.Version); err != nil {
			slog.Error("plandist: activate error", "error", err)
		}
	}
	return nil, nil
}

func (r *MessageRouter) handleAckMessage(ctx context.Context, msg []byte) ([]byte, error) {
	am, err := plandist.AckMessageFromBytes(msg)
	if err != nil {
		return nil, fmt.Errorf("plandist: unmarshal ack: %w", err)
	}
	r.planDist.RecordAck(*am)
	return nil, nil
}

func (r *MessageRouter) handlePartitionMessage(ctx context.Context, msg []byte) ([]byte, error) {
	if err := r.partitions.HandleAssignmentMessage(msg); err != nil {
		slog.Error("partition: handle message error", "error", err)
	}
	return nil, nil
}
