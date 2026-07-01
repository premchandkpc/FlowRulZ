package plandist

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/premchandkpc/FlowRulZ/go/internal/transport"
)

const (
	DefaultPlanTopic  = "_flowrulz_plans"
	DefaultAckTopic   = "_flowrulz_acks"
	defaultAckTimeout = 10 * time.Second
)

type PlanMessage struct {
	Type    string `json:"type"`
	RuleID  string `json:"rule_id"`
	Version uint64 `json:"version"`
	Term    uint64 `json:"term"`
	Plan    []byte `json:"plan,omitempty"`
	DSL     string `json:"dsl,omitempty"`
	NodeID  string `json:"node_id,omitempty"`
}

type PlanHandler func(ctx context.Context, msg PlanMessage) error

type QuorumProvider interface {
	AliveCount() int
}

type PlanDistributor struct {
	nodeID         string
	planTopic      string
	ackTopic       string
	planProducer   transport.MessageProducer
	ackProducer    transport.MessageProducer
	planConsumer   transport.MessageConsumer
	ackConsumer    transport.MessageConsumer
	planHandler    PlanHandler
	ackHandler     AckHandler
	pendingAcks    sync.Map
	clusterTerm    atomic.Uint64
	quorumProvider QuorumProvider
	started        bool
	mu             sync.Mutex
	stopCh         chan struct{}
}

func New(nodeID string, opts ...Option) *PlanDistributor {
	pd := &PlanDistributor{
		nodeID:    nodeID,
		planTopic: DefaultPlanTopic,
		ackTopic:  DefaultAckTopic,
		stopCh:    make(chan struct{}),
	}
	for _, o := range opts {
		o(pd)
	}
	return pd
}

type Option func(*PlanDistributor)

func WithPlanTopic(t string) Option {
	return func(pd *PlanDistributor) { pd.planTopic = t }
}

func WithAckTopic(t string) Option {
	return func(pd *PlanDistributor) { pd.ackTopic = t }
}

func WithPlanConsumer(c transport.MessageConsumer) Option {
	return func(pd *PlanDistributor) { pd.planConsumer = c }
}

func WithPlanProducer(p transport.MessageProducer) Option {
	return func(pd *PlanDistributor) { pd.planProducer = p }
}

func WithAckConsumer(c transport.MessageConsumer) Option {
	return func(pd *PlanDistributor) { pd.ackConsumer = c }
}

func WithAckProducer(p transport.MessageProducer) Option {
	return func(pd *PlanDistributor) { pd.ackProducer = p }
}

func WithPlanHandler(h PlanHandler) Option {
	return func(pd *PlanDistributor) { pd.planHandler = h }
}

func WithAckHandler(h AckHandler) Option {
	return func(pd *PlanDistributor) { pd.ackHandler = h }
}

func WithQuorumProvider(qp QuorumProvider) Option {
	return func(pd *PlanDistributor) { pd.quorumProvider = qp }
}

func WithClusterTerm(term uint64) Option {
	return func(pd *PlanDistributor) { pd.clusterTerm.Store(term) }
}

func (pd *PlanDistributor) Start(ctx context.Context) {
	pd.mu.Lock()
	if pd.started {
		pd.mu.Unlock()
		return
	}
	pd.started = true
	pd.mu.Unlock()

	if pd.planConsumer != nil {
		go pd.planConsumer.Start(ctx)
	}
	if pd.ackConsumer != nil {
		go pd.ackConsumer.Start(ctx)
	}

	log.Printf("plandist: node=%s started", pd.nodeID)
}

func (pd *PlanDistributor) Stop() {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	if !pd.started {
		return
	}
	close(pd.stopCh)
	if pd.planConsumer != nil {
		pd.planConsumer.Stop()
	}
	if pd.ackConsumer != nil {
		pd.ackConsumer.Stop()
	}
	if pd.planProducer != nil {
		pd.planProducer.Close()
	}
	if pd.ackProducer != nil {
		pd.ackProducer.Close()
	}
	pd.started = false
}

func (pd *PlanDistributor) SetTerm(term uint64) {
	pd.clusterTerm.Store(term)
}

func (pd *PlanDistributor) CurrentTerm() uint64 {
	return pd.clusterTerm.Load()
}

func (pd *PlanDistributor) PublishPlan(ctx context.Context, ruleID string, version uint64, plan []byte, dsl string) error {
	if pd.planProducer == nil {
		return fmt.Errorf("plandist: no plan producer configured")
	}
	msg := PlanMessage{
		Type:    "plan",
		RuleID:  ruleID,
		Version: version,
		Term:    pd.clusterTerm.Load(),
		Plan:    plan,
		DSL:     dsl,
		NodeID:  pd.nodeID,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("plandist marshal plan: %w", err)
	}
	return pd.planProducer.Send(ctx, []byte(ruleID), data)
}

func (pd *PlanDistributor) ActivatePlan(ctx context.Context, ruleID string, version uint64) error {
	if pd.planProducer == nil {
		return fmt.Errorf("plandist: no plan producer configured")
	}
	msg := PlanMessage{
		Type:    "activate",
		RuleID:  ruleID,
		Version: version,
		Term:    pd.clusterTerm.Load(),
		NodeID:  pd.nodeID,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("plandist marshal activate: %w", err)
	}
	return pd.planProducer.Send(ctx, []byte(ruleID), data)
}

func PlanMessageFromBytes(data []byte) (*PlanMessage, error) {
	var msg PlanMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}
