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

type AckMessage struct {
	NodeID  string `json:"node_id"`
	RuleID  string `json:"rule_id"`
	Version uint64 `json:"version"`
	Status  string `json:"status"`
}

type PlanHandler func(ctx context.Context, msg PlanMessage) error
type AckHandler func(ctx context.Context, msg AckMessage)

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
	nextSerial     atomic.Uint64
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

func (pd *PlanDistributor) SendAck(ctx context.Context, ruleID string, version uint64, status string) error {
	if pd.ackProducer == nil {
		return fmt.Errorf("plandist: no ACK producer configured")
	}
	msg := AckMessage{
		NodeID:  pd.nodeID,
		RuleID:  ruleID,
		Version: version,
		Status:  status,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("plandist marshal ack: %w", err)
	}
	return pd.ackProducer.Send(ctx, []byte(fmt.Sprintf("%s:%d", ruleID, version)), data)
}

func (pd *PlanDistributor) WaitForAcks(ctx context.Context, ruleID string, version uint64, quorum int, timeout time.Duration) error {
	if quorum == 0 {
		if pd.quorumProvider != nil {
			n := pd.quorumProvider.AliveCount()
			quorum = n/2 + 1
		}
	}
	if quorum < 0 {
		if pd.quorumProvider != nil {
			quorum = pd.quorumProvider.AliveCount()
		}
	}
	if quorum <= 0 {
		return nil
	}

	key := ackKey(ruleID, version)
	done := make(chan int, 1)
	received := new(atomic.Int32)
	q32 := int32(quorum)

	pd.pendingAcks.Store(key, pendingAck{done: done, received: received, quorum: q32})
	defer pd.pendingAcks.Delete(key)

	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	select {
	case <-waitCtx.Done():
		return fmt.Errorf("plandist: ack timeout for %s v%d (got %d/%d)", ruleID, version, received.Load(), quorum)
	case n := <-done:
		if n >= quorum {
			return nil
		}
		return fmt.Errorf("plandist: insufficient acks for %s v%d (%d/%d)", ruleID, version, n, quorum)
	}
}



func (pd *PlanDistributor) handleAck(ack AckMessage) {
	key := ackKey(ack.RuleID, ack.Version)
	v, ok := pd.pendingAcks.Load(key)
	if !ok {
		return
	}
	pa, ok := v.(pendingAck)
	if !ok {
		return
	}
	n := pa.received.Add(1)
	if n >= pa.quorum {
		select {
		case pa.done <- int(n):
		default:
		}
	}
}

func (pd *PlanDistributor) RecordAck(msg AckMessage) {
	pd.handleAck(msg)
}

type pendingAck struct {
	serial   uint64
	done     chan int
	received *atomic.Int32
	quorum   int32
}

func ackKey(ruleID string, version uint64) string {
	return fmt.Sprintf("%s:%d", ruleID, version)
}

func PlanMessageFromBytes(data []byte) (*PlanMessage, error) {
	var msg PlanMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

func AckMessageFromBytes(data []byte) (*AckMessage, error) {
	var msg AckMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}
