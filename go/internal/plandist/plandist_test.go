package plandist

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/premchandkpc/FlowRulZ/go/internal/transport"
)

func makePlanProducer(planCh chan []byte) transport.MessageProducer {
	return &chanProducer{ch: planCh}
}

func makeAckProducer(ackCh chan []byte) transport.MessageProducer {
	return &chanProducer{ch: ackCh}
}

func makePlanConsumer(topic string, planCh chan []byte, handler transport.MessageHandler) *transport.Consumer {
	c := transport.NewConsumer(topic, handler)
	go func() {
		for msg := range planCh {
			c.Inject(msg)
		}
	}()
	return c
}

func makeAckConsumer(topic string, ackCh chan []byte, handler transport.MessageHandler) *transport.Consumer {
	c := transport.NewConsumer(topic, handler)
	go func() {
		for msg := range ackCh {
			c.Inject(msg)
		}
	}()
	return c
}

type chanProducer struct {
	ch chan<- []byte
}

func (cp *chanProducer) Send(ctx context.Context, key, value []byte) error {
	select {
	case cp.ch <- value:
	default:
	}
	return nil
}

func (cp *chanProducer) Close() {}

func TestPublishAndReceivePlan(t *testing.T) {
	planCh := make(chan []byte, 10)
	ackCh := make(chan []byte, 10)

	received := make(chan PlanMessage, 1)
	handler := func(ctx context.Context, msg PlanMessage) error {
		received <- msg
		return nil
	}

	planConsumer := makePlanConsumer("_flowrulz_plans", planCh, func(ctx context.Context, msg []byte) ([]byte, error) {
		pm, err := PlanMessageFromBytes(msg)
		if err != nil {
			return nil, err
		}
		return nil, handler(ctx, *pm)
	})
	planProducer := makePlanProducer(planCh)
	ackConsumer := makeAckConsumer("_flowrulz_acks", ackCh, func(ctx context.Context, msg []byte) ([]byte, error) {
		return nil, nil
	})
	ackProducer := makeAckProducer(ackCh)

	pd := New("node-a",
		WithPlanConsumer(planConsumer),
		WithPlanProducer(planProducer),
		WithAckConsumer(ackConsumer),
		WithAckProducer(ackProducer),
		WithPlanHandler(handler),
	)

	ctx, cancel := context.WithCancel(context.Background())
	pd.Start(ctx)

	err := pd.PublishPlan(ctx, "rule-1", 1, []byte("plan-data"), "dsl content")
	if err != nil {
		cancel()
		pd.Stop()
		t.Fatal(err)
	}

	select {
	case msg := <-received:
		if msg.RuleID != "rule-1" {
			t.Fatalf("expected rule-1, got %s", msg.RuleID)
		}
		if msg.Version != 1 {
			t.Fatalf("expected version 1, got %d", msg.Version)
		}
		if string(msg.Plan) != "plan-data" {
			t.Fatalf("expected plan-data, got %s", msg.Plan)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for plan")
	}

	cancel()
	pd.Stop()
}

func TestSendAndReceiveAck(t *testing.T) {
	planCh := make(chan []byte, 10)
	ackCh := make(chan []byte, 10)

	receivedAck := make(chan AckMessage, 1)

	planConsumer := makePlanConsumer("_flowrulz_plans", planCh, func(ctx context.Context, msg []byte) ([]byte, error) {
		return nil, nil
	})
	planProducer := makePlanProducer(planCh)
	ackConsumer := makeAckConsumer("_flowrulz_acks", ackCh, func(ctx context.Context, msg []byte) ([]byte, error) {
		var ack AckMessage
		if err := json.Unmarshal(msg, &ack); err != nil {
			return nil, err
		}
		receivedAck <- ack
		return nil, nil
	})
	ackProducer := makeAckProducer(ackCh)

	pd := New("node-a",
		WithPlanConsumer(planConsumer),
		WithPlanProducer(planProducer),
		WithAckConsumer(ackConsumer),
		WithAckProducer(ackProducer),
		WithPlanHandler(func(ctx context.Context, msg PlanMessage) error { return nil }),
	)

	ctx, cancel := context.WithCancel(context.Background())
	pd.Start(ctx)

	err := pd.SendAck(ctx, "rule-1", 1, "received")
	if err != nil {
		cancel()
		pd.Stop()
		t.Fatal(err)
	}

	select {
	case msg := <-receivedAck:
		if msg.RuleID != "rule-1" {
			t.Fatalf("expected rule-1, got %s", msg.RuleID)
		}
		if msg.Version != 1 {
			t.Fatalf("expected version 1, got %d", msg.Version)
		}
		if msg.Status != "received" {
			t.Fatalf("expected 'received', got %s", msg.Status)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for ack")
	}

	cancel()
	pd.Stop()
}

func TestWaitForAcks(t *testing.T) {
	pd := New("node-a",
		WithPlanHandler(func(ctx context.Context, msg PlanMessage) error { return nil }),
	)

	ctx, cancel := context.WithCancel(context.Background())
	pd.Start(ctx)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(50 * time.Millisecond)
		pd.RecordAck(AckMessage{NodeID: "node-b", RuleID: "rule-1", Version: 1, Status: "received"})
		pd.RecordAck(AckMessage{NodeID: "node-c", RuleID: "rule-1", Version: 1, Status: "received"})
	}()

	err := pd.WaitForAcks(ctx, "rule-1", 1, 2, 2*time.Second)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	wg.Wait()
	cancel()
	pd.Stop()
}

func TestWaitForAcksTimeout(t *testing.T) {
	pd := New("node-a")
	ctx := context.Background()
	err := pd.WaitForAcks(ctx, "rule-1", 1, 2, 100*time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

type mockQuorumProvider struct {
	count int
}

func (m *mockQuorumProvider) AliveCount() int { return m.count }

func TestQuorumZeroWithMajority(t *testing.T) {
	pd := New("leader", WithQuorumProvider(&mockQuorumProvider{count: 5}))
	err := pd.WaitForAcks(context.Background(), "rule-1", 1, 0, 100*time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout (quorum=3 > acks=0)")
	}
}

func TestQuorumNegativeAll(t *testing.T) {
	pd := New("leader", WithQuorumProvider(&mockQuorumProvider{count: 3}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		time.Sleep(20 * time.Millisecond)
		pd.RecordAck(AckMessage{RuleID: "rule-1", Version: 1, NodeID: "node-b", Status: "received"})
		pd.RecordAck(AckMessage{RuleID: "rule-1", Version: 1, NodeID: "node-c", Status: "received"})
	}()
	err := pd.WaitForAcks(ctx, "rule-1", 1, -1, 2*time.Second)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestQuorumZeroSingleNode(t *testing.T) {
	pd := New("leader", WithQuorumProvider(&mockQuorumProvider{count: 1}))
	err := pd.WaitForAcks(context.Background(), "rule-1", 1, 0, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("expected immediate return with 0 quorum, got %v", err)
	}
}

func TestQuorumZeroNoProvider(t *testing.T) {
	pd := New("leader")
	err := pd.WaitForAcks(context.Background(), "rule-1", 1, 0, 100*time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout (fallback quorum=1 > acks=0)")
	}
}

func TestSetTerm(t *testing.T) {
	pd := New("leader")
	if pd.CurrentTerm() != 0 {
		t.Fatalf("expected initial term 0, got %d", pd.CurrentTerm())
	}
	pd.SetTerm(42)
	if pd.CurrentTerm() != 42 {
		t.Fatalf("expected term 42, got %d", pd.CurrentTerm())
	}
	pd.SetTerm(99)
	if pd.CurrentTerm() != 99 {
		t.Fatalf("expected term 99, got %d", pd.CurrentTerm())
	}
}

func TestHandleAckNoPending(t *testing.T) {
	pd := New("leader")
	pd.handleAck(AckMessage{RuleID: "no-such", Version: 99, NodeID: "node-b", Status: "received"})
}

func TestHandleAckDuplicate(t *testing.T) {
	pd := New("leader")

	done := make(chan int, 1)
	received := new(atomic.Int32)
	q32 := int32(2)
	key := ackKey("rule-dup", 1)
	pd.pendingAcks.Store(key, pendingAck{done: done, received: received, quorum: q32})

	pd.RecordAck(AckMessage{RuleID: "rule-dup", Version: 1, NodeID: "node-b", Status: "received"})
	pd.RecordAck(AckMessage{RuleID: "rule-dup", Version: 1, NodeID: "node-c", Status: "received"})

	select {
	case n := <-done:
		if n != 2 {
			t.Fatalf("expected done signal with 2, got %d", n)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for done signal")
	}

	// Extra ACK should not panic or block
	pd.RecordAck(AckMessage{RuleID: "rule-dup", Version: 1, NodeID: "node-d", Status: "received"})
}

func TestPublishPlanNoProducer(t *testing.T) {
	pd := New("leader")
	err := pd.PublishPlan(context.Background(), "rule-1", 1, []byte("data"), "dsl")
	if err == nil {
		t.Fatal("expected error when no plan producer configured")
	}
	if err.Error() != "plandist: no plan producer configured" {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestActivatePlan(t *testing.T) {
	planCh := make(chan []byte, 10)
	ackCh := make(chan []byte, 10)

	received := make(chan PlanMessage, 1)

	planConsumer := makePlanConsumer("_flowrulz_plans", planCh, func(ctx context.Context, msg []byte) ([]byte, error) {
		pm, err := PlanMessageFromBytes(msg)
		if err != nil {
			return nil, err
		}
		received <- *pm
		return nil, nil
	})
	planProducer := makePlanProducer(planCh)
	ackConsumer := makeAckConsumer("_flowrulz_acks", ackCh, func(ctx context.Context, msg []byte) ([]byte, error) {
		return nil, nil
	})
	ackProducer := makeAckProducer(ackCh)

	pd := New("leader",
		WithPlanConsumer(planConsumer),
		WithPlanProducer(planProducer),
		WithAckConsumer(ackConsumer),
		WithAckProducer(ackProducer),
		WithPlanHandler(func(ctx context.Context, msg PlanMessage) error { return nil }),
	)

	ctx, cancel := context.WithCancel(context.Background())
	pd.Start(ctx)

	err := pd.ActivatePlan(ctx, "rule-1", 1)
	if err != nil {
		cancel()
		pd.Stop()
		t.Fatal(err)
	}

	select {
	case msg := <-received:
		if msg.Type != "activate" {
			t.Fatalf("expected type 'activate', got %s", msg.Type)
		}
		if msg.RuleID != "rule-1" {
			t.Fatalf("expected rule-1, got %s", msg.RuleID)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for activate")
	}

	cancel()
	pd.Stop()
}
