package plandist

import (
	"context"
	"encoding/json"
	"sync"
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
