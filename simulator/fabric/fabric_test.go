package fabric

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/premchandkpc/FlowRulZ/server/pkg/transport"
)

func TestFabricBasic(t *testing.T) {
	f := New()
	f.RegisterNode("node-1", "localhost:8001")
	f.RegisterNode("node-2", "localhost:8002")

	nodes := f.Nodes()
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(nodes))
	}
}

func TestBusPublishSubscribe(t *testing.T) {
	f := New()
	f.RegisterNode("node-1", "localhost:8001")
	f.RegisterNode("node-2", "localhost:8002")

	// Set zero latency for fast test.
	f.Link("node-1", "node-2").Latency(0).Apply()

	bus1 := NewBus(f, "node-1")
	bus2 := NewBus(f, "node-2")

	var received atomic.Int32
	var wg sync.WaitGroup

	wg.Add(1)
	_, err := bus2.Subscribe(context.Background(), "test-topic", func(ctx context.Context, msg *transport.Message) {
		received.Add(1)
		wg.Done()
	})
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	msg := &transport.Message{Body: []byte("hello")}
	if err := bus1.Publish(context.Background(), "test-topic", msg); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	wg.Wait()
	if received.Load() != 1 {
		t.Fatalf("expected 1 message, got %d", received.Load())
	}
}

func TestFabricLatency(t *testing.T) {
	f := New()
	f.RegisterNode("node-1", "localhost:8001")
	f.RegisterNode("node-2", "localhost:8002")

	f.Link("node-1", "node-2").
		Latency(10 * time.Millisecond).
		Apply()

	// Test that EffectiveLatency returns the configured value.
	latency := f.EffectiveLatency("node-1", "node-2")
	if latency < 10*time.Millisecond {
		t.Fatalf("expected at least 10ms latency, got %v", latency)
	}
}

func TestFabricPacketLoss(t *testing.T) {
	f := New()
	f.RegisterNode("node-1", "localhost:8001")
	f.RegisterNode("node-2", "localhost:8002")

	f.Link("node-1", "node-2").
		Loss(1.0). // 100% loss
		Apply()

	dropped := 0
	for i := 0; i < 100; i++ {
		if f.ShouldDrop("node-1", "node-2") {
			dropped++
		}
	}

	if dropped != 100 {
		t.Fatalf("expected 100 drops, got %d", dropped)
	}
}

func TestFabricPartition(t *testing.T) {
	f := New()
	f.RegisterNode("node-1", "localhost:8001")
	f.RegisterNode("node-2", "localhost:8002")

	f.Partition("node-1", "node-2")

	if !f.ShouldDrop("node-1", "node-2") {
		t.Fatal("expected partition to drop messages")
	}

	f.Heal("node-1", "node-2")

	if f.ShouldDrop("node-1", "node-2") {
		t.Fatal("expected partition to be healed")
	}
}

func TestBusRequestReply(t *testing.T) {
	f := New()
	f.RegisterNode("node-1", "localhost:8001")
	f.RegisterNode("node-2", "localhost:8002")

	bus1 := NewBus(f, "node-1")
	bus2 := NewBus(f, "node-2")

	// node-2 handles requests.
	_, err := bus2.Subscribe(context.Background(), "echo", func(ctx context.Context, msg *transport.Message) {
		// Echo back the body.
		bus2.Reply(ctx, msg.CorrelationID, &transport.Message{
			Body: msg.Body,
		})
	})
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	// node-1 sends a request.
	msg := &transport.Message{Body: []byte("ping")}
	reply, err := bus1.Request(context.Background(), "echo", msg, 5*time.Second)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}

	if string(reply.Body) != "ping" {
		t.Fatalf("expected 'ping', got '%s'", string(reply.Body))
	}
}
