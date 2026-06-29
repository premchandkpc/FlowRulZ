package grpctransport

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/premchandkpc/FlowRulZ/go/pkg/transport"
)

func waitForSubscriber(bus *GRPCBus, topic string, timeout time.Duration) bool {
	deadline := time.After(timeout)
	for {
		bus.mu.RLock()
		_, ok := bus.subscribers[topic]
		bus.mu.RUnlock()
		if ok {
			return true
		}
		select {
		case <-deadline:
			return false
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestGRPCPublishSubscribe(t *testing.T) {
	bus := NewGRPCBus(":0")
	if err := bus.Start(); err != nil {
		t.Fatal(err)
	}
	defer bus.Stop()

	addr := bus.lis.Addr().String()
	client := NewGRPCClient(addr)
	if err := client.Connect(); err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	var mu sync.Mutex
	received := 0
	client.Subscribe("test-topic", func(ctx context.Context, msg *transport.Message) {
		mu.Lock()
		received++
		mu.Unlock()
	})

	if !waitForSubscriber(bus, "test-topic", time.Second) {
		t.Fatal("subscriber not registered")
	}

	if err := client.Publish("test-topic", &transport.Message{
		ID:   "msg-1",
		Body: []byte("hello"),
	}); err != nil {
		t.Fatal(err)
	}

	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	if received != 1 {
		t.Fatalf("expected 1 message, got %d", received)
	}
	mu.Unlock()
}

func TestGRPCRequestReply(t *testing.T) {
	bus := NewGRPCBus(":0")
	if err := bus.Start(); err != nil {
		t.Fatal(err)
	}
	defer bus.Stop()

	addr := bus.lis.Addr().String()
	client := NewGRPCClient(addr)
	if err := client.Connect(); err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	client.Subscribe("request-topic", func(ctx context.Context, msg *transport.Message) {
		client.Reply("reply-topic", msg.CorrelationID, &transport.Message{
			Body: []byte("pong"),
		})
	})

	if !waitForSubscriber(bus, "request-topic", time.Second) {
		t.Fatal("subscriber not registered")
	}

	time.Sleep(50 * time.Millisecond)

	resp, err := client.Request("request-topic", &transport.Message{
		ID:            "req-1",
		Body:          []byte("ping"),
		CorrelationID: "corr-1",
	}, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	if string(resp.Body) != "pong" {
		t.Fatalf("expected pong, got %s", resp.Body)
	}
}

func TestGRPCBroadcast(t *testing.T) {
	bus := NewGRPCBus(":0")
	if err := bus.Start(); err != nil {
		t.Fatal(err)
	}
	defer bus.Stop()

	addr := bus.lis.Addr().String()
	client := NewGRPCClient(addr)
	if err := client.Connect(); err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	var count int32
	client.Subscribe("broadcast-topic", func(ctx context.Context, msg *transport.Message) {
		atomic.AddInt32(&count, 1)
	})
	client.Subscribe("broadcast-topic", func(ctx context.Context, msg *transport.Message) {
		atomic.AddInt32(&count, 1)
	})

	if !waitForSubscriber(bus, "broadcast-topic", time.Second) {
		t.Fatal("subscriber not registered")
	}

	time.Sleep(50 * time.Millisecond)

	if err := client.Broadcast("broadcast-topic", &transport.Message{
		ID:   "bc-1",
		Body: []byte("broadcast"),
	}); err != nil {
		t.Fatal(err)
	}

	time.Sleep(200 * time.Millisecond)

	if n := atomic.LoadInt32(&count); n != 2 {
		t.Fatalf("expected 2 deliveries, got %d", n)
	}
}

func TestGRPCUnsubscribe(t *testing.T) {
	bus := NewGRPCBus(":0")
	if err := bus.Start(); err != nil {
		t.Fatal(err)
	}
	defer bus.Stop()

	addr := bus.lis.Addr().String()
	client := NewGRPCClient(addr)
	if err := client.Connect(); err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	var count int32
	sub := client.Subscribe("unsub-topic", func(ctx context.Context, msg *transport.Message) {
		atomic.AddInt32(&count, 1)
	})

	if !waitForSubscriber(bus, "unsub-topic", time.Second) {
		t.Fatal("subscriber not registered")
	}

	time.Sleep(50 * time.Millisecond)
	client.Unsubscribe(sub.ID)

	if err := client.Publish("unsub-topic", &transport.Message{
		ID:   "msg-1",
		Body: []byte("gone"),
	}); err != nil {
		t.Fatal(err)
	}

	time.Sleep(200 * time.Millisecond)

	if n := atomic.LoadInt32(&count); n != 0 {
		t.Fatalf("expected 0 after unsubscribe, got %d", n)
	}
}
