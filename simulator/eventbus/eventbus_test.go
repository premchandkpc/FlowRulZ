package eventbus

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/premchandkpc/FlowRulZ/server/pkg/transport"
)

func TestPublishSubscribe(t *testing.T) {
	b := New(10)
	defer b.Close()

	var received atomic.Int32
	b.Subscribe("test", func(ctx context.Context, msg *transport.Message) {
		received.Add(1)
	})

	err := b.Publish("test", &transport.Message{Body: []byte("hello")})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	if n := received.Load(); n != 1 {
		t.Fatalf("expected 1 message, got %d", n)
	}
}

func TestPublishMultipleSubscribers(t *testing.T) {
	b := New(10)
	defer b.Close()

	var count atomic.Int32
	for i := 0; i < 3; i++ {
		b.Subscribe("test", func(ctx context.Context, msg *transport.Message) {
			count.Add(1)
		})
	}

	b.Publish("test", &transport.Message{Body: []byte("hello")})
	time.Sleep(50 * time.Millisecond)
	if n := count.Load(); n != 3 {
		t.Fatalf("expected 3 deliveries, got %d", n)
	}
}

func TestPublishToNoSubscribers(t *testing.T) {
	b := New(10)
	defer b.Close()

	err := b.Publish("nonexistent", &transport.Message{Body: []byte("hi")})
	if err != nil {
		t.Fatalf("Publish to no subscribers: %v", err)
	}
}

func TestRequestReply(t *testing.T) {
	b := New(10)
	defer b.Close()

	b.Subscribe("ping", func(ctx context.Context, msg *transport.Message) {
		b.Reply("ping", msg.CorrelationID, &transport.Message{
			Body: []byte("pong"),
		})
	})

	reply, err := b.Request("ping", &transport.Message{Body: []byte("ping")}, 1*time.Second)
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	if string(reply.Body) != "pong" {
		t.Fatalf("expected 'pong', got '%s'", string(reply.Body))
	}
}

func TestRequestTimeout(t *testing.T) {
	b := New(10)
	defer b.Close()

	_, err := b.Request("nowhere", &transport.Message{Body: []byte("hello")}, 100*time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestUnsubscribe(t *testing.T) {
	b := New(10)
	defer b.Close()

	var count atomic.Int32
	sub := b.Subscribe("test", func(ctx context.Context, msg *transport.Message) {
		count.Add(1)
	})

	b.Publish("test", &transport.Message{Body: []byte("a")})
	time.Sleep(50 * time.Millisecond)
	b.Unsubscribe(sub.ID)

	b.Publish("test", &transport.Message{Body: []byte("b")})
	time.Sleep(50 * time.Millisecond)

	if n := count.Load(); n != 1 {
		t.Fatalf("expected 1 delivery after unsubscribe, got %d", n)
	}
}

func TestDelayedMessage(t *testing.T) {
	b := New(10)
	defer b.Close()

	var received atomic.Int32
	b.Subscribe("delayed", func(ctx context.Context, msg *transport.Message) {
		received.Add(1)
	})

	start := time.Now()
	b.Publish("delayed", &transport.Message{Body: []byte("late"), Delay: 200 * time.Millisecond})
	time.Sleep(50 * time.Millisecond)

	if n := received.Load(); n != 0 {
		t.Fatalf("expected 0 before delay, got %d", n)
	}

	time.Sleep(250 * time.Millisecond)
	if n := received.Load(); n != 1 {
		t.Fatalf("expected 1 after delay, got %d", n)
	}
	if time.Since(start) < 200*time.Millisecond {
		t.Fatal("message arrived too early")
	}
}

func TestCloseRejectsPublish(t *testing.T) {
	b := New(10)
	b.Close()

	err := b.Publish("test", &transport.Message{Body: []byte("x")})
	if err == nil {
		t.Fatal("expected error on closed bus")
	}
}

func TestMultipleTopics(t *testing.T) {
	b := New(10)
	defer b.Close()

	var countA, countB atomic.Int32
	b.Subscribe("topic-a", func(ctx context.Context, msg *transport.Message) {
		countA.Add(1)
	})
	b.Subscribe("topic-b", func(ctx context.Context, msg *transport.Message) {
		countB.Add(1)
	})

	b.Publish("topic-a", &transport.Message{Body: []byte("a")})
	b.Publish("topic-b", &transport.Message{Body: []byte("b")})
	b.Publish("topic-a", &transport.Message{Body: []byte("a2")})
	time.Sleep(50 * time.Millisecond)

	if n := countA.Load(); n != 2 {
		t.Fatalf("expected 2 on topic-a, got %d", n)
	}
	if n := countB.Load(); n != 1 {
		t.Fatalf("expected 1 on topic-b, got %d", n)
	}
}

func TestMessageIDAutoAssign(t *testing.T) {
	b := New(10)
	defer b.Close()

	ch := make(chan string, 1)
	b.Subscribe("test", func(ctx context.Context, msg *transport.Message) {
		ch <- msg.ID
	})

	b.Publish("test", &transport.Message{Body: []byte("hello")})

	select {
	case id := <-ch:
		if id == "" {
			t.Fatal("expected non-empty message ID")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for message")
	}
}


