package replyrouter

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/premchandkpc/FlowRulZ/server/pkg/transport"
)

func TestRegisterAndDeliver(t *testing.T) {
	rr := New()
	rr.StartCleanup(context.Background())
	defer rr.StopCleanup()

	ch := make(chan<- *transport.Message, 1)
	err := rr.Register(context.Background(), "corr-1", ch, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	expected := &transport.Message{Body: []byte("hello")}
	ok := rr.Deliver(context.Background(), "corr-1", expected)
	if !ok {
		t.Fatal("expected Deliver to return true")
	}

	if rr.PendingCount() != 0 {
		t.Fatalf("expected 0 pending, got %d", rr.PendingCount())
	}
}

func TestDeliverNonExistent(t *testing.T) {
	rr := New()
	ok := rr.Deliver(context.Background(), "nonexistent", &transport.Message{Body: []byte("data")})
	if ok {
		t.Fatal("expected Deliver to return false for non-existent correlation ID")
	}
}

func TestDuplicateCorrelationID(t *testing.T) {
	rr := New()

	ch := make(chan<- *transport.Message, 1)
	err := rr.Register(context.Background(), "corr-1", ch, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	err = rr.Register(context.Background(), "corr-1", make(chan<- *transport.Message, 1), 5*time.Second)
	if err != ErrDuplicateCorrID {
		t.Fatalf("expected ErrDuplicateCorrID, got %v", err)
	}
}

func TestExpiredCleanup(t *testing.T) {
	rr := New(WithCleanupInterval(50*time.Millisecond), WithMaxPending(100))
	rr.StartCleanup(context.Background())
	defer rr.StopCleanup()

	ch := make(chan *transport.Message, 1)
	err := rr.Register(context.Background(), "corr-1", ch, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(200 * time.Millisecond)

	if rr.PendingCount() != 0 {
		t.Fatalf("expected 0 pending after expiry, got %d", rr.PendingCount())
	}

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected closed channel after expiry")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for channel close")
	}
}

func TestMaxPending(t *testing.T) {
	rr := New(WithMaxPending(3))

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		err := rr.Register(ctx, string(rune('a'+i)), make(chan<- *transport.Message, 1), 5*time.Second)
		if err != nil {
			t.Fatalf("unexpected error on register %d: %v", i, err)
		}
	}

	err := rr.Register(ctx, "overflow", make(chan<- *transport.Message, 1), 5*time.Second)
	if err != ErrPendingLimit {
		t.Fatalf("expected ErrPendingLimit, got %v", err)
	}
}

func TestConcurrentAccess(t *testing.T) {
	rr := New()
	rr.StartCleanup(context.Background())
	defer rr.StopCleanup()

	var wg sync.WaitGroup
	n := 100

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			corrID := string(rune('a' + idx%26))
			ch := make(chan *transport.Message, 1)
			err := rr.Register(context.Background(), corrID, ch, 100*time.Millisecond)
			if err != nil {
				return
			}
			select {
			case <-ch:
			case <-time.After(200 * time.Millisecond):
			}
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			corrID := string(rune('a' + i%26))
			rr.Deliver(context.Background(), corrID, &transport.Message{Body: []byte("response")})
		}
	}()

	wg.Wait()
}

func TestEmptyCorrelationID(t *testing.T) {
	rr := New()
	err := rr.Register(context.Background(), "", make(chan<- *transport.Message, 1), 5*time.Second)
	if err == nil {
		t.Fatal("expected error for empty correlation ID")
	}
}

func TestPendingCount(t *testing.T) {
	rr := New()

	if rr.PendingCount() != 0 {
		t.Fatalf("expected 0, got %d", rr.PendingCount())
	}

	rr.Register(context.Background(), "corr-1", make(chan<- *transport.Message, 1), 5*time.Second)
	if rr.PendingCount() != 1 {
		t.Fatalf("expected 1, got %d", rr.PendingCount())
	}

	rr.Deliver(context.Background(), "corr-1", &transport.Message{Body: []byte("ok")})
	if rr.PendingCount() != 0 {
		t.Fatalf("expected 0 after deliver, got %d", rr.PendingCount())
	}
}

func TestCancel(t *testing.T) {
	rr := New()

	ch := make(chan *transport.Message, 1)
	err := rr.Register(context.Background(), "corr-1", ch, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	if rr.PendingCount() != 1 {
		t.Fatalf("expected 1 pending, got %d", rr.PendingCount())
	}

	rr.Cancel("corr-1")
	if rr.PendingCount() != 0 {
		t.Fatalf("expected 0 after cancel, got %d", rr.PendingCount())
	}

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected closed channel after cancel")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for channel close")
	}
}

func TestMultipleReplies(t *testing.T) {
	rr := New()

	ch := make(chan *transport.Message, 1)
	err := rr.Register(context.Background(), "corr-1", ch, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	ok := rr.Deliver(context.Background(), "corr-1", &transport.Message{Body: []byte("first")})
	if !ok {
		t.Fatal("expected Deliver to return true")
	}

	select {
	case resp, ok := <-ch:
		if !ok {
			t.Fatal("channel should be open for first read")
		}
		if string(resp.Body) != "first" {
			t.Fatalf("expected 'first', got %s", resp.Body)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected closed channel after deliver")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for close")
	}
}

func TestCancelAndDeliverRace(t *testing.T) {
	rr := New()

	const iterations = 200
	for i := 0; i < iterations; i++ {
		corrID := "race-" + string(rune('A'+i%26)) + "-" + string(rune('0'+i/26))
		ch := make(chan *transport.Message, 1)
		err := rr.Register(context.Background(), corrID, ch, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			rr.Cancel(corrID)
		}()
		go func() {
			defer wg.Done()
			rr.Deliver(context.Background(), corrID, &transport.Message{Body: []byte("data")})
		}()
		wg.Wait()

		// Drain channel to unblock any pending sender.
		select {
		case <-ch:
		default:
		}
	}
}

func TestCleanupAndCancelRace(t *testing.T) {
	rr := New(WithCleanupInterval(5*time.Millisecond))
	rr.StartCleanup(context.Background())
	defer rr.StopCleanup()

	const iterations = 100
	for i := 0; i < iterations; i++ {
		corrID := "cc-" + string(rune('A'+i%26)) + "-" + string(rune('0'+i/26))
		ch := make(chan *transport.Message, 1)
		// Use very short timeout so cleanup expires it quickly.
		err := rr.Register(context.Background(), corrID, ch, 1*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}

		// Let cleanup fire and possibly expire the request.
		time.Sleep(15 * time.Millisecond)

		// Cancel in parallel with any in-flight cleanup iteration.
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			rr.Cancel(corrID)
		}()
		wg.Wait()

		// Drain channel.
		select {
		case <-ch:
		default:
		}
	}
}

func TestStopCleanupIdempotent(t *testing.T) {
	rr := New()
	rr.StartCleanup(context.Background())
	rr.StopCleanup()
	rr.StopCleanup()
	rr.StopCleanup()
}
