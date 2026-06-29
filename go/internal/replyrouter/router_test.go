package replyrouter

import (
	"sync"
	"testing"
	"time"
)

func TestSendAndRoute(t *testing.T) {
	rr := New()
	rr.StartCleanup()
	defer rr.StopCleanup()

	ch, err := rr.Send("corr-1", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	expected := []byte("hello")
	err = rr.Route("corr-1", expected)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case resp := <-ch:
		if string(resp) != string(expected) {
			t.Fatalf("expected %s, got %s", expected, resp)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for reply")
	}

	if rr.PendingCount() != 0 {
		t.Fatalf("expected 0 pending, got %d", rr.PendingCount())
	}
}

func TestRouteNonExistent(t *testing.T) {
	rr := New()
	err := rr.Route("nonexistent", []byte("data"))
	if err != ErrPendingNotFound {
		t.Fatalf("expected ErrPendingNotFound, got %v", err)
	}
}

func TestDuplicateCorrelationID(t *testing.T) {
	rr := New()

	_, err := rr.Send("corr-1", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	_, err = rr.Send("corr-1", 5*time.Second)
	if err != ErrDuplicateCorrID {
		t.Fatalf("expected ErrDuplicateCorrID, got %v", err)
	}
}

func TestExpiredCleanup(t *testing.T) {
	rr := New(WithCleanupInterval(50 * time.Millisecond), WithMaxPending(100))
	rr.StartCleanup()
	defer rr.StopCleanup()

	ch, err := rr.Send("corr-1", 10*time.Millisecond)
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

	for i := 0; i < 3; i++ {
		_, err := rr.Send(string(rune('a'+i)), 5*time.Second)
		if err != nil {
			t.Fatalf("unexpected error on send %d: %v", i, err)
		}
	}

	_, err := rr.Send("overflow", 5*time.Second)
	if err != ErrPendingLimit {
		t.Fatalf("expected ErrPendingLimit, got %v", err)
	}
}

func TestConcurrentAccess(t *testing.T) {
	rr := New()
	rr.StartCleanup()
	defer rr.StopCleanup()

	var wg sync.WaitGroup
	n := 100

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			corrID := string(rune('a' + idx%26))
			ch, err := rr.Send(corrID, 100*time.Millisecond)
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
			rr.Route(corrID, []byte("response"))
		}
	}()

	wg.Wait()
}

func TestEmptyCorrelationID(t *testing.T) {
	rr := New()
	_, err := rr.Send("", 5*time.Second)
	if err == nil {
		t.Fatal("expected error for empty correlation ID")
	}
}

func TestPendingCount(t *testing.T) {
	rr := New()

	if rr.PendingCount() != 0 {
		t.Fatalf("expected 0, got %d", rr.PendingCount())
	}

	rr.Send("corr-1", 5*time.Second)
	if rr.PendingCount() != 1 {
		t.Fatalf("expected 1, got %d", rr.PendingCount())
	}

	rr.Route("corr-1", []byte("ok"))
	if rr.PendingCount() != 0 {
		t.Fatalf("expected 0 after route, got %d", rr.PendingCount())
	}
}

func TestMultipleReplies(t *testing.T) {
	rr := New()

	ch, err := rr.Send("corr-1", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	err = rr.Route("corr-1", []byte("first"))
	if err != nil {
		t.Fatal(err)
	}

	select {
	case resp, ok := <-ch:
		if !ok {
			t.Fatal("channel should be open for first read")
		}
		if string(resp) != "first" {
			t.Fatalf("expected 'first', got %s", resp)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected closed channel after route")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for close")
	}
}
