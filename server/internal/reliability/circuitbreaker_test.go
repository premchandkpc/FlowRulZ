package reliability

import (
	"sync"
	"testing"
	"time"
)

func TestCircuitBreaker_ConcurrentAccess(t *testing.T) {
	cb := NewCircuitBreaker(5, 100*time.Millisecond)
	var wg sync.WaitGroup

	// Simulate concurrent success and failure calls
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			cb.Allow()
		}()
		go func() {
			defer wg.Done()
			cb.Failure()
		}()
	}
	wg.Wait()
}

func TestCircuitBreaker_StateTransitions(t *testing.T) {
	cb := NewCircuitBreaker(3, 50*time.Millisecond)

	// Initially closed
	if !cb.Allow() {
		t.Error("expected Allow in closed state")
	}

	// Trip the breaker
	for i := 0; i < 3; i++ {
		cb.Failure()
	}

	// Should be open now
	if cb.Allow() {
		t.Error("expected reject in open state")
	}

	// Wait for recovery
	time.Sleep(60 * time.Millisecond)

	// Should transition to half-open
	if !cb.Allow() {
		t.Error("expected Allow in half-open state")
	}

	// Success should close it
	cb.Success()
	if !cb.Allow() {
		t.Error("expected Allow in closed state")
	}
}

func TestCircuitBreaker_FailureCountConsistency(t *testing.T) {
	cb := NewCircuitBreaker(5, time.Second)

	for i := 0; i < 3; i++ {
		cb.Failure()
	}

	count := cb.FailureCount()
	if count != 3 {
		t.Fatalf("expected failure count 3, got %d", count)
	}

	cb.Success()
	count = cb.FailureCount()
	if count != 0 {
		t.Fatalf("expected failure count 0 after success, got %d", count)
	}
}

func TestCircuitBreaker_FailureCountConcurrent(t *testing.T) {
	cb := NewCircuitBreaker(1000, time.Second)
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cb.Failure()
		}()
	}
	wg.Wait()

	count := cb.FailureCount()
	if count != 100 {
		t.Fatalf("expected failure count 100, got %d", count)
	}
}

func TestCircuitBreaker_HalfOpenMaxReqs(t *testing.T) {
	cb := NewCircuitBreaker(2, 10*time.Millisecond)

	cb.Failure()
	cb.Failure()

	time.Sleep(15 * time.Millisecond)

	allowed := 0
	for i := 0; i < 10; i++ {
		if cb.Allow() {
			allowed++
		}
	}

	if allowed < 2 || allowed > 4 {
		t.Fatalf("expected 2-4 half-open requests allowed, got %d", allowed)
	}
}

func TestCircuitBreaker_HalfOpenFailureReopens(t *testing.T) {
	cb := NewCircuitBreaker(2, 10*time.Millisecond)

	cb.Failure()
	cb.Failure()

	time.Sleep(15 * time.Millisecond)

	if !cb.Allow() {
		t.Fatal("expected Allow in half-open")
	}

	cb.Failure()

	if cb.Allow() {
		t.Error("expected reject after half-open failure")
	}
}
