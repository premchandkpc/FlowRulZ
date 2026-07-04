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
