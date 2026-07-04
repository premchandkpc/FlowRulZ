package reliability

import (
	"sync"
	"sync/atomic"
	"time"
)

type State int32

const (
	StateClosed State = iota
	StateHalfOpen
	StateOpen
)

type CircuitBreaker struct {
	mu              sync.Mutex
	state           State
	failureCount    int64
	lastFailureTime time.Time
	threshold       int
	recoveryTimeout time.Duration
	halfOpenMaxReqs int
	halfOpenReqs    int64
}

func NewCircuitBreaker(threshold int, recoveryTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:           StateClosed,
		threshold:       threshold,
		recoveryTimeout: recoveryTimeout,
		halfOpenMaxReqs: 3,
	}
}

// Allow checks if a request is allowed through the circuit breaker.
// Uses mutex for state transitions to prevent TOCTOU races.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		return true
	case StateOpen:
		if time.Since(cb.lastFailureTime) > cb.recoveryTimeout {
			cb.state = StateHalfOpen
			cb.halfOpenReqs = 0
			return true
		}
		return false
	case StateHalfOpen:
		n := cb.halfOpenReqs
		cb.halfOpenReqs++
		return n < int64(cb.halfOpenMaxReqs)
	}
	return false
}

// Success records a successful call and closes the breaker.
func (cb *CircuitBreaker) Success() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.state = StateClosed
	cb.failureCount = 0
}

// Failure records a failed call.
// In half-open state, any failure reopens the breaker (standard behavior).
func (cb *CircuitBreaker) Failure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failureCount++
	cb.lastFailureTime = time.Now()

	// Standard circuit breaker: any failure in half-open reopens
	if cb.state == StateHalfOpen || cb.failureCount >= int64(cb.threshold) {
		cb.state = StateOpen
	}
}

// FailureCount returns the current failure count.
func (cb *CircuitBreaker) FailureCount() int64 {
	return atomic.LoadInt64(&cb.failureCount)
}
