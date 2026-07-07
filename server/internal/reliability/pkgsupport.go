package reliability

import (
	"context"
	"time"

	pkgreliability "github.com/premchandkpc/FlowRulZ/server/pkg/reliability"
)

var (
	_ pkgreliability.CircuitBreaker = (*CircuitBreaker)(nil)
	_ pkgreliability.Deduplicator   = (*DedupTracker)(nil)
)

func (cb *CircuitBreaker) Execute(ctx context.Context, name string, fn func(context.Context) error) error {
	if !cb.Allow() {
		return pkgreliability.ErrCircuitOpen
	}
	err := fn(ctx)
	if err != nil {
		cb.Failure()
		return err
	}
	cb.Success()
	return nil
}

func (cb *CircuitBreaker) State(name string) pkgreliability.CircuitState {
	switch State(cb.state) {
	case StateHalfOpen:
		return pkgreliability.CircuitHalfOpen
	case StateOpen:
		return pkgreliability.CircuitOpen
	default:
		return pkgreliability.CircuitClosed
	}
}

func (cb *CircuitBreaker) Reset(name string) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.state = StateClosed
	cb.failureCount = 0
}

func (dt *DedupTracker) IsDuplicate(ctx context.Context, id string) bool {
	return dt.Seen(id)
}

func (dt *DedupTracker) MarkSeen(ctx context.Context, id string) error {
	dt.Mark(id)
	return nil
}

func (dt *DedupTracker) StopCleanup() {}

// RateLimiter pkg adapter helpers

func (rl *RateLimiter) AllowWithCtx(_ context.Context, key string) bool {
	return rl.Allow(key)
}

func (rl *RateLimiter) WaitCtx(ctx context.Context, key string) error {
	for {
		if rl.Allow(key) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (rl *RateLimiter) SetBucketRate(key string, rate float64, burst int) {
	rl.SetBucket(key, rate, burst)
}

// SagaTracker pkg adapter helpers

func (st *SagaTracker) CompensateCtx(_ context.Context, sagaID string) error {
	return st.Compensate(sagaID)
}

func (st *SagaTracker) StatusInfo(ctx context.Context, sagaID string) (*pkgreliability.SagaStatus, error) {
	_ = ctx
	steps := st.StepsFor(sagaID)
	completed := make([]string, 0, len(steps))
	for _, s := range steps {
		completed = append(completed, s.ServiceName)
	}
	return &pkgreliability.SagaStatus{
		SagaID:    sagaID,
		State:     "active",
		Completed: completed,
	}, nil
}
