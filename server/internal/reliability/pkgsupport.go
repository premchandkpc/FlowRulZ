package reliability

import (
	"context"
	"fmt"
	"sync"
	"time"

	pkgreliability "github.com/premchandkpc/FlowRulZ/server/pkg/reliability"
)

var (
	_ pkgreliability.CircuitBreaker   = (*CircuitBreaker)(nil)
	_ pkgreliability.Deduplicator     = (*DedupTracker)(nil)
	_ pkgreliability.RateLimiter      = (*RateLimiterPkgAdapter)(nil)
	_ pkgreliability.DLQ              = (*DLQPkgAdapter)(nil)
	_ pkgreliability.SagaOrchestrator = (*SagaPkgAdapter)(nil)
)

// --- CircuitBreaker (direct: methods have correct pkg signature) ---

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
	cb.mu.Lock()
	defer cb.mu.Unlock()
	switch cb.state {
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

// --- DedupTracker (direct: methods have correct pkg signature) ---

func (dt *DedupTracker) IsDuplicate(ctx context.Context, id string) bool {
	return dt.Seen(id)
}

func (dt *DedupTracker) MarkSeen(ctx context.Context, id string) error {
	dt.Mark(id)
	return nil
}

func (dt *DedupTracker) StopCleanup() {}

// --- RateLimiterPkgAdapter ---
// Wraps internal RateLimiter to satisfy pkg/reliability.RateLimiter.
// Cannot add Allow(ctx,key) directly because Allow(string) already exists.

type RateLimiterPkgAdapter struct {
	inner *RateLimiter
}

func NewRateLimiterPkgAdapter(inner *RateLimiter) *RateLimiterPkgAdapter {
	return &RateLimiterPkgAdapter{inner: inner}
}

func (a *RateLimiterPkgAdapter) Allow(_ context.Context, key string) bool {
	return a.inner.Allow(key)
}

func (a *RateLimiterPkgAdapter) Wait(ctx context.Context, key string) error {
	timer := time.NewTimer(10 * time.Millisecond)
	defer timer.Stop()
	for {
		if a.inner.Allow(key) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			timer.Reset(10 * time.Millisecond)
		}
	}
}

func (a *RateLimiterPkgAdapter) SetRate(key string, rate float64, burst int) {
	a.inner.SetBucket(key, rate, burst)
}

// --- DLQPkgAdapter ---
// Wraps internal DLQ to satisfy pkg/reliability.DLQ.
// Cannot add Clear(ctx) error directly because Clear() already exists.

type DLQPkgAdapter struct {
	inner *DLQ
}

func NewDLQPkgAdapter(inner *DLQ) *DLQPkgAdapter {
	return &DLQPkgAdapter{inner: inner}
}

func (a *DLQPkgAdapter) Push(_ context.Context, msg *pkgreliability.DeadLetterMessage) error {
	entry := &DeadLetterEntry{
		ID:         fmt.Sprintf("dlq-%d", time.Now().UnixNano()),
		Topic:      msg.OriginalTopic,
		Body:       msg.Body,
		Error:      msg.LastError,
		RetryCount: msg.FailCount,
		FailedAt:   msg.FailedAt,
	}
	return a.inner.Send(entry)
}

func (a *DLQPkgAdapter) Pop(_ context.Context) (*pkgreliability.DeadLetterMessage, error) {
	a.inner.mu.Lock()
	if len(a.inner.entries) == 0 {
		a.inner.mu.Unlock()
		return nil, fmt.Errorf("dlq: empty")
	}
	entry := a.inner.entries[0]
	a.inner.entries = a.inner.entries[1:]
	a.inner.mu.Unlock()

	a.inner.removePersisted(entry.ID)
	return &pkgreliability.DeadLetterMessage{
		OriginalTopic: entry.Topic,
		Body:          entry.Body,
		FailCount:     entry.RetryCount,
		LastError:     entry.Error,
		FailedAt:      entry.FailedAt,
	}, nil
}

func (a *DLQPkgAdapter) Peek(_ context.Context) (*pkgreliability.DeadLetterMessage, error) {
	a.inner.mu.RLock()
	defer a.inner.mu.RUnlock()
	if len(a.inner.entries) == 0 {
		return nil, fmt.Errorf("dlq: empty")
	}
	entry := a.inner.entries[0]
	return &pkgreliability.DeadLetterMessage{
		OriginalTopic: entry.Topic,
		Body:          entry.Body,
		FailCount:     entry.RetryCount,
		LastError:     entry.Error,
		FailedAt:      entry.FailedAt,
	}, nil
}

func (a *DLQPkgAdapter) Len() int {
	return a.inner.Len()
}

func (a *DLQPkgAdapter) Clear(_ context.Context) error {
	a.inner.Clear()
	return nil
}

// --- SagaPkgAdapter ---
// Wraps internal SagaTracker to satisfy pkg/reliability.SagaOrchestrator.

type sagaInstance struct {
	steps   []pkgreliability.SagaStep
	stepIdx int
	mu      sync.Mutex
}

type SagaPkgAdapter struct {
	inner     *SagaTracker
	instances map[string]*sagaInstance
	mu        sync.Mutex
}

func NewSagaPkgAdapter(inner *SagaTracker) *SagaPkgAdapter {
	return &SagaPkgAdapter{
		inner:     inner,
		instances: make(map[string]*sagaInstance),
	}
}

func (a *SagaPkgAdapter) Begin(_ context.Context, sagaID string, steps []pkgreliability.SagaStep) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.instances[sagaID] = &sagaInstance{steps: steps}
	return nil
}

func (a *SagaPkgAdapter) ExecuteStep(ctx context.Context, sagaID string, stepName string) error {
	a.mu.Lock()
	si, ok := a.instances[sagaID]
	a.mu.Unlock()
	if !ok {
		return fmt.Errorf("saga: %s not found", sagaID)
	}
	si.mu.Lock()
	defer si.mu.Unlock()

	for i, s := range si.steps {
		if s.Name == stepName {
			if i < si.stepIdx {
				return fmt.Errorf("saga: step %s already executed", stepName)
			}
			if err := s.Execute(ctx); err != nil {
				return err
			}
			si.stepIdx = i + 1
			return nil
		}
	}
	return fmt.Errorf("saga: step %s not found", stepName)
}

func (a *SagaPkgAdapter) Compensate(ctx context.Context, sagaID string) error {
	a.mu.Lock()
	si, ok := a.instances[sagaID]
	a.mu.Unlock()
	if !ok {
		return a.inner.Compensate(sagaID)
	}

	si.mu.Lock()
	steps := make([]pkgreliability.SagaStep, len(si.steps))
	copy(steps, si.steps)
	idx := si.stepIdx
	si.stepIdx = 0
	si.mu.Unlock()

	var errs []error
	for i := idx - 1; i >= 0; i-- {
		if steps[i].Compensate != nil {
			if err := steps[i].Compensate(ctx); err != nil {
				errs = append(errs, fmt.Errorf("compensate %s: %w", steps[i].Name, err))
			}
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("saga compensation errors: %v", errs)
	}
	return nil
}

func (a *SagaPkgAdapter) Status(_ context.Context, sagaID string) (*pkgreliability.SagaStatus, error) {
	a.mu.Lock()
	si, ok := a.instances[sagaID]
	a.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("saga: %s not found", sagaID)
	}
	si.mu.Lock()
	defer si.mu.Unlock()

	completed := make([]string, 0, si.stepIdx)
	for i := 0; i < si.stepIdx; i++ {
		completed = append(completed, si.steps[i].Name)
	}
	state := "active"
	if si.stepIdx >= len(si.steps) {
		state = "completed"
	}
	return &pkgreliability.SagaStatus{
		SagaID:    sagaID,
		State:     state,
		Completed: completed,
	}, nil
}
