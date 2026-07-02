package reliability

import (
	"context"
	"time"

	pkgreliability "github.com/premchandkpc/FlowRulZ/server/pkg/reliability"
)

var (
	_ pkgreliability.CircuitBreaker   = (*CircuitBreaker)(nil)
	_ pkgreliability.RateLimiter      = (*RateLimiterAdapter)(nil)
	_ pkgreliability.Deduplicator     = (*DedupTracker)(nil)
	_ pkgreliability.DLQ             = (*DLQAdapter)(nil)
	_ pkgreliability.SagaOrchestrator = (*SagaTrackerAdapter)(nil)
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

type RateLimiterAdapter struct {
	inner *RateLimiter
}

func NewRateLimiterAdapter(rl *RateLimiter) *RateLimiterAdapter {
	return &RateLimiterAdapter{inner: rl}
}

func (rla *RateLimiterAdapter) Allow(ctx context.Context, key string) bool {
	return rla.inner.Allow(key)
}

func (rla *RateLimiterAdapter) Wait(ctx context.Context, key string) error {
	for !rla.inner.Allow(key) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	return nil
}

func (rla *RateLimiterAdapter) SetRate(key string, rate float64, burst int) {
	rla.inner.SetBucket(key, rate, burst)
}

func (dt *DedupTracker) IsDuplicate(ctx context.Context, id string) bool {
	return dt.Seen(id)
}

func (dt *DedupTracker) MarkSeen(ctx context.Context, id string) error {
	dt.Mark(id)
	return nil
}

func (dt *DedupTracker) StopCleanup() {
}

type DLQAdapter struct {
	inner *DLQ
}

func NewDLQAdapter(d *DLQ) *DLQAdapter {
	return &DLQAdapter{inner: d}
}

func (da *DLQAdapter) Push(ctx context.Context, msg *pkgreliability.DeadLetterMessage) error {
	entry := &DeadLetterEntry{
		ID:    msg.OriginalTopic + "-" + msg.LastError,
		Topic: msg.OriginalTopic,
		Body:  msg.Body,
		Error: msg.LastError,
	}
	return da.inner.Send(entry)
}

func (da *DLQAdapter) Pop(ctx context.Context) (*pkgreliability.DeadLetterMessage, error) {
	da.inner.mu.Lock()
	defer da.inner.mu.Unlock()

	if len(da.inner.entries) == 0 {
		return nil, nil
	}

	entry := da.inner.entries[0]
	da.inner.entries = da.inner.entries[1:]

	return &pkgreliability.DeadLetterMessage{
		OriginalTopic: entry.Topic,
		Body:          entry.Body,
		FailCount:     entry.RetryCount,
		LastError:     entry.Error,
		FailedAt:      entry.FailedAt,
	}, nil
}

func (da *DLQAdapter) Peek(ctx context.Context) (*pkgreliability.DeadLetterMessage, error) {
	da.inner.mu.RLock()
	defer da.inner.mu.RUnlock()

	if len(da.inner.entries) == 0 {
		return nil, nil
	}

	entry := da.inner.entries[0]
	return &pkgreliability.DeadLetterMessage{
		OriginalTopic: entry.Topic,
		Body:          entry.Body,
		FailCount:     entry.RetryCount,
		LastError:     entry.Error,
		FailedAt:      entry.FailedAt,
	}, nil
}

func (da *DLQAdapter) Len() int {
	return da.inner.Len()
}

func (da *DLQAdapter) Clear(ctx context.Context) error {
	da.inner.Clear()
	return nil
}

type SagaTrackerAdapter struct {
	inner *SagaTracker
}

func NewSagaTrackerAdapter(st *SagaTracker) *SagaTrackerAdapter {
	return &SagaTrackerAdapter{inner: st}
}

func (sta *SagaTrackerAdapter) Begin(ctx context.Context, sagaID string, steps []pkgreliability.SagaStep) error {
	for _, step := range steps {
		sta.inner.RegisterStep(sagaID, SagaStep{
			ServiceName: step.Name,
			Body:        nil,
		})
	}
	return nil
}

func (sta *SagaTrackerAdapter) ExecuteStep(ctx context.Context, sagaID string, stepName string) error {
	return nil
}

func (sta *SagaTrackerAdapter) Compensate(ctx context.Context, sagaID string) error {
	return sta.inner.Compensate(sagaID)
}

func (sta *SagaTrackerAdapter) Status(ctx context.Context, sagaID string) (*pkgreliability.SagaStatus, error) {
	steps := sta.inner.StepsFor(sagaID)
	completed := make([]string, len(steps))
	for i, s := range steps {
		completed[i] = s.ServiceName
	}
	return &pkgreliability.SagaStatus{
		SagaID:    sagaID,
		Completed: completed,
	}, nil
}
