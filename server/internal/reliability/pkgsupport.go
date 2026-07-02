package reliability

import (
	"context"

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

func (dt *DedupTracker) StopCleanup() {
}
