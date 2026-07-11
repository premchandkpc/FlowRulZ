// Package replyrouter provides async request/reply correlation for cross-node communication.
//
// NOTE: This component is currently unused in the execution path. It is wired into
// ProdNode for lifecycle management (StartCleanup/StopCleanup) and metrics (PendingCount),
// but Register()/Deliver()/Cancel() are never called from production code.
//
// Preserved for future use when implementing async cross-node request/reply patterns
// (e.g., for distributed saga compensation or inter-service async communication).
package replyrouter

import (
	"context"
	"errors"
	"sync"
	"time"

	pkgreplyrouter "github.com/premchandkpc/FlowRulZ/server/pkg/replyrouter"
	"github.com/premchandkpc/FlowRulZ/server/pkg/transport"
)

var (
	ErrPendingLimit    = errors.New("replyrouter: max pending requests reached")
	ErrDuplicateCorrID = errors.New("replyrouter: duplicate correlation ID")
)

var _ pkgreplyrouter.ReplyRouter = (*ReplyRouter)(nil)

type PendingRequest struct {
	CorrelationID string
	ReplyCh       chan<- *transport.Message
	Deadline      time.Time
	closed        bool
	closedMu      sync.Mutex
}

func (pr *PendingRequest) closeOnce() {
	pr.closedMu.Lock()
	defer pr.closedMu.Unlock()
	if !pr.closed {
		pr.closed = true
		close(pr.ReplyCh)
	}
}

type ReplyRouter struct {
	mu          sync.RWMutex
	pending     map[string]*PendingRequest
	cleanupStop chan struct{}
	cleanupTick time.Duration
	maxPending  int
	stopOnce    sync.Once
}

type Option func(*ReplyRouter)

func WithCleanupInterval(d time.Duration) Option {
	return func(r *ReplyRouter) {
		r.cleanupTick = d
	}
}

func WithMaxPending(n int) Option {
	return func(r *ReplyRouter) {
		r.maxPending = n
	}
}

func New(opts ...Option) *ReplyRouter {
	rr := &ReplyRouter{
		pending:     make(map[string]*PendingRequest),
		cleanupStop: make(chan struct{}),
		cleanupTick: 1 * time.Second,
		maxPending:  10000,
	}
	for _, o := range opts {
		o(rr)
	}
	return rr
}

func (rr *ReplyRouter) Register(ctx context.Context, correlationID string, ch chan<- *transport.Message, timeout time.Duration) error {
	if correlationID == "" {
		return errors.New("replyrouter: empty correlation ID")
	}

	deadline := time.Now().Add(timeout)

	rr.mu.Lock()
	defer rr.mu.Unlock()

	if _, exists := rr.pending[correlationID]; exists {
		return ErrDuplicateCorrID
	}
	if rr.maxPending > 0 && len(rr.pending) >= rr.maxPending {
		return ErrPendingLimit
	}

	rr.pending[correlationID] = &PendingRequest{
		CorrelationID: correlationID,
		ReplyCh:       ch,
		Deadline:      deadline,
	}
	return nil
}

func (rr *ReplyRouter) Cancel(correlationID string) {
	rr.mu.Lock()
	pr, ok := rr.pending[correlationID]
	if ok {
		delete(rr.pending, correlationID)
	}
	rr.mu.Unlock()

	if ok {
		pr.closeOnce()
	}
}

func (rr *ReplyRouter) Deliver(ctx context.Context, correlationID string, msg *transport.Message) bool {
	rr.mu.Lock()
	pr, ok := rr.pending[correlationID]
	if ok {
		delete(rr.pending, correlationID)
	}
	rr.mu.Unlock()

	if ok {
		select {
		case pr.ReplyCh <- msg:
		default:
		}
		pr.closeOnce()
	}

	return ok
}

func (rr *ReplyRouter) PendingCount() int {
	rr.mu.RLock()
	defer rr.mu.RUnlock()
	return len(rr.pending)
}

func (rr *ReplyRouter) StartCleanup(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(rr.cleanupTick)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				rr.cleanup()
			case <-rr.cleanupStop:
				return
			}
		}
	}()
}

func (rr *ReplyRouter) StopCleanup() {
	rr.stopOnce.Do(func() {
		close(rr.cleanupStop)
	})
}

func (rr *ReplyRouter) cleanup() {
	now := time.Now()
	rr.mu.Lock()
	var expired []*PendingRequest
	for corrID, pr := range rr.pending {
		if now.After(pr.Deadline) {
			delete(rr.pending, corrID)
			expired = append(expired, pr)
		}
	}
	rr.mu.Unlock()

	for _, pr := range expired {
		pr.closeOnce()
	}
}
