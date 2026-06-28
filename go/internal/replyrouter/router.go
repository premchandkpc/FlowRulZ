package replyrouter

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrPendingNotFound   = errors.New("replyrouter: pending request not found")
	ErrPendingExpired    = errors.New("replyrouter: pending request expired")
	ErrPendingLimit      = errors.New("replyrouter: max pending requests reached")
	ErrDuplicateCorrID   = errors.New("replyrouter: duplicate correlation ID")
)

type PendingRequest struct {
	CorrelationID string
	ReplyCh       chan []byte
	Deadline      time.Time
	CreatedAt     time.Time
	SourceNode    string
}

type ReplyRouter struct {
	mu           sync.RWMutex
	pending      map[string]*PendingRequest
	cleanupStop  chan struct{}
	cleanupTick  time.Duration
	maxPending   int
	evictedCount atomic.Uint64
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

func (rr *ReplyRouter) Send(corrID string, timeout time.Duration) (<-chan []byte, error) {
	if corrID == "" {
		return nil, errors.New("replyrouter: empty correlation ID")
	}

	deadline := time.Now().Add(timeout)
	ch := make(chan []byte, 1)

	rr.mu.Lock()
	if _, exists := rr.pending[corrID]; exists {
		rr.mu.Unlock()
		return nil, ErrDuplicateCorrID
	}
	if rr.maxPending > 0 && len(rr.pending) >= rr.maxPending {
		rr.mu.Unlock()
		return nil, ErrPendingLimit
	}

	rr.pending[corrID] = &PendingRequest{
		CorrelationID: corrID,
		ReplyCh:       ch,
		Deadline:      deadline,
		CreatedAt:     time.Now(),
	}
	rr.mu.Unlock()

	return ch, nil
}

func (rr *ReplyRouter) Route(corrID string, response []byte) error {
	rr.mu.Lock()
	pr, ok := rr.pending[corrID]
	if !ok {
		rr.mu.Unlock()
		return ErrPendingNotFound
	}
	delete(rr.pending, corrID)
	rr.mu.Unlock()

	select {
	case pr.ReplyCh <- response:
	default:
	}

	close(pr.ReplyCh)
	return nil
}

func (rr *ReplyRouter) RouteOrStore(corrID string, response []byte) {
	err := rr.Route(corrID, response)
	if err == ErrPendingNotFound {
		return
	}
}

func (rr *ReplyRouter) Cancel(corrID string) {
	rr.mu.Lock()
	pr, ok := rr.pending[corrID]
	if !ok {
		rr.mu.Unlock()
		return
	}
	delete(rr.pending, corrID)
	rr.mu.Unlock()

	close(pr.ReplyCh)
}

func (rr *ReplyRouter) PendingCount() int {
	rr.mu.RLock()
	defer rr.mu.RUnlock()
	return len(rr.pending)
}

func (rr *ReplyRouter) EvictedCount() uint64 {
	return rr.evictedCount.Load()
}

func (rr *ReplyRouter) StartCleanup() {
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
	close(rr.cleanupStop)
}

func (rr *ReplyRouter) cleanup() {
	now := time.Now()
	rr.mu.Lock()
	for corrID, pr := range rr.pending {
		if now.After(pr.Deadline) {
			delete(rr.pending, corrID)
			rr.evictedCount.Add(1)
			close(pr.ReplyCh)
		}
	}
	rr.mu.Unlock()
}
