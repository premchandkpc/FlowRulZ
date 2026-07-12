package execution

import (
	"sync"
	"time"
)

// ReadyQueue is a thread-safe FIFO queue for execution contexts.
// Uses sync.Cond for efficient blocking instead of polling.
type ReadyQueue struct {
	mu    sync.Mutex
	cond  *sync.Cond
	items []*ExecutionContext
}

func NewReadyQueue() *ReadyQueue {
	q := &ReadyQueue{items: make([]*ExecutionContext, 0, 1024)}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *ReadyQueue) Push(ctx *ExecutionContext) {
	q.mu.Lock()
	q.items = append(q.items, ctx)
	q.mu.Unlock()
	q.cond.Signal()
}

func (q *ReadyQueue) Pop() *ExecutionContext {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return nil
	}
	item := q.items[0]
	q.items[0] = nil // avoid memory leak
	q.items = q.items[1:]
	if len(q.items) == 0 {
		q.items = q.items[:0] // reset slice when drained
	}
	return item
}

// PopWait blocks until an item is available or the context is done.
func (q *ReadyQueue) PopWait() *ExecutionContext {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.items) == 0 {
		q.cond.Wait()
	}
	item := q.items[0]
	q.items[0] = nil
	q.items = q.items[1:]
	if len(q.items) == 0 {
		q.items = q.items[:0]
	}
	return item
}

func (q *ReadyQueue) Len() int {
	q.mu.Lock()
	n := len(q.items)
	q.mu.Unlock()
	return n
}

type WaitingEntry struct {
	Ctx           *ExecutionContext
	Service       string
	CorrelationID string
}

type WaitingQueue struct {
	mu    sync.RWMutex
	items map[string]*WaitingEntry
	order []string
}

func NewWaitingQueue() *WaitingQueue {
	return &WaitingQueue{
		items: make(map[string]*WaitingEntry),
		order: make([]string, 0),
	}
}

func (q *WaitingQueue) Add(correlationID string, ctx *ExecutionContext, service string) {
	q.mu.Lock()
	q.items[correlationID] = &WaitingEntry{
		Ctx:           ctx,
		Service:       service,
		CorrelationID: correlationID,
	}
	q.order = append(q.order, correlationID)
	q.mu.Unlock()
}

// Remove uses swap-and-truncate for O(1) removal instead of O(n) rebuild.
func (q *WaitingQueue) Remove(correlationID string) *ExecutionContext {
	q.mu.Lock()
	entry, ok := q.items[correlationID]
	if !ok {
		q.mu.Unlock()
		return nil
	}
	delete(q.items, correlationID)
	for i, id := range q.order {
		if id == correlationID {
			q.order[i] = q.order[len(q.order)-1]
			q.order[len(q.order)-1] = ""
			q.order = q.order[:len(q.order)-1]
			break
		}
	}
	q.mu.Unlock()
	return entry.Ctx
}

func (q *WaitingQueue) Len() int {
	q.mu.RLock()
	n := len(q.items)
	q.mu.RUnlock()
	return n
}

// Ensure time import is used.
var _ = time.Now
