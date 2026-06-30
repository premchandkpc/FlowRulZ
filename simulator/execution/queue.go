package execution

import (
	"sync"
	"time"
)

type ReadyQueue struct {
	mu    sync.Mutex
	items []*ExecutionContext
}

func NewReadyQueue() *ReadyQueue {
	return &ReadyQueue{items: make([]*ExecutionContext, 0, 1024)}
}

func (q *ReadyQueue) Push(ctx *ExecutionContext) {
	q.mu.Lock()
	q.items = append(q.items, ctx)
	q.mu.Unlock()
}

func (q *ReadyQueue) Pop() *ExecutionContext {
	q.mu.Lock()
	if len(q.items) == 0 {
		q.mu.Unlock()
		return nil
	}
	item := q.items[0]
	q.items = q.items[1:]
	q.mu.Unlock()
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
	QueuedAt      time.Time
}

type WaitingQueue struct {
	mu    sync.Mutex
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
		QueuedAt:      time.Now(),
	}
	q.order = append(q.order, correlationID)
	q.mu.Unlock()
}

func (q *WaitingQueue) Remove(correlationID string) *ExecutionContext {
	q.mu.Lock()
	entry, ok := q.items[correlationID]
	if !ok {
		q.mu.Unlock()
		return nil
	}
	delete(q.items, correlationID)
	newOrder := make([]string, 0, len(q.order))
	for _, id := range q.order {
		if id != correlationID {
			newOrder = append(newOrder, id)
		}
	}
	q.order = newOrder
	q.mu.Unlock()
	return entry.Ctx
}

func (q *WaitingQueue) Len() int {
	q.mu.Lock()
	n := len(q.items)
	q.mu.Unlock()
	return n
}


