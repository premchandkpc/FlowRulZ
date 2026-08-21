package agent

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Task represents a unit of work to be processed by an agent.
type Task struct {
	ID       string
	Priority int
	Body     []byte
	Execute  func(ctx context.Context, task *Task) ([]byte, error)
	ResultCh chan TaskResult
	Created  time.Time
}

// TaskResult carries the result of task execution.
type TaskResult struct {
	Output []byte
	Error  error
}

// TaskQueue is a bounded, thread-safe task queue with backpressure.
type TaskQueue struct {
	mu       sync.RWMutex
	queue    chan *Task
	maxSize  int
	totalEnq atomic.Int64
	totalDeq atomic.Int64
	totalRej atomic.Int64
	stopCh   chan struct{}
	stopOnce sync.Once
}

// NewTaskQueue creates a bounded task queue.
func NewTaskQueue(maxSize int) *TaskQueue {
	if maxSize <= 0 {
		maxSize = 10000
	}
	return &TaskQueue{
		queue:   make(chan *Task, maxSize),
		maxSize: maxSize,
		stopCh:  make(chan struct{}),
	}
}

// Enqueue adds a task to the queue. Returns false if the queue is full or stopped.
func (tq *TaskQueue) Enqueue(task *Task) bool {
	if task == nil {
		return false
	}

	select {
	case <-tq.stopCh:
		return false
	default:
	}

	task.Created = time.Now()

	select {
	case tq.queue <- task:
		tq.totalEnq.Add(1)
		return true
	case <-tq.stopCh:
		return false
	default:
		// Queue full — reject immediately
		tq.totalRej.Add(1)
		return false
	}
}

// EnqueueWait adds a task, waiting until space is available or context is cancelled.
func (tq *TaskQueue) EnqueueWait(ctx context.Context, task *Task) bool {
	if task == nil {
		return false
	}
	task.Created = time.Now()

	select {
	case tq.queue <- task:
		tq.totalEnq.Add(1)
		return true
	case <-tq.stopCh:
		return false
	case <-ctx.Done():
		tq.totalRej.Add(1)
		return false
	}
}

// Dequeue removes a task from the queue. Returns nil if the queue is empty or stopped.
func (tq *TaskQueue) Dequeue() *Task {
	select {
	case task := <-tq.queue:
		tq.totalDeq.Add(1)
		return task
	case <-tq.stopCh:
		return nil
	default:
		return nil
	}
}

// DequeueWait waits for a task or stop signal.
func (tq *TaskQueue) DequeueWait(ctx context.Context) *Task {
	select {
	case task := <-tq.queue:
		tq.totalDeq.Add(1)
		return task
	case <-tq.stopCh:
		return nil
	case <-ctx.Done():
		return nil
	}
}

// Len returns the current queue depth.
func (tq *TaskQueue) Len() int {
	return len(tq.queue)
}

// Cap returns the queue capacity.
func (tq *TaskQueue) Cap() int {
	return tq.maxSize
}

// Stats returns queue statistics.
func (tq *TaskQueue) Stats() QueueStats {
	return QueueStats{
		Enqueued: tq.totalEnq.Load(),
		Dequeued: tq.totalDeq.Load(),
		Rejected: tq.totalRej.Load(),
		Pending:  int64(len(tq.queue)),
		Capacity: int64(tq.maxSize),
	}
}

// Stop signals the queue to stop accepting tasks and wake up waiting dequeue calls.
func (tq *TaskQueue) Stop() {
	tq.stopOnce.Do(func() {
		close(tq.stopCh)
	})
}

// QueueStats holds queue statistics.
type QueueStats struct {
	Enqueued int64
	Dequeued int64
	Rejected int64
	Pending  int64
	Capacity int64
}

func (qs QueueStats) String() string {
	return fmt.Sprintf("enqueued=%d dequeued=%d rejected=%d pending=%d/%d",
		qs.Enqueued, qs.Dequeued, qs.Rejected, qs.Pending, qs.Capacity)
}
