package scheduler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestEnqueueAndExecute(t *testing.T) {
	s := New(nil)
	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)
	defer cancel()
	defer s.Stop()

	executed := make(chan struct{})
	task := &Task{
		ID:       "test-1",
		Priority: PriorityFast,
		Body:     []byte("hello"),
		Execute: func(ctx context.Context, task *Task) ([]byte, error) {
			close(executed)
			return []byte("result"), nil
		},
	}

	err := s.Enqueue(task)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-executed:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for execution")
	}
}

func TestEnqueueAndWait(t *testing.T) {
	s := New(nil)
	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)
	defer cancel()
	defer s.Stop()

	task := &Task{
		ID:       "test-2",
		Priority: PriorityFast,
		Body:     []byte("hello"),
		Execute: func(ctx context.Context, task *Task) ([]byte, error) {
			return []byte("result"), nil
		},
	}

	out, err := s.EnqueueAndWait(ctx, task)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "result" {
		t.Fatalf("expected 'result', got %s", out)
	}
}

func TestPriorityOrdering(t *testing.T) {
	s := New([]LaneConfig{
		{Name: PriorityFast, MaxConcurrent: 1, QueueSize: 100, RejectOnFull: false},
		{Name: PriorityNormal, MaxConcurrent: 1, QueueSize: 100, RejectOnFull: false},
		{Name: PriorityHeavy, MaxConcurrent: 1, QueueSize: 100, RejectOnFull: false},
	})

	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)
	defer cancel()
	defer s.Stop()

	var execOrder []string
	done := make(chan struct{})

	task := &Task{
		ID: "blocker", Priority: PriorityFast,
		Execute: func(ctx context.Context, task *Task) ([]byte, error) {
			<-done
			return nil, nil
		},
	}
	s.Enqueue(task)

	s.Enqueue(&Task{ID: "heavy", Priority: PriorityHeavy, Execute: func(ctx context.Context, task *Task) ([]byte, error) {
		execOrder = append(execOrder, "heavy")
		return nil, nil
	}})
	s.Enqueue(&Task{ID: "normal", Priority: PriorityNormal, Execute: func(ctx context.Context, task *Task) ([]byte, error) {
		execOrder = append(execOrder, "normal")
		return nil, nil
	}})
	s.Enqueue(&Task{ID: "fast", Priority: PriorityFast, Execute: func(ctx context.Context, task *Task) ([]byte, error) {
		execOrder = append(execOrder, "fast")
		return nil, nil
	}})

	time.Sleep(50 * time.Millisecond)
	close(done)
	time.Sleep(200 * time.Millisecond)

	if len(execOrder) != 3 {
		t.Fatalf("expected 3 executions, got %d", len(execOrder))
	}
}

func TestQueueFull(t *testing.T) {
	s := New([]LaneConfig{
		{Name: PriorityFast, MaxConcurrent: 1, QueueSize: 1, RejectOnFull: true},
	})

	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)
	defer cancel()
	defer s.Stop()

	// Fill semaphore with a long-running task
	started := make(chan struct{})
	block := make(chan struct{})
	s.Enqueue(&Task{ID: "blocker", Priority: PriorityFast,
		Execute: func(ctx context.Context, task *Task) ([]byte, error) {
			close(started)
			<-block
			return nil, nil
		},
	})
	<-started
	// At this point, sem is full (1/1). Queue is empty because worker already dequeued.

	// Fill the queue (size 1) with one more task
	s.Enqueue(&Task{ID: "fill", Priority: PriorityFast,
		Execute: func(ctx context.Context, task *Task) ([]byte, error) {
			return nil, nil
		},
	})
	// Queue is now full. Next enqueue should reject.
	err := s.Enqueue(&Task{ID: "reject", Priority: PriorityFast,
		Execute: func(ctx context.Context, task *Task) ([]byte, error) {
			return nil, nil
		},
	})
	if err == nil {
		t.Fatal("expected queue full error")
	}

	close(block)
}

func TestConcurrencyLimit(t *testing.T) {
	s := New([]LaneConfig{
		{Name: PriorityFast, MaxConcurrent: 2, QueueSize: 100, RejectOnFull: true},
	})

	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)
	defer cancel()
	defer s.Stop()

	var concurrent atomic.Int32
	var maxConcurrent atomic.Int32

	for i := 0; i < 10; i++ {
		s.Enqueue(&Task{
			ID:       string(rune('0' + i)),
			Priority: PriorityFast,
			Execute: func(ctx context.Context, task *Task) ([]byte, error) {
				v := concurrent.Add(1)
				if v > maxConcurrent.Load() {
					maxConcurrent.Store(v)
				}
				time.Sleep(50 * time.Millisecond)
				concurrent.Add(-1)
				return nil, nil
			},
		})
	}

	time.Sleep(300 * time.Millisecond)
	if maxConcurrent.Load() > 2 {
		t.Fatalf("expected max concurrency 2, got %d", maxConcurrent.Load())
	}
}

func TestNilTask(t *testing.T) {
	s := New(nil)
	err := s.Enqueue(nil)
	if err == nil {
		t.Fatal("expected error for nil task")
	}
}

func TestContextCancellation(t *testing.T) {
	s := New(nil)
	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)

	task := &Task{
		ID:       "cancel-test",
		Priority: PriorityFast,
		Execute: func(ctx context.Context, task *Task) ([]byte, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	ch := make(chan struct{})
	go func() {
		s.EnqueueAndWait(ctx, task)
		close(ch)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for cancellation")
	}

	s.Stop()
}
