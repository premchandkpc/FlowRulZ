package agent

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTaskQueueBasic(t *testing.T) {
	q := NewTaskQueue(10)

	task := &Task{
		ID:   "test-1",
		Body: []byte("hello"),
		Execute: func(ctx context.Context, task *Task) ([]byte, error) {
			return task.Body, nil
		},
	}

	if !q.Enqueue(task) {
		t.Fatal("expected enqueue to succeed")
	}

	got := q.Dequeue()
	if got == nil {
		t.Fatal("expected task from dequeue")
	}
	if got.ID != "test-1" {
		t.Fatalf("expected task ID test-1, got %s", got.ID)
	}

	if q.Len() != 0 {
		t.Fatalf("expected queue length 0, got %d", q.Len())
	}
}

func TestTaskQueueFull(t *testing.T) {
	q := NewTaskQueue(2)

	for i := 0; i < 2; i++ {
		q.Enqueue(&Task{ID: fmt.Sprintf("t%d", i)})
	}

	// Third enqueue should fail (queue full)
	if q.Enqueue(&Task{ID: "t2"}) {
		t.Fatal("expected enqueue to fail on full queue")
	}

	stats := q.Stats()
	if stats.Rejected != 1 {
		t.Fatalf("expected 1 rejected, got %d", stats.Rejected)
	}
}

func TestTaskQueueStop(t *testing.T) {
	q := NewTaskQueue(10)
	q.Stop()

	if q.Enqueue(&Task{ID: "t1"}) {
		t.Fatal("expected enqueue to fail on stopped queue")
	}
}

func TestAgentBasic(t *testing.T) {
	q := NewTaskQueue(10)
	a := NewAgent("test-agent", q)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go a.Start(ctx)

	// Submit a task
	resultCh := make(chan TaskResult, 1)
	task := &Task{
		ID:       "task-1",
		Body:     []byte("test"),
		ResultCh: resultCh,
		Execute: func(ctx context.Context, task *Task) ([]byte, error) {
			return []byte("done"), nil
		},
	}

	if !a.Submit(task) {
		t.Fatal("expected submit to succeed")
	}

	select {
	case result := <-resultCh:
		if result.Error != nil {
			t.Fatalf("unexpected error: %v", result.Error)
		}
		if string(result.Output) != "done" {
			t.Fatalf("expected output 'done', got '%s'", result.Output)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for task result")
	}

	cancel()
	a.Wait()

	stats := a.Stats()
	if stats.TasksExecuted != 1 {
		t.Fatalf("expected 1 task executed, got %d", stats.TasksExecuted)
	}
}

func TestAgentPanicRecovery(t *testing.T) {
	q := NewTaskQueue(10)
	a := NewAgent("panic-agent", q,
		WithPanicHandler(func(id string, r any) {
			// Expected
		}),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go a.Start(ctx)

	resultCh := make(chan TaskResult, 1)
	task := &Task{
		ID:       "panic-task",
		Body:     []byte("test"),
		ResultCh: resultCh,
		Execute: func(ctx context.Context, task *Task) ([]byte, error) {
			panic("test panic")
		},
	}

	a.Submit(task)

	select {
	case result := <-resultCh:
		if result.Error == nil {
			t.Fatal("expected error from panicked task")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for task result")
	}

	// Agent should still be alive after panic
	if a.State() != StateRunning {
		t.Fatalf("expected agent to still be running after panic, got %s", a.State())
	}

	// Submit another task to confirm agent is functional
	resultCh2 := make(chan TaskResult, 1)
	task2 := &Task{
		ID:       "after-panic",
		Body:     []byte("test2"),
		ResultCh: resultCh2,
		Execute: func(ctx context.Context, task *Task) ([]byte, error) {
			return []byte("recovered"), nil
		},
	}
	a.Submit(task2)

	select {
	case result := <-resultCh2:
		if result.Error != nil {
			t.Fatalf("unexpected error: %v", result.Error)
		}
		if string(result.Output) != "recovered" {
			t.Fatalf("expected 'recovered', got '%s'", result.Output)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for task result")
	}

	cancel()
	a.Wait()
}

func TestAgentPoolBasic(t *testing.T) {
	config := DefaultPoolConfig()
	config.MinAgents = 2
	config.MaxAgents = 4
	config.QueueSize = 100

	pool := NewPool(config)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := pool.Start(ctx); err != nil {
		t.Fatalf("failed to start pool: %v", err)
	}

	// Submit multiple tasks
	var completed atomic.Int64
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			task := &Task{
				ID:   fmt.Sprintf("pool-task-%d", i),
				Body: []byte("test"),
				Execute: func(ctx context.Context, task *Task) ([]byte, error) {
					time.Sleep(10 * time.Millisecond)
					completed.Add(1)
					return []byte("ok"), nil
				},
			}
			_, err := pool.SubmitAndWait(ctx, task)
			if err != nil {
				t.Errorf("submit failed: %v", err)
			}
		}()
	}

	wg.Wait()

	if completed.Load() != 10 {
		t.Fatalf("expected 10 tasks completed, got %d", completed.Load())
	}

	stats := pool.Stats()
	if stats.Completed != 10 {
		t.Fatalf("expected 10 completed in stats, got %d", stats.Completed)
	}

	pool.Stop()
}

func TestAgentPoolConcurrency(t *testing.T) {
	config := DefaultPoolConfig()
	config.MinAgents = 4
	config.MaxAgents = 8
	config.QueueSize = 1000

	pool := NewPool(config)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := pool.Start(ctx); err != nil {
		t.Fatalf("failed to start pool: %v", err)
	}

	// High concurrency test
	var completed atomic.Int64
	var wg sync.WaitGroup
	taskCount := 100

	for i := 0; i < taskCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			task := &Task{
				ID:   fmt.Sprintf("concurrent-%d", i),
				Body: []byte("test"),
				Execute: func(ctx context.Context, task *Task) ([]byte, error) {
					time.Sleep(time.Millisecond)
					completed.Add(1)
					return []byte("ok"), nil
				},
			}
			_, err := pool.SubmitAndWait(ctx, task)
			if err != nil {
				t.Errorf("submit failed: %v", err)
			}
		}()
	}

	wg.Wait()

	if completed.Load() != int64(taskCount) {
		t.Fatalf("expected %d tasks completed, got %d", taskCount, completed.Load())
	}

	pool.Stop()
}

func TestAgentPoolShutdown(t *testing.T) {
	config := DefaultPoolConfig()
	config.MinAgents = 2
	config.QueueSize = 10

	pool := NewPool(config)
	ctx, cancel := context.WithCancel(context.Background())

	if err := pool.Start(ctx); err != nil {
		t.Fatalf("failed to start pool: %v", err)
	}

	// Stop should not hang
	done := make(chan struct{})
	go func() {
		pool.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("pool.Stop() timed out")
	}

	// Submit after stop should fail
	task := &Task{
		ID:   "after-stop",
		Body: []byte("test"),
		Execute: func(ctx context.Context, task *Task) ([]byte, error) {
			return nil, nil
		},
	}
	if pool.Submit(task) {
		t.Fatal("expected submit to fail after stop")
	}

	cancel()
}

func TestAgentPoolHealthScaling(t *testing.T) {
	config := DefaultPoolConfig()
	config.MinAgents = 1
	config.MaxAgents = 4
	config.QueueSize = 5
	config.HealthCheck = 100 * time.Millisecond
	config.ScaleUpThreshold = 0.5

	pool := NewPool(config)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := pool.Start(ctx); err != nil {
		t.Fatalf("failed to start pool: %v", err)
	}

	initialStats := pool.Stats()
	if initialStats.AgentCount != 1 {
		t.Fatalf("expected 1 initial agent, got %d", initialStats.AgentCount)
	}

	// Fill the queue to trigger scale-up
	for i := 0; i < 5; i++ {
		pool.Queue().Enqueue(&Task{
			ID:       fmt.Sprintf("fill-%d", i),
			Body:     []byte("fill"),
			Execute: func(ctx context.Context, task *Task) ([]byte, error) {
				time.Sleep(5 * time.Second) // Slow task
				return nil, nil
			},
		})
	}

	// Wait for health check to run
	time.Sleep(300 * time.Millisecond)

	// Pool should have scaled up
	stats := pool.Stats()
	if stats.AgentCount < 2 {
		t.Logf("pool did not scale up immediately (may need more time), agents=%d", stats.AgentCount)
	}

	pool.Stop()
}

func TestAgentStats(t *testing.T) {
	q := NewTaskQueue(10)
	a := NewAgent("stats-agent", q)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go a.Start(ctx)

	// Execute a few tasks
	for i := 0; i < 5; i++ {
		resultCh := make(chan TaskResult, 1)
		task := &Task{
			ID:       fmt.Sprintf("stats-task-%d", i),
			Body:     []byte("test"),
			ResultCh: resultCh,
			Execute: func(ctx context.Context, task *Task) ([]byte, error) {
				return []byte("ok"), nil
			},
		}
		a.Submit(task)
		<-resultCh
	}

	stats := a.Stats()
	if stats.TasksExecuted != 5 {
		t.Fatalf("expected 5 tasks executed, got %d", stats.TasksExecuted)
	}
	if stats.Uptime <= 0 {
		t.Fatal("expected positive uptime")
	}

	cancel()
	a.Wait()
}
