package agent

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// AgentState represents the current state of an agent.
type AgentState int32

const (
	StateIdle    AgentState = iota
	StateRunning
	StateDraining
	StateStopped
)

func (s AgentState) String() string {
	switch s {
	case StateIdle:
		return "idle"
	case StateRunning:
		return "running"
	case StateDraining:
		return "draining"
	case StateStopped:
		return "stopped"
	default:
		return "unknown"
	}
}

// Agent is a single worker that processes tasks in a loop.
type Agent struct {
	id       string
	queue    *TaskQueue
	state    atomic.Int32
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	// Health tracking
	lastTaskTime  atomic.Value // time.Time
	tasksExecuted atomic.Int64
	tasksFailed   atomic.Int64
	panicCount    atomic.Int64
	startTime     time.Time

	// Configuration
	execTimeout time.Duration
	onPanic     func(id string, r any)
}

// AgentOption configures an agent.
type AgentOption func(*Agent)

// WithExecTimeout sets the default execution timeout for tasks.
func WithExecTimeout(d time.Duration) AgentOption {
	return func(a *Agent) { a.execTimeout = d }
}

// WithPanicHandler sets a custom panic handler.
func WithPanicHandler(fn func(id string, r any)) AgentOption {
	return func(a *Agent) { a.onPanic = fn }
}

// NewAgent creates a new agent with the given ID and task queue.
func NewAgent(id string, queue *TaskQueue, opts ...AgentOption) *Agent {
	a := &Agent{
		id:          id,
		queue:       queue,
		stopCh:      make(chan struct{}),
		startTime:   time.Now(),
		execTimeout: 30 * time.Second,
	}
	a.lastTaskTime.Store(time.Time{})
	for _, opt := range opts {
		opt(a)
	}
	return a
}

// ID returns the agent's unique identifier.
func (a *Agent) ID() string { return a.id }

// State returns the agent's current state.
func (a *Agent) State() AgentState {
	return AgentState(a.state.Load())
}

// Stats returns the agent's statistics.
func (a *Agent) Stats() AgentStats {
	lastTask, _ := a.lastTaskTime.Load().(time.Time)
	return AgentStats{
		ID:            a.id,
		State:         a.State().String(),
		TasksExecuted: a.tasksExecuted.Load(),
		TasksFailed:   a.tasksFailed.Load(),
		PanicCount:    a.panicCount.Load(),
		StartTime:     a.startTime,
		LastTaskTime:  lastTask,
		Uptime:        time.Since(a.startTime),
	}
}

// Submit adds a task directly to this agent's queue.
func (a *Agent) Submit(task *Task) bool {
	return a.queue.Enqueue(task)
}

// Start begins the agent's event loop. It blocks until Stop() is called.
func (a *Agent) Start(ctx context.Context) {
	a.wg.Add(1)
	defer a.wg.Done()

	a.setState(StateRunning)
	slog.Info("agent: started", "agent_id", a.id)

	for {
		select {
		case <-a.stopCh:
			a.setState(StateDraining)
			a.drainRemaining()
			a.setState(StateStopped)
			slog.Info("agent: stopped", "agent_id", a.id,
				"tasks_executed", a.tasksExecuted.Load(),
				"tasks_failed", a.tasksFailed.Load())
			return
		default:
		}

		task := a.queue.DequeueWait(ctx)
		if task == nil {
			// Context cancelled or queue stopped
			select {
			case <-a.stopCh:
				continue // Will drain on next iteration
			default:
				if ctx.Err() != nil {
					a.setState(StateStopped)
					return
				}
				continue
			}
		}

		a.executeTask(ctx, task)
	}
}

// Stop signals the agent to stop and waits for it to finish.
func (a *Agent) Stop() {
	a.stopOnce.Do(func() {
		close(a.stopCh)
	})
	a.wg.Wait()
}

// StopAsync signals the agent to stop without waiting.
func (a *Agent) StopAsync() {
	a.stopOnce.Do(func() {
		close(a.stopCh)
	})
}

// Wait waits for the agent to stop.
func (a *Agent) Wait() {
	a.wg.Wait()
}

func (a *Agent) executeTask(ctx context.Context, task *Task) {
	a.setState(StateRunning)
	a.lastTaskTime.Store(time.Now())

	// Apply per-task timeout
	taskCtx := ctx
	var cancel context.CancelFunc
	if task.Created.Add(a.execTimeout).After(time.Now()) {
		taskCtx, cancel = context.WithTimeout(ctx, a.execTimeout)
		defer cancel()
	}

	// Catch panics to prevent agent death
	defer func() {
		if r := recover(); r != nil {
			a.panicCount.Add(1)
			a.tasksFailed.Add(1)
			slog.Error("agent: task panic",
				"agent_id", a.id,
				"task_id", task.ID,
				"panic", r)
			if a.onPanic != nil {
				a.onPanic(a.id, r)
			}
			if task.ResultCh != nil {
				task.ResultCh <- TaskResult{
					Error: fmt.Errorf("agent %s: task panicked: %v", a.id, r),
				}
			}
		}
	}()

	// Call task.Execute
	out, err := task.Execute(taskCtx, task)
	if err != nil {
		a.tasksFailed.Add(1)
	} else {
		a.tasksExecuted.Add(1)
	}

	if task.ResultCh != nil {
		task.ResultCh <- TaskResult{Output: out, Error: err}
	}
}

func (a *Agent) drainRemaining() {
	for {
		select {
		case task := <-a.queue.queue:
			a.tasksExecuted.Add(1)
			if task.ResultCh != nil {
				task.ResultCh <- TaskResult{
					Error: fmt.Errorf("agent %s: stopped", a.id),
				}
			}
		default:
			return
		}
	}
}

func (a *Agent) setState(state AgentState) {
	a.state.Store(int32(state))
}

// AgentStats holds agent statistics.
type AgentStats struct {
	ID            string        `json:"id"`
	State         string        `json:"state"`
	TasksExecuted int64         `json:"tasks_executed"`
	TasksFailed   int64         `json:"tasks_failed"`
	PanicCount    int64         `json:"panic_count"`
	StartTime     time.Time     `json:"start_time"`
	LastTaskTime  time.Time     `json:"last_task_time"`
	Uptime        time.Duration `json:"uptime"`
}
