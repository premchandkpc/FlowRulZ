package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/premchandkpc/FlowRulZ/server/internal/observability"
)

var ErrQueueFull = errors.New("scheduler: queue full")

type Priority int

const (
	PriorityFast   Priority = 0
	PriorityNormal Priority = 1
	PriorityHeavy  Priority = 2
)

type Task struct {
	ID       string
	Priority Priority
	Body     []byte
	Deadline time.Time
	Execute  func(ctx context.Context, task *Task) ([]byte, error)
	ResultCh chan TaskResult
}

type TaskResult struct {
	Output []byte
	Error  error
}

type LaneConfig struct {
	Name          Priority
	MaxConcurrent int
	QueueSize     int
	RejectOnFull  bool
}

type Scheduler struct {
	mu       sync.RWMutex
	lanes    map[Priority]*lane
	started  bool
	stopCh   chan struct{}
	totalEnq atomic.Int64
	totalDeq atomic.Int64
	totalRej atomic.Int64

	// Metrics
	metrics          *observability.MetricsCollector
	enqueueCount     *observability.Counter
	dequeueCount     *observability.Counter
	rejectCount      *observability.Counter
	activeWorkers    *observability.Gauge
	latencyHistogram *observability.Histogram
}

var DefaultLanes = []LaneConfig{
	{Name: PriorityFast, MaxConcurrent: 50, QueueSize: 5000, RejectOnFull: false},
	{Name: PriorityNormal, MaxConcurrent: 20, QueueSize: 2000, RejectOnFull: false},
	{Name: PriorityHeavy, MaxConcurrent: 5, QueueSize: 500, RejectOnFull: true},
}

func New(lanes []LaneConfig) *Scheduler {
	if lanes == nil {
		lanes = DefaultLanes
	}
	s := &Scheduler{
		lanes:  make(map[Priority]*lane),
		stopCh: make(chan struct{}),
	}
	for _, lc := range lanes {
		s.lanes[lc.Name] = newLane(lc, s.stopCh)
	}
	return s
}

// NewWithMetrics creates a scheduler with observability metrics.
func NewWithMetrics(lanes []LaneConfig, mc *observability.MetricsCollector) *Scheduler {
	s := New(lanes)
	if mc != nil {
		s.metrics = mc
		s.enqueueCount = mc.Counter("scheduler.enqueue_total")
		s.dequeueCount = mc.Counter("scheduler.dequeue_total")
		s.rejectCount = mc.Counter("scheduler.reject_total")
		s.activeWorkers = mc.Gauge("scheduler.active_workers")
		s.latencyHistogram = mc.Histogram("scheduler.exec_latency_ms", []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000})
	}
	return s
}

func (s *Scheduler) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return nil
	}
	s.started = true
	s.mu.Unlock()

	for _, l := range s.lanes {
		l.wg.Add(l.cfg.MaxConcurrent)
	}
	for p, l := range s.lanes {
		go s.laneWorker(ctx, p, l)
	}

	slog.Info("scheduler: started", "lanes", len(s.lanes))
	return nil
}

func (s *Scheduler) Stop() error {
	s.mu.Lock()
	if !s.started {
		s.mu.Unlock()
		return nil
	}
	s.started = false
	close(s.stopCh)
	s.mu.Unlock()

	for _, l := range s.lanes {
		l.wg.Wait()
	}
	return nil
}

func (s *Scheduler) EnqueueTask(task *Task) error {
	if task == nil {
		return errors.New("scheduler: nil task")
	}
	if task.Priority < PriorityFast || task.Priority > PriorityHeavy {
		task.Priority = PriorityNormal
	}

	l, ok := s.lanes[task.Priority]
	if !ok {
		return errors.New("scheduler: unknown priority")
	}

	if !l.enqueue(task) {
		s.totalRej.Add(1)
		if s.rejectCount != nil {
			s.rejectCount.Inc()
		}
		return ErrQueueFull
	}
	s.totalEnq.Add(1)
	if s.enqueueCount != nil {
		s.enqueueCount.Inc()
	}
	return nil
}

func (s *Scheduler) EnqueueAndWait(ctx context.Context, task *Task) ([]byte, error) {
	task.ResultCh = make(chan TaskResult, 1)
	if err := s.EnqueueTask(task); err != nil {
		return nil, err
	}
	select {
	case res := <-task.ResultCh:
		return res.Output, res.Error
	case <-ctx.Done():
		go func() {
			select {
			case <-task.ResultCh:
			case <-s.stopCh:
			}
		}()
		return nil, ctx.Err()
	}
}

func (s *Scheduler) laneWorker(ctx context.Context, _ Priority, l *lane) {
	for i := 0; i < l.cfg.MaxConcurrent; i++ {
		go s.slotWorker(ctx, l)
	}
}

func (s *Scheduler) slotWorker(ctx context.Context, l *lane) {
	defer l.wg.Done()

	for {
		task := s.dequeueOrSteal(ctx, l)
		if task == nil {
			return
		}
		s.execTask(ctx, task)
	}
}

func (s *Scheduler) dequeueOrSteal(ctx context.Context, myLane *lane) *Task {
	select {
	case <-s.stopCh:
		return nil
	case <-ctx.Done():
		return nil
	case task := <-myLane.queue:
		return task
	default:
	}

	for p := PriorityHeavy; p >= PriorityFast; p-- {
		l, ok := s.lanes[p]
		if !ok || l == myLane {
			continue
		}
		select {
		case task := <-l.queue:
			return task
		default:
		}
	}

	select {
	case <-s.stopCh:
		return nil
	case <-ctx.Done():
		return nil
	case task := <-myLane.queue:
		return task
	}
}

func (s *Scheduler) execTask(ctx context.Context, task *Task) {
	s.totalDeq.Add(1)
	if s.dequeueCount != nil {
		s.dequeueCount.Inc()
	}
	if s.activeWorkers != nil {
		s.activeWorkers.Add(1)
		defer s.activeWorkers.Add(-1)
	}

	start := time.Now()
	execCtx := ctx
	if !task.Deadline.IsZero() {
		var cancel context.CancelFunc
		execCtx, cancel = context.WithDeadline(ctx, task.Deadline)
		defer cancel()
	}

	defer func() {
		if r := recover(); r != nil {
			slog.Error("task panicked", "task_id", task.ID, "panic", r)
			if task.ResultCh != nil {
				task.ResultCh <- TaskResult{Error: fmt.Errorf("task panicked: %v", r)}
			}
		}
		if s.latencyHistogram != nil {
			s.latencyHistogram.Observe(float64(time.Since(start).Milliseconds()))
		}
	}()

	out, err := task.Execute(execCtx, task)
	if task.ResultCh != nil {
		task.ResultCh <- TaskResult{Output: out, Error: err}
	}
}
