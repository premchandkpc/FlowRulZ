package scheduler

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"
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
	mu        sync.Mutex
	lanes     map[Priority]*lane
	started   bool
	stopCh    chan struct{}
	totalEnq  atomic.Int64
	totalDeq  atomic.Int64
	totalRej  atomic.Int64
}

type lane struct {
	cfg     LaneConfig
	queue   chan *Task
	sem     chan struct{}
	wg      sync.WaitGroup
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
		s.lanes[lc.Name] = &lane{
			cfg:   lc,
			queue: make(chan *Task, lc.QueueSize),
			sem:   make(chan struct{}, lc.MaxConcurrent),
		}
	}
	return s
}

func (s *Scheduler) Start(ctx context.Context) {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return
	}
	s.started = true
	s.mu.Unlock()

	for p, l := range s.lanes {
		go s.laneWorker(ctx, p, l)
	}

	log.Printf("scheduler: started with %d lanes", len(s.lanes))
}

func (s *Scheduler) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.started {
		return
	}
	close(s.stopCh)
	for _, l := range s.lanes {
		l.wg.Wait()
	}
	s.started = false
}

func (s *Scheduler) Enqueue(task *Task) error {
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

	if l.cfg.RejectOnFull {
		select {
		case l.queue <- task:
			s.totalEnq.Add(1)
			return nil
		default:
			s.totalRej.Add(1)
			return ErrQueueFull
		}
	}

	l.queue <- task
	s.totalEnq.Add(1)
	return nil
}

func (s *Scheduler) EnqueueAndWait(ctx context.Context, task *Task) ([]byte, error) {
	task.ResultCh = make(chan TaskResult, 1)
	if err := s.Enqueue(task); err != nil {
		return nil, err
	}
	select {
	case res := <-task.ResultCh:
		return res.Output, res.Error
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *Scheduler) laneWorker(ctx context.Context, p Priority, l *lane) {
	l.wg.Add(1)
	defer l.wg.Done()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ctx.Done():
			return
		case l.sem <- struct{}{}:
			select {
			case <-s.stopCh:
				<-l.sem
				return
			case <-ctx.Done():
				<-l.sem
				return
			case task := <-l.queue:
				s.totalDeq.Add(1)
				go s.execTask(ctx, task, l)
			}
		}
	}
}

func (s *Scheduler) execTask(ctx context.Context, task *Task, l *lane) {
	defer func() { <-l.sem }()

	execCtx := ctx
	if !task.Deadline.IsZero() {
		var cancel context.CancelFunc
		execCtx, cancel = context.WithDeadline(ctx, task.Deadline)
		defer cancel()
	}

	out, err := task.Execute(execCtx, task)
	if task.ResultCh != nil {
		task.ResultCh <- TaskResult{Output: out, Error: err}
	}
}

func (s *Scheduler) QueuedCount(p Priority) int {
	l, ok := s.lanes[p]
	if !ok {
		return 0
	}
	return len(l.queue)
}

func (s *Scheduler) RunningCount(p Priority) int {
	l, ok := s.lanes[p]
	if !ok {
		return 0
	}
	return len(l.sem)
}

func (s *Scheduler) TotalEnqueued() int64 {
	return s.totalEnq.Load()
}

func (s *Scheduler) TotalDequeued() int64 {
	return s.totalDeq.Load()
}

func (s *Scheduler) TotalRejected() int64 {
	return s.totalRej.Load()
}

func PriorityForScore(score uint32) Priority {
	switch {
	case score < 10:
		return PriorityFast
	case score <= 50:
		return PriorityNormal
	default:
		return PriorityHeavy
	}
}
