package scheduler

import (
	"context"
	"time"

	pkgscheduler "github.com/premchandkpc/FlowRulZ/server/pkg/scheduler"
)

var _ pkgscheduler.Scheduler = (*Scheduler)(nil)

func (s *Scheduler) ID() string {
	return "scheduler"
}

func (s *Scheduler) Enqueue(ctx *pkgscheduler.ExecutionContext) error {
	task := contextToTask(ctx)
	return s.EnqueueTask(task)
}

func (s *Scheduler) Snapshot() pkgscheduler.SchedulerSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	laneCounts := make(map[pkgscheduler.Lane]int)
	readyTotal := 0
	activeTotal := 0
	for p, l := range s.lanes {
		n := len(l.queue)
		laneCounts[pkgscheduler.Lane(p)] = n
		readyTotal += n
		activeTotal += l.cfg.MaxConcurrent
	}

	return pkgscheduler.SchedulerSnapshot{
		ReadyQueueLen:   readyTotal,
		WaitingQueueLen: 0,
		ActiveWorkers:   activeTotal,
		LaneCounts:      laneCounts,
	}
}

func (s *Scheduler) ExecCount() int64 {
	return s.totalEnq.Load()
}

func contextToTask(ctx *pkgscheduler.ExecutionContext) *Task {
	priority := PriorityNormal
	switch ctx.Lane {
	case pkgscheduler.LaneFast:
		priority = PriorityFast
	case pkgscheduler.LaneHeavy:
		priority = PriorityHeavy
	}

	var deadline time.Time
	planBytes := ctx.Plan.PlanBytes
	incomingBody := ctx.IncomingBody

	return &Task{
		ID:       string(ctx.ID),
		Priority: priority,
		Body:     incomingBody,
		Deadline: deadline,
		Execute: func(execCtx context.Context, task *Task) ([]byte, error) {
			if len(planBytes) == 0 {
				return nil, nil
			}
			return planBytes, nil
		},
	}
}
