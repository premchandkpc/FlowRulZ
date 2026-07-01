package scheduler

import "context"

type Scheduler interface {
	ID() string
	Start(ctx context.Context) error
	Stop() error
	Enqueue(ctx *ExecutionContext) error
	Snapshot() SchedulerSnapshot
	ExecCount() int64
}
