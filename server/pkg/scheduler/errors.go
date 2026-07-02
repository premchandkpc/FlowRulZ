package scheduler

import "errors"

var (
	ErrQueueFull = errors.New("scheduler queue is full")
	ErrStopped   = errors.New("scheduler is stopped")
)
