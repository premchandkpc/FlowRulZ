package scheduler

import "sync"

type lane struct {
	cfg   LaneConfig
	queue chan *Task
	wg    sync.WaitGroup
}

func newLane(cfg LaneConfig) *lane {
	return &lane{cfg: cfg, queue: make(chan *Task, cfg.QueueSize)}
}

func (l *lane) enqueue(task *Task) bool {
	if l.cfg.RejectOnFull {
		select {
		case l.queue <- task:
			return true
		default:
			return false
		}
	}
	l.queue <- task
	return true
}
