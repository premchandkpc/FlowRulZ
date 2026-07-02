package scheduler

import "sync"

type lane struct {
	cfg    LaneConfig
	queue  chan *Task
	wg     sync.WaitGroup
	stopCh chan struct{}
}

func newLane(cfg LaneConfig, stopCh chan struct{}) *lane {
	return &lane{cfg: cfg, queue: make(chan *Task, cfg.QueueSize), stopCh: stopCh}
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
	select {
	case l.queue <- task:
		return true
	case <-l.stopCh:
		return false
	}
}
