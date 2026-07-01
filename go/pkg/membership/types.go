package membership

import "time"

type NodeInfo struct {
	ID       string
	Address  string
	IsAlive  bool
	LastSeen time.Time
}

type CancelFunc func()
