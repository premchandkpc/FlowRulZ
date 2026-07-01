package partition

import (
	"log"
	"sort"
	"sync"
	"sync/atomic"
)

type RebalanceNotifier struct {
	manager   *Manager
	aliveFn   func() []string
	termFn    func() uint64
	notifyFn  func()
	mu        sync.Mutex
	lastNodes []string
}

func NewRebalanceNotifier(m *Manager, aliveFn func() []string, termFn func() uint64) *RebalanceNotifier {
	return &RebalanceNotifier{
		manager: m,
		aliveFn: aliveFn,
		termFn:  termFn,
	}
}

func (rn *RebalanceNotifier) SetNotify(fn func()) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.notifyFn = fn
}

func (rn *RebalanceNotifier) CheckAndRebalance() bool {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	nodes := rn.aliveFn()
	if len(nodes) == 0 {
		return false
	}

	sort.Strings(nodes)
	same := len(nodes) == len(rn.lastNodes)
	if same {
		for i, n := range nodes {
			if n != rn.lastNodes[i] {
				same = false
				break
			}
		}
	}
	if same {
		return false
	}

	rn.lastNodes = make([]string, len(nodes))
	copy(rn.lastNodes, nodes)
	log.Printf("partition: membership changed (%d nodes), triggering rebalance", len(nodes))

	rn.manager.Rebalance(nodes, rn.termFn())
	if rn.notifyFn != nil {
		rn.notifyFn()
	}
	return true
}

type atomicCounter struct {
	v atomic.Uint64
}

func (a *atomicCounter) inc() uint64 { return a.v.Add(1) }
func (a *atomicCounter) get() uint64 { return a.v.Load() }
