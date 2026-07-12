package partition

import (
	"log/slog"
	"sort"
	"sync"

	pkgpartition "github.com/premchandkpc/FlowRulZ/server/pkg/partition"
)

var _ pkgpartition.RebalanceNotifier = (*RebalanceNotifier)(nil)

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
	slog.Info("partition: membership changed, triggering rebalance", "node_count", len(nodes))

	rn.manager.Rebalance(nodes, rn.termFn())
	if rn.notifyFn != nil {
		rn.notifyFn()
	}
	return true
}
