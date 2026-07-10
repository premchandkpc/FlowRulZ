package membership

import (
	"sort"
	"sync"
	"time"

	pkgmembership "github.com/premchandkpc/FlowRulZ/server/pkg/membership"
)

const (
	DefaultHeartbeatInterval = 3 * time.Second
	DefaultHeartbeatTimeout  = 10 * time.Second
	DefaultLeaderLease       = 8 * time.Second
)

var _ pkgmembership.Membership = (*Membership)(nil)

type LeaseCallback func(leaderID string)

type Membership struct {
	mu               sync.RWMutex
	nodes            map[string]*pkgmembership.NodeInfo
	heartbeatTimeout time.Duration
	leaderLease      time.Duration
	leaseCallback    LeaseCallback
}

func New() *Membership {
	return &Membership{
		nodes:            make(map[string]*pkgmembership.NodeInfo),
		heartbeatTimeout: DefaultHeartbeatTimeout,
		leaderLease:      DefaultLeaderLease,
	}
}

func (m *Membership) SetLeaderLease(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.leaderLease = d
}

func (m *Membership) OnLeaseExpiry(cb func(leaderID string)) pkgmembership.CancelFunc {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.leaseCallback = cb
	return func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.leaseCallback = nil
	}
}

func (m *Membership) Add(id, address string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[id] = &pkgmembership.NodeInfo{
		ID:       id,
		Address:  address,
		IsAlive:  true,
		LastSeen: time.Now(),
	}
}

func (m *Membership) Remove(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.nodes, id)
}

func (m *Membership) MarkDead(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n, ok := m.nodes[id]; ok {
		n.IsAlive = false
	}
}

func (m *Membership) MarkAlive(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n, ok := m.nodes[id]; ok {
		n.IsAlive = true
		n.LastSeen = time.Now()
	}
}

func (m *Membership) Heartbeat(id, address string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	if n, ok := m.nodes[id]; ok {
		n.IsAlive = true
		n.LastSeen = now
		if address != "" {
			n.Address = address
		}
	} else {
		m.nodes[id] = &pkgmembership.NodeInfo{
			ID:       id,
			Address:  address,
			IsAlive:  true,
			LastSeen: now,
		}
	}
}

func (m *Membership) AliveCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	count := 0
	for _, n := range m.nodes {
		if n.IsAlive {
			count++
		}
	}
	return count
}

func (m *Membership) AliveNodes() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []string
	for _, n := range m.nodes {
		if n.IsAlive {
			out = append(out, n.ID)
		}
	}
	sort.Strings(out)
	return out
}

// LeaderID returns the lexicographically smallest alive node ID.
// WARNING: This is a deterministic heuristic for single-node deployments only.
// It does NOT provide consensus and MUST NOT be used for leader election
// in multi-node clusters — use RaftCluster for that.
func (m *Membership) LeaderID() string {
	nodes := m.AliveNodes()
	if len(nodes) == 0 {
		return ""
	}
	return nodes[0]
}

func (m *Membership) leaderIDLocked() string {
	var leader string
	for id, n := range m.nodes {
		if n.IsAlive && (leader == "" || id < leader) {
			leader = id
		}
	}
	return leader
}

func (m *Membership) Snapshot() []pkgmembership.NodeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]pkgmembership.NodeInfo, 0, len(m.nodes))
	for _, n := range m.nodes {
		out = append(out, *n)
	}
	return out
}

func (m *Membership) Lookup(id string) *pkgmembership.NodeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	n, ok := m.nodes[id]
	if !ok {
		return nil
	}
	cp := *n
	return &cp
}
