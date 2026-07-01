package membership

import (
	"context"
	"log"
	"sort"
	"sync"
	"time"
)

const (
	DefaultHeartbeatInterval = 3 * time.Second
	DefaultHeartbeatTimeout  = 10 * time.Second
	DefaultLeaderLease       = 8 * time.Second // 2.5x heartbeat interval before leader considered dead
)

type NodeInfo struct {
	ID        string
	Address   string
	IsAlive   bool
	LastSeen  time.Time
}

type LeaseCallback func(leaderID string)

type Membership struct {
	mu               sync.RWMutex
	nodes            map[string]*NodeInfo
	heartbeatTimeout time.Duration
	leaderLease      time.Duration
	leaseCallback    LeaseCallback
}

func New() *Membership {
	return &Membership{
		nodes:            make(map[string]*NodeInfo),
		heartbeatTimeout: DefaultHeartbeatTimeout,
		leaderLease:      DefaultLeaderLease,
	}
}

func (m *Membership) SetLeaderLease(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.leaderLease = d
}

func (m *Membership) OnLeaseExpiry(cb LeaseCallback) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.leaseCallback = cb
}

func (m *Membership) Add(id, address string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[id] = &NodeInfo{
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
		m.nodes[id] = &NodeInfo{
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

func (m *Membership) LeaderID() string {
	nodes := m.AliveNodes()
	if len(nodes) == 0 {
		return ""
	}
	return nodes[0]
}

// leaderIDLocked returns the lowest-ID alive node; caller must hold m.mu
func (m *Membership) leaderIDLocked() string {
	var leader string
	for id, n := range m.nodes {
		if n.IsAlive && (leader == "" || id < leader) {
			leader = id
		}
	}
	return leader
}

func (m *Membership) Snapshot() []NodeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]NodeInfo, 0, len(m.nodes))
	for _, n := range m.nodes {
		out = append(out, *n)
	}
	return out
}

func (m *Membership) Lookup(id string) *NodeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	n, ok := m.nodes[id]
	if !ok {
		return nil
	}
	cp := *n
	return &cp
}

func (m *Membership) evictStale() {
	m.mu.Lock()
	now := time.Now()
	leaderBefore := m.leaderIDLocked()
	for id, n := range m.nodes {
		if n.IsAlive && now.Sub(n.LastSeen) > m.heartbeatTimeout {
			n.IsAlive = false
			log.Printf("membership: node %s timed out (last seen %v ago)", id, now.Sub(n.LastSeen))
		}
	}
	leaderAfter := m.leaderIDLocked()
	m.mu.Unlock()

	if leaderBefore != "" && leaderBefore != leaderAfter && m.leaseCallback != nil {
		log.Printf("membership: leader %s lost due to heartbeat timeout, notifying lease callback", leaderBefore)
		m.leaseCallback(leaderBefore)
	}
}

func (m *Membership) LeaderLastSeen() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	leaderID := m.leaderIDLocked()
	if leaderID == "" {
		return time.Time{}
	}
	n, ok := m.nodes[leaderID]
	if !ok {
		return time.Time{}
	}
	return n.LastSeen
}

func (m *Membership) StartLeaderLeaseChecker(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				m.mu.Lock()
				leaderID := m.leaderIDLocked()
				if leaderID == "" {
					m.mu.Unlock()
					continue
				}
				n, ok := m.nodes[leaderID]
				if !ok || !n.IsAlive {
					m.mu.Unlock()
					continue
				}
				if time.Since(n.LastSeen) > m.leaderLease {
					n.IsAlive = false
					log.Printf("membership: leader %s lease expired (last seen %v ago)", leaderID, time.Since(n.LastSeen))
					m.mu.Unlock()
					if m.leaseCallback != nil {
						m.leaseCallback(leaderID)
					}
				} else {
					m.mu.Unlock()
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (m *Membership) StartEviction(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				m.evictStale()
			case <-ctx.Done():
				return
			}
		}
	}()
}
