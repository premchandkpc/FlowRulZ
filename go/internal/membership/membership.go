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
)

type NodeInfo struct {
	ID        string
	Address   string
	IsAlive   bool
	LastSeen  time.Time
}

type Membership struct {
	mu              sync.RWMutex
	nodes           map[string]*NodeInfo
	heartbeatTimeout time.Duration
}

func New() *Membership {
	return &Membership{
		nodes:            make(map[string]*NodeInfo),
		heartbeatTimeout: DefaultHeartbeatTimeout,
	}
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
	defer m.mu.Unlock()
	now := time.Now()
	for id, n := range m.nodes {
		if n.IsAlive && now.Sub(n.LastSeen) > m.heartbeatTimeout {
			n.IsAlive = false
			log.Printf("membership: node %s timed out (last seen %v ago)", id, now.Sub(n.LastSeen))
		}
	}
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
