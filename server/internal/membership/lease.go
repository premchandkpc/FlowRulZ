package membership

import (
	"context"
	"log"
	"time"
)

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
