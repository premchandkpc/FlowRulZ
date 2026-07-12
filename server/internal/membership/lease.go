package membership

import (
	"context"
	"log/slog"
	"time"
)

func (m *Membership) evictStale() {
	m.mu.Lock()
	now := time.Now()
	leaderBefore := m.leaderIDLocked()
	var expiredLeader string
	var cb func(string)
	for id, n := range m.nodes {
		if n.IsAlive && now.Sub(n.LastSeen) > m.heartbeatTimeout {
			n.IsAlive = false
			slog.Warn("membership: node timed out", "node", id, "last_seen_ago", now.Sub(n.LastSeen))
			if id == leaderBefore {
				expiredLeader = id
			}
		}
	}
	cb = m.leaseCallback
	m.mu.Unlock()

	if expiredLeader != "" && cb != nil {
		slog.Warn("membership: leader lost due to heartbeat timeout, notifying lease callback", "leader", expiredLeader)
		cb(expiredLeader)
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
		var lastNotified string
		for {
			select {
			case <-ticker.C:
				m.mu.Lock()
				leaderID := m.leaderIDLocked()
				if leaderID == "" {
					m.mu.Unlock()
					lastNotified = ""
					continue
				}
				n, ok := m.nodes[leaderID]
				if !ok || !n.IsAlive {
					m.mu.Unlock()
					lastNotified = ""
					continue
				}
				if time.Since(n.LastSeen) > m.leaderLease {
					n.IsAlive = false
					slog.Warn("membership: leader lease expired", "leader", leaderID, "last_seen_ago", time.Since(n.LastSeen))
					cb := m.leaseCallback
					m.mu.Unlock()
					if cb != nil && lastNotified != leaderID {
						lastNotified = leaderID
						cb(leaderID)
					}
				} else {
					m.mu.Unlock()
					lastNotified = ""
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
