package membership

import (
	"context"
	"testing"
	"time"
)

func TestNewMembership(t *testing.T) {
	m := New()
	if m == nil {
		t.Fatal("expected non-nil membership")
	}
	if m.AliveCount() != 0 {
		t.Fatalf("expected 0 alive, got %d", m.AliveCount())
	}
}

func TestAddAndAlive(t *testing.T) {
	m := New()
	m.Add("node-a", "localhost:9001")
	m.Add("node-b", "localhost:9002")

	if m.AliveCount() != 2 {
		t.Fatalf("expected 2 alive, got %d", m.AliveCount())
	}
}

func TestRemove(t *testing.T) {
	m := New()
	m.Add("node-a", "localhost:9001")
	m.Remove("node-a")

	if m.AliveCount() != 0 {
		t.Fatalf("expected 0 after remove, got %d", m.AliveCount())
	}
}

func TestMarkDead(t *testing.T) {
	m := New()
	m.Add("node-a", "localhost:9001")
	m.MarkDead("node-a")

	if m.AliveCount() != 0 {
		t.Fatalf("expected 0 after dead, got %d", m.AliveCount())
	}
}

func TestMarkAlive(t *testing.T) {
	m := New()
	m.Add("node-a", "localhost:9001")
	m.MarkDead("node-a")
	m.MarkAlive("node-a")

	if m.AliveCount() != 1 {
		t.Fatalf("expected 1 after mark alive, got %d", m.AliveCount())
	}
}

func TestAliveNodes(t *testing.T) {
	m := New()
	m.Add("node-a", "localhost:9001")
	m.Add("node-b", "localhost:9002")
	m.MarkDead("node-b")

	nodes := m.AliveNodes()
	if len(nodes) != 1 || nodes[0] != "node-a" {
		t.Fatalf("expected [node-a], got %v", nodes)
	}
}

func TestSnapshot(t *testing.T) {
	m := New()
	m.Add("node-a", "localhost:9001")

	snap := m.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("expected 1 node, got %d", len(snap))
	}
	if snap[0].ID != "node-a" || snap[0].IsAlive != true {
		t.Fatalf("unexpected snapshot: %+v", snap[0])
	}
}

func TestLookup(t *testing.T) {
	m := New()
	m.Add("node-a", "localhost:9001")

	n := m.Lookup("node-a")
	if n == nil || n.ID != "node-a" {
		t.Fatal("expected to find node-a")
	}

	n = m.Lookup("node-b")
	if n != nil {
		t.Fatal("expected nil for unknown node")
	}
}

func TestHeartbeat(t *testing.T) {
	m := New()
	m.Heartbeat("node-a", "localhost:9001")

	if m.AliveCount() != 1 {
		t.Fatalf("expected 1 after heartbeat, got %d", m.AliveCount())
	}

	n := m.Lookup("node-a")
	if n == nil || n.Address != "localhost:9001" || !n.IsAlive {
		t.Fatalf("unexpected node: %+v", n)
	}
}

func TestLeaderID(t *testing.T) {
	m := New()
	if m.LeaderID() != "" {
		t.Fatalf("expected empty leader, got %s", m.LeaderID())
	}

	m.Add("node-b", "localhost:9002")
	m.Add("node-a", "localhost:9001")

	if m.LeaderID() != "node-a" {
		t.Fatalf("expected node-a as leader (lowest ID), got %s", m.LeaderID())
	}
}

func TestLeaderIDPicksLowestAlive(t *testing.T) {
	m := New()
	m.Add("node-c", "")
	m.Add("node-a", "")
	m.Add("node-b", "")
	m.MarkDead("node-a")

	if m.LeaderID() != "node-b" {
		t.Fatalf("expected node-b (lowest alive), got %s", m.LeaderID())
	}
}

func TestHeartbeatAutoAdds(t *testing.T) {
	m := New()
	m.Heartbeat("node-new", "addr:1")
	if m.AliveCount() != 1 {
		t.Fatalf("expected 1 alive, got %d", m.AliveCount())
	}
}

func TestSetLeaderLease(t *testing.T) {
	m := New()
	if m.leaderLease != DefaultLeaderLease {
		t.Fatalf("expected default lease %v, got %v", DefaultLeaderLease, m.leaderLease)
	}
	m.SetLeaderLease(5 * time.Second)
	if m.leaderLease != 5*time.Second {
		t.Fatalf("expected leaderLease 5s, got %v", m.leaderLease)
	}
}

func TestOnLeaseExpiry(t *testing.T) {
	m := New()
	var captured string
	cb := func(leaderID string) {
		captured = leaderID
	}
	m.OnLeaseExpiry(cb)
	if m.leaseCallback == nil {
		t.Fatal("leaseCallback should be set")
	}
	m.leaseCallback("node-a")
	if captured != "node-a" {
		t.Fatalf("expected callback with node-a, got %s", captured)
	}
}

func TestLeaderLastSeen(t *testing.T) {
	t.Run("no leader returns zero time", func(t *testing.T) {
		m := New()
		if !m.LeaderLastSeen().IsZero() {
			t.Fatal("expected zero time for no leader")
		}
	})

	t.Run("returns last seen of leader", func(t *testing.T) {
		m := New()
		before := time.Now()
		m.Heartbeat("node-a", "addr:1")
		after := time.Now()
		lastSeen := m.LeaderLastSeen()
		if lastSeen.Before(before) || lastSeen.After(after) {
			t.Fatal("expected last seen between before and after")
		}
	})
}

func TestEvictStaleWithLeaseCallback(t *testing.T) {
	m := New()
	m.heartbeatTimeout = 50 * time.Millisecond

	callbackCh := make(chan string, 1)
	m.OnLeaseExpiry(func(leaderID string) {
		callbackCh <- leaderID
	})

	m.Heartbeat("node-a", "addr:1")
	m.Heartbeat("node-b", "addr:2")

	if m.LeaderID() != "node-a" {
		t.Fatalf("expected node-a as leader, got %s", m.LeaderID())
	}

	time.Sleep(60 * time.Millisecond)

	m.evictStale()

	n := m.Lookup("node-a")
	if n == nil || n.IsAlive {
		t.Fatal("expected node-a to be dead after eviction")
	}

	select {
	case leaderID := <-callbackCh:
		if leaderID != "node-a" {
			t.Fatalf("expected callback with node-a, got %s", leaderID)
		}
	case <-time.After(time.Second):
		t.Fatal("expected lease callback to be fired")
	}
}

func TestStartEviction(t *testing.T) {
	m := New()
	m.heartbeatTimeout = 50 * time.Millisecond

	callbackCh := make(chan string, 1)
	m.OnLeaseExpiry(func(leaderID string) {
		callbackCh <- leaderID
	})

	m.Heartbeat("node-a", "addr:1")
	m.Heartbeat("node-b", "addr:2")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.StartEviction(ctx, 10*time.Millisecond)

	time.Sleep(80 * time.Millisecond)

	n := m.Lookup("node-a")
	if n == nil || n.IsAlive {
		t.Fatal("expected node-a to be dead after eviction")
	}

	select {
	case leaderID := <-callbackCh:
		if leaderID != "node-a" {
			t.Fatalf("expected callback with node-a, got %s", leaderID)
		}
	case <-time.After(time.Second):
		t.Fatal("expected lease callback to be fired")
	}
}

func TestStartLeaderLeaseCheckerExpires(t *testing.T) {
	m := New()
	m.SetLeaderLease(50 * time.Millisecond)
	m.Heartbeat("node-a", "addr:1")

	if m.LeaderID() != "node-a" {
		t.Fatalf("expected node-a as leader, got %s", m.LeaderID())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.StartLeaderLeaseChecker(ctx, 10*time.Millisecond)

	time.Sleep(150 * time.Millisecond)

	leaderID := m.LeaderID()
	if leaderID != "" {
		t.Fatalf("expected no leader after lease expiry, got %s", leaderID)
	}
}
