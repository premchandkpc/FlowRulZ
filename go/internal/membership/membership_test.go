package membership

import (
	"testing"
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
