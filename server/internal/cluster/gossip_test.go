package cluster

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

func newMockGossiper(nodeID, grpcAddr string) *Gossiper {
	return &Gossiper{
		nodeID:    nodeID,
		grpcAddr:  grpcAddr,
		node:      &ClusterNode{peers: make(map[string]*Peer)},
		states:    make(map[string]GossipState),
		myState:   GossipState{NodeID: nodeID, Address: grpcAddr},
		fanout:    2,
		pushInterval: 50 * time.Millisecond,
		syncInterval: 100 * time.Millisecond,
		stopCh:    make(chan struct{}),
	}
}

func TestGossiperOnNodeJoinCallback(t *testing.T) {
	g := newMockGossiper("node-a", ":9090")

	joinCh := make(chan struct {
		nodeID  string
		address string
	}, 1)

	g.OnNodeJoin(func(nodeID, address string) {
		joinCh <- struct {
			nodeID  string
			address string
		}{nodeID, address}
	})

	state := GossipState{NodeID: "node-b", Address: ":9091", Term: 1, Epoch: 1}
	g.UpdateState("node-b", state)

	select {
	case join := <-joinCh:
		if join.nodeID != "node-b" || join.address != ":9091" {
			t.Fatalf("expected node-b/:9091, got %s/%s", join.nodeID, join.address)
		}
	case <-time.After(time.Second):
		t.Fatal("expected onNodeJoin callback to fire")
	}
}

func TestGossiperUpdateStateExisting(t *testing.T) {
	g := newMockGossiper("node-a", ":9090")

	callbackCalled := false
	g.OnNodeJoin(func(nodeID, address string) {
		callbackCalled = true
	})

	// First update triggers callback
	g.UpdateState("node-b", GossipState{NodeID: "node-b", Address: ":9091", Term: 1, Epoch: 1})
	if !callbackCalled {
		t.Fatal("expected callback on first update")
	}

	// Second update with same epoch should not trigger callback
	callbackCalled = false
	g.UpdateState("node-b", GossipState{NodeID: "node-b", Address: ":9091", Term: 1, Epoch: 1})
	if callbackCalled {
		t.Fatal("expected no callback on duplicate update")
	}

	// Update with higher epoch should update but not trigger callback (not new)
	callbackCalled = false
	g.UpdateState("node-b", GossipState{NodeID: "node-b", Address: ":9092", Term: 2, Epoch: 2})
	if callbackCalled {
		t.Fatal("expected no callback on existing node update")
	}

	// Verify state was updated
	s, ok := g.GetState("node-b")
	if !ok {
		t.Fatal("expected node-b state to exist")
	}
	if s.Term != 2 || s.Epoch != 2 || s.Address != ":9092" {
		t.Fatalf("expected updated state, got %+v", s)
	}
}

func TestGossiperAllStates(t *testing.T) {
	g := newMockGossiper("node-a", ":9090")
	g.SetState(1)

	g.UpdateState("node-b", GossipState{NodeID: "node-b", Address: ":9091", Term: 1, Epoch: 1})
	g.UpdateState("node-c", GossipState{NodeID: "node-c", Address: ":9092", Term: 1, Epoch: 1})

	states := g.AllStates()
	if len(states) != 3 {
		t.Fatalf("expected 3 states (self + 2), got %d", len(states))
	}
}

func TestGossiperGetState(t *testing.T) {
	g := newMockGossiper("node-a", ":9090")
	g.UpdateState("node-b", GossipState{NodeID: "node-b", Address: ":9091", Term: 1, Epoch: 1})

	s, ok := g.GetState("node-b")
	if !ok || s.NodeID != "node-b" {
		t.Fatal("expected to find node-b")
	}

	_, ok = g.GetState("unknown")
	if ok {
		t.Fatal("expected false for unknown node")
	}
}

func TestGossiperSetStateIncrementsEpoch(t *testing.T) {
	g := newMockGossiper("node-a", ":9090")
	myState := g.GetMyState()
	initialEpoch := myState.Epoch

	g.SetState(1)
	myState = g.GetMyState()
	if myState.Epoch <= initialEpoch {
		t.Fatal("expected epoch to increment on SetState")
	}
	if myState.Term != 1 {
		t.Fatalf("expected term 1, got %d", myState.Term)
	}
}

func TestHandleGossipPush(t *testing.T) {
	g := newMockGossiper("node-a", ":9090")

	var mu sync.Mutex
	var joined []string
	g.OnNodeJoin(func(nodeID, address string) {
		mu.Lock()
		joined = append(joined, nodeID)
		mu.Unlock()
	})

	pushMsg, _ := json.Marshal(GossipMessage{
		Type:   "push",
		Sender: "node-b",
		States: []GossipState{
			{NodeID: "node-b", Address: ":9091", Term: 1, Epoch: 1},
			{NodeID: "node-c", Address: ":9092", Term: 1, Epoch: 1},
		},
	})

	g.HandleGossipMessage(context.Background(), "_flowrulz_gossip", pushMsg)

	if len(joined) != 2 {
		t.Fatalf("expected 2 join callbacks, got %d: %v", len(joined), joined)
	}
	if !contains(joined, "node-b") || !contains(joined, "node-c") {
		t.Fatal("expected both node-b and node-c to trigger join callback")
	}
}

func contains(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}
