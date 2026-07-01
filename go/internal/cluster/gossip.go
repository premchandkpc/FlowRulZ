package cluster

import (
	"context"
	"encoding/json"
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

type GossipState struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address"`
	Term    uint64 `json:"term"`
	Epoch   uint64 `json:"epoch"`
}

type GossipMessage struct {
	Type    string        `json:"type"` // "push" or "pull_req" or "pull_resp"
	Sender  string        `json:"sender"`
	States  []GossipState `json:"states,omitempty"`
	Epochs  map[string]uint64 `json:"epochs,omitempty"`
}

type Gossiper struct {
	nodeID    string
	grpcAddr  string
	node      *ClusterNode
	statesMu  sync.RWMutex
	states    map[string]GossipState
	myState   GossipState
	fanout    int
	pushInterval time.Duration
	syncInterval time.Duration
	stopCh    chan struct{}
}

func NewGossiper(nodeID, grpcAddr string, node *ClusterNode) *Gossiper {
	return &Gossiper{
		nodeID:    nodeID,
		grpcAddr:  grpcAddr,
		node:      node,
		states:    make(map[string]GossipState),
		myState:   GossipState{NodeID: nodeID, Address: grpcAddr},
		fanout:    2,
		pushInterval: 2 * time.Second,
		syncInterval: 10 * time.Second,
	}
}

func (g *Gossiper) SetState(term uint64) {
	g.statesMu.Lock()
	defer g.statesMu.Unlock()
	g.myState.Epoch++
	g.myState.Term = term
}

func (g *Gossiper) UpdateState(nodeID string, state GossipState) {
	g.statesMu.Lock()
	defer g.statesMu.Unlock()
	existing, ok := g.states[nodeID]
	if !ok || state.Epoch > existing.Epoch || (state.Epoch == existing.Epoch && state.Term > existing.Term) {
		g.states[nodeID] = state
	}
}

func (g *Gossiper) GetState(nodeID string) (GossipState, bool) {
	g.statesMu.RLock()
	defer g.statesMu.RUnlock()
	s, ok := g.states[nodeID]
	return s, ok
}

func (g *Gossiper) AllStates() []GossipState {
	g.statesMu.RLock()
	defer g.statesMu.RUnlock()
	out := make([]GossipState, 0, len(g.states)+1)
	out = append(out, g.myState)
	for _, s := range g.states {
		out = append(out, s)
	}
	return out
}

func (g *Gossiper) GetMyState() GossipState {
	g.statesMu.RLock()
	defer g.statesMu.RUnlock()
	return g.myState
}

func (g *Gossiper) randomPeers(n int) []string {
	g.node.peersMu.RLock()
	defer g.node.peersMu.RUnlock()
	if len(g.node.peers) == 0 {
		return nil
	}
	ids := make([]string, 0, len(g.node.peers))
	for id := range g.node.peers {
		ids = append(ids, id)
	}
	if n >= len(ids) {
		return ids
	}
	rand.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
	return ids[:n]
}

func (g *Gossiper) Start(ctx context.Context) {
	pushTicker := time.NewTicker(g.pushInterval)
	syncTicker := time.NewTicker(g.syncInterval)
	defer pushTicker.Stop()
	defer syncTicker.Stop()

	for {
		select {
		case <-pushTicker.C:
			g.doPush()
		case <-syncTicker.C:
			g.doSync()
		case <-ctx.Done():
			return
		case <-g.stopCh:
			return
		}
	}
}

func (g *Gossiper) Stop() {
	select {
	case <-g.stopCh:
	default:
		close(g.stopCh)
	}
}

func (g *Gossiper) doPush() {
	peers := g.randomPeers(g.fanout)
	if len(peers) == 0 {
		return
	}

	g.statesMu.RLock()
	states := make([]GossipState, 0, len(g.states)+1)
	states = append(states, g.myState)
	for _, s := range g.states {
		states = append(states, s)
	}
	g.statesMu.RUnlock()

	msg := GossipMessage{Type: "push", Sender: g.nodeID, States: states}
	data, err := json.Marshal(msg)
	if err != nil {
		slog.Error("gossip: push marshal error", "error", err)
		return
	}

	for _, peerID := range peers {
		g.node.Publish("_flowrulz_gossip", peerID, data)
	}
}

func (g *Gossiper) doSync() {
	peer := g.randomPeers(1)
	if len(peer) == 0 {
		return
	}
	peerID := peer[0]

	g.statesMu.RLock()
	epochs := make(map[string]uint64, len(g.states)+1)
	epochs[g.nodeID] = g.myState.Epoch
	for id, s := range g.states {
		epochs[id] = s.Epoch
	}
	g.statesMu.RUnlock()

	msg := GossipMessage{Type: "pull_req", Sender: g.nodeID, Epochs: epochs}
	data, err := json.Marshal(msg)
	if err != nil {
		slog.Error("gossip: pull_req marshal error", "error", err)
		return
	}
	g.node.Publish("_flowrulz_gossip", peerID, data)
}

func (g *Gossiper) HandleGossipMessage(ctx context.Context, topic string, body []byte) {
	var msg GossipMessage
	if err := json.Unmarshal(body, &msg); err != nil {
		slog.Error("gossip: unmarshal error", "error", err)
		return
	}

	switch msg.Type {
	case "push":
		for _, state := range msg.States {
			if state.NodeID == g.nodeID {
				continue
			}
			g.UpdateState(state.NodeID, state)
		}
		slog.Info("gossip: received push", "sender", msg.Sender, "states", len(msg.States))

	case "pull_req":
		g.statesMu.RLock()
		myEpoch := g.myState.Epoch
		missingStates := make([]GossipState, 0)
		if e, ok := msg.Epochs[g.nodeID]; !ok || e < myEpoch {
			missingStates = append(missingStates, g.myState)
		}
		for id, s := range g.states {
			if id == g.nodeID {
				continue
			}
			if e, ok := msg.Epochs[id]; !ok || e < s.Epoch {
				missingStates = append(missingStates, s)
			}
		}
		g.statesMu.RUnlock()

		resp := GossipMessage{Type: "pull_resp", Sender: g.nodeID, States: missingStates}
		data, err := json.Marshal(resp)
		if err != nil {
			slog.Error("gossip: pull_resp marshal error", "error", err)
			return
		}
		g.node.Publish("_flowrulz_gossip", msg.Sender, data)
		slog.Info("gossip: responding to pull", "sender", msg.Sender, "states", len(missingStates))

	case "pull_resp":
		for _, state := range msg.States {
			if state.NodeID == g.nodeID {
				continue
			}
			g.UpdateState(state.NodeID, state)
		}
		slog.Info("gossip: received pull_resp", "sender", msg.Sender, "states", len(msg.States))
	}
}
