package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	grpctransport "github.com/premchandkpc/FlowRulZ/server/internal/transport/grpc"
)

type Peer struct {
	ID     string
	Addr   string
	client *grpctransport.GRPCClient
	mu     sync.Mutex
}

type ClusterNode struct {
	nodeID   string
	grpcAddr string
	bus      *grpctransport.GRPCBus

	peersMu sync.RWMutex
	peers   map[string]*Peer

	handlersMu sync.RWMutex
	handlers   map[string]SubscribeHandler

	gossiper     *Gossiper
	gossipCancel context.CancelFunc

	started bool
	mu      sync.Mutex
}

type SubscribeHandler func(ctx context.Context, topic string, body []byte)

func NewClusterNode(nodeID, grpcAddr string) *ClusterNode {
	cn := &ClusterNode{
		nodeID:   nodeID,
		grpcAddr: grpcAddr,
		bus:      grpctransport.NewGRPCBus(grpcAddr),
		peers:    make(map[string]*Peer),
		handlers: make(map[string]SubscribeHandler),
	}
	cn.gossiper = NewGossiper(nodeID, grpcAddr, cn)
	return cn
}

func (cn *ClusterNode) Gossiper() *Gossiper {
	return cn.gossiper
}

func (cn *ClusterNode) Start() error {
	cn.mu.Lock()
	if cn.started {
		cn.mu.Unlock()
		return nil
	}
	cn.started = true
	cn.mu.Unlock()

	if err := cn.bus.Start(); err != nil {
		return fmt.Errorf("cluster node: start bus: %w", err)
	}

	cn.Subscribe("_flowrulz_gossip", cn.gossiper.HandleGossipMessage)

	gossipCtx, gossipCancel := context.WithCancel(context.Background())
	cn.gossipCancel = gossipCancel
	go cn.gossiper.Start(gossipCtx)

	slog.Info("cluster node: listening", "node_id", cn.nodeID, "addr", cn.grpcAddr)
	return nil
}

func (cn *ClusterNode) AddPeer(id, addr string) error {
	cn.peersMu.Lock()
	defer cn.peersMu.Unlock()

	if _, ok := cn.peers[id]; ok {
		return nil
	}

	client := grpctransport.NewGRPCClient(addr)
	if err := client.Connect(); err != nil {
		return fmt.Errorf("cluster node: connect to peer %s at %s: %w", id, addr, err)
	}

	cn.peers[id] = &Peer{ID: id, Addr: addr, client: client}
	slog.Info("cluster node: connected to peer", "node_id", cn.nodeID, "peer_id", id, "addr", addr)
	return nil
}

func (cn *ClusterNode) RemovePeer(id string) {
	cn.peersMu.Lock()
	if p, ok := cn.peers[id]; ok {
		p.mu.Lock()
		p.client.Close()
		p.mu.Unlock()
		delete(cn.peers, id)
		slog.Info("cluster node: disconnected peer", "node_id", cn.nodeID, "peer_id", id)
	}
	cn.peersMu.Unlock()
}

func (cn *ClusterNode) PublishToPeer(peerID, topic string, body []byte) error {
	cn.peersMu.RLock()
	p, ok := cn.peers[peerID]
	cn.peersMu.RUnlock()
	if !ok {
		return fmt.Errorf("peer %s not connected", peerID)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	_, err := p.client.PublishRaw(context.Background(), topic, "", body)
	if err != nil {
		slog.Error("cluster node: publish to peer", "peer_id", peerID, "error", err)
	}
	return err
}

func (cn *ClusterNode) Publish(topic, key string, body []byte) error {
	if _, err := cn.bus.Publish(context.Background(), &grpctransport.PublishRequest{
		Topic: topic,
		Msg: &grpctransport.BusMessage{
			Id:           fmt.Sprintf("%s-%d", cn.nodeID, time.Now().UnixNano()),
			Topic:        topic,
			Body:         body,
			PartitionKey: key,
		},
	}); err != nil {
		slog.Error("cluster node: local bus publish error", "error", err)
	}

	cn.peersMu.RLock()
	peers := make([]*Peer, 0, len(cn.peers))
	for _, p := range cn.peers {
		peers = append(peers, p)
	}
	cn.peersMu.RUnlock()

	for _, peer := range peers {
		p := peer
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			p.mu.Lock()
			defer p.mu.Unlock()
			_, err := p.client.PublishRaw(ctx, topic, key, body)
			if err != nil {
				slog.Error("cluster node: publish to peer", "peer_id", p.ID, "error", err)
			}
		}()
	}

	return nil
}

func (cn *ClusterNode) Subscribe(topic string, handler SubscribeHandler) {
	cn.handlersMu.Lock()
	cn.handlers[topic] = handler
	cn.handlersMu.Unlock()

	cn.bus.AddTopicHandler(topic, func(ctx context.Context, msg *grpctransport.BusMessage) {
		handler(ctx, msg.Topic, msg.Body)
	})
}

func (cn *ClusterNode) Unsubscribe(topic string) {
	cn.bus.RemoveTopicHandler(topic)
	cn.handlersMu.Lock()
	delete(cn.handlers, topic)
	cn.handlersMu.Unlock()
}

func (cn *ClusterNode) Stop() {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	if !cn.started {
		return
	}

	if cn.gossipCancel != nil {
		cn.gossipCancel()
	}
	cn.gossiper.Stop()

	cn.peersMu.Lock()
	for _, p := range cn.peers {
		p.mu.Lock()
		p.client.Close()
		p.mu.Unlock()
	}
	cn.peers = make(map[string]*Peer)
	cn.peersMu.Unlock()

	cn.bus.Stop()
	cn.started = false
}
