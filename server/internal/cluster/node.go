package cluster

import (
	"context"
	"fmt"
	"log"
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

	log.Printf("cluster node %s: listening on %s", cn.nodeID, cn.grpcAddr)
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
	log.Printf("cluster node %s: connected to peer %s at %s", cn.nodeID, id, addr)
	return nil
}

func (cn *ClusterNode) RemovePeer(id string) {
	cn.peersMu.Lock()
	if p, ok := cn.peers[id]; ok {
		p.mu.Lock()
		p.client.Close()
		p.mu.Unlock()
		delete(cn.peers, id)
		log.Printf("cluster node %s: disconnected peer %s", cn.nodeID, id)
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
		log.Printf("cluster node: publish to peer %s: %v", peerID, err)
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
		log.Printf("cluster node: local bus publish error: %v", err)
	}

	cn.peersMu.RLock()
	for _, p := range cn.peers {
		go func(peer *Peer) {
			peer.mu.Lock()
			defer peer.mu.Unlock()
			_, err := peer.client.PublishRaw(context.Background(), topic, key, body)
			if err != nil {
				log.Printf("cluster node: publish to peer %s: %v", peer.ID, err)
			}
		}(p)
	}
	cn.peersMu.RUnlock()

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
