// Package cluster implements the ClusterTransport port.
// Wraps internal/cluster.ClusterNode to satisfy ports.ClusterTransport.
package cluster

import (
	"github.com/premchandkpc/FlowRulZ/server/internal/cluster"
	"github.com/premchandkpc/FlowRulZ/server/internal/ports"
)

// TransportAdapter wraps cluster.ClusterNode to implement ports.ClusterTransport.
type TransportAdapter struct {
	node *cluster.ClusterNode
}

// NewTransportAdapter creates a ClusterTransport from a ClusterNode.
func NewTransportAdapter(node *cluster.ClusterNode) ports.ClusterTransport {
	if node == nil {
		return nil
	}
	return &TransportAdapter{node: node}
}

func (a *TransportAdapter) Start() error {
	return a.node.Start()
}

func (a *TransportAdapter) Stop() {
	a.node.Stop()
}

func (a *TransportAdapter) AddPeer(id, addr string) error {
	return a.node.AddPeer(id, addr)
}

func (a *TransportAdapter) Publish(topic, key string, body []byte) error {
	return a.node.Publish(topic, key, body)
}

// Compile-time interface compliance check
var _ ports.ClusterTransport = (*TransportAdapter)(nil)
