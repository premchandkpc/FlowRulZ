package node

import (
	"github.com/premchandkpc/FlowRulZ/server/internal/cluster"
)

// clusterTransportAdapter wraps cluster.ClusterNode to implement ClusterTransport interface.
type clusterTransportAdapter struct {
	node *cluster.ClusterNode
}

// NewClusterTransportAdapter creates a ClusterTransport from a ClusterNode.
func NewClusterTransportAdapter(node *cluster.ClusterNode) ClusterTransport {
	if node == nil {
		return nil
	}
	return &clusterTransportAdapter{node: node}
}

func (a *clusterTransportAdapter) Start() error {
	return a.node.Start()
}

func (a *clusterTransportAdapter) Stop() {
	a.node.Stop()
}

func (a *clusterTransportAdapter) AddPeer(id, addr string) error {
	return a.node.AddPeer(id, addr)
}

func (a *clusterTransportAdapter) Publish(topic, key string, body []byte) error {
	return a.node.Publish(topic, key, body)
}

func (a *clusterTransportAdapter) Gossiper() GossipProvider {
	return a.node.Gossiper()
}
