package cluster

import (
	"log/slog"

	"github.com/premchandkpc/FlowRulZ/server/internal/transport"
)

// RegisterClusterTransport registers Cluster producer and consumer factories
// with the TransportFactory.
func RegisterClusterTransport(factory *transport.TransportFactory, node *ClusterNode) {
	if node == nil {
		return
	}

	factory.RegisterProducer(transport.KindCluster, func(topic string) transport.MessageProducer {
		return NewClusterProducer(topic, node)
	})

	factory.RegisterConsumer(transport.KindCluster, func(topic string, handler transport.MessageHandler) transport.MessageConsumer {
		return NewClusterConsumer(topic, handler, node)
	})

	slog.Info("transport: registered cluster backend")
}
