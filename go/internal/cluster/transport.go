package cluster

import (
	"context"
	"log"

	"github.com/premchandkpc/FlowRulZ/go/internal/transport"
)

type ClusterProducer struct {
	topic string
	node  *ClusterNode
}

func NewClusterProducer(topic string, node *ClusterNode) *ClusterProducer {
	return &ClusterProducer{topic: topic, node: node}
}

func (p *ClusterProducer) Send(ctx context.Context, key, value []byte) error {
	return p.node.Publish(p.topic, string(key), value)
}

func (p *ClusterProducer) Close() {}

type ClusterConsumer struct {
	topic   string
	node    *ClusterNode
	handler transport.MessageHandler
	started bool
}

func NewClusterConsumer(topic string, handler transport.MessageHandler, node *ClusterNode) *ClusterConsumer {
	return &ClusterConsumer{
		topic:   topic,
		node:    node,
		handler: handler,
	}
}

func (c *ClusterConsumer) Topic() string { return c.topic }

func (c *ClusterConsumer) Start(ctx context.Context) {
	if c.started {
		return
	}
	c.started = true

	c.node.Subscribe(c.topic, func(ctx context.Context, topic string, body []byte) {
		_, err := c.handler(ctx, body)
		if err != nil {
			log.Printf("cluster consumer %s: handler error: %v", c.topic, err)
		}
	})

	go func() {
		<-ctx.Done()
		c.node.Unsubscribe(c.topic)
		c.started = false
	}()
}

func (c *ClusterConsumer) Stop() {
	c.node.Unsubscribe(c.topic)
	c.started = false
}
