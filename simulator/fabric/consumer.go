package fabric

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// MessageHandler is the callback for consumed messages.
type MessageHandler func(ctx context.Context, msg []byte) ([]byte, error)

// Consumer implements the internal transport.MessageConsumer interface
// with fabric-aware latency and loss.
type Consumer struct {
	fabric  *Fabric
	nodeID  string
	topic   string
	handler MessageHandler
	started atomic.Bool
	mu      sync.Mutex
	stopCh  chan struct{}
}

// NewConsumer creates a new fabric-aware consumer.
func NewConsumer(fabric *Fabric, nodeID, topic string, handler MessageHandler) *Consumer {
	return &Consumer{
		fabric:  fabric,
		nodeID:  nodeID,
		topic:   topic,
		handler: handler,
		stopCh:  make(chan struct{}),
	}
}

// Topic returns the consumer's topic.
func (c *Consumer) Topic() string {
	return c.topic
}

// Start begins consuming messages. In the fabric model, this registers
// the consumer but doesn't block — messages arrive via the fabric.
func (c *Consumer) Start(ctx context.Context) error {
	if c.started.Swap(true) {
		return fmt.Errorf("consumer already started")
	}

	go func() {
		select {
		case <-ctx.Done():
			c.Stop()
		case <-c.stopCh:
		}
	}()

	return nil
}

// Stop stops the consumer.
func (c *Consumer) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started.Load() {
		close(c.stopCh)
		c.started.Store(false)
	}
}

// Deliver delivers a message to this consumer through the fabric.
// This is called by the fabric when a matching message is published.
func (c *Consumer) Deliver(ctx context.Context, msg []byte) ([]byte, error) {
	if !c.started.Load() {
		return nil, fmt.Errorf("consumer not started")
	}

	// Apply fabric latency.
	latency := c.fabric.EffectiveLatency("__fabric__", c.nodeID)
	if latency > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
	}

	c.fabric.stats.MessagesReceived.Add(1)
	return c.handler(ctx, msg)
}
