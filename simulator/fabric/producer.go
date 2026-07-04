package fabric

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Producer implements the internal transport.MessageProducer interface
// with fabric-aware latency and loss.
type Producer struct {
	fabric *Fabric
	nodeID string
	topic  string
	mu     sync.Mutex
	closed bool
}

// NewProducer creates a new fabric-aware producer.
func NewProducer(fabric *Fabric, nodeID, topic string) *Producer {
	return &Producer{
		fabric: fabric,
		nodeID: nodeID,
		topic:  topic,
	}
}

// Send sends a message through the fabric.
func (p *Producer) Send(ctx context.Context, key, value []byte) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return fmt.Errorf("producer closed")
	}
	p.mu.Unlock()

	p.fabric.stats.MessagesSent.Add(1)

	// Check packet loss.
	if p.fabric.ShouldDrop(p.nodeID, "__producer__") {
		return nil // Silent drop (matches Kafka semantics).
	}

	// Apply fabric latency.
	latency := p.fabric.EffectiveLatency(p.nodeID, "__producer__")
	if latency > 0 {
		time.Sleep(latency)
	}

	p.fabric.stats.MessagesReceived.Add(1)
	return nil
}

// Close closes the producer.
func (p *Producer) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
}
