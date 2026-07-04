// Package fabric provides a simulated network fabric for the FlowRulZ
// simulator. It implements the real transport interfaces (FullEventBus,
// MessageProducer, MessageConsumer) with pluggable latency, jitter,
// packet loss, and partition injection.
//
// The fabric sits below the transport layer — individual call sites
// don't inject faults; the fabric does it transparently based on
// per-link configuration.
package fabric

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/premchandkpc/FlowRulZ/server/pkg/transport"
)

// LinkConfig configures the network characteristics between two nodes.
type LinkConfig struct {
	// Latency is the base one-way delay for messages on this link.
	Latency time.Duration

	// Jitter is the maximum random variation added to Latency.
	Jitter time.Duration

	// PacketLoss is the probability [0.0, 1.0] that a message is dropped.
	PacketLoss float64

	// PartitionEnabled allows manual partition injection.
	PartitionEnabled bool

	// partitioned is set when a link is partitioned.
	partitioned atomic.Bool
}

// Fabric is the shared network fabric all simulated nodes attach to.
// It manages node-to-node links and provides transport factories.
type Fabric struct {
	mu sync.RWMutex

	// links maps "nodeA->nodeB" to its LinkConfig.
	links map[string]*LinkConfig

	// nodes maps nodeID to its gRPC address for discovery.
	nodes map[string]string

	// buses maps nodeID to its Bus for cross-node delivery.
	buses map[string]*Bus

	// registry holds the service registry address (shared across nodes).
	registryAddr string

	// stats tracks network-level metrics.
	stats Stats
}

// Stats tracks network-level metrics.
type Stats struct {
	MessagesSent     atomic.Int64
	MessagesReceived atomic.Int64
	MessagesDropped  atomic.Int64
	MessagesDelayed  atomic.Int64
	PartitionsActive atomic.Int64
}

// New creates a new Fabric with default link configuration.
func New() *Fabric {
	return &Fabric{
		links:  make(map[string]*LinkConfig),
		nodes:  make(map[string]string),
		buses:  make(map[string]*Bus),
	}
}

// RegisterNode registers a node's address for discovery.
func (f *Fabric) RegisterNode(nodeID, addr string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nodes[nodeID] = addr
}

// UnregisterNode removes a node from the fabric.
func (f *Fabric) UnregisterNode(nodeID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.nodes, nodeID)
}

// Nodes returns all registered node addresses.
func (f *Fabric) Nodes() map[string]string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	nodes := make(map[string]string, len(f.nodes))
	for k, v := range f.nodes {
		nodes[k] = v
	}
	return nodes
}

// SetLinkConfig sets the link configuration between two nodes.
func (f *Fabric) SetLinkConfig(from, to string, cfg LinkConfig) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := linkKey(from, to)
	f.links[key] = &cfg
}

// GetLinkConfig returns the link configuration between two nodes.
// If no explicit config exists, returns defaults (1ms latency, no loss).
func (f *Fabric) GetLinkConfig(from, to string) LinkConfig {
	f.mu.RLock()
	defer f.mu.RUnlock()
	key := linkKey(from, to)
	if cfg, ok := f.links[key]; ok {
		return *cfg
	}
	return LinkConfig{
		Latency: 1 * time.Millisecond,
		Jitter:  500 * time.Microsecond,
	}
}

// Partition creates a unidirectional network partition (from -> to blocked).
func (f *Fabric) Partition(from, to string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := linkKey(from, to)
	cfg := f.links[key]
	if cfg == nil {
		cfg = &LinkConfig{Latency: 1 * time.Millisecond}
	}
	if !cfg.PartitionEnabled {
		cfg.PartitionEnabled = true
		f.stats.PartitionsActive.Add(1)
	}
	f.links[key] = cfg
}

// Heal removes a partition between two nodes.
func (f *Fabric) Heal(from, to string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := linkKey(from, to)
	if cfg, ok := f.links[key]; ok {
		if cfg.PartitionEnabled {
			cfg.PartitionEnabled = false
			f.stats.PartitionsActive.Add(-1)
		}
	}
}

// HealAll removes all partitions.
func (f *Fabric) HealAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, cfg := range f.links {
		if cfg.PartitionEnabled {
			cfg.PartitionEnabled = false
			f.stats.PartitionsActive.Add(-1)
		}
	}
}

// ShouldDrop returns true if a message from from->to should be dropped.
// It applies both configured packet loss and active partitions.
func (f *Fabric) ShouldDrop(from, to string) bool {
	cfg := f.GetLinkConfig(from, to)
	if cfg.PartitionEnabled {
		f.stats.MessagesDropped.Add(1)
		return true
	}
	if cfg.PacketLoss > 0 && randFloat64() < cfg.PacketLoss {
		f.stats.MessagesDropped.Add(1)
		return true
	}
	return false
}

// EffectiveLatency returns the simulated latency for a link (base + jitter).
func (f *Fabric) EffectiveLatency(from, to string) time.Duration {
	cfg := f.GetLinkConfig(from, to)
	latency := cfg.Latency
	if cfg.Jitter > 0 {
		latency += time.Duration(randInt63n(int64(cfg.Jitter)))
	}
	return latency
}

// StatsSnapshot returns a copy of the current stats.
func (f *Fabric) StatsSnapshot() Stats {
	return f.stats
}

// registerBus registers a bus with the fabric for cross-node delivery.
func (f *Fabric) registerBus(nodeID string, bus *Bus) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.buses[nodeID] = bus
}

// unregisterBus removes a bus from the fabric.
func (f *Fabric) unregisterBus(nodeID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.buses, nodeID)
}

// dispatchRemote sends a message to all remote buses through the fabric.
func (f *Fabric) dispatchRemote(fromNode, topic string, msg *transport.Message) {
	f.mu.RLock()
	buses := make(map[string]*Bus, len(f.buses))
	for id, bus := range f.buses {
		if id != fromNode {
			buses[id] = bus
		}
	}
	f.mu.RUnlock()

	for toNode, bus := range buses {
		// Check if we should drop the message (packet loss or partition).
		if f.ShouldDrop(fromNode, toNode) {
			continue
		}

		// Get effective latency from fabric.
		latency := f.EffectiveLatency(fromNode, toNode)

		// Dispatch with simulated latency.
		go func(bus *Bus, latency time.Duration) {
			if latency > 0 {
				time.Sleep(latency)
			}
			f.stats.MessagesReceived.Add(1)
			bus.deliver(context.Background(), msg)
		}(bus, latency)
	}
}

// linkKey generates a unique key for a unidirectional link.
func linkKey(from, to string) string {
	return fmt.Sprintf("%s->%s", from, to)
}
