package fabric

import "time"

// LinkBuilder provides a fluent API for configuring a link between two nodes.
type LinkBuilder struct {
	fabric *Fabric
	from   string
	to     string
	config LinkConfig
}

// Link starts configuring a link between two nodes.
func (f *Fabric) Link(from, to string) *LinkBuilder {
	return &LinkBuilder{
		fabric: f,
		from:   from,
		to:     to,
		config: LinkConfig{
			Latency: 1 * time.Millisecond,
		},
	}
}

// Latency sets the base one-way delay.
func (lb *LinkBuilder) Latency(d time.Duration) *LinkBuilder {
	lb.config.Latency = d
	return lb
}

// Jitter sets the maximum random variation added to latency.
func (lb *LinkBuilder) Jitter(d time.Duration) *LinkBuilder {
	lb.config.Jitter = d
	return lb
}

// Loss sets the packet loss probability [0.0, 1.0].
func (lb *LinkBuilder) Loss(rate float64) *LinkBuilder {
	lb.config.PacketLoss = rate
	return lb
}

// Partition creates a unidirectional partition.
func (lb *LinkBuilder) Partition() *LinkBuilder {
	lb.config.PartitionEnabled = true
	return lb
}

// Apply saves the link configuration.
func (lb *LinkBuilder) Apply() {
	lb.fabric.SetLinkConfig(lb.from, lb.to, lb.config)
}

// ScenarioConfig is a preset configuration for common network scenarios.
type ScenarioConfig struct {
	Name    string
	Setup   func(f *Fabric)
	Cleanup func(f *Fabric)
}

// Common scenario configurations.
var (
	// DefaultNetwork is a fast, reliable network.
	DefaultNetwork = ScenarioConfig{
		Name: "default",
		Setup: func(f *Fabric) {
			// No-op: defaults are already fast and reliable.
		},
	}

	// SlowNetwork simulates a high-latency network.
	SlowNetwork = ScenarioConfig{
		Name: "slow",
		Setup: func(f *Fabric) {
			for nodeA := range f.Nodes() {
				for nodeB := range f.Nodes() {
					if nodeA != nodeB {
						f.Link(nodeA, nodeB).
							Latency(50 * time.Millisecond).
							Jitter(20 * time.Millisecond).
							Apply()
					}
				}
			}
		},
	}

	// LossyNetwork simulates a network with packet loss.
	LossyNetwork = ScenarioConfig{
		Name: "lossy",
		Setup: func(f *Fabric) {
			for nodeA := range f.Nodes() {
				for nodeB := range f.Nodes() {
					if nodeA != nodeB {
						f.Link(nodeA, nodeB).
							Loss(0.05).
							Apply()
					}
				}
			}
		},
	}

	// PartitionedNetwork simulates network partitions.
	PartitionedNetwork = ScenarioConfig{
		Name: "partitioned",
		Setup: func(f *Fabric) {
			// Partition the first two nodes.
			var nodes []string
			for id := range f.Nodes() {
				nodes = append(nodes, id)
			}
			if len(nodes) >= 2 {
				f.Partition(nodes[0], nodes[1])
				f.Partition(nodes[1], nodes[0])
			}
		},
		Cleanup: func(f *Fabric) {
			f.HealAll()
		},
	}
)
