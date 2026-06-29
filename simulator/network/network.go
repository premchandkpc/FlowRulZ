package network

import (
	"context"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/premchandkpc/FlowRulZ/simulator/services"
)

type Config struct {
	MinLatency      time.Duration
	MaxLatency      time.Duration
	PacketLossRate  float64
	DuplicateRate   float64
	ReorderRate     float64
}

var DefaultConfig = Config{
	MinLatency:      1 * time.Millisecond,
	MaxLatency:      5 * time.Millisecond,
	PacketLossRate:  0.0,
	DuplicateRate:   0.0,
	ReorderRate:     0.0,
}

type ChaosConfig struct {
	KillNodeEvery time.Duration
	DropPackets   bool
	DuplicatePct  float64
	SlowNetwork   bool
	SlowFactor    float64
}

type Network struct {
	cfg       Config
	chaos     atomic.Value
	dropped   atomic.Int64
	duplicated atomic.Int64
	reordered atomic.Int64
}

func New(cfg Config) *Network {
	n := &Network{cfg: cfg}
	n.chaos.Store(ChaosConfig{})
	return n
}

func (n *Network) SetChaos(c ChaosConfig) {
	n.chaos.Store(c)
}

func (n *Network) Dropped() int64   { return n.dropped.Load() }
func (n *Network) Duplicated() int64 { return n.duplicated.Load() }

func (n *Network) CallService(ctx context.Context, svc *services.MockService, body []byte, cb func(services.CallResult)) {
	latency := n.cfg.MinLatency
	if n.cfg.MaxLatency > n.cfg.MinLatency {
		jitter := time.Duration(rand.Int63n(int64(n.cfg.MaxLatency - n.cfg.MinLatency)))
		latency += jitter
	}

	chaos := n.chaos.Load().(ChaosConfig)
	if chaos.SlowNetwork && chaos.SlowFactor > 0 {
		latency = time.Duration(float64(latency) * chaos.SlowFactor)
	}

	if chaos.DropPackets && rand.Float64() < 0.05 {
		n.dropped.Add(1)
		return
	}

	if n.cfg.PacketLossRate > 0 && rand.Float64() < n.cfg.PacketLossRate {
		n.dropped.Add(1)
		return
	}

	doDuplicate := false
	if n.cfg.DuplicateRate > 0 && rand.Float64() < n.cfg.DuplicateRate {
		doDuplicate = true
		n.duplicated.Add(1)
	}

	sendCall := func() {
		select {
		case <-time.After(latency):
			result := svc.Call(ctx, body)
			cb(result)
		case <-ctx.Done():
			cb(services.CallResult{Error: ctx.Err()})
		}
	}

	sendCall()
	if doDuplicate {
		go sendCall()
	}
}
