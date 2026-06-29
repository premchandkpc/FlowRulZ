package scenarios

import (
	"time"

	"github.com/premchandkpc/FlowRulZ/simulator/execution"
	"github.com/premchandkpc/FlowRulZ/simulator/loadgen"
	"github.com/premchandkpc/FlowRulZ/simulator/network"
	"github.com/premchandkpc/FlowRulZ/simulator/services"
)

type Scenario struct {
	Name        string
	Description string
	Apply       func(r *services.ServiceRegistry) (network.Config, loadgen.Config)
}

var BlackFriday = Scenario{
	Name:        "black-friday",
	Description: "High load spike, inventory slow, payment normal, email slow",
	Apply: func(r *services.ServiceRegistry) (network.Config, loadgen.Config) {
		inv := r.Get("inventory")
		if inv != nil {
			inv.BaseLatency = 50 * time.Millisecond
			inv.FailureRate = 0.05
		}
		pay := r.Get("payment")
		if pay != nil {
			pay.BaseLatency = 40 * time.Millisecond
		}
		email := r.Get("email")
		if email != nil {
			email.BaseLatency = 15 * time.Millisecond
		}

		return network.Config{
			MinLatency: 2 * time.Millisecond,
			MaxLatency: 10 * time.Millisecond,
		}, loadgen.Config{
			RequestsPerSec: 1000,
			Duration:       60 * time.Second,
			Pattern:        loadgen.PatternConstant,
		}
	},
}

var PaymentOutage = Scenario{
	Name:        "payment-outage",
	Description: "Payment service 100% failure, observe compensation flows",
	Apply: func(r *services.ServiceRegistry) (network.Config, loadgen.Config) {
		pay := r.Get("payment")
		if pay != nil {
			pay.FailureRate = 1.0
		}

		return network.Config{
			MinLatency: 1 * time.Millisecond,
			MaxLatency: 3 * time.Millisecond,
		}, loadgen.Config{
			RequestsPerSec: 50,
			Duration:       30 * time.Second,
			Pattern:        loadgen.PatternConstant,
		}
	},
}

var SpikeTest = Scenario{
	Name:        "spike-test",
	Description: "Burst traffic every 5 seconds",
	Apply: func(r *services.ServiceRegistry) (network.Config, loadgen.Config) {
		return network.Config{
			MinLatency: 1 * time.Millisecond,
			MaxLatency: 3 * time.Millisecond,
		}, loadgen.Config{
			BurstSize:     200,
			BurstInterval: 5 * time.Second,
			Duration:      30 * time.Second,
			Pattern:       loadgen.PatternBurst,
		}
	},
}

var ChaosMonkey = Scenario{
	Name:        "chaos-monkey",
	Description: "Packet loss, slow network, high failure rates",
	Apply: func(r *services.ServiceRegistry) (network.Config, loadgen.Config) {
		for _, name := range r.Names() {
			svc := r.Get(name)
			if svc != nil {
				svc.FailureRate = 0.1
				svc.BaseLatency *= 3
			}
		}
		return network.Config{
			MinLatency:     5 * time.Millisecond,
			MaxLatency:     50 * time.Millisecond,
			PacketLossRate: 0.05,
		}, loadgen.Config{
			RequestsPerSec: 200,
			Duration:       60 * time.Second,
			Pattern:        loadgen.PatternConstant,
		}
	},
}

var RampUp = Scenario{
	Name:        "ramp-up",
	Description: "Gradually increase load from 0 to target over duration",
	Apply: func(r *services.ServiceRegistry) (network.Config, loadgen.Config) {
		return network.Config{
			MinLatency: 1 * time.Millisecond,
			MaxLatency: 3 * time.Millisecond,
		}, loadgen.Config{
			RequestsPerSec: 500,
			Duration:       60 * time.Second,
			Pattern:        loadgen.PatternRampUp,
		}
	},
}

var All = []Scenario{BlackFriday, PaymentOutage, SpikeTest, ChaosMonkey, RampUp}

func ByName(name string) *Scenario {
	for _, s := range All {
		if s.Name == name {
			return &s
		}
	}
	return nil
}

func DefaultPlans() []*execution.Plan {
	return []*execution.Plan{
		execution.OrderFlow,
		execution.PaymentFlow,
		execution.RefundFlow,
	}
}
