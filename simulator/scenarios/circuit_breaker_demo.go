package scenarios

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/premchandkpc/FlowRulZ/simulator/loadgen"
	"github.com/premchandkpc/FlowRulZ/simulator/network"
	"github.com/premchandkpc/FlowRulZ/simulator/services"
)

var CircuitBreakerDemo = Scenario{
	Name:        "circuit-breaker",
	Description: "Circuit breaker behavior with fallback execution",
	Apply: func(r *services.ServiceRegistry) (network.Config, loadgen.Config) {
		payment := r.Get("payment")
		if payment != nil {
			payment.BaseLatency = 40 * time.Millisecond
			payment.FailureRate = 0.95
		}
		notification := r.Get("notification")
		if notification != nil {
			notification.BaseLatency = 3 * time.Millisecond
			notification.FailureRate = 0.01
		}
		return network.Config{
			MinLatency: 1 * time.Millisecond,
			MaxLatency: 10 * time.Millisecond,
		}, loadgen.Config{
			RequestsPerSec: 50,
			Duration:       180 * time.Second,
			Pattern:        loadgen.PatternConstant,
		}
	},
	Setup: func(client ScenarioClient) error {
		if err := client.AddRule("circuit-breaker-demo",
			"schema:{order_id:string} n:payment f:notification",
		); err != nil {
			return err
		}
		plan := client.Plan("circuit-breaker-demo")
		if plan != nil {
			client.SetLoadGenPlan(plan)
			client.SetLoadGenBodyFunc(fmtOrderForCircuitBreakerDemo)
		}
		return nil
	},
}

func fmtOrderForCircuitBreakerDemo() []byte {
	orderID := uuid4()
	amount := 100.0 + rand.Float64()*500.0
	return []byte(fmt.Sprintf(`{"order_id":"%s","amount":%.2f}`, orderID, amount))
}