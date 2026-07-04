package scenarios

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/premchandkpc/FlowRulZ/simulator/loadgen"
	"github.com/premchandkpc/FlowRulZ/simulator/network"
	"github.com/premchandkpc/FlowRulZ/simulator/services"
)

var OrderProcessing = Scenario{
	Name:        "order-processing",
	Description: "Full order processing with retries, timeouts, and parallel execution",
	Apply: func(r *services.ServiceRegistry) (network.Config, loadgen.Config) {
		payment := r.Get("payment")
		if payment != nil {
			payment.BaseLatency = 40 * time.Millisecond
			payment.FailureRate = 0.02
		}
		inventory := r.Get("inventory")
		if inventory != nil {
			inventory.BaseLatency = 8 * time.Millisecond
			inventory.FailureRate = 0.02
		}
		shipping := r.Get("shipping")
		if shipping != nil {
			shipping.BaseLatency = 15 * time.Millisecond
			shipping.FailureRate = 0.01
		}
		notification := r.Get("notification")
		if notification != nil {
			notification.BaseLatency = 3 * time.Millisecond
			notification.FailureRate = 0.005
		}
		return network.Config{
			MinLatency: 1 * time.Millisecond,
			MaxLatency: 10 * time.Millisecond,
		}, loadgen.Config{
			RequestsPerSec: 100,
			Duration:       120 * time.Second,
			Pattern:        loadgen.PatternConstant,
		}
	},
	Setup: func(client ScenarioClient) error {
		if err := client.AddRule("order-processing",
			"schema:{!order_id:string,!product:string,!amount:float} n:validate n:inventory n:payment n:shipping n:notification",
		); err != nil {
			return err
		}
		plan := client.Plan("order-processing")
		if plan != nil {
			client.SetLoadGenPlan(plan)
			client.SetLoadGenBodyFunc(randomOrderProcessingBody)
		}
		return nil
	},
}

func randomOrderProcessingBody() []byte {
	orderID := uuid4()
	product := []string{"laptop", "smartphone", "tablet", "monitor", "keyboard"}[rand.Intn(5)]
	amount := 100.0 + rand.Float64()*1000.0
	return []byte(fmt.Sprintf(`{"order_id":"%s","product":"%s","amount":%.2f}`, orderID, product, amount))
}