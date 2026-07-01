// Scenario: order-routing — demonstrates Gate-based conditional routing.
//
// One producer ("service-a") emits order events with randomized amounts.
// A single DSL rule with a Gate operator routes events to either
// "service-b" (manual-review, amount > 10000) or "service-c" (auto-process,
// amount <= 10000). Useful for documentation/demo of conditional routing,
// not for load testing.
package scenarios

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/premchandkpc/FlowRulZ/simulator/loadgen"
	"github.com/premchandkpc/FlowRulZ/simulator/network"
	"github.com/premchandkpc/FlowRulZ/simulator/services"
)

var OrderRouting = Scenario{
	Name:        "order-routing",
	Description: "Gate-based conditional routing: small vs large orders",
	Apply: func(r *services.ServiceRegistry) (network.Config, loadgen.Config) {
		r.Register(&services.MockService{
			Name:        "service-b",
			BaseLatency: 150 * time.Millisecond,
		})
		r.Register(&services.MockService{
			Name:        "service-c",
			BaseLatency: 30 * time.Millisecond,
		})

		return network.Config{
			MinLatency: 1 * time.Millisecond,
			MaxLatency: 5 * time.Millisecond,
		}, loadgen.Config{
			RequestsPerSec: 20,
			Duration:       60 * time.Second,
			Pattern:        loadgen.PatternConstant,
			MaxConcurrent:  50,
		}
	},
	Setup: func(client ScenarioClient) error {
		if err := client.AddRule("order-routing",
			"schema:{!order_id:string,!amount:float} g:amount>10000 n:service-b g:amount<=10000 n:service-c",
		); err != nil {
			return err
		}
		plan := client.Plan("order-routing")
		if plan != nil {
			client.SetLoadGenPlan(plan)
			client.SetLoadGenBodyFunc(randomOrderBody)
		}
		return nil
	},
}

// randomOrderBody generates a JSON order event with a skewed amount distribution:
// ~75% of amounts are ≤ 10000 (routes to service-c), ~25% > 10000 (routes to
// service-b). Uses a power-law skew so most orders are small with occasional
// large ones.
func randomOrderBody() []byte {
	orderID := uuid4()
	amount := skewedAmount()
	return []byte(fmt.Sprintf(`{"order_id":"%s","amount":%.2f}`, orderID, amount))
}

// skewedAmount returns a random order amount with ~75% below 10000 and ~25% above.
func skewedAmount() float64 {
	u := rand.Float64()
	if u < 0.75 {
		return 100 + u/0.75*9900
	}
	return 10000 + (u-0.75)/0.25*10000
}

// uuid4 returns a random UUID v4 string.
func uuid4() string {
	b := make([]byte, 16)
	rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
