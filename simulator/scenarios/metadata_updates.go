package scenarios

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/premchandkpc/FlowRulZ/simulator/loadgen"
	"github.com/premchandkpc/FlowRulZ/simulator/network"
	"github.com/premchandkpc/FlowRulZ/simulator/services"
)

var MetadataUpdates = Scenario{
	Name:        "metadata-updates",
	Description: "Live metadata updates and rule deployment without restart",
	Apply: func(r *services.ServiceRegistry) (network.Config, loadgen.Config) {
		payment := r.Get("payment")
		if payment != nil {
			payment.BaseLatency = 40 * time.Millisecond
		}
		inventory := r.Get("inventory")
		if inventory != nil {
			inventory.BaseLatency = 8 * time.Millisecond
		}
		notification := r.Get("notification")
		if notification != nil {
			notification.BaseLatency = 3 * time.Millisecond
		}
		return network.Config{
			MinLatency: 1 * time.Millisecond,
			MaxLatency: 5 * time.Millisecond,
		}, loadgen.Config{
			RequestsPerSec: 50,
			Duration:       180 * time.Second,
			Pattern:        loadgen.PatternConstant,
		}
	},
	Setup: func(client ScenarioClient) error {
		if err := client.AddRule("metadata-updates-v1",
			"schema:{order_id:string} n:payment n:inventory",
		); err != nil {
			return err
		}
		plan := client.Plan("metadata-updates-v1")
		if plan != nil {
			client.SetLoadGenPlan(plan)
			client.SetLoadGenBodyFunc(fmtOrderForMetadataUpdates)
		}
		return nil
	},
}

func fmtOrderForMetadataUpdates() []byte {
	orderID := uuid4()
	amount := 100.0 + rand.Float64()*500.0
	return []byte(fmt.Sprintf(`{"order_id":"%s","amount":%.2f}`, orderID, amount))
}