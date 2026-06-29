package simulator

import (
	"context"
	"testing"
	"time"

	"github.com/premchandkpc/FlowRulZ/simulator/config"
	"github.com/premchandkpc/FlowRulZ/simulator/services"
)

func startSim(t *testing.T, nodes, workers int) *Simulator {
	t.Helper()
	sim := New(config.SimConfig{
		Nodes:   nodes,
		Workers: workers,
	})
	sim.Dispatcher.StartAll()
	return sim
}

func TestClientSendBridgeRule(t *testing.T) {
	sim := startSim(t, 1, 4)
	defer sim.Stop()
	time.Sleep(10 * time.Millisecond)

	cli := sim.Client()

	// Register a simple echo service
	cli.RegisterService(&services.MockService{
		Name:          "echo",
		BaseLatency:   1 * time.Millisecond,
		MaxConcurrent: 100,
	})

	// Add a compiled rule: validate then call echo
	err := cli.AddRule("test-echo", "n:validate n:echo")
	if err != nil {
		t.Fatalf("AddRule: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := cli.Send(ctx, "test-echo", []byte(`{"hello":"world"}`))
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if result.Duration <= 0 {
		t.Errorf("expected positive duration, got %v", result.Duration)
	}
}

func TestClientSendRuleNotFound(t *testing.T) {
	sim := startSim(t, 1, 4)
	defer sim.Stop()
	time.Sleep(10 * time.Millisecond)

	cli := sim.Client()
	_, err := cli.Send(context.Background(), "nonexistent", nil)
	if err == nil {
		t.Fatal("expected error for nonexistent rule")
	}
}

func TestClientAddRule(t *testing.T) {
	sim := New(config.SimConfig{
		Nodes:   3,
		Workers: 4,
	})
	defer sim.Stop()

	cli := sim.Client()
	err := cli.AddRule("custom-rule", "n:validate n:inventory")
	if err != nil {
		t.Fatalf("AddRule: %v", err)
	}

	plans := cli.Plans()
	found := false
	for _, p := range plans {
		if p == "custom-rule" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected custom-rule in plans, got %v", plans)
	}

	// Verify registered on all nodes
	for _, node := range sim.Nodes {
		if node.Plans.Get("custom-rule") == nil {
			t.Fatal("custom-rule missing from node", node.ID)
		}
	}
}

func TestClientRegisterService(t *testing.T) {
	sim := New(config.SimConfig{
		Nodes:   1,
		Workers: 4,
	})
	defer sim.Stop()

	cli := sim.Client()
	cli.RegisterService(&services.MockService{
		Name:          "custom-svc",
		BaseLatency:   5 * time.Millisecond,
		MaxConcurrent: 10,
	})

	found := false
	for _, s := range cli.Services() {
		if s.Name == "custom-svc" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected custom-svc in services, got %+v", cli.Services())
	}
}
