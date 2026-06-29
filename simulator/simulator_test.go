package simulator

import (
	"testing"
	"time"

	"github.com/premchandkpc/FlowRulZ/simulator/config"
	"github.com/premchandkpc/FlowRulZ/simulator/dispatcher"
	"github.com/premchandkpc/FlowRulZ/simulator/execution"
	"github.com/premchandkpc/FlowRulZ/simulator/metrics"
	"github.com/premchandkpc/FlowRulZ/simulator/network"
	"github.com/premchandkpc/FlowRulZ/simulator/scheduler"
	"github.com/premchandkpc/FlowRulZ/simulator/services"
	"github.com/premchandkpc/FlowRulZ/simulator/timeline"
)

func TestOrderFlowExecution(t *testing.T) {
	svcs := services.DefaultServices()
	tl := timeline.NewStore()
	mc := metrics.NewCollector()
	net := network.New(network.Config{MinLatency: 0, MaxLatency: 0})
	node := scheduler.New("test-node", 2, svcs, net, tl, mc)
	node.Plans.Add(execution.OrderFlow)
	node.Start()
	defer node.Stop()

	ctx := execution.NewContext(execution.OrderFlow, []byte(`{"order_id":"123"}`))
	node.Enqueue(ctx)

	time.Sleep(200 * time.Millisecond)

	if ctx.State != execution.StateCompleted {
		t.Fatalf("expected completed, got %s", ctx.State)
	}
	if len(ctx.Variables) == 0 {
		t.Fatal("expected variables to be populated")
	}
	if ctx.Variables["published"] != "order_confirmed" {
		t.Fatalf("expected published=order_confirmed, got %v", ctx.Variables["published"])
	}
	if mc.Completed() != 1 {
		t.Fatalf("expected 1 completed, got %d", mc.Completed())
	}
}

func TestSuspensionResume(t *testing.T) {
	svcs := services.DefaultServices()
	svc := svcs.Get("payment")
	svc.BaseLatency = 20 * time.Millisecond

	tl := timeline.NewStore()
	mc := metrics.NewCollector()
	net := network.New(network.Config{MinLatency: 0, MaxLatency: 0})
	node := scheduler.New("test-node", 2, svcs, net, tl, mc)
	node.Plans.Add(execution.PaymentFlow)
	node.Start()
	defer node.Stop()

	ctx := execution.NewContext(execution.PaymentFlow, []byte(`{"amount":100}`))
	node.Enqueue(ctx)
	time.Sleep(200 * time.Millisecond)

	if ctx.State != execution.StateCompleted {
		t.Fatalf("expected completed, got %s", ctx.State)
	}
	events := tl.ForExec(ctx.ID)
	if len(events) == 0 {
		t.Fatal("expected timeline events")
	}

	hasWaiting := false
	hasResponse := false
	for _, e := range events {
		if e.Type == timeline.EventServiceCall {
			hasWaiting = true
		}
		if e.Type == timeline.EventServiceResponse {
			hasResponse = true
		}
	}
	if !hasWaiting {
		t.Fatal("expected service call event")
	}
	if !hasResponse {
		t.Fatal("expected service response event")
	}
}

func TestServiceFailure(t *testing.T) {
	svcs := services.DefaultServices()
	pay := svcs.Get("payment")
	pay.FailureRate = 1.0

	tl := timeline.NewStore()
	mc := metrics.NewCollector()
	net := network.New(network.Config{MinLatency: 0, MaxLatency: 0})
	node := scheduler.New("test-node", 2, svcs, net, tl, mc)
	node.Plans.Add(execution.OrderFlow)
	node.Start()
	defer node.Stop()

	ctx := execution.NewContext(execution.OrderFlow, []byte(`{"order_id":"123"}`))
	node.Enqueue(ctx)
	time.Sleep(200 * time.Millisecond)

	if ctx.State != execution.StateFailed {
		t.Fatalf("expected failed, got %s (payment should be 100%% failure)", ctx.State)
	}
	if mc.Failed() != 1 {
		t.Fatalf("expected 1 failure, got %d", mc.Failed())
	}
}

func TestMultiNodeDispatch(t *testing.T) {
	svcs := services.DefaultServices()
	svcs.Get("payment").BaseLatency = 5 * time.Millisecond
	svcs.Get("inventory").BaseLatency = 2 * time.Millisecond
	svcs.Get("fraud").BaseLatency = 3 * time.Millisecond
	svcs.Get("email").BaseLatency = 1 * time.Millisecond

	tl := timeline.NewStore()
	mc := metrics.NewCollector()
	net := network.New(network.Config{MinLatency: 0, MaxLatency: 0})

	nodes := make([]*scheduler.Scheduler, 3)
	for i := 0; i < 3; i++ {
		nd := scheduler.New("node", 4, svcs, net, tl, mc)
		nd.Plans.Add(execution.OrderFlow)
		nodes[i] = nd
	}

	disp := dispatcher.New(nodes, tl)
	disp.StartAll()
	defer disp.StopAll()

	for i := 0; i < 10; i++ {
		ctx := execution.NewContext(execution.OrderFlow, []byte(`{"order_id":"123"}`))
		disp.Dispatch(ctx)
	}

	// Wait for all 10 executions to complete (10s max, avoids flakiness under CPU load)
	for start := time.Now(); time.Since(start) < 10*time.Second; {
		if mc.Completed() == 10 {
			return
		}
		if mc.Failed() > 3 {
			t.Fatalf("too many failures: %d out of 10", mc.Failed())
		}
		time.Sleep(20 * time.Millisecond)
	}
	completed := mc.Completed()
	if completed < 8 {
		t.Fatalf("expected >=8 completed in 10s, got %d", completed)
	}
}

func TestFullSimulatorRun(t *testing.T) {
	cfg := config.SimConfig{
		Nodes:     2,
		Workers:   2,
		Scenario:  "ramp-up",
		Duration:  3 * time.Second,
		Rate:      50,
		Dashboard: false,
	}
	sim := New(cfg)
	err := sim.Run()
	if err != nil {
		t.Fatalf("simulator run failed: %v", err)
	}
	if sim.Metrics.Completed()+sim.Metrics.Failed() == 0 {
		t.Fatal("expected some executions to complete")
	}
}

func TestPaymentOutageAllFail(t *testing.T) {
	cfg := config.SimConfig{
		Nodes:     1,
		Workers:   1,
		Scenario:  "payment-outage",
		Duration:  2 * time.Second,
		Rate:      10,
		Dashboard: false,
	}
	sim := New(cfg)
	err := sim.Run()
	if err != nil {
		t.Fatalf("simulator run failed: %v", err)
	}
	if sim.Metrics.Failed() == 0 {
		t.Fatal("expected all executions to fail (payment 100% failure)")
	}
	if sim.Metrics.Completed()+sim.Metrics.Failed() == 0 {
		t.Fatal("expected some executions")
	}
}
