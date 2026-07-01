package simulator

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/premchandkpc/FlowRulZ/go/bridge"
	"github.com/premchandkpc/FlowRulZ/go/pkg/transport"
	"github.com/premchandkpc/FlowRulZ/simulator/config"
	"github.com/premchandkpc/FlowRulZ/simulator/dashboard"
	"github.com/premchandkpc/FlowRulZ/simulator/dispatcher"
	"github.com/premchandkpc/FlowRulZ/simulator/eventbus"
	"github.com/premchandkpc/FlowRulZ/simulator/execution"
	"github.com/premchandkpc/FlowRulZ/simulator/loadgen"
	"github.com/premchandkpc/FlowRulZ/simulator/metrics"
	"github.com/premchandkpc/FlowRulZ/simulator/network"
	"github.com/premchandkpc/FlowRulZ/simulator/scenarios"
	"github.com/premchandkpc/FlowRulZ/simulator/scheduler"
	"github.com/premchandkpc/FlowRulZ/simulator/services"
	"github.com/premchandkpc/FlowRulZ/simulator/timeline"
)

// compileDSL compiles a DSL rule and attaches the bytecode + service name map to the plan.
func compileDSL(plan *execution.Plan, dsl string) error {
	planBytes, err := bridge.Compile(dsl, plan.ID)
	if err != nil {
		return fmt.Errorf("compile %s: %w", plan.ID, err)
	}
	plan.PlanBytes = planBytes

	svcs, err := bridge.PlanServices(planBytes)
	if err != nil {
		return fmt.Errorf("plan services %s: %w", plan.ID, err)
	}
	plan.ServiceNames = make(map[uint16]string, len(svcs))
	for _, svc := range svcs {
		plan.ServiceNames[svc.ID] = svc.Name
	}
	return nil
}

type Simulator struct {
	cfg        config.SimConfig
	Services   *services.ServiceRegistry
	Network    *network.Network
	Timeline   *timeline.Store
	Metrics    *metrics.Collector
	Nodes      []*scheduler.Scheduler
	Dispatcher *dispatcher.Dispatcher
	Bus        transport.EventBus
	LoadGen    *loadgen.Generator
	Dashboard  *dashboard.Dashboard
	Scenario   *scenarios.Scenario
}

func New(cfg config.SimConfig) *Simulator {
	svcs := services.DefaultServices()
	tl := timeline.NewStore()
	mc := metrics.NewCollector()

	netCfg := network.Config{
		MinLatency: 1 * time.Millisecond,
		MaxLatency: 3 * time.Millisecond,
	}

	var net *network.Network
	var lgCfg loadgen.Config
	var scenario *scenarios.Scenario

	if s := scenarios.ByName(cfg.Scenario); s != nil {
		scenario = s
		scenarioCfg, scenarioLg := s.Apply(svcs)
		netCfg = scenarioCfg
		lgCfg = scenarioLg
		log.Printf("scenario: %s (%s)", s.Name, s.Description)
	} else {
		lgCfg = loadgen.DefaultConfig()
	}

	if cfg.Rate > 0 {
		lgCfg.RequestsPerSec = cfg.Rate
	}
	if cfg.Duration > 0 {
		lgCfg.Duration = cfg.Duration
	}
	if cfg.Speed != 1.0 && cfg.Speed > 0 {
		netCfg.MinLatency = time.Duration(float64(netCfg.MinLatency) / cfg.Speed)
		netCfg.MaxLatency = time.Duration(float64(netCfg.MaxLatency) / cfg.Speed)
		lgCfg.RequestsPerSec = int(float64(lgCfg.RequestsPerSec) * cfg.Speed)
		log.Printf("speed: %.1fx (rate=%d, net=%v/%v)", cfg.Speed, lgCfg.RequestsPerSec, netCfg.MinLatency, netCfg.MaxLatency)
	}

	net = network.New(netCfg)
	if cfg.Chaos.DropPackets || cfg.Chaos.SlowNetwork {
		net.SetChaos(cfg.Chaos)
		log.Printf("chaos mode: drop=%v slow=%.1fx", cfg.Chaos.DropPackets, cfg.Chaos.SlowFactor)
	}

	// Compile default plans from DSL into real bytecode
	dslPlans := map[string]string{
		"order-flow-v1":  "n:validate n:inventory n:fraud n:payment.authorize n:email",
		"payment-flow-v1": "n:validate n:payment.capture n:loyalty",
		"refund-flow-v1": "n:validate n:payment.refund n:invoice",
	}
	type compiledPlan struct {
		id         string
		planBytes  []byte
		svcNames   map[uint16]string
	}
	var compiled []compiledPlan
	for _, plan := range scenarios.DefaultPlans() {
		if dsl, ok := dslPlans[plan.ID]; ok {
			planBytes, err := bridge.Compile(dsl, plan.ID)
			if err != nil {
				log.Printf("warning: compile %s: %v", plan.ID, err)
				continue
			}
			svcNames := make(map[uint16]string)
			svcs2, err := bridge.PlanServices(planBytes)
			if err == nil {
				for _, svc := range svcs2 {
					svcNames[svc.ID] = svc.Name
				}
			}
			compiled = append(compiled, compiledPlan{plan.ID, planBytes, svcNames})
			log.Printf("compiled %s (%d bytes, %d services)", plan.ID, len(planBytes), len(svcNames))
		}
	}

	nodes := make([]*scheduler.Scheduler, cfg.Nodes)
	for i := 0; i < cfg.Nodes; i++ {
		id := fmt.Sprintf("node-%d", i+1)
		nodes[i] = scheduler.New(id, cfg.Workers, svcs, net, tl, mc)
		for _, plan := range scenarios.DefaultPlans() {
			// Clone the plan and attach compiled bytecode if available
			p := *plan
			for _, cp := range compiled {
				if cp.id == p.ID {
					p.PlanBytes = cp.planBytes
					p.ServiceNames = cp.svcNames
					break
				}
			}
			nodes[i].Plans.Add(&p)
		}
	}

	disp := dispatcher.New(nodes, tl)
	lg := loadgen.New(lgCfg, disp, mc)
	var dash *dashboard.Dashboard
	if cfg.Dashboard {
		dash = dashboard.New(cfg.DashboardAddr, nodes, tl, mc)
	}

	bus := eventbus.New(100)
	for _, node := range nodes {
		node.SetBus(bus)
		node.SubscribeBus()
	}

	sim := &Simulator{
		cfg:        cfg,
		Services:   svcs,
		Network:    net,
		Timeline:   tl,
		Metrics:    mc,
		Nodes:      nodes,
		Dispatcher: disp,
		Bus:        bus,
		LoadGen:    lg,
		Dashboard:  dash,
		Scenario:   scenario,
	}

	if scenario != nil && scenario.Setup != nil {
		if err := scenario.Setup(sim.Client()); err != nil {
			log.Printf("scenario setup error: %v", err)
		}
	}

	return sim
}

func (s *Simulator) Run() error {
	log.Printf("simulator: starting (%d nodes, %d workers/node)", s.cfg.Nodes, s.cfg.Workers)
	s.Dispatcher.StartAll()

	if s.Dashboard != nil {
		s.Dashboard.Start()
	}

	s.LoadGen.Start()

	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.Duration+2*time.Second)
	defer cancel()

	<-ctx.Done()
	s.Stop()

	snapshot := s.Metrics.Snapshot()
	fmt.Printf("\n=== Results ===\n")
	fmt.Printf("Completed: %d\n", snapshot.Completed)
	fmt.Printf("Failed:    %d\n", snapshot.Failed)
	fmt.Printf("Dropped:   %d\n", snapshot.Dropped)
	fmt.Printf("Throughput: %.0f req/s\n", snapshot.Throughput)
	fmt.Printf("Avg latency: %v\n", snapshot.AvgLatency)
	fmt.Printf("P50: %v  P95: %v  P99: %v\n", snapshot.P50, snapshot.P95, snapshot.P99)
	if len(snapshot.ServiceLatency) > 0 {
		fmt.Println("\nService Latencies:")
		for name, st := range snapshot.ServiceLatency {
			errRate := snapshot.ServiceErrorRate[name]
			fmt.Printf("  %-12s avg=%v p50=%v p95=%v p99=%v err=%.1f%%\n",
				name, st.Avg, st.P50, st.P95, st.P99, errRate*100)
		}
	}
	return nil
}

func (s *Simulator) Stop() {
	s.LoadGen.Stop()
	s.Dispatcher.StopAll()
	if s.Dashboard != nil {
		s.Dashboard.Stop()
	}
	log.Printf("simulator: stopped")
}
