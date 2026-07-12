// Package harness orchestrates N simulated FlowRulZ nodes, wires them
// to the fabric and to a pool of MockServices, and runs scenarios
// end-to-end exactly as a real deployment would.
package harness

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/premchandkpc/FlowRulZ/server/pkg/transport"
	"github.com/premchandkpc/FlowRulZ/simulator/fabric"
	"github.com/premchandkpc/FlowRulZ/simulator/node"
	"github.com/premchandkpc/FlowRulZ/simulator/services"
)

// Harness orchestrates multiple simulated nodes.
type Harness struct {
	Fabric       *fabric.Fabric
	Nodes        []*node.SimNode
	MockServices *services.ServiceRegistry
	Config       Config

	tmpDir string
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Config configures the harness.
type Config struct {
	NumNodes   int
	Workers    int
	ExecDir    string
	Scenario   string
	ScenarioCfg *fabric.ScenarioConfig
}

// New creates a new Harness with N nodes wired to the fabric.
func New(cfg Config) *Harness {
	// Create temp directory for state.
	execDir := cfg.ExecDir
	if execDir == "" {
		var err error
		execDir, err = os.MkdirTemp("", "flowrulz-harness-*")
		if err != nil {
			slog.Error("failed to create temp dir", "error", err)
			execDir = "/tmp/flowrulz-sim"
		}
	}
	os.MkdirAll(execDir, 0755)

	// Create fabric.
	f := fabric.New()

	// Create mock services.
	mockServices := services.DefaultServices()

	// Create service invoker (decoupled from concrete registry).
	invoker := services.NewServiceRegistryInvoker(mockServices)

	// Create nodes.
	nodes := make([]*node.SimNode, cfg.NumNodes)
	for i := 0; i < cfg.NumNodes; i++ {
		nodeID := fmt.Sprintf("node-%d", i+1)
		addr := fmt.Sprintf("localhost:%d", 8001+i)

		// Register node with fabric.
		f.RegisterNode(nodeID, addr)

		// Create node.
		nodeCfg := node.Config{
			ID:      nodeID,
			NodeID:  nodeID,
			Workers: cfg.Workers,
			ExecDir: execDir,
		}
		nodes[i] = node.New(nodeCfg, f, invoker)
	}

	// Apply scenario configuration.
	if cfg.ScenarioCfg != nil {
		cfg.ScenarioCfg.Setup(f)
	}

	h := &Harness{
		Fabric:       f,
		Nodes:        nodes,
		MockServices: mockServices,
		Config:       cfg,
		tmpDir:       execDir,
	}

	return h
}

// Start starts all nodes.
func (h *Harness) Start(ctx context.Context) {
	ctx, h.cancel = context.WithCancel(ctx)

	for _, node := range h.Nodes {
		node.Start(ctx)
	}

	slog.Info("harness started", "nodes", len(h.Nodes))
}

// Stop stops all nodes and cleans up.
func (h *Harness) Stop() {
	if h.cancel != nil {
		h.cancel()
	}

	for _, node := range h.Nodes {
		node.Stop()
	}

	// Cleanup scenario.
	if h.Config.ScenarioCfg != nil && h.Config.ScenarioCfg.Cleanup != nil {
		h.Config.ScenarioCfg.Cleanup(h.Fabric)
	}

	// Cleanup temp directory.
	os.RemoveAll(h.tmpDir)

	slog.Info("harness stopped")
}

// DeployRule deploys a DSL rule to all nodes.
func (h *Harness) DeployRule(ruleID, dsl string) error {
	for _, node := range h.Nodes {
		if err := node.DeployRule(ruleID, dsl); err != nil {
			return fmt.Errorf("deploy to %s: %w", node.ID, err)
		}
	}
	return nil
}

// Execute sends an execution request to a node via the fabric.
func (h *Harness) Execute(ctx context.Context, nodeID, ruleID string, body []byte) ([]byte, error) {
	// Find the node.
	var target *node.SimNode
	for _, n := range h.Nodes {
		if n.ID == nodeID {
			target = n
			break
		}
	}
	if target == nil {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}

	// Send execution request via fabric.
	msg := &transport.Message{
		Body: body,
		Headers: map[string]string{
			"rule_id": ruleID,
		},
	}

	reply, err := target.Bus.Request(ctx, "execution", msg, 30*time.Second)
	if err != nil {
		return nil, fmt.Errorf("execute on %s: %w", nodeID, err)
	}

	return reply.Body, nil
}

// ExecuteRoundRobin sends execution requests to nodes in round-robin order.
func (h *Harness) ExecuteRoundRobin(ctx context.Context, ruleID string, body []byte) ([]byte, error) {
	// Simple round-robin.
	idx := 0
	nodeID := h.Nodes[idx].ID
	h.wg.Add(1)
	go func() { h.wg.Done() }() // placeholder
	return h.Execute(ctx, nodeID, ruleID, body)
}

// KillNode stops a node (simulates crash).
func (h *Harness) KillNode(nodeID string) error {
	for _, n := range h.Nodes {
		if n.ID == nodeID {
			n.Stop()
			slog.Info("killed node", "id", nodeID)
			return nil
		}
	}
	return fmt.Errorf("node %s not found", nodeID)
}

// RestartNode restarts a stopped node.
func (h *Harness) RestartNode(nodeID string) error {
	for _, n := range h.Nodes {
		if n.ID == nodeID {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			n.Start(ctx)
			slog.Info("restarted node", "id", nodeID)
			return nil
		}
	}
	return fmt.Errorf("node %s not found", nodeID)
}

// Partition creates a network partition between two nodes.
func (h *Harness) Partition(from, to string) {
	h.Fabric.Partition(from, to)
	slog.Info("partition created", "from", from, "to", to)
}

// Heal removes a partition between two nodes.
func (h *Harness) Heal(from, to string) {
	h.Fabric.Heal(from, to)
	slog.Info("partition healed", "from", from, "to", to)
}

// Snapshot returns a snapshot of all nodes' states.
func (h *Harness) Snapshot() []map[string]interface{} {
	snapshots := make([]map[string]interface{}, len(h.Nodes))
	for i, n := range h.Nodes {
		snapshots[i] = n.Snapshot()
	}
	return snapshots
}

// Cleanup removes all temporary files.
func (h *Harness) Cleanup() {
	os.RemoveAll(h.tmpDir)
}
