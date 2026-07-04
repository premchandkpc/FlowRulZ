// Package node provides a simulated FlowRulZ node that wraps the real
// VM bridge and uses fabric-aware transport components.
package node

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/premchandkpc/FlowRulZ/server/bridge"
	"github.com/premchandkpc/FlowRulZ/server/pkg/transport"
	"github.com/premchandkpc/FlowRulZ/simulator/fabric"
	"github.com/premchandkpc/FlowRulZ/simulator/services"
)

// SimNode represents a simulated FlowRulZ node with isolated state
// that communicates through the fabric.
type SimNode struct {
	ID string

	// Fabric-aware transport.
	Bus    *fabric.Bus
	Fabric *fabric.Fabric

	// Simulated services.
	MockServices *services.ServiceRegistry
	Network      *fabric.Fabric

	// Execution state.
	planCache   map[string][]byte // ruleID -> planBytes
	stateStore  map[string]*ExecState
	stateMu     sync.RWMutex

	// Lifecycle.
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// ExecState tracks execution state for a simulated execution.
type ExecState struct {
	ID        string
	RuleID    string
	Status    string
	CtxBytes  []byte
	Output    []byte
	Error     string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Config configures a simulated node.
type Config struct {
	ID      string
	NodeID  string
	Workers int
	ExecDir string
}

// New creates a new SimNode with fabric-aware components.
func New(cfg Config, f *fabric.Fabric, mockServices *services.ServiceRegistry) *SimNode {
	// Create isolated state directory.
	execDir := filepath.Join(cfg.ExecDir, cfg.ID)
	os.MkdirAll(execDir, 0755)

	// Create fabric-aware bus.
	bus := fabric.NewBus(f, cfg.ID)

	sim := &SimNode{
		ID:           cfg.ID,
		Bus:          bus,
		Fabric:       f,
		MockServices: mockServices,
		Network:      f,
		planCache:    make(map[string][]byte),
		stateStore:   make(map[string]*ExecState),
	}

	return sim
}

// Start starts the simulated node.
func (s *SimNode) Start(ctx context.Context) {
	ctx, s.cancel = context.WithCancel(ctx)

	// Subscribe to execution requests.
	s.Bus.Subscribe(ctx, "execution", s.handleExecution)

	slog.Info("sim node started", "id", s.ID)
}

// Stop stops the simulated node.
func (s *SimNode) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
	s.Bus.Close()
	slog.Info("sim node stopped", "id", s.ID)
}

// handleExecution handles incoming execution requests from the fabric.
func (s *SimNode) handleExecution(ctx context.Context, msg *transport.Message) {
	ruleID := msg.Headers["rule_id"]
	if ruleID == "" {
		return
	}

	// Get the compiled plan.
	s.stateMu.RLock()
	planBytes, ok := s.planCache[ruleID]
	s.stateMu.RUnlock()

	if !ok {
		slog.Warn("rule not found", "rule_id", ruleID, "node", s.ID)
		return
	}

	// Execute the plan.
	output, err := s.executePlan(ctx, ruleID, planBytes, msg.Body)
	if err != nil {
		slog.Error("execution failed", "rule_id", ruleID, "error", err)
		return
	}

	slog.Info("execution completed", "rule_id", ruleID, "output_len", len(output))
}

// executePlan executes a compiled plan through the real VM.
func (s *SimNode) executePlan(ctx context.Context, ruleID string, planBytes, body []byte) ([]byte, error) {
	// Initialize context.
	ctxBytes, err := bridge.InitContext(body)
	if err != nil {
		return nil, fmt.Errorf("init context: %w", err)
	}

	// Execute steps until done.
	for step := 0; step < 100; step++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		out, err := bridge.ExecuteStep(planBytes, ctxBytes, nil, nil)
		if err != nil {
			return nil, fmt.Errorf("step %d: %w", step, err)
		}

		ctxBytes = out.CtxBytes

		if out.Error != "" {
			return nil, fmt.Errorf("step %d: %s", step, out.Error)
		}

		switch out.Result {
		case bridge.StepDone:
			return out.Output, nil

		case bridge.StepPending:
			// Handle service call through fabric.
			svcName := out.PendingSvc
			_ = svcName // TODO: look up and call service

		case bridge.StepContinue:
			// Continue to next step.
		}
	}

	return nil, fmt.Errorf("exceeded max steps")
}

// DeployRule compiles and deploys a DSL rule to this node.
func (s *SimNode) DeployRule(ruleID, dsl string) error {
	// Compile the DSL.
	planBytes, err := bridge.Compile(dsl, ruleID)
	if err != nil {
		return fmt.Errorf("compile rule %s: %w", ruleID, err)
	}

	// Store the compiled plan.
	s.stateMu.Lock()
	s.planCache[ruleID] = planBytes
	s.stateMu.Unlock()

	slog.Info("deployed rule", "node", s.ID, "rule", ruleID)
	return nil
}

// Snapshot returns a snapshot of the node's state.
func (s *SimNode) Snapshot() map[string]interface{} {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()

	return map[string]interface{}{
		"id":     s.ID,
		"plans":  len(s.planCache),
		"states": len(s.stateStore),
	}
}
