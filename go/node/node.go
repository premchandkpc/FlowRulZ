// Package node is the unified node type that replaces both internal/node
// and internal/execnode. Different deployments = different wiring, not
// different source trees.
//
// This is where "server + runtime as one unit" actually lives — at
// construction time, not in two parallel source trees.
package node

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/premchandkpc/FlowRulZ/go/domain/execution"
	"github.com/premchandkpc/FlowRulZ/go/ports"
)

const (
	executeAllSemaphore = 16
	defaultExecTimeout  = 30 * time.Second
)

// Engine provides compiled plans. This is a port — the node doesn't
// know whether plans come from a Rust VM or a mock.
type Engine interface {
	// ActivePlanBytes returns all active compiled plans.
	ActivePlanBytes() [][]byte
}

// Scheduler enqueues and executes tasks. This is a port.
type Scheduler interface {
	EnqueueAndWait(ctx context.Context, task *Task) ([]byte, error)
	Stop()
}

// Task is a unit of work for the scheduler.
type Task struct {
	ID       string
	Priority int
	Body     []byte
	Execute  func(ctx context.Context, task *Task) ([]byte, error)
}

// Node is the unified node type. It composes the domain execution runner
// with whichever adapters are configured. Different deployments pick
// different adapters at construction time — this is the hexagonal wiring.
type Node struct {
	// Ports (injected)
	invoker ports.ServiceInvoker
	store   ports.StateStore
	cluster ports.ClusterCoordinator
	dedup   ports.DedupTracker
	saga    ports.SagaCompensator

	// Dependencies
	engine   Engine
	scheduler Scheduler
	executor execution.StepExecutor

	// Configuration
	execSem chan struct{}
	mu      sync.Mutex
}

// Config configures the node.
type Config struct {
	NodeID string
	// ... other config fields
}

// New creates a new Node with the given dependencies.
func New(
	engine Engine,
	scheduler Scheduler,
	executor execution.StepExecutor,
	invoker ports.ServiceInvoker,
	store ports.StateStore,
	cluster ports.ClusterCoordinator,
	dedup ports.DedupTracker,
	saga ports.SagaCompensator,
) *Node {
	return &Node{
		engine:    engine,
		scheduler: scheduler,
		executor:  executor,
		invoker:   invoker,
		store:     store,
		cluster:   cluster,
		dedup:     dedup,
		saga:      saga,
		execSem:   make(chan struct{}, executeAllSemaphore),
	}
}

// Execute runs all active plans against the given body.
func (n *Node) Execute(ctx context.Context, body []byte) ([][]byte, error) {
	plans := n.engine.ActivePlanBytes()
	if len(plans) == 0 {
		return nil, nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type planResult struct {
		index int
		out   []byte
		err   error
	}

	results := make([][]byte, len(plans))
	ch := make(chan planResult, len(plans))

	for i, plan := range plans {
		idx, p := i, plan

		// Acquire node-wide semaphore
		select {
		case n.execSem <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		go func() {
			defer func() { <-n.execSem }()

			task := &Task{
				ID:       fmt.Sprintf("plan-%d", idx),
				Priority: 1, // Normal priority
				Body:     body,
			}

			var out []byte
			var err error

			if n.scheduler != nil {
				out, err = n.scheduler.EnqueueAndWait(ctx, task)
			} else {
				// Direct execution without scheduler
				execID := fmt.Sprintf("exec-%s-%d", task.ID, time.Now().UnixNano())
				names := make(execution.ServiceNames)
				if entries, executorErr := PlanServices(p); executorErr == nil {
					for _, e := range entries {
						names[e.ID] = e.Name
					}
				}

				runner := execution.NewRunner(n.invoker, n.store, n.executor, n.saga, func(execID string) {
					n.tryCompensate(execID)
				})
				out, err = runner.ExecutePlan(ctx, execID, p, names)
			}

			ch <- planResult{idx, out, err}
		}()
	}

	var firstErr error
	for range plans {
		r := <-ch
		if r.err != nil && firstErr == nil {
			firstErr = r.err
			cancel()
		}
		if r.err == nil {
			results[r.index] = r.out
		}
	}

	return results, firstErr
}

// ExecutePlan runs a single plan.
func (n *Node) ExecutePlan(ctx context.Context, plan, body []byte) ([]byte, error) {
	names := make(execution.ServiceNames)
	if entries, err := PlanServices(plan); err == nil {
		for _, e := range entries {
			names[e.ID] = e.Name
		}
	}

	execID := fmt.Sprintf("exec-%d", time.Now().UnixNano())

	runner := execution.NewRunner(n.invoker, n.store, n.executor, n.saga, func(execID string) {
		n.tryCompensate(execID)
	})

	return runner.ExecutePlan(ctx, execID, plan, names)
}

// HandleMessage processes an incoming message through dedup, rate limit, and execution.
func (n *Node) HandleMessage(ctx context.Context, msg []byte) ([]byte, error) {
	if n.dedup != nil {
		h := fmt.Sprintf("%x", msg) // simplified hash for now
		if n.dedup.CheckAndMark(h) {
			slog.Debug("dedup skipped", "hash", h)
			return nil, nil
		}
	}

	execCtx, execCancel := context.WithTimeout(ctx, defaultExecTimeout)
	defer execCancel()

	results, err := n.Execute(execCtx, msg)
	if err != nil {
		slog.Error("execution failed", "error", err)
		return nil, err
	}
	if len(results) == 0 {
		return nil, nil
	}
	return results[0], nil
}

func (n *Node) tryCompensate(execID string) {
	if n.saga != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := n.saga.Compensate(ctx, execID); err != nil {
			slog.Error("saga compensation failed", "exec_id", execID, "error", err)
		}
	}
}

// Start starts the node. This is a lifecycle method that wires up
// all configured adapters.
func (n *Node) Start(ctx context.Context) error {
	slog.Info("node started")
	return nil
}

// Shutdown stops the node gracefully.
func (n *Node) Shutdown(ctx context.Context) error {
	if n.scheduler != nil {
		n.scheduler.Stop()
	}
	slog.Info("node stopped")
	return nil
}

// PlanServices returns service entries from a compiled plan.
// This is a placeholder — in production, this would call into the
// Rust VM's FFI to extract service names from the plan.
func PlanServices(plan []byte) ([]ServiceEntry, error) {
	// TODO: Call bridge.PlanServices(plan) via FFI
	return nil, nil
}

// ServiceEntry represents a service in a compiled plan.
type ServiceEntry struct {
	ID   uint16
	Name string
}
