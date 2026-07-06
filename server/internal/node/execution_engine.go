package node

import (
	"context"
	"fmt"

	"github.com/premchandkpc/FlowRulZ/server/bridge"
	"github.com/premchandkpc/FlowRulZ/server/internal/core/execution"
	"github.com/premchandkpc/FlowRulZ/server/internal/execstate"
	"github.com/premchandkpc/FlowRulZ/server/internal/observability"
)

// ExecutionEngine wraps core/execution.Engine and provides the node-local API.
// Business logic lives in core/execution; this file is a thin adapter.
type ExecutionEngine struct {
	core    *execution.Engine
	engine  NodeEngine
	state   StateStore
	execs   ExecRegistry
	saga    NodeSagaTracker
	invoker ServiceInvoker
}

// NewExecutionEngine creates an ExecutionEngine delegating to core/execution.Engine.
func NewExecutionEngine(
	engine NodeEngine,
	_ interface{}, // scheduler (unused in core engine)
	stateStore StateStore,
	execs ExecRegistry,
	saga NodeSagaTracker,
	invoker ServiceInvoker,
) *ExecutionEngine {
	coreEngine := execution.NewEngine(
		engine,
		&stateStoreAdapter{inner: stateStore},
		&execRegistryAdapter{inner: execs},
		&sagaAdapter{inner: saga},
		invoker,
		&metricsAdapter{inner: observability.NewMetricsCollector()},
	)
	return &ExecutionEngine{
		core:    coreEngine,
		engine:  engine,
		state:   stateStore,
		execs:   execs,
		saga:    saga,
		invoker: invoker,
	}
}

// ExecutePlan delegates to core/execution.Engine.
func (e *ExecutionEngine) ExecutePlan(ctx context.Context, plan []byte, body []byte) ([]byte, error) {
	return e.core.ExecutePlan(ctx, plan, body)
}

// ExecuteAll delegates to core/execution.Engine.
func (e *ExecutionEngine) ExecuteAll(ctx context.Context, body []byte) ([][]byte, error) {
	return e.core.ExecuteAll(ctx, body)
}

// runSteps is used by recovery.go — bridges core engine's private runSteps.
func (e *ExecutionEngine) runSteps(ctx context.Context, execID string, plan []byte, names map[uint16]string, startCtx, startResp []byte, st *execstate.State) ([]byte, error) {
	ctxBytes, respBytes := startCtx, startResp

	for step := 0; step < execution.MaxExecutionSteps; step++ {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("execution cancelled at step %d: %w", step, ctx.Err())
		default:
		}

		out, err := bridge.ExecuteStep(plan, ctxBytes, respBytes, nil)
		if err != nil {
			return nil, fmt.Errorf("step %d: %w", step, err)
		}

		ctxBytes = out.CtxBytes

		switch out.Result {
		case bridge.StepDone:
			observability.RecordExec("completed")
			return out.Output, nil

		case bridge.StepPending:
			observability.RecordExec("svc_pending")
			rawName, ok := names[out.PendingSvc]
			if !ok {
				rawName = fmt.Sprintf("svc-%d", out.PendingSvc)
			}
			svcName, method, _, _ := bridge.ParseCompensation(rawName)

			resp, err := e.invoker.Invoke(ctx, svcName, method, out.PendingBody)
			if err != nil {
				return nil, fmt.Errorf("service %s: %w", svcName, err)
			}
			respBytes = resp

		case bridge.StepContinue:
			respBytes = nil
		}
	}

	return nil, fmt.Errorf("execution exceeded max steps")
}
