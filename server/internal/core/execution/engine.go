// Package execution implements the core rule execution engine.
// Depends only on ports — no imports from adapters/.
package execution

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/premchandkpc/FlowRulZ/server/bridge"
	"github.com/premchandkpc/FlowRulZ/server/internal/ports"
)

const (
	MaxExecutionSteps   = 1000
	ExecuteAllSemaphore = 16
	DefaultExecTimeout  = 30 * time.Second
)

// Engine runs bytecode execution plans through the VM step-loop.
type Engine struct {
	engine     ports.RuleEngine
	stateStore ports.StateStore
	execs      ports.ExecTracker
	saga       ports.SagaTracker
	invoker    ports.ServiceInvoker
	metrics    ports.MetricsCollector

	circuitBreakers sync.Map
	execSem         chan struct{}
}

// NewEngine creates an execution Engine with the given dependencies.
func NewEngine(
	engine ports.RuleEngine,
	stateStore ports.StateStore,
	execs ports.ExecTracker,
	saga ports.SagaTracker,
	invoker ports.ServiceInvoker,
	metrics ports.MetricsCollector,
) *Engine {
	return &Engine{
		engine:     engine,
		stateStore: stateStore,
		execs:      execs,
		saga:       saga,
		invoker:    invoker,
		metrics:    metrics,
		execSem:    make(chan struct{}, ExecuteAllSemaphore),
	}
}

// ExecutePlan runs a single bytecode plan to completion.
func (e *Engine) ExecutePlan(ctx context.Context, plan []byte, body []byte) ([]byte, error) {
	names := make(map[uint16]string)
	if entries, err := bridge.PlanServices(plan); err == nil {
		for _, entry := range entries {
			names[entry.ID] = entry.Name
		}
	}

	execID := uuid.New().String()
	now := time.Now().UTC()

	execCtx, cancel := context.WithCancel(ctx)
	e.execs.Register(execID, cancel, "")

	defer func() {
		e.execs.Unregister(execID)
		cancel()
	}()

	if e.stateStore != nil {
		rec := &ports.ExecutionRecord{
			ID:        ports.ExecutionID(execID),
			State:     "created",
			CreatedAt: now,
		}
		if err := e.stateStore.Create(execCtx, rec); err != nil {
			slog.Warn("state store create failed", "exec_id", execID, "error", err)
		}
	}

	out, err := e.runSteps(execCtx, execID, plan, names, nil, nil)
	if e.stateStore != nil {
		rec := &ports.ExecutionRecord{
			ID:    ports.ExecutionID(execID),
			State: "completed",
			Output: out,
		}
		if err != nil {
			rec.State = "failed"
			rec.Error = err.Error()
		}
		if saveErr := e.stateStore.Save(execCtx, rec); saveErr != nil {
			slog.Warn("state store save failed", "exec_id", execID, "error", saveErr)
		}
	}
	return out, err
}

func (e *Engine) runSteps(ctx context.Context, execID string, plan []byte, names map[uint16]string, startCtx, startResp []byte) ([]byte, error) {
	ctxBytes, respBytes := startCtx, startResp

	for step := 0; step < MaxExecutionSteps; step++ {
		select {
		case <-ctx.Done():
			e.tryCompensate(execID)
			return nil, fmt.Errorf("execution cancelled at step %d: %w", step, ctx.Err())
		default:
		}

		out, err := bridge.ExecuteStep(plan, ctxBytes, respBytes, nil)
		if err != nil {
			e.tryCompensate(execID)
			return nil, fmt.Errorf("step %d: %w", step, err)
		}

		ctxBytes = out.CtxBytes

		switch out.Result {
		case bridge.StepDone:
			if e.metrics != nil {
				e.metrics.RecordExec("completed")
			}
			if e.saga != nil {
				e.saga.Clear(execID)
			}
			return out.Output, nil

		case bridge.StepPending:
			if e.metrics != nil {
				e.metrics.RecordExec("svc_pending")
			}

			rawName, ok := names[out.PendingSvc]
			if !ok {
				rawName = fmt.Sprintf("svc-%d", out.PendingSvc)
			}
			svcName, method, compSvc, compMethod := bridge.ParseCompensation(rawName)

			if e.saga != nil && compSvc != "" {
				e.saga.RegisterStep(execID, ports.SagaStep{
					ServiceName: svcName,
					Method:      method,
					Body:        out.PendingBody,
					CompSvc:     compSvc,
					CompMethod:  compMethod,
				})
			}

			resp, err := e.callService(svcName, method, out.PendingBody, out.TimeoutMs)
			if err != nil {
				e.tryCompensate(execID)
				return nil, fmt.Errorf("service %s: %w", svcName, err)
			}
			respBytes = resp

		case bridge.StepContinue:
			respBytes = nil
		}
	}

	e.tryCompensate(execID)
	return nil, fmt.Errorf("execution exceeded max steps")
}

// ExecuteAll runs all active plans concurrently.
func (e *Engine) ExecuteAll(ctx context.Context, body []byte) ([][]byte, error) {
	plans := e.engine.ActivePlanBytes()
	if len(plans) == 0 {
		return nil, nil
	}

	type planResult struct {
		index int
		out   []byte
		err   error
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make([][]byte, len(plans))
	ch := make(chan planResult, len(plans))

	for i, plan := range plans {
		idx, p := i, plan

		select {
		case e.execSem <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		go func() {
			defer func() { <-e.execSem }()
			out, err := e.ExecutePlan(ctx, p, body)
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

func (e *Engine) callService(svcName, method string, body []byte, timeoutMs uint64) ([]byte, error) {
	if e.metrics != nil {
		e.metrics.RecordExec("svc_call")
	}

	svcTimeout := 10 * time.Second
	if timeoutMs > 0 {
		svcTimeout = time.Duration(timeoutMs) * time.Millisecond
	}
	svcCtx, svcCancel := context.WithTimeout(context.Background(), svcTimeout)
	defer svcCancel()

	resp, err := e.invoker.Invoke(svcCtx, svcName, method, body)
	if err != nil {
		if e.metrics != nil {
			e.metrics.RecordError("svc_call")
		}
		return nil, fmt.Errorf("service %s: %w", svcName, err)
	}

	return resp, nil
}

func (e *Engine) tryCompensate(execID string) {
	if e.saga == nil {
		return
	}
	if err := e.saga.Compensate(execID); err != nil {
		slog.Error("saga compensation failed", "exec_id", execID, "error", err)
	}
}
