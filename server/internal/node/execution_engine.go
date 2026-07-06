package node

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/premchandkpc/FlowRulZ/server/bridge"
	"github.com/premchandkpc/FlowRulZ/server/internal/execstate"
	"github.com/premchandkpc/FlowRulZ/server/internal/observability"
	"github.com/premchandkpc/FlowRulZ/server/internal/reliability"
	"github.com/premchandkpc/FlowRulZ/server/internal/scheduler"
)

const (
	maxExecutionSteps   = 1000
	executeAllSemaphore = 16
	defaultExecTimeout  = 30 * time.Second
)

// ExecutionEngine runs bytecode execution plans through the VM step-loop.
type ExecutionEngine struct {
	engine     NodeEngine
	scheduler  *scheduler.Scheduler
	stateStore StateStore
	execs      ExecRegistry
	saga       NodeSagaTracker
	invoker    ServiceInvoker

	circuitBreakers sync.Map
	execSem         chan struct{}
}

// NewExecutionEngine creates an ExecutionEngine with the given dependencies.
func NewExecutionEngine(
	engine NodeEngine,
	sched *scheduler.Scheduler,
	stateStore StateStore,
	execs ExecRegistry,
	saga NodeSagaTracker,
	invoker ServiceInvoker,
) *ExecutionEngine {
	return &ExecutionEngine{
		engine:     engine,
		scheduler:  sched,
		stateStore: stateStore,
		execs:      execs,
		saga:       saga,
		invoker:    invoker,
		execSem:    make(chan struct{}, executeAllSemaphore),
	}
}

// ExecutePlan runs a single bytecode plan to completion.
func (e *ExecutionEngine) ExecutePlan(ctx context.Context, plan []byte, body []byte) ([]byte, error) {
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

	st := &execstate.State{
		ID:        execID,
		PlanBytes: plan,
		Status:    execstate.StatusCreated,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if e.stateStore != nil {
		if err := e.stateStore.Create(execCtx, st); err != nil {
			slog.Warn("state store create failed", "exec_id", execID, "error", err)
		}
	}

	out, err := e.runSteps(execCtx, execID, plan, names, nil, nil, st)
	if e.stateStore != nil {
		if err != nil {
			st.Status = execstate.StatusFailed
			st.Error = err.Error()
			if saveErr := e.stateStore.Save(execCtx, st); saveErr != nil {
				slog.Warn("state store save failed", "exec_id", execID, "error", saveErr)
			}
		} else {
			st.Status = execstate.StatusCompleted
			st.Output = out
			if saveErr := e.stateStore.Save(execCtx, st); saveErr != nil {
				slog.Warn("state store save failed", "exec_id", execID, "error", saveErr)
			}
		}
	}
	return out, err
}

func (e *ExecutionEngine) runSteps(ctx context.Context, execID string, plan []byte, names map[uint16]string, startCtx, startResp []byte, st *execstate.State) ([]byte, error) {
	ctxBytes, respBytes := startCtx, startResp

	for step := 0; step < maxExecutionSteps; step++ {
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
			observability.RecordExec("completed")
			if e.saga != nil {
				e.saga.Clear(execID)
			}
			return out.Output, nil

		case bridge.StepPending:
			observability.RecordExec("svc_pending")
			if e.stateStore != nil {
				st.Status = execstate.StatusWaitingForService
				st.PendingSvc = out.PendingSvc
				st.PendingBody = out.PendingBody
				st.CtxBytes = ctxBytes
				if err := e.stateStore.Save(ctx, st); err != nil {
					slog.Warn("state store save failed", "exec_id", execID, "error", err)
				}
			}

			rawName, ok := names[out.PendingSvc]
			if !ok {
				rawName = fmt.Sprintf("svc-%d", out.PendingSvc)
			}
			svcName, method, compSvc, compMethod := bridge.ParseCompensation(rawName)

			if e.saga != nil && compSvc != "" {
				e.saga.RegisterStep(execID, reliability.SagaStep{
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

			if e.stateStore != nil {
				st.Status = execstate.StatusRunning
				st.PendingSvc = 0
				st.PendingBody = nil
				st.CtxBytes = ctxBytes
				if err := e.stateStore.Save(ctx, st); err != nil {
					slog.Warn("state store save failed", "exec_id", execID, "error", err)
				}
			}
			respBytes = resp

		case bridge.StepContinue:
			respBytes = nil
			if e.stateStore != nil {
				st.Status = execstate.StatusRunning
				st.CtxBytes = ctxBytes
				if err := e.stateStore.Save(ctx, st); err != nil {
					slog.Warn("state store save failed", "exec_id", execID, "error", err)
				}
			}
		}
	}

	e.tryCompensate(execID)
	return nil, fmt.Errorf("execution exceeded max steps")
}

// ExecuteAll runs all active plans concurrently.
func (e *ExecutionEngine) ExecuteAll(ctx context.Context, body []byte) ([][]byte, error) {
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
			task := &scheduler.Task{
				ID:       fmt.Sprintf("plan-%d", idx),
				Priority: scheduler.PriorityNormal,
				Body:     body,
				Execute: func(execCtx context.Context, task *scheduler.Task) ([]byte, error) {
					return e.ExecutePlan(execCtx, p, task.Body)
				},
			}
			out, err := e.scheduler.EnqueueAndWait(ctx, task)
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

func (e *ExecutionEngine) callService(svcName, method string, body []byte, timeoutMs uint64) ([]byte, error) {
	observability.RecordExec("svc_call")

	svcTimeout := 10 * time.Second
	if timeoutMs > 0 {
		svcTimeout = time.Duration(timeoutMs) * time.Millisecond
	}
	svcCtx, svcCancel := context.WithTimeout(context.Background(), svcTimeout)
	defer svcCancel()

	cbI, _ := e.circuitBreakers.LoadOrStore(svcName, reliability.NewCircuitBreaker(5, 30*time.Second))
	cb := cbI.(*reliability.CircuitBreaker)

	if !cb.Allow() {
		observability.RecordError("circuit_breaker_open")
		slog.Warn("circuit breaker open for service", "service", svcName)
		return nil, fmt.Errorf("circuit breaker open for service %s", svcName)
	}

	resp, err := e.invoker.Invoke(svcCtx, svcName, method, body)
	if err != nil {
		cb.Failure()
		return nil, fmt.Errorf("service %s: %w", svcName, err)
	}

	cb.Success()
	return resp, nil
}

func (e *ExecutionEngine) tryCompensate(execID string) {
	if e.saga == nil {
		return
	}
	if err := e.saga.Compensate(execID); err != nil {
		slog.Error("saga compensation failed", "exec_id", execID, "error", err)
	}
}
