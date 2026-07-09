package node

import (
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
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

func (n *ProdNode) executePlan(ctx context.Context, plan []byte, body []byte) ([]byte, error) {
	names := make(map[uint16]string)
	if entries, err := bridge.PlanServices(plan); err == nil {
		for _, e := range entries {
			names[e.ID] = e.Name
		}
	}

	execID := uuid.New().String()
	now := time.Now().UTC()

	execCtx, cancel := context.WithCancel(ctx)
	n.Execs.Register(execID, cancel, "")

	defer func() {
		n.Execs.Unregister(execID)
		cancel()
	}()

	st := &execstate.State{
		ID:        execID,
		PlanBytes: plan,
		Status:    execstate.StatusCreated,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if n.StateStore != nil {
		if err := n.StateStore.Create(execCtx, st); err != nil {
			slog.Warn("state store create failed", "exec_id", execID, "error", err)
		}
	}

	out, err := n.runSteps(execCtx, execID, plan, names, nil, nil, st)
	if n.StateStore != nil {
		if err != nil {
			st.Status = execstate.StatusFailed
			st.Error = err.Error()
			if saveErr := n.StateStore.Save(execCtx, st); saveErr != nil {
				slog.Warn("state store save failed", "exec_id", execID, "error", saveErr)
			}
		} else {
			st.Status = execstate.StatusCompleted
			st.Output = out
			if saveErr := n.StateStore.Save(execCtx, st); saveErr != nil {
				slog.Warn("state store save failed", "exec_id", execID, "error", saveErr)
			}
		}
	}
	return out, err
}

func (n *ProdNode) runSteps(ctx context.Context, execID string, plan []byte, names map[uint16]string, startCtx, startResp []byte, st *execstate.State) ([]byte, error) {
	ctxBytes, respBytes := startCtx, startResp

	for step := 0; step < maxExecutionSteps; step++ {
		select {
		case <-ctx.Done():
			n.tryCompensate(execID)
			return nil, fmt.Errorf("execution cancelled at step %d: %w", step, ctx.Err())
		default:
		}

		out, err := bridge.ExecuteStep(plan, ctxBytes, respBytes, nil)
		if err != nil {
			n.tryCompensate(execID)
			return nil, fmt.Errorf("step %d: %w", step, err)
		}

		ctxBytes = out.CtxBytes

		switch out.Result {
		case bridge.StepDone:
			observability.RecordExec("completed")
			if n.Saga != nil {
				n.Saga.Clear(execID)
			}
			return out.Output, nil

		case bridge.StepPending:
			observability.RecordExec("svc_pending")
			if n.StateStore != nil {
				st.Status = execstate.StatusWaitingForService
				st.PendingSvc = out.PendingSvc
				st.PendingBody = out.PendingBody
				st.CtxBytes = ctxBytes
				if err := n.StateStore.Save(ctx, st); err != nil {
					slog.Warn("state store save failed", "exec_id", execID, "error", err)
				}
			}

			rawName, ok := names[out.PendingSvc]
			if !ok {
				rawName = fmt.Sprintf("svc-%d", out.PendingSvc)
			}
			svcName, method, compSvc, compMethod := bridge.ParseCompensation(rawName)

			if n.Saga != nil && compSvc != "" {
				n.Saga.RegisterStep(execID, reliability.SagaStep{
					ServiceName: svcName,
					Method:      method,
					Body:        out.PendingBody,
					CompSvc:     compSvc,
					CompMethod:  compMethod,
				})
			}

			resp, err := n.callService(svcName, method, out.PendingBody, out.TimeoutMs)
			if err != nil {
				n.tryCompensate(execID)
				return nil, fmt.Errorf("service %s: %w", svcName, err)
			}

			if n.StateStore != nil {
				st.Status = execstate.StatusRunning
				st.PendingSvc = 0
				st.PendingBody = nil
				st.CtxBytes = ctxBytes
				if err := n.StateStore.Save(ctx, st); err != nil {
					slog.Warn("state store save failed", "exec_id", execID, "error", err)
				}
			}
			respBytes = resp

		case bridge.StepContinue:
			respBytes = nil
			if n.StateStore != nil {
				st.Status = execstate.StatusRunning
				st.CtxBytes = ctxBytes
				if err := n.StateStore.Save(ctx, st); err != nil {
					slog.Warn("state store save failed", "exec_id", execID, "error", err)
				}
			}
		}
	}

	n.tryCompensate(execID)
	return nil, fmt.Errorf("execution exceeded max steps")
}

func (n *ProdNode) executeAll(ctx context.Context, body []byte) ([][]byte, error) {
	plans := n.Engine.ActivePlanBytes()
	if len(plans) == 0 {
		return nil, nil
	}

	sched, ok := n.Scheduler.(*scheduler.Scheduler)
	if !ok {
		return nil, fmt.Errorf("scheduler not available")
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
		
		// Acquire node-wide semaphore to limit total concurrency
		select {
		case n.execSem <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		
		go func() {
			defer func() { <-n.execSem }()
			task := &scheduler.Task{
				ID:       fmt.Sprintf("plan-%d", idx),
				Priority: scheduler.PriorityNormal,
				Body:     body,
				Execute: func(execCtx context.Context, task *scheduler.Task) ([]byte, error) {
					return n.executePlan(execCtx, p, task.Body)
				},
			}
			out, err := sched.EnqueueAndWait(ctx, task)
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

func (n *ProdNode) handleIncomingMessage(ctx context.Context, msg []byte) ([]byte, error) {
	if !n.RateLimiter.Allow("ingress") {
		observability.RecordError("rate_limited")
		h := fnv.New128a()
		h.Write(msg)
		msgID := fmt.Sprintf("rl-%x", h.Sum(nil))
		n.DLQ.Send(&reliability.DeadLetterEntry{
			ID:    msgID,
			Body:  msg,
			Error: "rate limited",
		})
		return nil, nil
	}

	h := fnv.New128a()
	h.Write(msg)
	msgIDStr := fmt.Sprintf("%x", h.Sum(nil))

	if n.Dedup.CheckAndMark(msgIDStr) {
		observability.RecordExec("dedup_skipped")
		return nil, nil
	}

	execCtx, execCancel := context.WithTimeout(ctx, defaultExecTimeout)
	defer execCancel()

	results, err := n.executeAll(execCtx, msg)
	if err != nil {
		observability.RecordError("exec")
		n.DLQ.Send(&reliability.DeadLetterEntry{
			ID:    "exec-error",
			Body:  msg,
			Error: err.Error(),
		})
		return nil, err
	}
	if len(results) == 0 {
		return nil, nil
	}
	observability.RecordExec("msg")
	return results[0], nil
}

func (n *ProdNode) callService(svcName, method string, body []byte, timeoutMs uint64) ([]byte, error) {
	observability.RecordExec("svc_call")

	svcTimeout := 10 * time.Second
	if timeoutMs > 0 {
		svcTimeout = time.Duration(timeoutMs) * time.Millisecond
	}
	svcCtx, svcCancel := context.WithTimeout(context.Background(), svcTimeout)
	defer svcCancel()

	cbI, _ := n.circuitBreakers.LoadOrStore(svcName, reliability.NewCircuitBreaker(5, 30*time.Second))
	cb := cbI.(*reliability.CircuitBreaker)

	if !cb.Allow() {
		observability.RecordError("circuit_breaker_open")
		slog.Warn("circuit breaker open for service", "service", svcName)
		return nil, fmt.Errorf("circuit breaker open for service %s", svcName)
	}

	inst, err := n.Registry.LookupInstance(svcName, method)
	if err != nil {
		slog.Warn("registry lookup failed", 
			"service", svcName, 
			"method", method, 
			"error", err)
		cb.Failure()
		return nil, fmt.Errorf("registry lookup %s: %w", svcName, err)
	}
	
	if inst == nil {
		slog.Info("service call (passthrough)", "service", svcName, "method", method, "body_bytes", len(body))
		return body, nil
	}

	// Protocol-aware dispatch
	resp, err := n.serviceCaller.CallService(svcCtx, inst, method, body, cb, n.Registry)
	if err != nil {
		return nil, fmt.Errorf("service %s: %w", svcName, err)
	}
	
	return resp, nil
}
