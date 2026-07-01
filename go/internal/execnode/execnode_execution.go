package execnode

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/premchandkpc/FlowRulZ/go/bridge"
	"github.com/premchandkpc/FlowRulZ/go/internal/execstate"
	"github.com/premchandkpc/FlowRulZ/go/internal/observability"
	"github.com/premchandkpc/FlowRulZ/go/internal/reliability"
)

func (en *ExecutionNode) httpCall(endpoint string, body []byte, cb *reliability.CircuitBreaker) ([]byte, error) {
	req, err := http.NewRequestWithContext(context.Background(), "POST", endpoint, bytes.NewReader(body))
	if err != nil {
		cb.Failure()
		return nil, fmt.Errorf("http call: request: %w", err)
	}
	resp, err := en.httpClient.Do(req)
	if err != nil {
		cb.Failure()
		return nil, fmt.Errorf("http call: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 500 {
		cb.Failure()
		return nil, fmt.Errorf("http call: status %d", resp.StatusCode)
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		cb.Failure()
		return nil, fmt.Errorf("http call: read: %w", err)
	}

	cb.Success()
	return respBody, nil
}

func (en *ExecutionNode) callService(svcName, method string, body []byte, timeoutMs uint64) ([]byte, error) {
	observability.RecordExec("svc_call")

	svcTimeout := 10 * time.Second
	if timeoutMs > 0 {
		svcTimeout = time.Duration(timeoutMs) * time.Millisecond
	}
	svcCtx, svcCancel := context.WithTimeout(context.Background(), svcTimeout)
	defer svcCancel()

	cbI, _ := en.circuitBreakers.LoadOrStore(svcName, reliability.NewCircuitBreaker(5, 30*time.Second))
	cb := cbI.(*reliability.CircuitBreaker)

	if !cb.Allow() {
		observability.RecordError("circuit_breaker_open")
		slog.Warn("circuit breaker open for service", "service", svcName)
		return nil, fmt.Errorf("circuit breaker open for service %s", svcName)
	}

	inst, _ := en.Registry.LookupInstance(svcName, method)
	if inst != nil {
		endpoint := fmt.Sprintf("%s://%s:%d", inst.Endpoint.Protocol, inst.Endpoint.Address, inst.Endpoint.Port)
		req, err := http.NewRequestWithContext(svcCtx, "POST", endpoint, bytes.NewReader(body))
		if err != nil {
			cb.Failure()
			return nil, fmt.Errorf("service %s: request: %w", svcName, err)
		}
		resp, err := en.httpClient.Do(req)
		if err != nil {
			cb.Failure()
			en.Registry.MarkUnhealthy(svcName, inst.Endpoint.NodeID)
			return nil, fmt.Errorf("service %s: call: %w", svcName, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 500 {
			cb.Failure()
			en.Registry.MarkUnhealthy(svcName, inst.Endpoint.NodeID)
			return nil, fmt.Errorf("service %s: status %d", svcName, resp.StatusCode)
		}

		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			cb.Failure()
			return nil, fmt.Errorf("service %s: read: %w", svcName, err)
		}

		cb.Success()
		return respBody, nil
	}

	if en.serviceResolver != nil {
		endpoint, err := en.serviceResolver.Resolve(0, method)
		if err != nil {
			cb.Failure()
			return nil, fmt.Errorf("service %s: resolve: %w", svcName, err)
		}
		return en.httpCall(endpoint, body, cb)
	}

	slog.Info("service call", "service", svcName, "method", method, "body_bytes", len(body))
	cb.Success()
	return body, nil
}

func (en *ExecutionNode) executePlan(ctx context.Context, plan []byte, body []byte) ([]byte, error) {
	names := make(map[uint16]string)
	if entries, err := bridge.PlanServices(plan); err == nil {
		for _, e := range entries {
			names[e.ID] = e.Name
		}
	}

	execID := uuid.New().String()
	now := time.Now().UTC()

	execCtx, cancel := context.WithCancel(ctx)
	en.Execs.Register(execID, cancel, "")

	defer func() {
		en.Execs.Unregister(execID)
		cancel()
	}()

	st := &execstate.State{
		ID:        execID,
		PlanBytes: plan,
		Status:    execstate.StatusCreated,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if en.StateStore != nil {
		en.StateStore.Create(execCtx, st)
	}

	out, err := en.runSteps(execCtx, execID, plan, names, nil, nil, st)
	if en.StateStore != nil {
		if err != nil {
			st.Status = execstate.StatusFailed
			st.Error = err.Error()
			en.StateStore.Save(execCtx, st)
		} else {
			en.StateStore.Delete(execCtx, execID)
		}
	}
	return out, err
}

func (en *ExecutionNode) runSteps(ctx context.Context, execID string, plan []byte, names map[uint16]string, startCtx, startResp []byte, st *execstate.State) ([]byte, error) {
	ctxBytes, respBytes := startCtx, startResp

	for step := 0; step < 1000; step++ {
		select {
		case <-ctx.Done():
			en.tryCompensate(execID)
			return nil, fmt.Errorf("execution cancelled at step %d: %w", step, ctx.Err())
		default:
		}

		out, err := bridge.ExecuteStep(plan, ctxBytes, respBytes, nil)
		if err != nil {
			en.tryCompensate(execID)
			return nil, fmt.Errorf("step %d: %w", step, err)
		}

		ctxBytes = out.CtxBytes

		switch out.Result {
		case bridge.StepDone:
			observability.RecordExec("completed")
			if en.Saga != nil {
				en.Saga.Clear(execID)
			}
			return out.Output, nil

		case bridge.StepPending:
			observability.RecordExec("svc_pending")
			if en.StateStore != nil {
				st.Status = execstate.StatusWaitingForService
				st.PendingSvc = out.PendingSvc
				st.PendingBody = out.PendingBody
				st.CtxBytes = ctxBytes
				en.StateStore.Save(context.Background(), st)
			}

			rawName, ok := names[out.PendingSvc]
			if !ok {
				rawName = fmt.Sprintf("svc-%d", out.PendingSvc)
			}
			svcName, method, compSvc, compMethod := bridge.ParseCompensation(rawName)

			if en.Saga != nil && compSvc != "" {
				en.Saga.RegisterStep(execID, reliability.SagaStep{
					ServiceName: svcName,
					Method:      method,
					Body:        out.PendingBody,
					CompSvc:     compSvc,
					CompMethod:  compMethod,
				})
			}

			resp, err := en.callService(svcName, method, out.PendingBody, out.TimeoutMs)
			if err != nil {
				en.tryCompensate(execID)
				return nil, fmt.Errorf("service %s: %w", svcName, err)
			}

			if en.StateStore != nil {
				st.Status = execstate.StatusRunning
				st.PendingSvc = 0
				st.PendingBody = nil
				st.CtxBytes = ctxBytes
				en.StateStore.Save(context.Background(), st)
			}
			respBytes = resp

		case bridge.StepContinue:
			respBytes = nil
			if en.StateStore != nil {
				st.Status = execstate.StatusRunning
				st.CtxBytes = ctxBytes
				en.StateStore.Save(context.Background(), st)
			}
		}
	}

	en.tryCompensate(execID)
	return nil, fmt.Errorf("execution exceeded max steps")
}

func (en *ExecutionNode) tryCompensate(execID string) {
	if en.Saga == nil {
		return
	}
	if err := en.Saga.Compensate(execID); err != nil {
		slog.Error("saga: compensation error", "exec_id", execID, "error", err)
	}
}

func (en *ExecutionNode) recoverInFlight(ctx context.Context) {
	if en.StateStore == nil {
		return
	}

	inflight, err := en.StateStore.List(ctx, execstate.StatusRunning, execstate.StatusWaitingForService)
	if err != nil {
		slog.Error("recovery: list error", "error", err)
		return
	}

	for _, st := range inflight {
		go en.recoverExecution(st)
	}
}

func (en *ExecutionNode) recoverExecution(st *execstate.State) {
	slog.Info("recovery: resuming execution", "exec_id", st.ID, "status", st.Status, "rule_id", st.RuleID)

	names := make(map[uint16]string)
	if entries, err := bridge.PlanServices(st.PlanBytes); err == nil {
		for _, e := range entries {
			names[e.ID] = e.Name
		}
	}

	var startResp []byte
	if st.Status == execstate.StatusWaitingForService {
		rawName, ok := names[st.PendingSvc]
		if !ok {
			rawName = fmt.Sprintf("svc-%d", st.PendingSvc)
		}
		svcName, method := bridge.ParseServiceMethod(rawName)
		resp, err := en.callService(svcName, method, st.PendingBody, 0)
		if err != nil {
			slog.Warn("recovery: exec retry failed", "exec_id", st.ID, "service", svcName, "error", err)
			st.Status = execstate.StatusFailed
			st.Error = fmt.Sprintf("recovery retry: %v", err)
			en.StateStore.Save(context.Background(), st)
			return
		}
		startResp = resp
		st.Status = execstate.StatusRunning
		st.PendingSvc = 0
		st.PendingBody = nil
		en.StateStore.Save(context.Background(), st)
	}

	out, err := en.runSteps(context.Background(), st.ID, st.PlanBytes, names, st.CtxBytes, startResp, st)
	if err != nil {
		slog.Error("recovery: exec failed", "exec_id", st.ID, "error", err)
		st.Status = execstate.StatusFailed
		st.Error = err.Error()
		en.StateStore.Save(context.Background(), st)
		return
	}

	slog.Info("recovery: exec completed", "exec_id", st.ID, "bytes", len(out))
	en.StateStore.Delete(context.Background(), st.ID)
}

func (en *ExecutionNode) executeAll(ctx context.Context, body []byte) ([][]byte, error) {
	plans := en.Engine.ActivePlanBytes()
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
	sem := make(chan struct{}, 10)

	for i, plan := range plans {
		sem <- struct{}{}
		go func(idx int, p []byte) {
			defer func() { <-sem }()
			out, err := en.executePlan(ctx, p, body)
			ch <- planResult{idx, out, err}
		}(i, plan)
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

func (en *ExecutionNode) handleIncomingMessage(ctx context.Context, msg []byte) ([]byte, error) {
	if !en.RateLimiter.Allow("ingress") {
		observability.RecordError("rate_limited")
		en.DLQ.Send(&reliability.DeadLetterEntry{
			ID:    "ratelimited",
			Body:  msg,
			Error: "rate limited",
		})
		return nil, nil
	}

	msgID := make([]byte, 16)
	if _, err := rand.Read(msgID); err != nil {
		return nil, fmt.Errorf("message id generation failed: %w", err)
	}
	msgIDStr := hex.EncodeToString(msgID)

	if en.Dedup.Seen(msgIDStr) {
		observability.RecordExec("dedup_skipped")
		return nil, nil
	}
	en.Dedup.Mark(msgIDStr)

	execCtx, execCancel := context.WithTimeout(ctx, 30*time.Second)
	defer execCancel()

	results, err := en.executeAll(execCtx, msg)
	if err != nil {
		observability.RecordError("exec")
		en.DLQ.Send(&reliability.DeadLetterEntry{
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
