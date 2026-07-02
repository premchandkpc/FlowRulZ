package node

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
	"github.com/premchandkpc/FlowRulZ/server/bridge"
	"github.com/premchandkpc/FlowRulZ/server/internal/execstate"
	"github.com/premchandkpc/FlowRulZ/server/internal/observability"
	"github.com/premchandkpc/FlowRulZ/server/internal/reliability"
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
		n.StateStore.Create(execCtx, st)
	}

	out, err := n.runSteps(execCtx, execID, plan, names, nil, nil, st)
	if n.StateStore != nil {
		if err != nil {
			st.Status = execstate.StatusFailed
			st.Error = err.Error()
			n.StateStore.Save(execCtx, st)
		} else {
			n.StateStore.Delete(execCtx, execID)
		}
	}
	return out, err
}

func (n *ProdNode) runSteps(ctx context.Context, execID string, plan []byte, names map[uint16]string, startCtx, startResp []byte, st *execstate.State) ([]byte, error) {
	ctxBytes, respBytes := startCtx, startResp

	for step := 0; step < 1000; step++ {
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
				n.StateStore.Save(context.Background(), st)
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
				n.StateStore.Save(context.Background(), st)
			}
			respBytes = resp

		case bridge.StepContinue:
			respBytes = nil
			if n.StateStore != nil {
				st.Status = execstate.StatusRunning
				st.CtxBytes = ctxBytes
				n.StateStore.Save(context.Background(), st)
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
			out, err := n.executePlan(ctx, p, body)
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

func (n *ProdNode) handleIncomingMessage(ctx context.Context, msg []byte) ([]byte, error) {
	if !n.RateLimiter.Allow("ingress") {
		observability.RecordError("rate_limited")
		n.DLQ.Send(&reliability.DeadLetterEntry{
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

	if n.Dedup.Seen(msgIDStr) {
		observability.RecordExec("dedup_skipped")
		return nil, nil
	}
	n.Dedup.Mark(msgIDStr)

	execCtx, execCancel := context.WithTimeout(ctx, 30*time.Second)
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

func (n *ProdNode) httpCall(endpoint string, body []byte, cb *reliability.CircuitBreaker) ([]byte, error) {
	req, err := http.NewRequestWithContext(context.Background(), "POST", endpoint, bytes.NewReader(body))
	if err != nil {
		cb.Failure()
		return nil, fmt.Errorf("http call: request: %w", err)
	}
	resp, err := n.httpClient.Do(req)
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

	inst, _ := n.Registry.LookupInstance(svcName, method)
	if inst != nil {
		endpoint := fmt.Sprintf("%s://%s:%d", inst.Endpoint.Protocol, inst.Endpoint.Address, inst.Endpoint.Port)
		req, err := http.NewRequestWithContext(svcCtx, "POST", endpoint, bytes.NewReader(body))
		if err != nil {
			cb.Failure()
			return nil, fmt.Errorf("service %s: request: %w", svcName, err)
		}
		resp, err := n.httpClient.Do(req)
		if err != nil {
			cb.Failure()
			n.Registry.MarkUnhealthy(svcName, inst.Endpoint.NodeID)
			return nil, fmt.Errorf("service %s: call: %w", svcName, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 500 {
			cb.Failure()
			n.Registry.MarkUnhealthy(svcName, inst.Endpoint.NodeID)
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

	slog.Info("service call", "service", svcName, "method", method, "body_bytes", len(body))
	cb.Success()
	return body, nil
}
