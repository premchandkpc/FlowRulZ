package execnode

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/premchandkpc/FlowRulZ/server/bridge"
	"github.com/premchandkpc/FlowRulZ/server/internal/execstate"
	"github.com/premchandkpc/FlowRulZ/server/internal/observability"
	"github.com/premchandkpc/FlowRulZ/server/internal/reliability"
)

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
