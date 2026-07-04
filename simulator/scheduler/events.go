package scheduler

import (
	"fmt"
	"time"

	"github.com/premchandkpc/FlowRulZ/server/bridge"
	"github.com/premchandkpc/FlowRulZ/simulator/execution"
	"github.com/premchandkpc/FlowRulZ/simulator/services"
	"github.com/premchandkpc/FlowRulZ/simulator/timeline"
)

func (s *Scheduler) executeContext(ctx *execution.ExecutionContext, workerID int) {
	// All execution goes through the real VM bridge.
	// If PlanBytes is empty, the plan was not compiled from DSL and cannot
	// be executed. Log an error and fail immediately.
	if len(ctx.Plan.PlanBytes) == 0 {
		ctx.MarkFailed(fmt.Errorf("plan %s has no compiled bytecode — deploy via DSL first", ctx.Plan.ID))
		s.Metrics.RecordFailed()
		s.sendResult(ctx)
		return
	}
	s.executeBridge(ctx)
}

func (s *Scheduler) executeBridge(ctx *execution.ExecutionContext) {
	var ctxBytes, respBytes []byte
	planBytes := ctx.Plan.PlanBytes

	if len(ctx.IncomingBody) > 0 {
		initBytes, err := bridge.InitContext(ctx.IncomingBody)
		if err != nil {
			ctx.MarkFailed(fmt.Errorf("init context: %v", err))
			s.Metrics.RecordFailed()
			s.sendResult(ctx)
			return
		}
		ctxBytes = initBytes
	}

	for step := 0; step < 100; step++ {
		select {
		case <-s.stopCh:
			ctx.MarkFailed(fmt.Errorf("scheduler stopped"))
			s.Metrics.RecordFailed()
			s.sendResult(ctx)
			return
		default:
		}

		out, err := bridge.ExecuteStep(planBytes, ctxBytes, respBytes, nil)
		if err != nil {
			ctx.MarkFailed(fmt.Errorf("step %d: %v", step, err))
			s.Timeline.Record(timeline.Event{
				ExecID:    ctx.ID,
				Timestamp: time.Now(),
				Type:      timeline.EventFailed,
				Op:        "vm_error",
				Meta:      err.Error(),
			})
			s.Metrics.RecordFailed()
			s.sendResult(ctx)
			return
		}

		ctxBytes = out.CtxBytes

		if out.Error != "" {
			ctx.MarkFailed(fmt.Errorf("step %d: %s", step, out.Error))
			s.Timeline.Record(timeline.Event{
				ExecID:    ctx.ID,
				Timestamp: time.Now(),
				Type:      timeline.EventFailed,
				Op:        "vm_error",
				Meta:      out.Error,
			})
			s.Metrics.RecordFailed()
			s.sendResult(ctx)
			return
		}

		switch out.Result {
		case bridge.StepDone:
			latency := time.Since(ctx.CreatedAt)
			ctx.Duration = latency
			ctx.Output = out.Output
			ctx.Transition(execution.StateCompleted, "real vm execution completed")
			s.Timeline.Record(timeline.Event{
				ExecID:    ctx.ID,
				Timestamp: time.Now(),
				Type:      timeline.EventCompleted,
				Meta:      fmt.Sprintf("duration=%v", latency),
			})
			s.Metrics.RecordCompleted(latency)
			s.sendResult(ctx)
			return

		case bridge.StepPending:
			rawName, ok := ctx.Plan.ServiceNames[out.PendingSvc]
			if !ok {
				rawName = fmt.Sprintf("svc-%d", out.PendingSvc)
			}
			svcName, _ := bridge.ParseServiceMethod(rawName)
			svc := s.Services.Get(svcName)
			if svc == nil {
				ctx.MarkFailed(fmt.Errorf("unknown service: %s", svcName))
				s.Metrics.RecordFailed()
				s.sendResult(ctx)
				return
			}

			start := time.Now()
			ctx.Transition(execution.StateWaitingForService, fmt.Sprintf("waiting for %s", svcName))
			ctx.WaitingService = svcName
			ctx.WaitingStartTime = start

			correlationID := fmt.Sprintf("%s-%s-%d", s.ID, ctx.ID, step)
			s.WaitingQ.Add(correlationID, ctx, svcName)

			s.Timeline.Record(timeline.Event{
				ExecID:    ctx.ID,
				Timestamp: start,
				Type:      timeline.EventServiceCall,
				Service:   svcName,
				Meta:      string(out.PendingBody),
				NodeID:    s.ID,
			})

			resultCh := make(chan services.CallResult, 1)
			s.Network.CallService(s.serviceCtx, svc, out.PendingBody, func(result services.CallResult) {
				resultCh <- result
			})

			var result services.CallResult
			select {
			case result = <-resultCh:
				s.WaitingQ.Remove(correlationID)
			case <-s.serviceCtx.Done():
				s.WaitingQ.Remove(correlationID)
				ctx.MarkFailed(s.serviceCtx.Err())
				s.Metrics.RecordFailed()
				s.sendResult(ctx)
				return
			}

			latency := time.Since(start)
			if result.Error != nil {
				s.Timeline.Record(timeline.Event{
					ExecID:    ctx.ID,
					Timestamp: time.Now(),
					Type:      timeline.EventServiceError,
					Service:   svcName,
					Meta:      result.Error.Error(),
				})
				ctx.MarkFailed(result.Error)
				s.Metrics.RecordFailed()
				s.sendResult(ctx)
				return
			}

			s.Timeline.Record(timeline.Event{
				ExecID:    ctx.ID,
				Timestamp: time.Now(),
				Type:      timeline.EventServiceResponse,
				Service:   svcName,
				Elapsed:   latency,
				Meta:      string(result.Body),
			})
			s.Metrics.RecordServiceCall(svcName, latency, false)
			ctx.Transition(execution.StateRunning, "service response received")
			respBytes = result.Body

		case bridge.StepContinue:
			respBytes = nil
		}
	}
}
