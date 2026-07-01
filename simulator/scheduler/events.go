package scheduler

import (
	"fmt"
	"time"

	"github.com/premchandkpc/FlowRulZ/go/bridge"
	"github.com/premchandkpc/FlowRulZ/simulator/execution"
	"github.com/premchandkpc/FlowRulZ/simulator/services"
	"github.com/premchandkpc/FlowRulZ/simulator/timeline"
)

func (s *Scheduler) executeContext(ctx *execution.ExecutionContext, workerID int) {
	if len(ctx.Plan.PlanBytes) > 0 {
		s.executeBridge(ctx)
		return
	}
	for ctx.IP < len(ctx.Plan.Instructions) {
		select {
		case <-s.stopCh:
			ctx.MarkFailed(fmt.Errorf("scheduler stopped"))
			s.Metrics.RecordFailed()
			s.sendResult(ctx)
			return
		default:
		}

		instr := ctx.Plan.Instructions[ctx.IP]
		ctx.Transition(execution.StateRunning, fmt.Sprintf("executing %s", instr.Op))

		s.Timeline.Record(timeline.Event{
			ExecID:    ctx.ID,
			Timestamp: time.Now(),
			Type:      timeline.EventInstruction,
			Op:        instr.Op.String(),
			IP:        ctx.IP,
			NodeID:    s.ID,
		})

		switch instr.Op {
		case execution.OpCallService:
			result := s.callService(ctx, instr)
			if result.Error != nil {
				ctx.MarkFailed(result.Error)
				s.Timeline.Record(timeline.Event{
					ExecID:    ctx.ID,
					Timestamp: time.Now(),
					Type:      timeline.EventFailed,
					Op:        "service_error",
					Service:   instr.Service,
					Meta:      result.Error.Error(),
				})
				s.Metrics.RecordFailed()
				s.sendResult(ctx)
				return
			}
			ctx.Variables[instr.Service+"_result"] = string(result.Body)
			ctx.Variables[instr.Service+"_latency"] = result.Latency.Milliseconds()
			s.Metrics.RecordServiceCall(instr.Service, result.Latency, false)

		case execution.OpValidate:
			ctx.Variables["validated"] = true

		case execution.OpBranch:
			condition := instr.Args[0]
			val, ok := ctx.Variables[condition]
			if !ok {
				ctx.Variables["branch_taken"] = false
				ctx.IP++
				continue
			}
			strVal := fmt.Sprintf("%v", val)
			if strVal == "true" || strVal == "1" || strVal == "high" {
				ctx.Variables["branch_taken"] = true
			} else {
				ctx.Variables["branch_taken"] = false
			}

		case execution.OpPublish:
			ctx.Variables["published"] = instr.Args[0]

		case execution.OpReturn:
			ctx.MarkDone()
			s.Timeline.Record(timeline.Event{
				ExecID:    ctx.ID,
				Timestamp: time.Now(),
				Type:      timeline.EventCompleted,
				Meta:      fmt.Sprintf("duration=%v", ctx.Duration),
			})
			s.Metrics.RecordCompleted(ctx.Duration)
			s.sendResult(ctx)
			return
		}
		ctx.IP++
	}
	ctx.MarkDone()
	s.Timeline.Record(timeline.Event{
		ExecID:    ctx.ID,
		Timestamp: time.Now(),
		Type:      timeline.EventCompleted,
	})
	s.Metrics.RecordCompleted(ctx.Duration)
	s.sendResult(ctx)
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
			case <-s.serviceCtx.Done():
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

func (s *Scheduler) callService(ctx *execution.ExecutionContext, instr execution.Instruction) services.CallResult {
	svc := s.Services.Get(instr.Service)
	if svc == nil {
		return services.CallResult{Error: fmt.Errorf("unknown service: %s", instr.Service)}
	}

	correlationID := fmt.Sprintf("%s-%s-%d", s.ID, ctx.ID, ctx.IP)

	ctx.WaitingService = instr.Service
	ctx.WaitingStartTime = time.Now()

	s.Timeline.Record(timeline.Event{
		ExecID:    ctx.ID,
		Timestamp: time.Now(),
		Type:      timeline.EventServiceCall,
		Service:   instr.Service,
		IP:        ctx.IP,
		Meta:      correlationID,
		NodeID:    s.ID,
	})

	ctx.Transition(execution.StateWaitingForService, fmt.Sprintf("waiting for %s", instr.Service))

	s.WaitingQ.Add(correlationID, ctx, instr.Service)

	resultCh := make(chan services.CallResult, 1)
	s.Network.CallService(s.serviceCtx, svc, ctx.IncomingBody, func(result services.CallResult) {
		resultCh <- result
	})

	select {
	case result := <-resultCh:
		s.WaitingQ.Remove(correlationID)
		latency := time.Since(ctx.WaitingStartTime)

		if result.Error != nil {
			s.Timeline.Record(timeline.Event{
				ExecID:    ctx.ID,
				Timestamp: time.Now(),
				Type:      timeline.EventServiceError,
				Service:   instr.Service,
				Meta:      result.Error.Error(),
			})
			return result
		}

		s.Timeline.Record(timeline.Event{
			ExecID:    ctx.ID,
			Timestamp: time.Now(),
			Type:      timeline.EventServiceResponse,
			Service:   instr.Service,
			Elapsed:   latency,
			Meta:      string(result.Body),
		})
		ctx.Transition(execution.StateRunning, "service response received")
		return result

	case <-s.serviceCtx.Done():
		s.WaitingQ.Remove(correlationID)
		return services.CallResult{Error: s.serviceCtx.Err()}
	}
}
