package scheduler

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/premchandkpc/FlowRulZ/go/bridge"
	"github.com/premchandkpc/FlowRulZ/go/pkg/transport"
	"github.com/premchandkpc/FlowRulZ/simulator/execution"
	"github.com/premchandkpc/FlowRulZ/simulator/metrics"
	"github.com/premchandkpc/FlowRulZ/simulator/network"
	"github.com/premchandkpc/FlowRulZ/simulator/services"
	"github.com/premchandkpc/FlowRulZ/simulator/timeline"
)

type Result struct {
	Ctx      *execution.ExecutionContext
	Error    error
}

type Scheduler struct {
	ID        string
	ReadyQ    *execution.ReadyQueue
	WaitingQ  *execution.WaitingQueue
	Metrics   *metrics.Collector
	Timeline  *timeline.Store
	Services  *services.ServiceRegistry
	Network   *network.Network
	Plans     *PlanCache
	Bus       transport.EventBus

	Workers    int
	ExecCount  atomic.Int64
	mu         sync.Mutex
	stopCh     chan struct{}
	stopped    bool
	wg         sync.WaitGroup
	serviceCtx context.Context
	cancel     context.CancelFunc
	busSub     *transport.Subscription
}

type PlanCache struct {
	mu     sync.RWMutex
	plans  map[string]*execution.Plan
}

func NewPlanCache() *PlanCache {
	return &PlanCache{plans: make(map[string]*execution.Plan)}
}

func (pc *PlanCache) Add(plan *execution.Plan) {
	pc.mu.Lock()
	pc.plans[plan.ID] = plan
	pc.mu.Unlock()
}

func (pc *PlanCache) List() []string {
	pc.mu.RLock()
	names := make([]string, 0, len(pc.plans))
	for n := range pc.plans {
		names = append(names, n)
	}
	pc.mu.RUnlock()
	return names
}

func (pc *PlanCache) Get(id string) *execution.Plan {
	pc.mu.RLock()
	p := pc.plans[id]
	pc.mu.RUnlock()
	return p
}

func New(id string, workers int, services *services.ServiceRegistry, net *network.Network, tl *timeline.Store, mc *metrics.Collector) *Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &Scheduler{
		ID:        id,
		ReadyQ:    execution.NewReadyQueue(),
		WaitingQ:  execution.NewWaitingQueue(),
		Metrics:   mc,
		Timeline:  tl,
		Services:  services,
		Network:   net,
		Plans:     NewPlanCache(),
		Workers:   workers,
		stopCh:    make(chan struct{}),
		serviceCtx: ctx,
		cancel:    cancel,
	}
}

func (s *Scheduler) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := 0; i < s.Workers; i++ {
		s.wg.Add(1)
		go s.worker(i)
	}
	log.Printf("scheduler[%s]: started %d workers", s.ID, s.Workers)
}

func (s *Scheduler) Stop() {
	s.cancel()
	s.mu.Lock()
	if !s.stopped {
		s.stopped = true
		close(s.stopCh)
	}
	s.mu.Unlock()
	s.wg.Wait()
}

func (s *Scheduler) Enqueue(ctx *execution.ExecutionContext) {
	ctx.Transition(execution.StateReady, "enqueued")
	s.ReadyQ.Push(ctx)
}

func (s *Scheduler) worker(id int) {
	defer s.wg.Done()
	for {
		select {
		case <-s.stopCh:
			return
		default:
		}
		ctx := s.ReadyQ.Pop()
		if ctx == nil {
			time.Sleep(100 * time.Microsecond)
			continue
		}
		s.executeContext(ctx, id)
	}
}

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

func (s *Scheduler) sendResult(ctx *execution.ExecutionContext) {
	if ctx.ResultCh == nil {
		return
	}
	var err error
	if ctx.State == execution.StateFailed {
		if len(ctx.StateChanges) > 0 {
			err = fmt.Errorf("%s", ctx.StateChanges[len(ctx.StateChanges)-1].Meta)
		} else {
			err = fmt.Errorf("execution failed")
		}
	}
	ctx.ResultCh <- &execution.Result{Body: ctx.Output, Error: err}
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

func (s *Scheduler) SetBus(bus transport.EventBus) {
	s.Bus = bus
}

func (s *Scheduler) SubscribeBus() {
	if s.Bus == nil {
		return
	}
	s.busSub = s.Bus.Subscribe("execution", func(ctx context.Context, msg *transport.Message) {
		ruleID := msg.Headers["rule_id"]
		if ruleID == "" {
			return
		}
		plan := s.Plans.Get(ruleID)
		if plan == nil {
			if msg.CorrelationID != "" {
			s.Bus.Reply("execution", msg.CorrelationID, &transport.Message{
					Body: []byte(`{"error":"rule not found"}`),
				})
			}
			return
		}

		resultCh := make(chan *execution.Result, 1)
		ec := execution.NewContext(plan, msg.Body)
		ec.ResultCh = resultCh
		ec.Transition(execution.StateRunning, "bus dispatch")

		s.Enqueue(ec)

		res := <-resultCh
		if msg.CorrelationID != "" {
			var respBody []byte
			if res.Error != nil {
				respBody = []byte(fmt.Sprintf(`{"error":"%s"}`, res.Error.Error()))
			} else {
				respBody = res.Body
			}
			s.Bus.Reply("execution", msg.CorrelationID, &transport.Message{
				Body: respBody,
				Headers: map[string]string{
					"duration": ec.Duration.String(),
				},
			})
		}
	})
}

func (s *Scheduler) Snapshot() map[string]int {
	return map[string]int{
		"ready":   s.ReadyQ.Len(),
		"waiting": s.WaitingQ.Len(),
	}
}

func (s *Scheduler) StopBus() {
	if s.busSub != nil {
		s.Bus.Unsubscribe(s.busSub.ID)
		s.busSub = nil
	}
}
