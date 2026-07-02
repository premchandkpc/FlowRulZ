package scheduler

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/premchandkpc/FlowRulZ/server/pkg/transport"
	"github.com/premchandkpc/FlowRulZ/simulator/execution"
	"github.com/premchandkpc/FlowRulZ/simulator/metrics"
	"github.com/premchandkpc/FlowRulZ/simulator/network"
	"github.com/premchandkpc/FlowRulZ/simulator/services"
	"github.com/premchandkpc/FlowRulZ/simulator/timeline"
)

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
	mu    sync.RWMutex
	plans map[string]*execution.Plan
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

func (pc *PlanCache) Remove(id string) {
	pc.mu.Lock()
	delete(pc.plans, id)
	pc.mu.Unlock()
}

func (pc *PlanCache) Len() int {
	pc.mu.RLock()
	n := len(pc.plans)
	pc.mu.RUnlock()
	return n
}

func New(id string, workers int, services *services.ServiceRegistry, net *network.Network, tl *timeline.Store, mc *metrics.Collector) *Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &Scheduler{
		ID:         id,
		ReadyQ:     execution.NewReadyQueue(),
		WaitingQ:   execution.NewWaitingQueue(),
		Metrics:    mc,
		Timeline:   tl,
		Services:   services,
		Network:    net,
		Plans:      NewPlanCache(),
		Workers:    workers,
		stopCh:     make(chan struct{}),
		serviceCtx: ctx,
		cancel:     cancel,
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
