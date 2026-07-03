package loadgen

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/premchandkpc/FlowRulZ/simulator/dispatcher"
	"github.com/premchandkpc/FlowRulZ/simulator/execution"
	"github.com/premchandkpc/FlowRulZ/simulator/metrics"
)

type Pattern int

const (
	PatternConstant Pattern = iota
	PatternBurst
	PatternRampUp
	PatternSine
)

type Config struct {
	RequestsPerSec  int
	BurstSize       int
	BurstInterval   time.Duration
	RampUpDuration  time.Duration
	Pattern         Pattern
	Plans           []*execution.Plan
	PlanWeights     []int
	Duration        time.Duration
	MaxConcurrent   int
	BodyFunc        func() []byte
}

func DefaultConfig() Config {
	return Config{
		RequestsPerSec: 100,
		Duration:       30 * time.Second,
		MaxConcurrent:  1000,
		Pattern:        PatternConstant,
	}
}

type Generator struct {
	cfg         Config
	dispatcher  *dispatcher.Dispatcher
	metrics     *metrics.Collector
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex
	totalSent   atomic.Int64
	totalFailed atomic.Int64
	concurrent  atomic.Int64
	planFunc    func() *execution.Plan
}

func (g *Generator) SetPlanFunc(fn func() *execution.Plan) {
	g.planFunc = fn
}

func (g *Generator) SetBodyFunc(fn func() []byte) {
	g.cfg.BodyFunc = fn
}

func New(cfg Config, d *dispatcher.Dispatcher, m *metrics.Collector) *Generator {
	ctx, cancel := context.WithCancel(context.Background())
	return &Generator{
		cfg:        cfg,
		dispatcher: d,
		metrics:    m,
		ctx:        ctx,
		cancel:     cancel,
	}
}

func (g *Generator) Start() {
	slog.Info("loadgen: starting", "rate", g.cfg.RequestsPerSec, "pattern", g.cfg.Pattern, "duration", g.cfg.Duration)

	switch g.cfg.Pattern {
	case PatternConstant:
		go g.constantRate()
	case PatternBurst:
		go g.burstPattern()
	case PatternRampUp:
		go g.rampUp()
	case PatternSine:
		go g.sineWave()
	}
}

func (g *Generator) Stop() {
	g.cancel()
}

func (g *Generator) selectPlan() *execution.Plan {
	if len(g.cfg.Plans) == 0 {
		plans := []*execution.Plan{
			execution.OrderFlow,
			execution.PaymentFlow,
			execution.RefundFlow,
		}
		idx := rand.Intn(len(plans))
		return plans[idx]
	}
	if len(g.cfg.PlanWeights) == len(g.cfg.Plans) {
		total := 0
		for _, w := range g.cfg.PlanWeights {
			total += w
		}
		if total > 0 {
			r := rand.Intn(total)
			cum := 0
			for i, w := range g.cfg.PlanWeights {
				cum += w
				if r < cum {
					return g.cfg.Plans[i]
				}
			}
		}
	}
	idx := rand.Intn(len(g.cfg.Plans))
	return g.cfg.Plans[idx]
}

func (g *Generator) sendOne() {
	var plan *execution.Plan
	if g.planFunc != nil {
		plan = g.planFunc()
	} else {
		plan = g.selectPlan()
	}
	var body []byte
	if g.cfg.BodyFunc != nil {
		body = g.cfg.BodyFunc()
	} else {
		body = []byte(fmt.Sprintf(`{"type":"%s"}`, plan.ID))
	}
	ctx := execution.NewContext(plan, body)
	g.concurrent.Add(1)
	ctx.OnDone = func() { g.concurrent.Add(-1) }
	g.dispatcher.Dispatch(ctx)
	g.totalSent.Add(1)
}

func (g *Generator) constantRate() {
	rate := g.cfg.RequestsPerSec
	if rate <= 0 {
		rate = 1
	}
	interval := time.Second / time.Duration(rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	timer := time.NewTimer(g.cfg.Duration)
	defer timer.Stop()

	for {
		select {
		case <-ticker.C:
			if g.cfg.MaxConcurrent > 0 && g.concurrent.Load() >= int64(g.cfg.MaxConcurrent) {
				continue
			}
			g.sendOne()
		case <-timer.C:
			slog.Info("loadgen: completed", "sent", g.totalSent.Load())
			return
		case <-g.ctx.Done():
			return
		}
	}
}

func (g *Generator) burstPattern() {
	ticker := time.NewTicker(g.cfg.BurstInterval)
	defer ticker.Stop()
	timer := time.NewTimer(g.cfg.Duration)
	defer timer.Stop()

	for {
		select {
		case <-ticker.C:
			for i := 0; i < g.cfg.BurstSize; i++ {
				g.sendOne()
			}
		case <-timer.C:
			return
		case <-g.ctx.Done():
			return
		}
	}
}

func (g *Generator) rampUp() {
	steps := 20
	stepDuration := g.cfg.Duration / time.Duration(steps)
	maxRate := g.cfg.RequestsPerSec

	for i := 0; i < steps; i++ {
		rate := maxRate * (i + 1) / steps
		if rate <= 0 {
			rate = 1
		}
		interval := time.Second / time.Duration(rate)
		g.runStep(interval, stepDuration)
	}
}

func (g *Generator) runStep(interval, stepDuration time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	timer := time.NewTimer(stepDuration)
	defer timer.Stop()

	for {
		select {
		case <-ticker.C:
			g.sendOne()
		case <-timer.C:
			return
		case <-g.ctx.Done():
			return
		}
	}
}

func (g *Generator) sineWave() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	start := time.Now()

	for {
		select {
		case <-ticker.C:
			elapsed := time.Since(start).Seconds()
			period := 10.0
			amplitude := float64(g.cfg.RequestsPerSec) * 0.5
			base := float64(g.cfg.RequestsPerSec) * 0.5
			rate := base + amplitude*math.Sin(elapsed/period*2*math.Pi)
			interval := time.Second / time.Duration(max(1, int(rate)))
			time.Sleep(interval)
			g.sendOne()
		case <-g.ctx.Done():
			return
		}
	}
}




