package loadgen

import (
	"context"
	"fmt"
	"log"
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
	log.Printf("loadgen: starting (%d req/s, pattern=%d, duration=%v)",
		g.cfg.RequestsPerSec, g.cfg.Pattern, g.cfg.Duration)

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
	plan := g.selectPlan()
	body := []byte(fmt.Sprintf(`{"type":"%s"}`, plan.Tags["type"]))
	ctx := execution.NewContext(plan, body)
	g.dispatcher.Dispatch(ctx)
	g.totalSent.Add(1)
}

func (g *Generator) constantRate() {
	interval := time.Second / time.Duration(g.cfg.RequestsPerSec)
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
			g.concurrent.Add(1)
			g.sendOne()
			g.concurrent.Add(-1)
		case <-timer.C:
			log.Printf("loadgen: completed (%d requests sent)", g.totalSent.Load())
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
		interval := time.Second / time.Duration(rate)
		ticker := time.NewTicker(interval)
		end := time.After(stepDuration)
	loop:
		for {
			select {
			case <-ticker.C:
				g.sendOne()
			case <-end:
				ticker.Stop()
				break loop
			case <-g.ctx.Done():
				ticker.Stop()
				return
			}
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
			rate := base + amplitude*sin(elapsed/period*2*3.14159)
			interval := time.Second / time.Duration(max(1, int(rate)))
			time.Sleep(interval)
			g.sendOne()
		case <-g.ctx.Done():
			return
		}
	}
}

func sin(x float64) float64 {
	if x < 0 {
		return -sin(-x)
	}
	if x > 3.14159*2 {
		x -= float64(int(x/(3.14159*2))) * 3.14159 * 2
	}
	if x < 3.14159/2 {
		return x * 2 / 3.14159
	}
	if x < 3.14159 {
		return 1 - (x-3.14159/2)*2/3.14159
	}
	if x < 3*3.14159/2 {
		return -(x - 3.14159) * 2 / 3.14159
	}
	return -(1 - (x-3*3.14159/2)*2/3.14159)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (g *Generator) TotalSent() int64    { return g.totalSent.Load() }
func (g *Generator) TotalFailed() int64  { return g.totalFailed.Load() }
