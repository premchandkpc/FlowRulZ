package observability

import (
	"sync"
	"sync/atomic"

	"github.com/premchandkpc/FlowRulZ/server/internal/ports"
)

type Counter struct {
	name  string
	value atomic.Int64
}

type Gauge struct {
	name  string
	value atomic.Int64
}

type Histogram struct {
	name    string
	buckets []float64
	counts  []atomic.Int64
	total   atomic.Int64
	mu      sync.Mutex
}

type MetricsCollector struct {
	mu         sync.RWMutex
	counters   map[string]*Counter
	gauges     map[string]*Gauge
	histograms map[string]*Histogram
}

var defaultCollector = NewMetricsCollector()

func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		counters:   make(map[string]*Counter),
		gauges:     make(map[string]*Gauge),
		histograms: make(map[string]*Histogram),
	}
}

// -- Counter --

func (mc *MetricsCollector) Counter(name string) *Counter {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if c, ok := mc.counters[name]; ok {
		return c
	}
	c := &Counter{name: name}
	mc.counters[name] = c
	return c
}

func (c *Counter) Inc() int64        { return c.Add(1) }
func (c *Counter) Add(n int64) int64 { return c.value.Add(n) }
func (c *Counter) Value() int64      { return c.value.Load() }
func (c *Counter) Name() string      { return c.name }
func (c *Counter) Reset()            { c.value.Store(0) }

// -- Gauge --

func (mc *MetricsCollector) Gauge(name string) *Gauge {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if g, ok := mc.gauges[name]; ok {
		return g
	}
	g := &Gauge{name: name}
	mc.gauges[name] = g
	return g
}

func (g *Gauge) Set(n int64)  { g.value.Store(n) }
func (g *Gauge) Add(n int64)  { g.value.Add(n) }
func (g *Gauge) Value() int64 { return g.value.Load() }
func (g *Gauge) Name() string { return g.name }

// -- Histogram --

func (mc *MetricsCollector) Histogram(name string, buckets []float64) *Histogram {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if h, ok := mc.histograms[name]; ok {
		return h
	}
	h := &Histogram{
		name:    name,
		buckets: buckets,
		counts:  make([]atomic.Int64, len(buckets)+1),
	}
	mc.histograms[name] = h
	return h
}

func (h *Histogram) Observe(v float64) {
	h.total.Add(1)
	for i, b := range h.buckets {
		if v <= b {
			h.counts[i].Add(1)
			return
		}
	}
	h.counts[len(h.buckets)].Add(1)
}

// -- Snapshot --

type MetricSnapshot = ports.MetricSnapshot

func (mc *MetricsCollector) Snapshot() MetricSnapshot {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	snap := MetricSnapshot{
		Counters: make(map[string]int64, len(mc.counters)),
		Gauges:   make(map[string]int64, len(mc.gauges)),
	}
	for name, c := range mc.counters {
		snap.Counters[name] = c.Value()
	}
	for name, g := range mc.gauges {
		snap.Gauges[name] = g.Value()
	}
	return snap
}

// -- Default collector shortcuts --

func GetCounter(name string) *Counter { return defaultCollector.Counter(name) }
func GetGauge(name string) *Gauge     { return defaultCollector.Gauge(name) }

func RecordExec(name string)  { GetCounter("exec." + name).Inc() }
func RecordError(name string) { GetCounter("error." + name).Inc() }

// RecordExecWithCollector records an exec event on the given collector.
func (mc *MetricsCollector) RecordExec(name string)  { mc.Counter("exec." + name).Inc() }

// RecordErrorWithCollector records an error event on the given collector.
func (mc *MetricsCollector) RecordError(name string) { mc.Counter("error." + name).Inc() }
