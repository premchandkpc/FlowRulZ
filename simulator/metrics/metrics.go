package metrics

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Snapshot struct {
	Running           int              `json:"running"`
	Waiting           int              `json:"waiting"`
	Queued            int              `json:"queued"`
	Completed         int64            `json:"completed"`
	Failed            int64            `json:"failed"`
	Dropped           int64            `json:"dropped"`
	Throughput        float64          `json:"throughput_per_sec"`
	AvgLatency        time.Duration    `json:"avg_latency_ms"`
	P50               time.Duration    `json:"p50_ms"`
	P95               time.Duration    `json:"p95_ms"`
	P99               time.Duration    `json:"p99_ms"`
	ServiceLatency    map[string]Stats `json:"service_latency,omitempty"`
	ServiceErrorRate  map[string]float64  `json:"service_error_rate,omitempty"`
	PerNode           map[string]NodeStats `json:"per_node,omitempty"`
}

type Stats struct {
	Count   int           `json:"count"`
	Avg     time.Duration `json:"avg_ms"`
	P50     time.Duration `json:"p50_ms"`
	P95     time.Duration `json:"p95_ms"`
	P99     time.Duration `json:"p99_ms"`
	Min     time.Duration `json:"min_ms"`
	Max     time.Duration `json:"max_ms"`
}

type NodeStats struct {
	Running   int   `json:"running"`
	Waiting   int   `json:"waiting"`
	Queued    int   `json:"queued"`
	Completed int64 `json:"completed"`
	Failed    int64 `json:"failed"`
}

type Collector struct {
	completed     atomic.Int64
	failed        atomic.Int64
	dropped       atomic.Int64
	totalDuration atomic.Int64

	mu             sync.Mutex
	latencySamples []time.Duration
	serviceLatency map[string][]time.Duration
	serviceErrors  map[string]int64
	serviceCalls   map[string]int64
	lastSampleTime time.Time
}

func NewCollector() *Collector {
	return &Collector{
		latencySamples: make([]time.Duration, 0, 10000),
		serviceLatency: make(map[string][]time.Duration),
		serviceErrors:  make(map[string]int64),
		serviceCalls:   make(map[string]int64),
		lastSampleTime: time.Now(),
	}
}

func (c *Collector) RecordCompleted(latency time.Duration) {
	c.completed.Add(1)
	c.totalDuration.Add(int64(latency))
	c.mu.Lock()
	c.latencySamples = append(c.latencySamples, latency)
	if len(c.latencySamples) > 100000 {
		c.latencySamples = c.latencySamples[len(c.latencySamples)-50000:]
	}
	c.mu.Unlock()
}

func (c *Collector) RecordFailed() {
	c.failed.Add(1)
}

func (c *Collector) RecordServiceCall(service string, latency time.Duration, err bool) {
	c.mu.Lock()
	c.serviceCalls[service]++
	c.serviceLatency[service] = append(c.serviceLatency[service], latency)
	if len(c.serviceLatency[service]) > 10000 {
		c.serviceLatency[service] = c.serviceLatency[service][len(c.serviceLatency[service])-5000:]
	}
	if err {
		c.serviceErrors[service]++
	}
	c.mu.Unlock()
}

func (c *Collector) Completed() int64 { return c.completed.Load() }
func (c *Collector) Failed() int64    { return c.failed.Load() }
func (c *Collector) Dropped() int64   { return c.dropped.Load() }

func (c *Collector) Throughput() float64 {
	c.mu.Lock()
	elapsed := time.Since(c.lastSampleTime).Seconds()
	c.mu.Unlock()
	if elapsed < 1 {
		return 0
	}
	return float64(c.completed.Load()) / elapsed
}

func percentiles(samples []time.Duration, ps ...float64) map[float64]time.Duration {
	if len(samples) == 0 {
		res := make(map[float64]time.Duration)
		for _, p := range ps {
			res[p] = 0
		}
		return res
	}
	sorted := make([]time.Duration, len(samples))
	copy(sorted, samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	res := make(map[float64]time.Duration)
	for _, p := range ps {
		idx := int(math.Ceil(p*float64(len(sorted))) - 1)
		if idx < 0 {
			idx = 0
		}
		if idx >= len(sorted) {
			idx = len(sorted) - 1
		}
		res[p] = sorted[idx]
	}
	return res
}

func (c *Collector) Snapshot() *Snapshot {
	c.mu.Lock()
	latencySamples := make([]time.Duration, len(c.latencySamples))
	copy(latencySamples, c.latencySamples)

	svcLatency := make(map[string][]time.Duration, len(c.serviceLatency))
	for k, v := range c.serviceLatency {
		svc := make([]time.Duration, len(v))
		copy(svc, v)
		svcLatency[k] = svc
	}

	svcCalls := make(map[string]int64, len(c.serviceCalls))
	for k, v := range c.serviceCalls {
		svcCalls[k] = v
	}
	svcErrs := make(map[string]int64, len(c.serviceErrors))
	for k, v := range c.serviceErrors {
		svcErrs[k] = v
	}
	c.mu.Unlock()

	snap := &Snapshot{
		Completed: c.completed.Load(),
		Failed:    c.failed.Load(),
		Dropped:   c.dropped.Load(),
	}

	pvals := percentiles(latencySamples, 0.50, 0.95, 0.99)
	snap.P50 = pvals[0.50]
	snap.P95 = pvals[0.95]
	snap.P99 = pvals[0.99]

	avg := time.Duration(0)
	if len(latencySamples) > 0 {
		avg = time.Duration(int64(c.totalDuration.Load()) / int64(len(latencySamples)))
	}
	snap.AvgLatency = avg
	snap.Throughput = c.Throughput()

	snap.ServiceLatency = make(map[string]Stats)
	for name, samples := range svcLatency {
		if len(samples) == 0 {
			continue
		}
		p := percentiles(samples, 0.50, 0.95, 0.99)
		sum := time.Duration(0)
		min := samples[0]
		max := samples[0]
		for _, s := range samples {
			sum += s
			if s < min {
				min = s
			}
			if s > max {
				max = s
			}
		}
		snap.ServiceLatency[name] = Stats{
			Count: len(samples),
			Avg:   time.Duration(int64(sum) / int64(len(samples))),
			P50:   p[0.50],
			P95:   p[0.95],
			P99:   p[0.99],
			Min:   min,
			Max:   max,
		}
	}

	snap.ServiceErrorRate = make(map[string]float64)
	for name, calls := range svcCalls {
		if calls > 0 {
			snap.ServiceErrorRate[name] = float64(svcErrs[name]) / float64(calls)
		}
	}

	return snap
}


