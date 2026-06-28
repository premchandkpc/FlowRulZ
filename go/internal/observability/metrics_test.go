package observability

import (
	"testing"
)

func TestCounter(t *testing.T) {
	mc := NewMetricsCollector()
	c := mc.Counter("test.calls")
	if c.Value() != 0 {
		t.Fatalf("expected 0, got %d", c.Value())
	}
	c.Inc()
	if c.Value() != 1 {
		t.Fatalf("expected 1, got %d", c.Value())
	}
	c.Add(5)
	if c.Value() != 6 {
		t.Fatalf("expected 6, got %d", c.Value())
	}
	c.Reset()
	if c.Value() != 0 {
		t.Fatalf("expected 0 after reset, got %d", c.Value())
	}
}

func TestCounterDedup(t *testing.T) {
	mc := NewMetricsCollector()
	c1 := mc.Counter("dedup")
	c2 := mc.Counter("dedup")
	if c1 != c2 {
		t.Fatal("expected same counter instance")
	}
}

func TestGauge(t *testing.T) {
	mc := NewMetricsCollector()
	g := mc.Gauge("test.gauge")
	g.Set(42)
	if g.Value() != 42 {
		t.Fatalf("expected 42, got %d", g.Value())
	}
	g.Add(-10)
	if g.Value() != 32 {
		t.Fatalf("expected 32, got %d", g.Value())
	}
}

func TestHistogram(t *testing.T) {
	mc := NewMetricsCollector()
	h := mc.Histogram("test.latency", []float64{0.1, 0.5, 1.0})

	h.Observe(0.05)
	h.Observe(0.3)
	h.Observe(0.8)
	h.Observe(2.0)

	total, buckets := h.Snapshot()
	if total != 4 {
		t.Fatalf("expected total 4, got %d", total)
	}
	if len(buckets) != 4 {
		t.Fatalf("expected 4 buckets, got %d", len(buckets))
	}
}

func TestSnapshot(t *testing.T) {
	mc := NewMetricsCollector()
	mc.Counter("c1").Inc()
	mc.Counter("c2").Add(5)
	mc.Gauge("g1").Set(10)

	snap := mc.Snapshot()
	if snap.Counters["c1"] != 1 {
		t.Fatalf("expected c1=1, got %d", snap.Counters["c1"])
	}
	if snap.Counters["c2"] != 5 {
		t.Fatalf("expected c2=5, got %d", snap.Counters["c2"])
	}
	if snap.Gauges["g1"] != 10 {
		t.Fatalf("expected g1=10, got %d", snap.Gauges["g1"])
	}
}

func TestGlobalShortcuts(t *testing.T) {
	GetCounter("global.calls").Inc()
	GetCounter("global.calls").Inc()
	if GetCounter("global.calls").Value() != 2 {
		t.Fatalf("expected 2 global calls, got %d", GetCounter("global.calls").Value())
	}
	GetGauge("global.gauge").Set(99)
	if GetGauge("global.gauge").Value() != 99 {
		t.Fatalf("expected 99, got %d", GetGauge("global.gauge").Value())
	}
	RecordExec("test")
	RecordError("test")
}
