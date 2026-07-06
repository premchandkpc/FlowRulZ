// Package observability implements the MetricsCollector port.
// Wraps the existing observability package to inject metrics via ports.
package observability

import (
	"github.com/premchandkpc/FlowRulZ/server/internal/ports"
	"github.com/premchandkpc/FlowRulZ/server/internal/observability"
)

// MetricsAdapter wraps the existing observability package to implement ports.MetricsCollector.
type MetricsAdapter struct {
	collector *observability.MetricsCollector
}

// NewMetricsAdapter creates a MetricsAdapter.
func NewMetricsAdapter() *MetricsAdapter {
	return &MetricsAdapter{
		collector: observability.NewMetricsCollector(),
	}
}

func (a *MetricsAdapter) RecordExec(name string) {
	observability.RecordExec(name)
}

func (a *MetricsAdapter) RecordError(name string) {
	observability.RecordError(name)
}

func (a *MetricsAdapter) Snapshot() ports.MetricSnapshot {
	snap := a.collector.Snapshot()
	result := ports.MetricSnapshot{
		Counters: make(map[string]int64, len(snap.Counters)),
		Gauges:   make(map[string]int64, len(snap.Gauges)),
	}
	for k, v := range snap.Counters {
		result.Counters[k] = v
	}
	for k, v := range snap.Gauges {
		result.Gauges[k] = v
	}
	return result
}

// Compile-time interface compliance check
var _ ports.MetricsCollector = (*MetricsAdapter)(nil)
