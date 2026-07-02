package dashboard

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/premchandkpc/FlowRulZ/simulator/metrics"
	"github.com/premchandkpc/FlowRulZ/simulator/scheduler"
	"github.com/premchandkpc/FlowRulZ/simulator/timeline"
)

type Dashboard struct {
	addr     string
	nodes    []*scheduler.Scheduler
	timeline *timeline.Store
	metrics  *metrics.Collector
	extra    map[string]http.HandlerFunc
	server   *http.Server
}

func New(addr string, nodes []*scheduler.Scheduler, tl *timeline.Store, mc *metrics.Collector) *Dashboard {
	return &Dashboard{
		addr:     addr,
		nodes:    nodes,
		timeline: tl,
		metrics:  mc,
		extra:    make(map[string]http.HandlerFunc),
	}
}

func (d *Dashboard) AddHandler(pattern string, handler http.HandlerFunc) {
	d.extra[pattern] = handler
}

func (d *Dashboard) Start() {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/metrics", d.handleMetrics)
	mux.HandleFunc("/api/nodes", d.handleNodes)
	mux.HandleFunc("/api/events", d.handleEvents)
	mux.HandleFunc("/api/executions/", d.handleExecution)
	mux.HandleFunc("/api/executions", d.handleExecutions)
	mux.HandleFunc("/api/stats", d.handleStats)
	for pattern, handler := range d.extra {
		mux.HandleFunc(pattern, handler)
	}
	mux.HandleFunc("/", d.handleIndex)

	d.server = &http.Server{Addr: d.addr, Handler: mux}
	go func() {
		slog.Info("dashboard: listening", "addr", d.addr)
		if err := d.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("dashboard error", "error", err)
		}
	}()
}

func (d *Dashboard) Stop() {
	if d.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		d.server.Shutdown(ctx)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
