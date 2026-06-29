package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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
	server   *http.Server
}

func New(addr string, nodes []*scheduler.Scheduler, tl *timeline.Store, mc *metrics.Collector) *Dashboard {
	return &Dashboard{
		addr:     addr,
		nodes:    nodes,
		timeline: tl,
		metrics:  mc,
	}
}

func (d *Dashboard) Start() {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/metrics", d.handleMetrics)
	mux.HandleFunc("/api/nodes", d.handleNodes)
	mux.HandleFunc("/api/events", d.handleEvents)
	mux.HandleFunc("/api/executions/", d.handleExecution)
	mux.HandleFunc("/api/stats", d.handleStats)
	mux.HandleFunc("/", d.handleIndex)

	d.server = &http.Server{Addr: d.addr, Handler: mux}
	go func() {
		log.Printf("dashboard: listening on %s", d.addr)
		if err := d.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("dashboard error: %v", err)
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

func (d *Dashboard) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	fmt.Fprint(w, indexHTML)
}

func (d *Dashboard) handleMetrics(w http.ResponseWriter, r *http.Request) {
	snap := d.metrics.Snapshot()
	snap.PerNode = make(map[string]metrics.NodeStats)
	for _, node := range d.nodes {
		s := node.Snapshot()
		snap.PerNode[node.ID] = metrics.NodeStats{
			Running:   s["ready"],
			Waiting:   s["waiting"],
			Completed: node.ExecCount.Load(),
		}
	}
	writeJSON(w, snap)
}

func (d *Dashboard) handleNodes(w http.ResponseWriter, r *http.Request) {
	type nodeInfo struct {
		ID     string         `json:"id"`
		Queues map[string]int `json:"queues"`
		Execs  int64          `json:"execs"`
	}
	var nodes []nodeInfo
	for _, n := range d.nodes {
		nodes = append(nodes, nodeInfo{
			ID:     n.ID,
			Queues: n.Snapshot(),
			Execs:  n.ExecCount.Load(),
		})
	}
	writeJSON(w, nodes)
}

func (d *Dashboard) handleEvents(w http.ResponseWriter, r *http.Request) {
	events := d.timeline.Recent(200)
	writeJSON(w, events)
}

func (d *Dashboard) handleExecution(w http.ResponseWriter, r *http.Request) {
	execID := r.URL.Path[len("/api/executions/"):]
	if execID == "" {
		http.Error(w, "missing exec id", 400)
		return
	}
	events := d.timeline.ForExec(execID)
	writeJSON(w, events)
}

func (d *Dashboard) handleStats(w http.ResponseWriter, r *http.Request) {
	stats := d.timeline.Stats()
	writeJSON(w, stats)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

const indexHTML = `<!DOCTYPE html>
<html>
<head>
<title>FlowRulZ Simulator</title>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
body { font-family: -apple-system, sans-serif; margin: 20px; background: #0d1117; color: #c9d1d9; }
h1 { color: #58a6ff; }
.metrics { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 12px; margin: 20px 0; }
.card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 16px; }
.card h3 { margin: 0 0 8px; font-size: 14px; color: #8b949e; }
.card .value { font-size: 28px; font-weight: bold; color: #58a6ff; }
table { width: 100%; border-collapse: collapse; margin: 20px 0; }
th, td { text-align: left; padding: 8px 12px; border-bottom: 1px solid #30363d; }
th { color: #8b949e; }
.events { max-height: 400px; overflow-y: auto; }
.bar { height: 20px; background: #238636; border-radius: 4px; }
</style>
</head>
<body>
<h1>FlowRulZ Simulator</h1>
<div id="metrics" class="metrics"></div>
<h2>Events</h2>
<table>
<thead><tr><th>Time</th><th>Exec</th><th>Type</th><th>Service</th><th>Meta</th></tr></thead>
<tbody id="events"></tbody>
</table>
<h2>Executions</h2>
<table>
<thead><tr><th>Exec ID</th><th>Step</th><th>Type</th><th>Service</th><th>Elapsed</th></tr></thead>
<tbody id="executions"></tbody>
</table>
<script>
async function refresh() {
  const [m, events, nodes] = await Promise.all([
    fetch('/api/metrics').then(r=>r.json()),
    fetch('/api/events').then(r=>r.json()),
    fetch('/api/nodes').then(r=>r.json())
  ]);
  renderMetrics(m);
  renderEvents(events, m);
  renderNodes(nodes);
  renderExecutions(events);
}
function renderMetrics(m) {
  const div = document.getElementById('metrics');
  div.innerHTML = [
    {label:'Sent/s', value:(m.throughput_per_sec|0)},
    {label:'Completed', value:m.completed},
    {label:'Failed', value:m.failed},
    {label:'Dropped', value:m.dropped},
    {label:'P50 (ms)', value:m.p50_ms},
    {label:'P95 (ms)', value:m.p95_ms},
    {label:'P99 (ms)', value:m.p99_ms},
  ].map(c => '<div class="card"><h3>'+c.label+'</h3><div class="value">'+c.value+'</div></div>').join('');
  if (m.service_latency) {
    div.innerHTML += '<div style="grid-column:1/-1"><table><tr><th>Service</th><th>Avg</th><th>P50</th><th>P95</th><th>P99</th><th>ErrRate</th></tr>'+
      Object.entries(m.service_latency).map(([s,v]) => '<tr><td>'+s+'</td><td>'+(v.avg_ms|0)+'ms</td><td>'+(v.p50_ms|0)+'ms</td><td>'+(v.p95_ms|0)+'ms</td><td>'+(v.p99_ms|0)+'ms</td><td>'+(m.service_error_rate&&m.service_error_rate[s]?(m.service_error_rate[s]*100|0)+'%':'0%')+'</td></tr>').join('')+
      '</table></div>';
  }
}
function renderEvents(events, m) {
  const tbody = document.getElementById('events');
  const recent = events.slice(-50).reverse();
  tbody.innerHTML = recent.map(e => '<tr><td>'+(e.elapsed_ms|0)+'ms</td><td>'+e.exec_id+'</td><td>'+e.type+'</td><td>'+(e.service||'')+'</td><td>'+(e.meta||'')+'</td></tr>').join('');
}
function renderNodes(nodes) {
  const div = document.getElementById('metrics');
  if (nodes.length) {
    div.innerHTML += '<div style="grid-column:1/-1"><table><tr><th>Node</th><th>Ready</th><th>Waiting</th><th>Execs</th></tr>'+
      nodes.map(n => '<tr><td>'+n.id+'</td><td><div class="bar" style="width:'+(n.queues.ready*10)+'px"></div>'+n.queues.ready+'</td><td>'+n.queues.waiting+'</td><td>'+n.execs+'</td></tr>').join('')+
      '</table></div>';
  }
}
function renderExecutions(events) {
  const tbody = document.getElementById('executions');
  const grouped = {};
  for (const e of events) {
    if (!grouped[e.exec_id]) grouped[e.exec_id] = [];
    grouped[e.exec_id].push(e);
  }
  const recent = Object.entries(grouped).slice(-10).reverse();
  tbody.innerHTML = recent.map(([id, evts]) => {
    const last = evts[evts.length-1]||{};
    return '<tr><td>'+id+'</td><td>'+evts.length+'</td><td>'+(last.type||'')+'</td><td>'+(last.service||'')+'</td><td>'+(last.elapsed_ms||'')+'ms</td></tr>';
  }).join('');
}
refresh();
setInterval(refresh, 1000);
</script>
</body>
</html>`
