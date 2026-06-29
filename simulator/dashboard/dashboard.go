package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
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

func (d *Dashboard) handleExecutions(w http.ResponseWriter, r *http.Request) {
	events := d.timeline.All()
	grouped := make(map[string][]timeline.Event)
	for _, e := range events {
		grouped[e.ExecID] = append(grouped[e.ExecID], e)
	}
	type execSummary struct {
		ID       string   `json:"id"`
		Events   int      `json:"events"`
		Services []string `json:"services"`
		Status   string   `json:"status"`
		Duration string   `json:"duration"`
	}
	var result []execSummary
	for id, evts := range grouped {
		svcs := make(map[string]bool)
		status := "running"
		var dur string
		for _, e := range evts {
			if e.Service != "" {
				svcs[e.Service] = true
			}
			if e.Type == timeline.EventCompleted {
				status = "completed"
			} else if e.Type == timeline.EventFailed || e.Type == timeline.EventDropped {
				status = e.Type.String()
			}
			if e.Type == timeline.EventDropped && e.Meta != "" {
				if n := strings.Index(e.Meta, "duration="); n >= 0 {
					dur = e.Meta[n+9:]
				}
			}
		}
		svcList := make([]string, 0, len(svcs))
		for s := range svcs {
			svcList = append(svcList, s)
		}
		result = append(result, execSummary{
			ID:       id,
			Events:   len(evts),
			Services: svcList,
			Status:   status,
			Duration: dur,
		})
	}
	writeJSON(w, result)
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
* { box-sizing: border-box; }
body { font-family: -apple-system, sans-serif; margin: 0; background: #0d1117; color: #c9d1d9; }
.header { background: #161b22; border-bottom: 1px solid #30363d; padding: 16px 24px; display: flex; align-items: center; gap: 16px; }
.header h1 { margin: 0; font-size: 20px; color: #58a6ff; }
.mode { font-size: 12px; color: #8b949e; background: #0d1117; padding: 2px 8px; border-radius: 4px; border: 1px solid #30363d; }
.layout { display: flex; gap: 16px; padding: 16px 24px; }
.sidebar { width: 280px; flex-shrink: 0; }
.main { flex: 1; min-width: 0; }
.metrics { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 16px; }
.card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 10px 14px; min-width: 100px; flex: 1; }
.card h3 { margin: 0 0 4px; font-size: 11px; color: #8b949e; text-transform: uppercase; letter-spacing: .5px; }
.card .value { font-size: 22px; font-weight: 700; color: #f0f6fc; }
.card .value.green { color: #3fb950; }
.card .value.red { color: #f85149; }
.card .value.yellow { color: #d29922; }
.card .value.blue { color: #58a6ff; }
.card .sub { font-size: 11px; color: #8b949e; margin-top: 2px; }
/* Execution Graph */
.graph-container { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 16px; margin-bottom: 16px; }
.graph-container h2 { margin: 0 0 12px; font-size: 14px; color: #8b949e; text-transform: uppercase; letter-spacing: .5px; }
.graph-svg { width: 100%; height: 300px; }
.service-node { fill: #1f2937; stroke: #30363d; stroke-width: 2; rx: 8; ry: 8; transition: all .3s; }
.service-node.active { stroke: #58a6ff; stroke-width: 2.5; }
.service-node.completed { stroke: #3fb950; }
.service-node.error { stroke: #f85149; }
.service-label { fill: #c9d1d9; font-size: 11px; text-anchor: middle; dominant-baseline: central; }
.path-line { stroke: #30363d; stroke-width: 2; fill: none; }
.path-line.active { stroke: #58a6ff; stroke-width: 2.5; stroke-dasharray: 6 4; animation: dashflow 1s linear infinite; }
.path-line.completed { stroke: #3fb950; stroke-width: 2; }
@keyframes dashflow { to { stroke-dashoffset: -20; } }
.execution-dot { fill: #58a6ff; r: 4; animation: pulse 1.5s ease-in-out infinite; }
@keyframes pulse { 0%,100% { opacity: .4; r: 3; } 50% { opacity: 1; r: 5; } }
/* Nodes section */
.section-title { font-size: 13px; color: #8b949e; margin: 12px 0 8px; text-transform: uppercase; letter-spacing: .5px; }
.node-row { display: flex; align-items: center; gap: 8px; padding: 6px 0; border-bottom: 1px solid #21262d; font-size: 13px; }
.node-row:last-child { border: none; }
.node-name { width: 60px; color: #58a6ff; }
.node-bar { flex: 1; height: 18px; background: #0d1117; border-radius: 4px; overflow: hidden; }
.node-fill { height: 100%; background: #238636; border-radius: 4px; transition: width .5s; }
.node-val { width: 30px; text-align: right; color: #8b949e; }
/* Tables */
.table-wrap { overflow-x: auto; margin-bottom: 16px; }
.table-wrap table { width: 100%; border-collapse: collapse; font-size: 12px; }
.table-wrap th { color: #8b949e; text-align: left; padding: 6px 8px; border-bottom: 2px solid #30363d; position: sticky; top: 0; background: #0d1117; }
.table-wrap td { padding: 5px 8px; border-bottom: 1px solid #21262d; white-space: nowrap; }
.badge { display: inline-block; padding: 1px 6px; border-radius: 4px; font-size: 10px; font-weight: 600; }
.badge-ok { background: #1a3a2a; color: #3fb950; }
.badge-err { background: #3a1a1a; color: #f85149; }
.badge-running { background: #1a2a3a; color: #58a6ff; animation: pulsebg 2s ease-in-out infinite; }
@keyframes pulsebg { 0%,100% { opacity: .6; } 50% { opacity: 1; } }
/* Send form */
.send-box { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 16px; margin-bottom: 16px; }
.send-box h3 { margin: 0 0 12px; font-size: 13px; color: #8b949e; text-transform: uppercase; letter-spacing: .5px; }
.send-row { display: flex; gap: 8px; margin-bottom: 8px; align-items: center; }
.send-row label { font-size: 12px; color: #8b949e; width: 60px; flex-shrink: 0; }
.send-row input, .send-row textarea { flex: 1; background: #0d1117; border: 1px solid #30363d; border-radius: 4px; color: #c9d1d9; padding: 6px 8px; font-size: 13px; font-family: monospace; }
.send-row textarea { min-height: 60px; resize: vertical; }
.send-btn { background: #238636; color: #fff; border: none; border-radius: 4px; padding: 6px 16px; font-size: 13px; cursor: pointer; }
.send-btn:hover { background: #2ea043; }
.send-btn:disabled { opacity: .5; cursor: default; }
.send-result { margin-top: 8px; padding: 8px 10px; background: #0d1117; border: 1px solid #30363d; border-radius: 4px; font-family: monospace; font-size: 12px; white-space: pre-wrap; word-break: break-all; display: none; max-height: 200px; overflow: auto; }
.send-result.show { display: block; }
.send-result .dur { color: #8b949e; font-size: 11px; margin-top: 4px; }
.send-result .err { color: #f85149; }
/* latency chart */
.latency-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px,1fr)); gap: 8px; margin-bottom: 16px; }
.latency-card { background: #161b22; border: 1px solid #30363d; border-radius: 6px; padding: 10px; }
.latency-card h4 { margin: 0 0 4px; font-size: 12px; color: #8b949e; }
.latency-val { font-size: 14px; font-weight: 600; }
.latency-bar-wrap { height: 4px; background: #0d1117; border-radius: 2px; margin-top: 6px; overflow: hidden; }
.latency-bar { height: 100%; border-radius: 2px; transition: width .5s; }
</style>
</head>
<body>
<div class="header">
  <h1>FlowRulZ Simulator</h1>
  <span class="mode" id="mode">live</span>
  <span style="flex:1"></span>
  <span id="clock" style="font-size:12px;color:#8b949e"></span>
</div>
<div class="layout">
<div class="main">
  <!-- Graph -->
  <div class="graph-container">
    <h2>Execution Flow</h2>
    <svg class="graph-svg" id="graph"></svg>
  </div>
  <!-- Metrics -->
  <div class="metrics" id="metrics"></div>
  <!-- Configure Flow -->
  <div class="send-box" style="margin-bottom:8px">
    <h3>Configure Flow</h3>
    <div class="send-row">
      <label>Rule ID</label>
      <input id="cfg-rule-id" type="text" placeholder="my-rule">
    </div>
    <div class="send-row">
      <label>DSL</label>
      <textarea id="cfg-dsl" placeholder='n:validate n:payment n:email' style="min-height:40px"></textarea>
    </div>
    <div style="display:flex;align-items:center;gap:8px">
      <button class="send-btn" id="cfg-btn" onclick="createRule()" style="background:#1f6feb">Create Rule</button>
      <span id="cfg-status" style="font-size:12px;color:#8b949e"></span>
    </div>
    <div class="send-result" id="cfg-result"></div>
  </div>
  <!-- Send Request -->
  <div class="send-box">
    <h3>Send Request</h3>
    <div class="send-row">
      <label>Rule</label>
      <select id="send-rule" style="flex:1;background:#0d1117;border:1px solid #30363d;border-radius:4px;color:#c9d1d9;padding:6px 8px;font-size:13px;font-family:monospace"></select>
    </div>
    <div class="send-row">
      <label>Body</label>
      <textarea id="send-body" placeholder='{"key":"value"}'></textarea>
    </div>
    <div style="display:flex;align-items:center;gap:8px">
      <button class="send-btn" id="send-btn" onclick="sendRequest()">Send</button>
      <span id="send-status" style="font-size:12px;color:#8b949e"></span>
      <span style="font-size:11px;color:#8b949e;margin-left:auto" id="svc-count"></span>
    </div>
    <div class="send-result" id="send-result"></div>
  </div>
  <!-- Service Latency -->
  <div class="section-title">Service Latency</div>
  <div class="latency-grid" id="latency-grid"></div>
  <!-- Executions -->
  <div class="section-title">Recent Executions</div>
  <div class="table-wrap"><table><thead><tr><th>Exec ID</th><th>Services</th><th>Events</th><th>Status</th></tr></thead><tbody id="execs-table"></tbody></table></div>
</div>
<div class="sidebar">
  <div class="section-title">Nodes</div>
  <div id="nodes-list"></div>
  <div class="section-title" style="margin-top:16px">Event Types</div>
  <div id="event-stats"></div>
  <div class="section-title" style="margin-top:16px">Recent Events</div>
  <div class="table-wrap" style="max-height:300px"><table><thead><tr><th>Time</th><th>Type</th><th>Svc</th></tr></thead><tbody id="events-table"></tbody></table></div>
</div>
</div>
<script>
const EVENT_TYPES = ['created','ready','instruction','service_call','service_response','service_error','suspend','resume','completed','failed','dropped'];
function fmt(ns) { const ms = ns / 1e6; return ms < 0.001 ? '<1µs' : ms < 1 ? (ms*1000|0)+'µs' : ms < 1000 ? ms.toFixed(1)+'ms' : (ms/1000).toFixed(2)+'s'; }
function esc(s) { return (s||'').replace(/[<>&]/g,c=>({'<':'&lt;','>':'&gt;','&':'&amp;'})[c]); }

function buildGraph(svcs, executions) {
  // Build service nodes with positions
  const known = ['validate','inventory','fraud','payment','shipping','email','loyalty','invoice','notification','echo'];
  const active = new Set();
  for (const exec of executions) {
    if (exec.status === 'running') {
      for (const s of exec.services) active.add(s);
    }
  }
  // All services seen in any execution
  const allSvcs = new Set(known);
  for (const exec of executions) {
    for (const s of exec.services) allSvcs.add(s);
  }
  const svcList = Array.from(allSvcs);
  const cols = Math.min(svcList.length, 5);
  const rows = Math.ceil(svcList.length / cols);

  // Layout grid
  const nodes = svcList.map((s, i) => {
    const col = i % cols;
    const row = Math.floor(i / cols);
    return { name: s, x: 60 + col * 130, y: 40 + row * 70 };
  });
  const w = Math.min(cols * 130 + 80, 800);
  const h = rows * 70 + 60;

  // Edges: connect in order services appear in execution
  const edges = new Set();
  for (const exec of executions) {
    for (let i = 0; i < exec.services.length - 1; i++) {
      edges.add(exec.services[i] + '|' + exec.services[i+1]);
    }
  }

  let svg = '<svg viewBox="0 0 '+w+' '+h+'" style="width:100%;height:100%">';
  // Draw edges
  for (const e of edges) {
    const [from, to] = e.split('|');
    const fn = nodes.find(n => n.name === from);
    const tn = nodes.find(n => n.name === to);
    if (!fn || !tn) continue;
    const cls = 'path-line';
    svg += '<line class="'+cls+'" x1="'+fn.x+'" y1="'+fn.y+'" x2="'+tn.x+'" y2="'+tn.y+'"/>';
  }
  // Draw nodes
  for (const n of nodes) {
    const cls = active.has(n.name) ? 'service-node active' : 'service-node';
    svg += '<rect class="'+cls+'" x="'+(n.x-40)+'" y="'+(n.y-14)+'" width="80" height="28"/>';
    svg += '<text class="service-label" x="'+n.x+'" y="'+n.y+'">'+esc(n.name)+'</text>';
  }
  // Draw active execution dots
  for (const exec of executions) {
    if (exec.status !== 'running') continue;
    for (let i = 0; i < exec.services.length; i++) {
      const n = nodes.find(nn => nn.name === exec.services[i]);
      if (!n) continue;
      const off = i * 8;
      svg += '<circle class="execution-dot" cx="'+(n.x+off-12)+'" cy="'+(n.y-18)+'"/>';
    }
  }
  svg += '</svg>';
  return svg;
}

async function populateRules() {
  const sel = document.getElementById('send-rule');
  try {
    const [rr, sr] = await Promise.all([
      fetch('/api/admin/rules').then(r=>r.json()),
      fetch('/api/admin/services').then(r=>r.json())
    ]);
    const rules = rr.rules || [];
    const svcs = sr.services || [];
    document.getElementById('svc-count').textContent = svcs.length + ' services, ' + rules.length + ' rules';
    if (sel.options.length !== rules.length + 1) {
      const cur = sel.value;
      sel.innerHTML = '<option value="">— select rule —</option>' + rules.map(rid =>
        '<option value="'+esc(rid)+'">'+esc(rid)+'</option>'
      ).join('');
      if (cur) sel.value = cur;
    }
  } catch(e) {}
}

async function createRule() {
  const btn = document.getElementById('cfg-btn');
  const status = document.getElementById('cfg-status');
  const result = document.getElementById('cfg-result');
  const id = document.getElementById('cfg-rule-id').value.trim();
  const dsl = document.getElementById('cfg-dsl').value.trim();
  if (!id || !dsl) { status.textContent = 'enter rule ID and DSL'; return; }
  btn.disabled = true;
  status.textContent = 'creating...';
  result.classList.remove('show');
  try {
    const r = await fetch('/api/admin/rules', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({id, dsl})
    });
    const data = await r.json();
    if (r.ok) {
      result.innerHTML = '<div style="color:#3fb950">rule created: '+esc(id)+'</div>';
      document.getElementById('cfg-rule-id').value = '';
      document.getElementById('cfg-dsl').value = '';
    } else {
      result.innerHTML = '<div class="err">'+esc(await r.text())+'</div>';
    }
    result.classList.add('show');
    status.textContent = '';
  } catch(e) {
    result.innerHTML = '<div class="err">fetch error: '+esc(e.message)+'</div>';
    result.classList.add('show');
    status.textContent = '';
  }
  btn.disabled = false;
}

async function sendRequest() {
  const btn = document.getElementById('send-btn');
  const status = document.getElementById('send-status');
  const result = document.getElementById('send-result');
  const rule = document.getElementById('send-rule').value.trim();
  const body = document.getElementById('send-body').value.trim();
  if (!rule) { status.textContent = 'enter a rule ID'; return; }
  btn.disabled = true;
  status.textContent = 'sending...';
  result.classList.remove('show');
  result.innerHTML = '';
  try {
    const r = await fetch('/api/admin/send', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({rule, body})
    });
    if (!r.ok) {
      result.innerHTML = '<div class="err">'+esc(await r.text())+'</div>';
      result.classList.add('show');
      status.textContent = '';
      btn.disabled = false;
      return;
    }
    const data = await r.json();
    if (data.body) {
      try { result.innerHTML = esc(JSON.stringify(JSON.parse(data.body), null, 2)); }
      catch { result.innerHTML = esc(data.body); }
      result.innerHTML += '<div class="dur">duration: '+esc(data.duration)+'</div>';
    } else if (data.error) {
      result.innerHTML = '<div class="err">'+esc(data.error)+'</div>';
    }
    result.classList.add('show');
    status.textContent = '';
  } catch(e) {
    result.innerHTML = '<div class="err">fetch error: '+esc(e.message)+'</div>';
    result.classList.add('show');
    status.textContent = '';
  }
  btn.disabled = false;
}

async function refresh() {
  try {
    const [m, events, nodes, execs] = await Promise.all([
      fetch('/api/metrics').then(r=>r.json()),
      fetch('/api/events').then(r=>r.json()),
      fetch('/api/nodes').then(r=>r.json()),
      fetch('/api/executions').then(r=>r.json())
    ]);
    renderMetrics(m);
    renderNodes(nodes);
    renderEvents(events);
    renderExecutions(execs, events);
    renderLatency(m);
    renderGraphSVG(execs);
    populateRules();
    document.getElementById('clock').textContent = new Date().toLocaleTimeString();
  } catch(e) {}
}

function renderMetrics(m) {
  const cards = [
    {label:'Completed', value:m.completed, cls:'green'},
    {label:'Failed', value:m.failed, cls:'red'},
    {label:'Dropped', value:m.dropped, cls:'yellow'},
    {label:'Throughput', value:(m.throughput_per_sec|0)+'/s', cls:'blue'},
    {label:'P50', value:fmt(m.p50_ms), cls:'blue'},
    {label:'P95', value:fmt(m.p95_ms), cls:'blue'},
    {label:'P99', value:fmt(m.p99_ms), cls:'blue'},
  ];
  document.getElementById('metrics').innerHTML = cards.map(c =>
    '<div class="card"><h3>'+c.label+'</h3><div class="value '+c.cls+'">'+c.value+'</div></div>'
  ).join('');
}

function renderNodes(nodes) {
  const div = document.getElementById('nodes-list');
  if (!nodes.length) { div.innerHTML = '<div style="color:#8b949e;font-size:12px">no nodes</div>'; return; }
  const maxq = Math.max(...nodes.map(n => Math.max(n.queues.ready, n.queues.waiting, 1)));
  div.innerHTML = nodes.map(n => {
    const rpct = (n.queues.ready / maxq * 100).toFixed(0);
    const wpct = (n.queues.waiting / maxq * 100).toFixed(0);
    return '<div class="node-row"><span class="node-name">'+esc(n.id)+'</span>'+
      '<div style="flex:1"><div class="node-row" style="padding:0;border:none;font-size:11px"><span>ready</span><div class="node-bar"><div class="node-fill" style="width:'+rpct+'%"></div></div><span class="node-val">'+n.queues.ready+'</span></div>'+
      '<div class="node-row" style="padding:0;border:none;font-size:11px"><span>wait</span><div class="node-bar"><div class="node-fill" style="width:'+wpct+'%;background:#d29922"></div></div><span class="node-val">'+n.queues.waiting+'</span></div>'+
      '</div><span class="node-val">'+n.execs+'</span></div>';
  }).join('');
}

function renderEvents(events) {
  const tbody = document.getElementById('events-table');
  const recent = events.slice(-30).reverse();
  tbody.innerHTML = recent.map(e => {
    const t = EVENT_TYPES[e.type] || 'unknown';
    return '<tr><td>'+(e.elapsed_ms ? fmt(e.elapsed_ms) : '')+'</td>'+
      '<td><span class="badge badge-ok">'+t+'</span></td>'+
      '<td>'+(e.service||'')+'</td></tr>';
  }).join('');
  // Event type stats
  const stats = {};
  for (const e of events) { const t = EVENT_TYPES[e.type]||'unknown'; stats[t] = (stats[t]||0)+1; }
  const st = document.getElementById('event-stats');
  st.innerHTML = Object.entries(stats).slice(-10).reverse().map(([k,v]) =>
    '<div class="node-row" style="font-size:12px"><span style="flex:1">'+k+'</span><span>'+v+'</span></div>'
  ).join('');
}

function renderExecutions(execs) {
  const tbody = document.getElementById('execs-table');
  const recent = execs.slice(-15).reverse();
  tbody.innerHTML = recent.map(e => {
    const cls = e.status === 'completed' ? 'badge-ok' : e.status === 'failed' || e.status === 'dropped' ? 'badge-err' : 'badge-running';
    return '<tr><td>'+esc(e.id)+'</td><td>'+e.services.map(s => esc(s)).join(', ')+'</td><td>'+e.events+'</td><td><span class="badge '+cls+'">'+e.status+'</span></td></tr>';
  }).join('');
}

function renderLatency(m) {
  const div = document.getElementById('latency-grid');
  if (!m.service_latency) { div.innerHTML = ''; return; }
  const maxLat = Math.max(...Object.values(m.service_latency).map(s => s.p95_ms || 0), 1);
  div.innerHTML = Object.entries(m.service_latency).map(([name, s]) => {
    const pct = (s.p95_ms / maxLat * 100).toFixed(0);
    const err = m.service_error_rate && m.service_error_rate[name] ? (m.service_error_rate[name] * 100).toFixed(1) : '0';
    return '<div class="latency-card"><h4>'+esc(name)+'</h4>'+
      '<div class="latency-val">'+fmt(s.avg_ms)+' avg</div>'+
      '<div style="display:flex;gap:8px;margin-top:4px;font-size:11px;color:#8b949e">'+
      '<span>p50 '+fmt(s.p50_ms)+'</span><span>p95 '+fmt(s.p95_ms)+'</span><span>err '+err+'%</span></div>'+
      '<div class="latency-bar-wrap"><div class="latency-bar" style="width:'+pct+'%;background:'+(err>5?'#f85149':'#3fb950')+'"></div></div></div>';
  }).join('');
}

function renderGraphSVG(execs) {
  const svg = buildGraph(null, execs);
  document.getElementById('graph').innerHTML = svg;
}

refresh();
setInterval(refresh, 1000);
</script>
</body>
</html>`
