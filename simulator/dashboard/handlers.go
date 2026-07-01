package dashboard

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/premchandkpc/FlowRulZ/simulator/metrics"
	"github.com/premchandkpc/FlowRulZ/simulator/timeline"
)

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
