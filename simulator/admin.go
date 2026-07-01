package simulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/premchandkpc/FlowRulZ/simulator/execution"
	"github.com/premchandkpc/FlowRulZ/simulator/services"
)

func (s *Simulator) RegisterAdminHandlers() {
	if s.Dashboard == nil {
		return
	}
	cli := s.Client()
	s.Dashboard.AddHandler("/api/admin/send", s.adminSend(cli))
	s.Dashboard.AddHandler("/api/admin/rules", s.adminRules(cli))
	s.Dashboard.AddHandler("/api/admin/rules/", s.adminRulesDetail(cli))
	s.Dashboard.AddHandler("/api/admin/services", s.adminServices(cli))
	s.Dashboard.AddHandler("/api/admin/lanes", s.adminLanes(cli))
	s.Dashboard.AddHandler("/api/admin/validate", s.adminValidate(cli))
	s.Dashboard.AddHandler("/api/admin/health", s.adminHealth(cli))
	s.Dashboard.AddHandler("/api/admin/partitions", s.adminPartitions(cli))
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func httpError(w http.ResponseWriter, msg string, code int) {
	http.Error(w, msg, code)
}

func (s *Simulator) adminSend(cli *Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			httpError(w, "POST required", 405)
			return
		}
		var req struct {
			Rule string `json:"rule"`
			Body string `json:"body"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			httpError(w, fmt.Sprintf("bad request: %v", err), 400)
			return
		}
		if req.Rule == "" {
			httpError(w, "rule required", 400)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()

		result, err := cli.Send(ctx, req.Rule, []byte(req.Body))
		if err != nil {
			httpError(w, fmt.Sprintf("send failed: %v", err), 500)
			return
		}
		writeJSON(w, map[string]any{
			"body":     string(result.Body),
			"duration": result.Duration.String(),
		})
	}
}

func (s *Simulator) adminRules(cli *Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			plans := cli.Plans()
			writeJSON(w, map[string]any{"rules": plans})

		case "POST":
			var req struct {
				ID  string `json:"id"`
				DSL string `json:"dsl"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				httpError(w, fmt.Sprintf("bad request: %v", err), 400)
				return
			}
			if req.ID == "" || req.DSL == "" {
				httpError(w, "id and dsl required", 400)
				return
			}
			if err := cli.AddRule(req.ID, req.DSL); err != nil {
				httpError(w, fmt.Sprintf("add rule failed: %v", err), 500)
				return
			}
			writeJSON(w, map[string]string{"status": "ok", "id": req.ID})

		default:
			httpError(w, "GET or POST required", 405)
		}
	}
}

func (s *Simulator) adminServices(cli *Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			svcs := cli.Services()
			writeJSON(w, map[string]any{"services": svcs})

		case "POST":
			var req struct {
				Name          string  `json:"name"`
				BaseLatencyMs int     `json:"base_latency_ms"`
				LatencyJitter int     `json:"latency_jitter_ms"`
				FailureRate   float64 `json:"failure_rate"`
				MaxConcurrent int     `json:"max_concurrent"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				httpError(w, fmt.Sprintf("bad request: %v", err), 400)
				return
			}
			if req.Name == "" {
				httpError(w, "name required", 400)
				return
			}
			svc := &services.MockService{
				Name:          req.Name,
				BaseLatency:   time.Duration(req.BaseLatencyMs) * time.Millisecond,
				LatencyJitter: time.Duration(req.LatencyJitter) * time.Millisecond,
				FailureRate:   req.FailureRate,
				MaxConcurrent: req.MaxConcurrent,
			}
			cli.RegisterService(svc)
			writeJSON(w, map[string]string{"status": "ok", "name": req.Name})

		default:
			httpError(w, "GET or POST required", 405)
		}
	}
}

func (s *Simulator) adminRulesDetail(cli *Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/api/admin/rules/")
		if id == "" {
			httpError(w, "rule id required", 400)
			return
		}
		switch r.Method {
		case "GET":
			plan := cli.Plan(id)
			if plan == nil {
				httpError(w, "rule not found", 404)
				return
			}
			writeJSON(w, plan)

		case "DELETE":
			if err := cli.RemoveRule(id); err != nil {
				httpError(w, fmt.Sprintf("remove rule: %v", err), 500)
				return
			}
			writeJSON(w, map[string]string{"status": "deleted", "id": id})

		default:
			httpError(w, "GET or DELETE required", 405)
		}
	}
}

func (s *Simulator) adminLanes(cli *Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"lanes": []map[string]any{
				{"name": "fast", "concurrency": 50, "queue_size": 5000},
				{"name": "normal", "concurrency": 20, "queue_size": 2000},
				{"name": "heavy", "concurrency": 5, "queue_size": 500},
			},
		})
	}
}

func (s *Simulator) adminValidate(cli *Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			httpError(w, "POST required", 405)
			return
		}
		var req struct {
			DSL string `json:"dsl"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			httpError(w, fmt.Sprintf("bad request: %v", err), 400)
			return
		}
		if req.DSL == "" {
			httpError(w, "dsl required", 400)
			return
		}
		plan := &execution.Plan{ID: "_validate"}
		if err := compileDSL(plan, req.DSL); err != nil {
			writeJSON(w, map[string]any{"valid": false, "error": err.Error()})
			return
		}
		writeJSON(w, map[string]any{"valid": true, "instructions": len(plan.Instructions)})
	}
}

func (s *Simulator) adminHealth(cli *Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"status":      "ok",
			"node_id":     "simulator",
			"is_leader":   true,
			"term":        1,
			"num_nodes":   len(s.Nodes),
			"num_services": len(s.Services.All()),
			"num_rules":   len(cli.Plans()),
		})
	}
}

func (s *Simulator) adminPartitions(cli *Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		numNodes := len(s.Nodes)
		assignments := make([]string, 64)
		nodeParts := make(map[int][]uint32)
		for i := range assignments {
			nodeIdx := i % numNodes
			assignments[i] = fmt.Sprintf("node-%d", nodeIdx+1)
			nodeParts[nodeIdx] = append(nodeParts[nodeIdx], uint32(i))
		}
		writeJSON(w, map[string]any{
			"num_partitions":  64,
			"assignments":     assignments,
			"node_partitions": nodeParts,
		})
	}
}


