package simulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/premchandkpc/FlowRulZ/simulator/services"
)

func (s *Simulator) RegisterAdminHandlers() {
	if s.Dashboard == nil {
		return
	}
	cli := s.Client()
	s.Dashboard.AddHandler("/api/admin/send", s.adminSend(cli))
	s.Dashboard.AddHandler("/api/admin/rules", s.adminRules(cli))
	s.Dashboard.AddHandler("/api/admin/services", s.adminServices(cli))
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


