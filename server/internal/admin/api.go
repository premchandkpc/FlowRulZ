package admin

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/premchandkpc/FlowRulZ/server/internal/compiler"
	"github.com/premchandkpc/FlowRulZ/server/internal/engine"
	"github.com/premchandkpc/FlowRulZ/server/internal/reliability"
)

const maxRequestBodySize = 1 << 20

type Server struct {
	engine      *engine.Engine
	mux         *http.ServeMux
	apiKey      string
	dlq         *reliability.DLQ
	compiler    compiler.Compiler
	rules       *ruleService
	rateLimiter *reliability.RateLimiter

	// Extended dependencies for admin operations
	schedulerSnapshot func() interface{}
	recoveryTrigger   func(ctx context.Context)
	recoveryInFlight  atomic.Bool
	nodeID            string
}

func New(eng *engine.Engine) *Server {
	return NewWithCompiler(eng, compiler.NewLocal())
}

func NewWithCompiler(eng *engine.Engine, comp compiler.Compiler) *Server {
	if comp == nil {
		comp = compiler.NewLocal()
	}
	apiKey := os.Getenv("FLOWRULZ_API_KEY")
	if apiKey == "" {
		slog.Warn("FLOWRULZ_API_KEY not set — admin API requires authentication; all mutating endpoints will reject unauthenticated requests")
	}
	rl := reliability.NewRateLimiter()
	rl.SetBucket("admin-api", 50, 100) // 50 req/s, burst of 100
	s := &Server{
		engine:      eng,
		mux:         http.NewServeMux(),
		apiKey:      apiKey,
		compiler:    comp,
		rules:       newRuleService(eng, comp),
		rateLimiter: rl,
	}
	s.mux.HandleFunc("POST /rules", s.auth(s.rateLimit(requireContentType(s.deployRule))))
	s.mux.HandleFunc("DELETE /rules/{id}", s.auth(s.rateLimit(s.removeRule)))
	s.mux.HandleFunc("GET /rules", s.auth(s.rateLimit(s.listRules)))
	s.mux.HandleFunc("GET /rules/{id}", s.auth(s.rateLimit(s.getRule)))
	s.mux.HandleFunc("GET /rules/{id}/versions", s.auth(s.rateLimit(s.listVersions)))
	s.mux.HandleFunc("POST /rules/{id}/validate", s.auth(s.rateLimit(requireContentType(s.validateRule))))
	s.mux.HandleFunc("POST /rules/{id}/promote", s.auth(s.rateLimit(s.promoteVersion)))
	s.mux.HandleFunc("POST /rules/{id}/rollback", s.auth(s.rateLimit(s.rollbackVersion)))
	s.mux.HandleFunc("GET /lanes", s.auth(s.rateLimit(s.listLanes)))
	s.mux.HandleFunc("GET /health", s.health)
	s.mux.HandleFunc("GET /metrics", s.auth(s.rateLimit(s.metrics)))
	s.mux.HandleFunc("GET /debug", s.auth(s.rateLimit(s.debug)))
	return s
}

func (s *Server) RegisterDLQ(dlq *reliability.DLQ) {
	s.dlq = dlq
	s.mux.HandleFunc("GET /dlq", s.auth(s.rateLimit(s.listDLQ)))
	s.mux.HandleFunc("POST /dlq/replay/{id}", s.auth(s.rateLimit(s.replayDLQ)))
	s.mux.HandleFunc("POST /dlq/replay", s.auth(s.rateLimit(s.replayAllDLQ)))
	s.mux.HandleFunc("DELETE /dlq", s.auth(s.rateLimit(s.clearDLQ)))
	s.mux.HandleFunc("POST /dlq/load", s.auth(s.rateLimit(requireContentType(s.loadDLQFromTopic))))
}

// RegisterExtended registers admin endpoints that require node-level dependencies.
func (s *Server) RegisterExtended(nodeID string, schedulerSnapshot func() interface{}, recoveryTrigger func(ctx context.Context)) {
	s.nodeID = nodeID
	s.schedulerSnapshot = schedulerSnapshot
	s.recoveryTrigger = recoveryTrigger
	s.mux.HandleFunc("GET /scheduler/snapshot", s.auth(s.rateLimit(s.getSchedulerSnapshot)))
	s.mux.HandleFunc("POST /recovery/trigger", s.auth(s.rateLimit(s.triggerRecovery)))
	s.mux.HandleFunc("GET /node/info", s.auth(s.rateLimit(s.getNodeInfo)))
}

func (s *Server) Handler() http.Handler {
	return s.mux
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("json encode error", "error", err)
	}
}

func requireContentType(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if ct := r.Header.Get("Content-Type"); ct != "" && !strings.HasPrefix(ct, "application/json") {
			http.Error(w, "Content-Type must be application/json", http.StatusUnsupportedMediaType)
			return
		}
		next(w, r)
	}
}

func (s *Server) rateLimit(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.rateLimiter.Allow("admin-api") {
			slog.Warn("admin API rate limited", "remote", r.RemoteAddr)
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		next(w, r)
	}
}

func (s *Server) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.apiKey == "" {
			slog.Warn("admin API request rejected: no API key configured",
				"method", r.Method,
				"path", r.URL.Path,
				"remote", r.RemoteAddr)
			http.Error(w, "admin API requires authentication; set FLOWRULZ_API_KEY", http.StatusUnauthorized)
			return
		}
		key := r.Header.Get("Authorization")
		if subtle.ConstantTimeCompare([]byte(key), []byte("Bearer "+s.apiKey)) != 1 {
			slog.Warn("admin API request rejected: invalid credentials",
				"method", r.Method,
				"path", r.URL.Path,
				"remote", r.RemoteAddr)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

func (s *Server) deployRule(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodySize)
	var req struct {
		ID  string `json:"id"`
		DSL string `json:"dsl"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.ID == "" || len(req.ID) > 256 {
		http.Error(w, "invalid rule ID: must be 1-256 characters", http.StatusBadRequest)
		return
	}
	if len(req.DSL) == 0 || len(req.DSL) > 1<<20 {
		http.Error(w, "invalid DSL: must be 1-1MB", http.StatusBadRequest)
		return
	}
	slog.Info("deploy rule", "id", req.ID, "remote", r.RemoteAddr)
	if err := s.rules.DeployRule(req.ID, req.DSL); err != nil {
		slog.Error("deploy rule failed", "id", req.ID, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"id": req.ID})
}

func (s *Server) removeRule(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" || len(id) > 256 {
		http.Error(w, "invalid rule ID", http.StatusBadRequest)
		return
	}
	slog.Info("remove rule", "id", id, "remote", r.RemoteAddr)
	s.rules.RemoveRule(id)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) listRules(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.rules.ListRules())
}

func (s *Server) getRule(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if detail, ok := s.rules.RuleDetail(id); ok {
		writeJSON(w, http.StatusOK, detail)
		return
	}
	http.Error(w, "rule not found", http.StatusNotFound)
}

func (s *Server) listVersions(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	writeJSON(w, http.StatusOK, s.rules.RuleVersions(id))
}

func (s *Server) validateRule(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodySize)
	var req struct {
		DSL string `json:"dsl"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if len(req.DSL) == 0 || len(req.DSL) > 1<<20 {
		http.Error(w, "invalid DSL: must be 1-1MB", http.StatusBadRequest)
		return
	}
	result, _ := s.rules.ValidateDSL(req.DSL)
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) promoteVersion(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	verStr := r.URL.Query().Get("version")
	if verStr == "" {
		http.Error(w, "missing version query param", http.StatusBadRequest)
		return
	}
	ver, err := strconv.ParseUint(verStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid version", http.StatusBadRequest)
		return
	}
	if err := s.rules.PromoteVersion(id, ver); err != nil {
		slog.Error("promote version failed", "id", id, "version", ver, "error", err)
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"id": id, "active_version": ver})
}

func (s *Server) rollbackVersion(w http.ResponseWriter, r *http.Request) {
	s.promoteVersion(w, r)
}

func (s *Server) listLanes(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.rules.Lanes())
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	count := 0
	if s.engine != nil {
		count = len(s.engine.Rules())
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintf(w, "# HELP flowrulz_goroutines Number of goroutines\n")
	fmt.Fprintf(w, "# TYPE flowrulz_goroutines gauge\n")
	fmt.Fprintf(w, "flowrulz_goroutines %d\n", runtime.NumGoroutine())
	fmt.Fprintf(w, "# HELP flowrulz_alloc_bytes Allocated heap bytes\n")
	fmt.Fprintf(w, "# TYPE flowrulz_alloc_bytes gauge\n")
	fmt.Fprintf(w, "flowrulz_alloc_bytes %d\n", m.Alloc)
	fmt.Fprintf(w, "# HELP flowrulz_heap_objects Number of heap objects\n")
	fmt.Fprintf(w, "# TYPE flowrulz_heap_objects gauge\n")
	fmt.Fprintf(w, "flowrulz_heap_objects %d\n", m.HeapObjects)
	fmt.Fprintf(w, "# HELP flowrulz_num_rules Number of deployed rules\n")
	fmt.Fprintf(w, "# TYPE flowrulz_num_rules gauge\n")
	fmt.Fprintf(w, "flowrulz_num_rules %d\n", count)
	fmt.Fprintf(w, "# HELP flowrulz_next_gc_bytes Next GC target\n")
	fmt.Fprintf(w, "# TYPE flowrulz_next_gc_bytes gauge\n")
	fmt.Fprintf(w, "flowrulz_next_gc_bytes %d\n", m.NextGC)
}

func (s *Server) debug(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.rules.DebugSnapshot())
}

func (s *Server) listDLQ(w http.ResponseWriter, r *http.Request) {
	if s.dlq == nil {
		writeJSON(w, http.StatusOK, []interface{}{})
		return
	}
	entries := s.dlq.List()
	writeJSON(w, http.StatusOK, entries)
}

func (s *Server) replayDLQ(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if s.dlq == nil {
		http.Error(w, "dlq not configured", http.StatusNotFound)
		return
	}
	if err := s.dlq.Replay(r.Context(), id); err != nil {
		slog.Error("dlq replay failed", "id", id, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "replayed", "id": id})
}

func (s *Server) replayAllDLQ(w http.ResponseWriter, r *http.Request) {
	if s.dlq == nil {
		http.Error(w, "dlq not configured", http.StatusNotFound)
		return
	}
	count := s.dlq.ReplayAll(r.Context())
	writeJSON(w, http.StatusOK, map[string]interface{}{"status": "replayed", "count": count})
}

func (s *Server) clearDLQ(w http.ResponseWriter, r *http.Request) {
	if s.dlq == nil {
		http.Error(w, "dlq not configured", http.StatusNotFound)
		return
	}
	s.dlq.Clear()
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) loadDLQFromTopic(w http.ResponseWriter, r *http.Request) {
	if s.dlq == nil {
		http.Error(w, "dlq not configured", http.StatusNotFound)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodySize)
	var req struct {
		Messages [][]byte `json:"messages"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	count := s.dlq.LoadFromMessages(r.Context(), req.Messages)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":     "loaded",
		"added":      count,
		"total":      s.dlq.Len(),
		"input_size": len(req.Messages),
	})
}

func (s *Server) getSchedulerSnapshot(w http.ResponseWriter, r *http.Request) {
	if s.schedulerSnapshot == nil {
		http.Error(w, "scheduler not configured", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, s.schedulerSnapshot())
}

func (s *Server) triggerRecovery(w http.ResponseWriter, r *http.Request) {
	if s.recoveryTrigger == nil {
		http.Error(w, "recovery not configured", http.StatusNotFound)
		return
	}
	if !s.recoveryInFlight.CompareAndSwap(false, true) {
		http.Error(w, "recovery already in progress", http.StatusConflict)
		return
	}
	go func() {
		defer s.recoveryInFlight.Store(false)
		s.recoveryTrigger(context.Background())
	}()
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "recovery triggered"})
}

func (s *Server) getNodeInfo(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"node_id":    s.nodeID,
		"dlq_size":   s.dlq.Len(),
		"num_rules":  len(s.engine.Rules()),
		"go_version": runtime.Version(),
		"goroutines": runtime.NumGoroutine(),
	})
}
