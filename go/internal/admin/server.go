package admin

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"runtime"
	"strconv"

	"github.com/premchandkpc/FlowRulZ/go/internal/compiler"
	"github.com/premchandkpc/FlowRulZ/go/internal/engine"
	"github.com/premchandkpc/FlowRulZ/go/internal/reliability"
)

type Server struct {
	engine   *engine.Engine
	mux      *http.ServeMux
	apiKey   string
	dlq      *reliability.DLQ
	compiler compiler.Compiler
	rules    *ruleService
}

func New(eng *engine.Engine) *Server {
	return NewWithCompiler(eng, compiler.NewLocal())
}

func NewWithCompiler(eng *engine.Engine, comp compiler.Compiler) *Server {
	if comp == nil {
		comp = compiler.NewLocal()
	}
	s := &Server{
		engine:   eng,
		mux:      http.NewServeMux(),
		apiKey:   os.Getenv("FLOWRULZ_API_KEY"),
		compiler: comp,
		rules:    newRuleService(eng, comp),
	}
	s.mux.HandleFunc("POST /rules", s.auth(s.deployRule))
	s.mux.HandleFunc("DELETE /rules/{id}", s.auth(s.removeRule))
	s.mux.HandleFunc("GET /rules", s.auth(s.listRules))
	s.mux.HandleFunc("GET /rules/{id}", s.auth(s.getRule))
	s.mux.HandleFunc("GET /rules/{id}/versions", s.auth(s.listVersions))
	s.mux.HandleFunc("POST /rules/{id}/validate", s.auth(s.validateRule))
	s.mux.HandleFunc("POST /rules/{id}/promote", s.auth(s.promoteVersion))
	s.mux.HandleFunc("POST /rules/{id}/rollback", s.auth(s.rollbackVersion))
	s.mux.HandleFunc("GET /lanes", s.auth(s.listLanes))
	s.mux.HandleFunc("GET /health", s.health)
	s.mux.HandleFunc("GET /metrics", s.auth(s.metrics))
	s.mux.HandleFunc("GET /debug", s.auth(s.debug))
	return s
}

func (s *Server) RegisterDLQ(dlq *reliability.DLQ) {
	s.dlq = dlq
	s.mux.HandleFunc("GET /dlq", s.auth(s.listDLQ))
	s.mux.HandleFunc("POST /dlq/replay/{id}", s.auth(s.replayDLQ))
	s.mux.HandleFunc("POST /dlq/replay", s.auth(s.replayAllDLQ))
	s.mux.HandleFunc("DELETE /dlq", s.auth(s.clearDLQ))
}

func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.apiKey != "" {
			key := r.Header.Get("Authorization")
			if subtle.ConstantTimeCompare([]byte(key), []byte("Bearer "+s.apiKey)) != 1 {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		}
		next(w, r)
	}
}

func (s *Server) deployRule(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID  string `json:"id"`
		DSL string `json:"dsl"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	log.Printf("deploy rule: id=%s", req.ID)
	if err := s.rules.DeployRule(req.ID, req.DSL); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]string{"id": req.ID})
}

func (s *Server) removeRule(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s.rules.RemoveRule(id)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) listRules(w http.ResponseWriter, r *http.Request) {
	_ = json.NewEncoder(w).Encode(s.rules.ListRules())
}

func (s *Server) getRule(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if detail, ok := s.rules.RuleDetail(id); ok {
		_ = json.NewEncoder(w).Encode(detail)
		return
	}
	http.Error(w, "rule not found", http.StatusNotFound)
}

func (s *Server) listVersions(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	_ = json.NewEncoder(w).Encode(s.rules.RuleVersions(id))
}

func (s *Server) validateRule(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DSL string `json:"dsl"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	result, err := s.rules.ValidateDSL(req.DSL)
	if err != nil {
		_ = json.NewEncoder(w).Encode(result)
		return
	}
	_ = json.NewEncoder(w).Encode(result)
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
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"id": id, "active_version": ver})
}

func (s *Server) rollbackVersion(w http.ResponseWriter, r *http.Request) {
	s.promoteVersion(w, r)
}

func (s *Server) listLanes(w http.ResponseWriter, r *http.Request) {
	_ = json.NewEncoder(w).Encode(s.rules.Lanes())
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	_ = json.NewEncoder(w).Encode(s.rules.HealthSnapshot())
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
	_ = json.NewEncoder(w).Encode(s.rules.DebugSnapshot())
}

func (s *Server) listDLQ(w http.ResponseWriter, r *http.Request) {
	if s.dlq == nil {
		_ = json.NewEncoder(w).Encode([]interface{}{})
		return
	}
	entries := s.dlq.List()
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(entries)
}

func (s *Server) replayDLQ(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if s.dlq == nil {
		http.Error(w, "dlq not configured", http.StatusNotFound)
		return
	}
	if err := s.dlq.Replay(r.Context(), id); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "replayed", "id": id})
}

func (s *Server) replayAllDLQ(w http.ResponseWriter, r *http.Request) {
	if s.dlq == nil {
		http.Error(w, "dlq not configured", http.StatusNotFound)
		return
	}
	count := s.dlq.ReplayAll(r.Context())
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "replayed", "count": count})
}

func (s *Server) clearDLQ(w http.ResponseWriter, r *http.Request) {
	if s.dlq == nil {
		http.Error(w, "dlq not configured", http.StatusNotFound)
		return
	}
	s.dlq.Clear()
	w.WriteHeader(http.StatusNoContent)
}
