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
	"time"

	"github.com/premchandkpc/FlowRulZ/go/internal/compiler"
	"github.com/premchandkpc/FlowRulZ/go/internal/engine"
	"github.com/premchandkpc/FlowRulZ/go/internal/reliability"
)

type Server struct {
	engine    *engine.Engine
	mux       *http.ServeMux
	apiKey    string
	dlq       *reliability.DLQ
	compiler  compiler.Compiler
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
	if err := s.engine.Deploy(req.ID, req.DSL); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"id": req.ID})
}

func (s *Server) removeRule(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s.engine.Remove(id)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) listRules(w http.ResponseWriter, r *http.Request) {
	rules := s.engine.Rules()
	type versionView struct {
		Version  uint64 `json:"version"`
		Active   bool   `json:"active"`
	}
	type ruleView struct {
		ID       string        `json:"id"`
		Versions []versionView `json:"versions"`
	}
	view := make([]ruleView, 0, len(rules))
	for _, ru := range rules {
		vvs := make([]versionView, len(ru.Versions))
		for i, v := range ru.Versions {
			vvs[i] = versionView{
				Version: v.Version,
				Active:  i == ru.ActiveVersion,
			}
		}
		view = append(view, ruleView{ID: ru.ID, Versions: vvs})
	}
	json.NewEncoder(w).Encode(view)
}

type versionView struct {
	Version uint64 `json:"version"`
	DSL     string `json:"dsl"`
	Active  bool   `json:"active"`
	Lane    string `json:"lane,omitempty"`
}

func (s *Server) getRule(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rules := s.engine.Rules()
	for _, ru := range rules {
		if ru.ID == id {
			vvs := make([]versionView, len(ru.Versions))
			for i, v := range ru.Versions {
				vvs[i] = versionView{
					Version: v.Version,
					DSL:     v.DSL,
					Active:  i == ru.ActiveVersion,
					Lane:    string(v.Lane),
				}
			}
			json.NewEncoder(w).Encode(map[string]interface{}{
				"id":       ru.ID,
				"versions": vvs,
			})
			return
		}
	}
	http.Error(w, "rule not found", http.StatusNotFound)
}

func (s *Server) listVersions(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rules := s.engine.Rules()
	for _, ru := range rules {
		if ru.ID == id {
			vvs := make([]versionView, len(ru.Versions))
			for i, v := range ru.Versions {
				vvs[i] = versionView{
					Version: v.Version,
					DSL:     v.DSL,
					Active:  i == ru.ActiveVersion,
				}
			}
			json.NewEncoder(w).Encode(vvs)
			return
		}
	}
	json.NewEncoder(w).Encode([]versionView{})
}

func (s *Server) validateRule(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DSL string `json:"dsl"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	result, err := s.compiler.Compile(req.DSL, "validate")
	if err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"valid": false,
			"error": err.Error(),
		})
		return
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"valid":            true,
		"complexity_score": result.Complexity,
		"plan_bytes":       len(result.Plan),
	})
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
	if err := s.engine.Promote(id, ver); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	json.NewEncoder(w).Encode(map[string]interface{}{"id": id, "active_version": ver})
}

func (s *Server) rollbackVersion(w http.ResponseWriter, r *http.Request) {
	s.promoteVersion(w, r)
}

func (s *Server) listLanes(w http.ResponseWriter, r *http.Request) {
	type laneView struct {
		Name        string `json:"name"`
		BatchSize   int    `json:"batch_size"`
		PollTimeout int    `json:"poll_timeout_ms"`
	}
	lanes := make([]laneView, len(engine.DefaultLanes))
	for i, l := range engine.DefaultLanes {
		lanes[i] = laneView{
			Name:        string(l.Name),
			BatchSize:   l.BatchSize,
			PollTimeout: l.PollTimeout,
		}
	}
	json.NewEncoder(w).Encode(lanes)
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":           "ok",
		"time":             time.Now().UTC().Format(time.RFC3339),
		"goroutines":       runtime.NumGoroutine(),
		"alloc_mb":         fmt.Sprintf("%.1f", float64(m.Alloc)/1024/1024),
		"heap_objects":     m.HeapObjects,
		"num_rules":        len(s.engine.Rules()),
		"go_version":       runtime.Version(),
	})
}

func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

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
	fmt.Fprintf(w, "flowrulz_num_rules %d\n", len(s.engine.Rules()))
	fmt.Fprintf(w, "# HELP flowrulz_next_gc_bytes Next GC target\n")
	fmt.Fprintf(w, "# TYPE flowrulz_next_gc_bytes gauge\n")
	fmt.Fprintf(w, "flowrulz_next_gc_bytes %d\n", m.NextGC)
}

func (s *Server) debug(w http.ResponseWriter, r *http.Request) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	json.NewEncoder(w).Encode(map[string]interface{}{
		"goroutines":  runtime.NumGoroutine(),
		"cgo_calls":   runtime.NumCgoCall(),
		"memory": map[string]interface{}{
			"alloc":       m.Alloc,
			"total_alloc": m.TotalAlloc,
			"sys":         m.Sys,
			"heap_alloc":  m.HeapAlloc,
			"heap_sys":    m.HeapSys,
			"heap_objects": m.HeapObjects,
			"gc_cycles":   m.NumGC,
			"gc_pause_ns": m.PauseNs[(m.NumGC+255)%256],
		},
		"num_rules":  len(s.engine.Rules()),
		"go_version": runtime.Version(),
	})
}

func (s *Server) listDLQ(w http.ResponseWriter, r *http.Request) {
	if s.dlq == nil {
		json.NewEncoder(w).Encode([]interface{}{})
		return
	}
	entries := s.dlq.List()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(entries)
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
	json.NewEncoder(w).Encode(map[string]string{"status": "replayed", "id": id})
}

func (s *Server) replayAllDLQ(w http.ResponseWriter, r *http.Request) {
	if s.dlq == nil {
		http.Error(w, "dlq not configured", http.StatusNotFound)
		return
	}
	count := s.dlq.ReplayAll(r.Context())
	json.NewEncoder(w).Encode(map[string]interface{}{"status": "replayed", "count": count})
}

func (s *Server) clearDLQ(w http.ResponseWriter, r *http.Request) {
	if s.dlq == nil {
		http.Error(w, "dlq not configured", http.StatusNotFound)
		return
	}
	s.dlq.Clear()
	w.WriteHeader(http.StatusNoContent)
}
