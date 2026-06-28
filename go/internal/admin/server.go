package admin

import (
	"crypto/subtle"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/premchandkpc/FlowRulZ/go/internal/bridge"
	"github.com/premchandkpc/FlowRulZ/go/internal/engine"
)

type Server struct {
	engine    *engine.Engine
	mux       *http.ServeMux
	apiKey    string
}

func New(eng *engine.Engine) *Server {
	s := &Server{
		engine: eng,
		mux:    http.NewServeMux(),
		apiKey: os.Getenv("FLOWRULZ_API_KEY"),
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
	return s
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
	plan, err := bridge.Compile(req.DSL, "validate")
	if err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"valid": false,
			"error": err.Error(),
		})
		return
	}
	score := bridge.PlanComplexity(plan)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"valid":            true,
		"complexity_score": score,
		"plan_bytes":       len(plan),
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
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}
