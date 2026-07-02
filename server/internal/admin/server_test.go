package admin

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/premchandkpc/FlowRulZ/server/internal/engine"
)

func newTestServer(eng *engine.Engine) *Server {
	os.Setenv("FLOWRULZ_API_KEY", "test-key")
	s := NewWithCompiler(eng, nil)
	return s
}

func authReq(req *http.Request) {
	req.Header.Set("Authorization", "Bearer test-key")
}

func TestHealth(t *testing.T) {
	eng := engine.New("")
	srv := newTestServer(eng)

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var resp map[string]string
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["status"] != "ok" {
		t.Errorf("expected 'ok', got %s", resp["status"])
	}
}

func TestDeployAndListRules(t *testing.T) {
	eng := engine.New("")
	srv := newTestServer(eng)

	body := `{"id":"test-1","dsl":"n:validate"}`
	req := httptest.NewRequest("POST", "/rules", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	authReq(req)
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("expected 201, got %d: %s", w.Code, w.Body.String())
	}

	req = httptest.NewRequest("GET", "/rules", nil)
	authReq(req)
	w = httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var rules []map[string]interface{}
	json.NewDecoder(w.Body).Decode(&rules)
	if len(rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(rules))
	}
	if rules[0]["id"] != "test-1" {
		t.Errorf("expected test-1, got %s", rules[0]["id"])
	}
}

func TestRemoveRule(t *testing.T) {
	eng := engine.New("")
	srv := newTestServer(eng)

	body := `{"id":"test-1","dsl":"n:validate"}`
	req := httptest.NewRequest("POST", "/rules", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	authReq(req)
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)

	req = httptest.NewRequest("DELETE", "/rules/test-1", nil)
	authReq(req)
	w = httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("expected 204, got %d", w.Code)
	}
}

func TestDeployInvalidDSL(t *testing.T) {
	eng := engine.New("")
	srv := newTestServer(eng)

	body := `{"id":"bad","dsl":"!!!invalid"}`
	req := httptest.NewRequest("POST", "/rules", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	authReq(req)
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", w.Code)
	}
}

func TestGetRule(t *testing.T) {
	eng := engine.New("")
	srv := newTestServer(eng)

	body := `{"id":"test-1","dsl":"n:validate"}`
	req := httptest.NewRequest("POST", "/rules", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	authReq(req)
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)

	req = httptest.NewRequest("GET", "/rules/test-1", nil)
	authReq(req)
	w = httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["id"] != "test-1" {
		t.Errorf("expected test-1, got %s", resp["id"])
	}
}

func TestPromoteVersion(t *testing.T) {
	eng := engine.New("")
	srv := newTestServer(eng)

	srv.engine.Deploy("test-1", "n:validate")
	rules := eng.Rules()
	v1 := rules[0].ActivePlan().Version

	srv.engine.Deploy("test-1", "n:validate")

	req := httptest.NewRequest("POST", "/rules/test-1/promote?version="+fmt.Sprintf("%d", v1), nil)
	authReq(req)
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	rules = eng.Rules()
	if rules[0].ActivePlan().Version != v1 {
		t.Errorf("expected active version %d, got %d", v1, rules[0].ActivePlan().Version)
	}
}

func TestListVersions(t *testing.T) {
	eng := engine.New("")
	srv := newTestServer(eng)

	srv.engine.Deploy("test-1", "n:validate")
	srv.engine.Deploy("test-1", "n:validate")

	req := httptest.NewRequest("GET", "/rules/test-1/versions", nil)
	authReq(req)
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var versions []map[string]interface{}
	json.NewDecoder(w.Body).Decode(&versions)
	if len(versions) != 2 {
		t.Fatalf("expected 2 versions, got %d", len(versions))
	}
}
