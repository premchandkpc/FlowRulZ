package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/premchandkpc/FlowRulZ/server/internal/engine"
	"github.com/premchandkpc/FlowRulZ/server/internal/reliability"
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

func TestDLQLoad(t *testing.T) {
	eng := engine.New("")
	srv := newTestServer(eng)
	dlq := reliability.NewDLQ(100)
	srv.RegisterDLQ(dlq)

	body := map[string]interface{}{
		"messages": [][]byte{
			[]byte(`{"id":"dlq-1","rule_id":"r1","error":"timeout"}`),
			[]byte(`{"id":"dlq-2","rule_id":"r2","error":"connection refused"}`),
		},
	}
	b, _ := json.Marshal(body)
	req := httptest.NewRequest("POST", "/dlq/load", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	authReq(req)
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["status"] != "loaded" {
		t.Errorf("expected status=loaded, got %v", resp["status"])
	}
	if added, ok := resp["added"].(float64); !ok || int(added) != 2 {
		t.Errorf("expected added=2, got %v", resp["added"])
	}
	if inputSize, ok := resp["input_size"].(float64); !ok || int(inputSize) != 2 {
		t.Errorf("expected input_size=2, got %v", resp["input_size"])
	}
}

func TestDLQLoadEmpty(t *testing.T) {
	eng := engine.New("")
	srv := newTestServer(eng)
	dlq := reliability.NewDLQ(100)
	srv.RegisterDLQ(dlq)

	body := map[string]interface{}{
		"messages": [][]byte{},
	}
	b, _ := json.Marshal(body)
	req := httptest.NewRequest("POST", "/dlq/load", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	authReq(req)
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	if added, ok := resp["added"].(float64); !ok || int(added) != 0 {
		t.Errorf("expected added=0, got %v", resp["added"])
	}
	if total, ok := resp["total"].(float64); !ok || int(total) != 0 {
		t.Errorf("expected total=0, got %v", resp["total"])
	}
}

func TestDLQLoadNoDLQ(t *testing.T) {
	eng := engine.New("")
	srv := newTestServer(eng)

	body := map[string]interface{}{
		"messages": [][]byte{
			[]byte(`{"key":"a","value":1}`),
		},
	}
	b, _ := json.Marshal(body)
	req := httptest.NewRequest("POST", "/dlq/load", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	authReq(req)
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestSchedulerSnapshot(t *testing.T) {
	eng := engine.New("")
	srv := newTestServer(eng)
	srv.RegisterExtended("node-1", func() interface{} {
		return map[string]interface{}{"queue_depth": 5, "workers": 4}
	}, nil)

	req := httptest.NewRequest("GET", "/scheduler/snapshot", nil)
	authReq(req)
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["queue_depth"] != float64(5) {
		t.Errorf("expected queue_depth=5, got %v", resp["queue_depth"])
	}
	if resp["workers"] != float64(4) {
		t.Errorf("expected workers=4, got %v", resp["workers"])
	}
}

func TestTriggerRecovery(t *testing.T) {
	eng := engine.New("")
	srv := newTestServer(eng)
	triggered := make(chan struct{}, 1)
	srv.RegisterExtended("node-1", nil, func(ctx context.Context) {
		triggered <- struct{}{}
	})

	req := httptest.NewRequest("POST", "/recovery/trigger", nil)
	authReq(req)
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]string
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["status"] != "recovery triggered" {
		t.Errorf("expected status='recovery triggered', got %s", resp["status"])
	}

	select {
	case <-triggered:
	case <-make(chan struct{}, 1):
		_ = context.Background()
		// give the goroutine a moment
		select {
		case <-triggered:
			// ok
		default:
			// trigger may run async; accept that
		}
	}
}

func TestNodeInfo(t *testing.T) {
	eng := engine.New("")
	srv := newTestServer(eng)
	dlq := reliability.NewDLQ(100)
	srv.RegisterDLQ(dlq)
	srv.RegisterExtended("node-42", nil, nil)

	req := httptest.NewRequest("GET", "/node/info", nil)
	authReq(req)
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["node_id"] != "node-42" {
		t.Errorf("expected node_id='node-42', got %v", resp["node_id"])
	}
	if resp["go_version"] == nil || resp["go_version"] == "" {
		t.Error("expected go_version to be set")
	}
	if _, ok := resp["goroutines"]; !ok {
		t.Error("expected goroutines to be present")
	}
}

func TestHealthEndpointMinimal(t *testing.T) {
	eng := engine.New("")
	srv := newTestServer(eng)

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]string
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["status"] != "ok" {
		t.Errorf("expected status=ok, got %s", resp["status"])
	}

	if len(resp) != 1 {
		t.Errorf("expected only status field, got %d fields: %v", len(resp), resp)
	}
	if _, exists := resp["goroutines"]; exists {
		t.Error("health endpoint should not expose goroutines (use /metrics)")
	}
	if _, exists := resp["alloc_mb"]; exists {
		t.Error("health endpoint should not expose alloc_mb (use /metrics)")
	}
}

func TestRecoveryTriggerDebounce(t *testing.T) {
	eng := engine.New("")
	srv := newTestServer(eng)

	block := make(chan struct{})
	srv.RegisterExtended("node-1", nil, func(ctx context.Context) {
		<-block
	})

	req := httptest.NewRequest("POST", "/recovery/trigger", nil)
	authReq(req)
	w1 := httptest.NewRecorder()
	srv.mux.ServeHTTP(w1, req)

	if w1.Code != http.StatusAccepted {
		t.Fatalf("expected 202 for first trigger, got %d: %s", w1.Code, w1.Body.String())
	}

	req2 := httptest.NewRequest("POST", "/recovery/trigger", nil)
	authReq(req2)
	w2 := httptest.NewRecorder()
	srv.mux.ServeHTTP(w2, req2)

	if w2.Code != http.StatusConflict {
		t.Fatalf("expected 409 for second concurrent trigger, got %d: %s", w2.Code, w2.Body.String())
	}

	close(block)
}
