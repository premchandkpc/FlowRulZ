package compiler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestLocalCompiler(t *testing.T) {
	c := NewLocal()
	result, err := c.Compile("n:test", "rule-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Plan) == 0 {
		t.Fatal("expected non-empty plan")
	}
	if result.Complexity == 0 {
		t.Fatal("expected non-zero complexity")
	}
}

func TestRemoteCompiler(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/compile" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		var req compileRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		if req.DSL != "n:hello" {
			t.Fatalf("expected dsl n:hello, got %s", req.DSL)
		}

		local := NewLocal()
		result, err := local.Compile(req.DSL, req.RuleID)
		if err != nil {
			json.NewEncoder(w).Encode(compileResponse{Error: err.Error()})
			return
		}

		json.NewEncoder(w).Encode(compileResponse{
			Plan:       result.Plan,
			Complexity: result.Complexity,
		})
	}))
	defer srv.Close()

	c := NewRemote(srv.URL)
	result, err := c.Compile("n:hello", "test-remote")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Plan) == 0 {
		t.Fatal("expected non-empty plan")
	}
	if result.Complexity == 0 {
		t.Fatal("expected non-zero complexity")
	}
}

func TestRemoteCompilerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(compileResponse{
			Error: "bad dsl",
		})
	}))
	defer srv.Close()

	c := NewRemote(srv.URL)
	_, err := c.Compile("invalid dsl", "test-err")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestCompileHandler(t *testing.T) {
	h := NewCompileHandler()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/compile" {
			h.HandleCompile(w, r)
		}
	}))
	defer srv.Close()

	c := NewRemote(srv.URL)
	result, err := c.Compile("n:echo", "handler-test")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Plan) == 0 {
		t.Fatal("expected non-empty plan")
	}
}

func TestRemoteCompilerValidate(t *testing.T) {
	h := NewCompileHandler()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/validate" {
			h.HandleValidate(w, r)
		}
	}))
	defer srv.Close()

	c := NewRemote(srv.URL)
	vr, err := c.Validate("n:echo")
	if err != nil {
		t.Fatal(err)
	}
	if !vr.Valid {
		t.Fatal("expected valid")
	}
	if vr.Complexity == 0 {
		t.Fatal("expected non-zero complexity")
	}
	if vr.PlanBytes == 0 {
		t.Fatal("expected non-zero plan bytes")
	}
}

func TestRemoteCompilerValidateInvalid(t *testing.T) {
	h := NewCompileHandler()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/validate" {
			h.HandleValidate(w, r)
		}
	}))
	defer srv.Close()

	c := NewRemote(srv.URL)
	vr, err := c.Validate("")
	if err != nil {
		t.Fatal(err)
	}
	if vr.Valid {
		t.Fatal("expected invalid for empty dsl")
	}
}
