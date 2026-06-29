package compiler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/premchandkpc/FlowRulZ/go/bridge"
)

type Result struct {
	Plan       []byte
	Complexity uint32
}

type Compiler interface {
	Compile(dsl, ruleID string) (*Result, error)
}

type LocalCompiler struct{}

func NewLocal() *LocalCompiler {
	return &LocalCompiler{}
}

func (c *LocalCompiler) Compile(dsl, ruleID string) (*Result, error) {
	plan, err := bridge.Compile(dsl, ruleID)
	if err != nil {
		return nil, err
	}
	complexity := bridge.PlanComplexity(plan)
	return &Result{Plan: plan, Complexity: complexity}, nil
}

type compileRequest struct {
	DSL    string `json:"dsl"`
	RuleID string `json:"rule_id"`
}

type compileResponse struct {
	Plan       []byte `json:"plan,omitempty"`
	Complexity uint32 `json:"complexity,omitempty"`
	Error      string `json:"error,omitempty"`
}

type RemoteCompiler struct {
	addr    string
	client  *http.Client
}

func NewRemote(addr string) *RemoteCompiler {
	return &RemoteCompiler{
		addr: addr,
		client: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:    10,
				IdleConnTimeout: 30 * time.Second,
			},
		},
	}
}

func (c *RemoteCompiler) Compile(dsl, ruleID string) (*Result, error) {
	reqBody := compileRequest{DSL: dsl, RuleID: ruleID}
	data, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("compile request marshal: %w", err)
	}

	resp, err := c.client.Post(c.addr+"/compile", "application/json", bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("compile call: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("compile read: %w", err)
	}

	var cr compileResponse
	if err := json.Unmarshal(body, &cr); err != nil {
		return nil, fmt.Errorf("compile response unmarshal: %w", err)
	}

	if cr.Error != "" {
		return nil, fmt.Errorf("compile error: %s", cr.Error)
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("compile: status %d", resp.StatusCode)
	}

	return &Result{Plan: cr.Plan, Complexity: cr.Complexity}, nil
}

type ValidateResult struct {
	Valid        bool
	Complexity   uint32
	PlanBytes    int
	Error        string
}

func (c *RemoteCompiler) Validate(dsl string) (*ValidateResult, error) {
	reqBody := compileRequest{DSL: dsl}
	data, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("validate request marshal: %w", err)
	}

	resp, err := c.client.Post(c.addr+"/validate", "application/json", bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("validate call: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("validate read: %w", err)
	}

	var vr ValidateResult
	if err := json.Unmarshal(body, &vr); err != nil {
		return nil, fmt.Errorf("validate response unmarshal: %w", err)
	}

	return &vr, nil
}

type CompileHandler struct {
	LocalCompiler
}

func NewCompileHandler() *CompileHandler {
	return &CompileHandler{}
}

func (h *CompileHandler) HandleCompile(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req compileRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("bad request: %v", err), http.StatusBadRequest)
		return
	}

	result, err := h.Compile(req.DSL, req.RuleID)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(compileResponse{Error: err.Error()})
		return
	}

	json.NewEncoder(w).Encode(compileResponse{
		Plan:       result.Plan,
		Complexity: result.Complexity,
	})
}

func (h *CompileHandler) HandleValidate(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		DSL string `json:"dsl"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("bad request: %v", err), http.StatusBadRequest)
		return
	}

	plan, err := bridge.Compile(req.DSL, "validate")
	if err != nil {
		json.NewEncoder(w).Encode(ValidateResult{Valid: false, Error: err.Error()})
		return
	}
	score := bridge.PlanComplexity(plan)
	json.NewEncoder(w).Encode(ValidateResult{
		Valid:      true,
		Complexity: score,
		PlanBytes:  len(plan),
	})
}
