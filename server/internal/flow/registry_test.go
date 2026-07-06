package flow

import (
	"context"
	"testing"

	"github.com/premchandkpc/FlowRulZ/server/internal/cache"
)

func TestFlowRegistryIntegration(t *testing.T) {
	// Create a registry with in-memory cache
	reg := NewRegistry(cache.NewMemoryCache())

	// Register a flow
	flowSource := `version 1

flow TestRegistry

description Tests registry integration

services
    auth
        type grpc
        address auth:50051

    users
        type grpc
        address users:50052

variables
    userId string
    orderId string

workflow

Start

-> auth.Login -> users.GetProfile -> End
`

	parser := NewParser()
	ast, err := parser.ParseString(flowSource)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	err = reg.Register(context.Background(), ast)
	if err != nil {
		t.Fatalf("register error: %v", err)
	}

	// Get the flow
	state, err := reg.Get(context.Background(), "TestRegistry")
	if err != nil {
		t.Fatalf("get error: %v", err)
	}

	if state.Name != "TestRegistry" {
		t.Errorf("expected name TestRegistry, got %s", state.Name)
	}

	if state.Status != "active" {
		t.Errorf("expected status active, got %s", state.Status)
	}

	if state.IR == nil {
		t.Fatal("expected IR to be set")
	}

	// List flows
	flows := reg.List(context.Background())
	if len(flows) != 1 {
		t.Errorf("expected 1 flow, got %d", len(flows))
	}

	// Format the flow
	formatted, err := reg.Format("TestRegistry")
	if err != nil {
		t.Fatalf("format error: %v", err)
	}

	if formatted == "" {
		t.Error("expected non-empty formatted output")
	}

	// Delete the flow
	err = reg.Delete(context.Background(), "TestRegistry")
	if err != nil {
		t.Fatalf("delete error: %v", err)
	}

	// Verify deletion
	_, err = reg.Get(context.Background(), "TestRegistry")
	if err == nil {
		t.Error("expected error after delete")
	}
}

func TestFlowRegistryMultipleFlows(t *testing.T) {
	reg := NewRegistry(cache.NewMemoryCache())
	ctx := context.Background()

	// Register multiple flows
	flows := []string{
		`version 1
flow FlowA
services
    svc_a
        type grpc
        address svc_a:50051
workflow
Start -> svc_a.Call -> End`,
		`version 1
flow FlowB
services
    svc_b
        type grpc
        address svc_b:50052
workflow
Start -> svc_b.Call -> End`,
		`version 1
flow FlowC
services
    svc_c
        type grpc
        address svc_c:50053
workflow
Start -> svc_c.Call -> End`,
	}

	parser := NewParser()
	for _, src := range flows {
		ast, err := parser.ParseString(src)
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		if err := reg.Register(ctx, ast); err != nil {
			t.Fatalf("register error: %v", err)
		}
	}

	// List all
	allFlows := reg.List(ctx)
	if len(allFlows) != 3 {
		t.Errorf("expected 3 flows, got %d", len(allFlows))
	}

	// Get each
	for _, name := range []string{"FlowA", "FlowB", "FlowC"} {
		state, err := reg.Get(ctx, name)
		if err != nil {
			t.Fatalf("get %s error: %v", name, err)
		}
		if state.Name != name {
			t.Errorf("expected name %s, got %s", name, state.Name)
		}
	}
}

func TestFlowRegistrySemanticErrors(t *testing.T) {
	// Flow with unknown service reference
	flowSource := `version 1
flow BadFlow
services
    auth
        type grpc
        address auth:50051
workflow
Start -> unknown.Call -> End
`

	parser := NewParser()
	analyzer := NewAnalyzer()

	ast, err := parser.ParseString(flowSource)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	errs := analyzer.Analyze(ast)
	if len(errs) == 0 {
		t.Error("expected semantic errors")
	}
}

func TestFlowRegistryCacheHit(t *testing.T) {
	reg := NewRegistry(cache.NewMemoryCache())
	ctx := context.Background()

	flowSource := `version 1
flow CachedFlow
services
    svc
        type grpc
        address svc:50051
workflow
Start -> svc.Call -> End
`

	parser := NewParser()
	ast, err := parser.ParseString(flowSource)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	err = reg.Register(ctx, ast)
	if err != nil {
		t.Fatalf("register error: %v", err)
	}

	// Get from memory
	state1, err := reg.Get(ctx, "CachedFlow")
	if err != nil {
		t.Fatalf("get error: %v", err)
	}

	// Delete from memory but cache should still have it
	reg.mu.Lock()
	delete(reg.flows, "CachedFlow")
	reg.mu.Unlock()

	// Get should still work from cache
	state2, err := reg.Get(ctx, "CachedFlow")
	if err != nil {
		t.Fatalf("get from cache error: %v", err)
	}

	if state2.IR == nil {
		t.Error("expected IR from cache")
	}

	// Verify same IR
	if len(state1.IR.Nodes) != len(state2.IR.Nodes) {
		t.Errorf("expected same number of nodes: %d vs %d", len(state1.IR.Nodes), len(state2.IR.Nodes))
	}
}
