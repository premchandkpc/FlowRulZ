package bridge

import (
	"testing"
)

// TestDAG_SequentialChain tests basic sequential service calls.
func TestDAG_SequentialChain(t *testing.T) {
	dsl := "n:validate n:inventory n:payment"
	plan, err := Compile(dsl, "dag-sequential")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx, err := InitContext([]byte(`{"order_id":"dag-1"}`))
	if err != nil {
		t.Fatalf("init: %v", err)
	}

	var lastOutput []byte
	for i := 0; i < 20; i++ {
		out, execErr := ExecuteStep(plan, ctx, lastOutput, nil)
		if execErr != nil {
			t.Fatalf("step %d: %v", i, execErr)
		}
		ctx = out.CtxBytes

		switch out.Result {
		case StepDone:
			if len(out.Output) == 0 {
				t.Fatal("expected non-empty output")
			}
			t.Logf("completed after %d steps, output: %s", i+1, string(out.Output))
			return
		case StepPending:
			lastOutput = []byte(`{"status":"ok"}`)
		case StepContinue:
			lastOutput = nil
		default:
			t.Fatalf("unexpected result %v at step %d", out.Result, i)
		}
	}
	t.Fatal("did not complete within 20 steps")
}

// TestDAG_SingleStep tests a single service call.
func TestDAG_SingleStep(t *testing.T) {
	dsl := "n:validate"
	plan, err := Compile(dsl, "dag-single")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx, err := InitContext([]byte(`{"valid":true}`))
	if err != nil {
		t.Fatalf("init: %v", err)
	}

	var lastOutput []byte
	for i := 0; i < 10; i++ {
		out, execErr := ExecuteStep(plan, ctx, lastOutput, nil)
		if execErr != nil {
			t.Fatalf("step %d: %v", i, execErr)
		}
		ctx = out.CtxBytes

		switch out.Result {
		case StepDone:
			t.Logf("single step completed after %d iterations", i+1)
			return
		case StepPending:
			lastOutput = []byte(`{"valid":true}`)
		case StepContinue:
			lastOutput = nil
		}
	}
	t.Fatal("did not complete within 10 steps")
}

// TestDAG_ComplexChain tests a longer chain with multiple services.
func TestDAG_ComplexChain(t *testing.T) {
	dsl := "n:validate n:inventory n:fraud n:payment n:email"
	plan, err := Compile(dsl, "dag-complex")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx, err := InitContext([]byte(`{"order_id":"dag-complex-1","amount":299.99}`))
	if err != nil {
		t.Fatalf("init: %v", err)
	}

	var steps int
	var lastOutput []byte
	for i := 0; i < 20; i++ {
		out, execErr := ExecuteStep(plan, ctx, lastOutput, nil)
		if execErr != nil {
			t.Fatalf("step %d: %v", i, execErr)
		}
		ctx = out.CtxBytes
		steps++

		switch out.Result {
		case StepDone:
			t.Logf("complex chain completed after %d steps, output: %s", steps, string(out.Output))
			return
		case StepPending:
			lastOutput = []byte(`{"status":"ok"}`)
		case StepContinue:
			lastOutput = nil
		}
	}
	t.Fatal("did not complete within 20 steps")
}

// TestDAG_EmptyBody tests execution with minimal body.
func TestDAG_EmptyBody(t *testing.T) {
	dsl := "n:validate"
	plan, err := Compile(dsl, "dag-empty")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx, err := InitContext([]byte(`{}`))
	if err != nil {
		t.Fatalf("init: %v", err)
	}

	var lastOutput []byte
	for i := 0; i < 10; i++ {
		out, execErr := ExecuteStep(plan, ctx, lastOutput, nil)
		if execErr != nil {
			t.Fatalf("step %d: %v", i, execErr)
		}
		ctx = out.CtxBytes

		switch out.Result {
		case StepDone:
			t.Logf("empty body flow completed after %d steps", i+1)
			return
		case StepPending:
			lastOutput = []byte(`{"valid":true}`)
		case StepContinue:
			lastOutput = nil
		}
	}
	t.Fatal("did not complete within 10 steps")
}

// TestDAG_ServicePlanServices verifies service name extraction from compiled plans.
func TestDAG_ServicePlanServices(t *testing.T) {
	tests := []struct {
		name     string
		dsl      string
		expected int
	}{
		{"single", "n:validate", 1},
		{"chain", "n:validate n:inventory n:payment", 3},
		{"complex", "n:validate n:inventory n:fraud n:payment n:email", 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan, err := Compile(tt.dsl, "svc-"+tt.name)
			if err != nil {
				t.Fatalf("compile: %v", err)
			}

			svcs, err := PlanServices(plan)
			if err != nil {
				t.Fatalf("plan services: %v", err)
			}

			if len(svcs) != tt.expected {
				t.Fatalf("expected %d services, got %d", tt.expected, len(svcs))
			}

			for _, svc := range svcs {
				t.Logf("  svc id=%d name=%q", svc.ID, svc.Name)
			}
		})
	}
}

// TestDAG_EmptyPlan tests that empty plan returns error.
func TestDAG_EmptyPlan(t *testing.T) {
	_, err := Compile("", "dag-empty-plan")
	if err == nil {
		t.Fatal("expected error for empty DSL")
	}
}
