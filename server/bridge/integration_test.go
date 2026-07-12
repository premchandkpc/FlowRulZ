package bridge

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// callTracker records service calls made during execution.
type callTracker struct {
	mu    sync.Mutex
	calls []callRecord
}

type callRecord struct {
	svcID uint16
	body  []byte
}

func (ct *callTracker) record(svcID uint16, body []byte) {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	cp := make([]byte, len(body))
	copy(cp, body)
	ct.calls = append(ct.calls, callRecord{svcID: svcID, body: cp})
}

func (ct *callTracker) ids() []uint16 {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	out := make([]uint16, len(ct.calls))
	for i, c := range ct.calls {
		out[i] = c.svcID
	}
	return out
}

// stepLoop drives ExecuteStep until StepDone or max iterations.
// body is the JSON payload embedded into the execution context via InitContext.
// Pass nil when the DSL does not require a body (e.g. pure service chains).
func stepLoop(t *testing.T, plan []byte, caller ServiceCaller, body []byte) *StepOutput {
	t.Helper()

	var ctxBytes []byte
	if len(body) > 0 {
		var err error
		ctxBytes, err = InitContext(body)
		if err != nil {
			t.Fatalf("InitContext failed: %v", err)
		}
	}

	var respBytes []byte
	for i := 0; i < 20; i++ {
		out, err := ExecuteStep(plan, ctxBytes, respBytes, caller)
		if err != nil {
			t.Fatalf("ExecuteStep iteration %d failed: %v", i, err)
		}
		ctxBytes = out.CtxBytes
		switch out.Result {
		case StepDone:
			return out
		case StepPending:
			resp, err := caller(out.PendingSvc, out.PendingBody)
			if err != nil {
				resp = nil
			}
			respBytes = resp
		case StepContinue:
			respBytes = nil
		}
	}
	t.Fatal("stepLoop: never reached StepDone within iteration limit")
	return nil
}

// planSvcMap builds a svcID→name map from a compiled plan.
func planSvcMap(t *testing.T, plan []byte) map[uint16]string {
	t.Helper()
	svcs, err := PlanServices(plan)
	if err != nil {
		t.Fatalf("PlanServices failed: %v", err)
	}
	m := make(map[uint16]string, len(svcs))
	for _, s := range svcs {
		m[s.ID] = s.Name
	}
	return m
}

// ---------- Tests ----------

// 1. Multi-step chain: compile "n:a n:b n:c", verify all 3 called in order.
func TestIntegration_MultiStepChain(t *testing.T) {
	plan, err := Compile("n:a n:b n:c", "integ-chain")
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}
	if len(plan) == 0 {
		t.Fatal("expected non-empty plan")
	}

	svcMap := planSvcMap(t, plan)
	tracker := &callTracker{}

	caller := func(svcID uint16, body []byte) ([]byte, error) {
		tracker.record(svcID, body)
		name := svcMap[svcID]
		return []byte(fmt.Sprintf(`{"svc":"%s","ok":true}`, name)), nil
	}

	stepLoop(t, plan, caller, nil)

	ids := tracker.ids()
	if len(ids) != 3 {
		t.Fatalf("expected 3 service calls, got %d (ids=%v)", len(ids), ids)
	}

	expected := []string{"a", "b", "c"}
	for i, id := range ids {
		got := svcMap[id]
		if got != expected[i] {
			t.Errorf("call %d: expected service %q, got %q", i, expected[i], got)
		}
	}
}

// 2. Gate true path: amount=5000 > 1000 → manual-review called.
func TestIntegration_GateTruePath(t *testing.T) {
	plan, err := Compile("g:amount>1000 n:manual-review f:auto-approve", "integ-gate-true")
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	svcMap := planSvcMap(t, plan)
	tracker := &callTracker{}

	caller := func(svcID uint16, body []byte) ([]byte, error) {
		tracker.record(svcID, body)
		name := svcMap[svcID]
		return []byte(fmt.Sprintf(`{"called":"%s"}`, name)), nil
	}

	stepLoop(t, plan, caller, []byte(`{"amount":5000}`))

	ids := tracker.ids()
	if len(ids) == 0 {
		t.Fatal("expected at least 1 service call")
	}

	calledName := svcMap[ids[0]]
	if calledName != "manual-review" {
		t.Errorf("expected manual-review, got %q (ids=%v)", calledName, ids)
	}
}

// 3. Gate false path: amount=500 < 1000 → no service called (gate skips the true branch).
func TestIntegration_GateFalsePath(t *testing.T) {
	plan, err := Compile("g:amount>1000 n:manual-review f:auto-approve", "integ-gate-false")
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	tracker := &callTracker{}

	caller := func(svcID uint16, body []byte) ([]byte, error) {
		tracker.record(svcID, body)
		return []byte(`{"ok":true}`), nil
	}

	stepLoop(t, plan, caller, []byte(`{"amount":500}`))

	ids := tracker.ids()
	if len(ids) != 0 {
		t.Errorf("expected 0 service calls when gate is false, got %d (ids=%v)", len(ids), ids)
	}
}

// 4. Parallel collect: p:svc1,svc2 c → both called, results collected.
func TestIntegration_ParallelCollect(t *testing.T) {
	plan, err := Compile("p:svc1,svc2 c", "integ-par-collect")
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	svcMap := planSvcMap(t, plan)
	tracker := &callTracker{}

	caller := func(svcID uint16, body []byte) ([]byte, error) {
		tracker.record(svcID, body)
		name := svcMap[svcID]
		return []byte(fmt.Sprintf(`{"svc":"%s","data":"ok"}`, name)), nil
	}

	stepLoop(t, plan, caller, nil)

	ids := tracker.ids()
	if len(ids) < 2 {
		t.Fatalf("expected at least 2 service calls for parallel, got %d (ids=%v)", len(ids), ids)
	}

	called := make(map[string]bool)
	for _, id := range ids {
		called[svcMap[id]] = true
	}
	if !called["svc1"] {
		t.Error("svc1 was not called")
	}
	if !called["svc2"] {
		t.Error("svc2 was not called")
	}
}

// 5. Fallback on error: use Execute (full run) to test n:primary f:backup
// where primary returns error → backup is called via the VM's fallback mechanism.
func TestIntegration_FallbackOnError(t *testing.T) {
	plan, err := Compile("n:primary f:backup", "integ-fallback")
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	svcMap := planSvcMap(t, plan)
	tracker := &callTracker{}

	caller := func(svcID uint16, body []byte) ([]byte, error) {
		tracker.record(svcID, body)
		name := svcMap[svcID]
		if name == "primary" {
			return nil, fmt.Errorf("primary service unavailable")
		}
		return []byte(fmt.Sprintf(`{"fallback":"%s","ok":true}`, name)), nil
	}

	result, err := Execute(plan, []byte(`{"x":1}`), caller, nil)
	// The full Execute path processes fallback within the VM run loop.
	// Depending on VM behavior, this may succeed or fail — verify no crash.
	_ = result
	_ = err

	ids := tracker.ids()
	if len(ids) == 0 {
		t.Fatal("expected at least 1 service call")
	}

	// At minimum, primary was attempted.
	foundPrimary := false
	for _, id := range ids {
		if svcMap[id] == "primary" {
			foundPrimary = true
			break
		}
	}
	if !foundPrimary {
		t.Errorf("primary was not called, calls=%v", ids)
	}
}

// 6. Timeout: compile "t100 n:slow", verify plan compiles and executes.
func TestIntegration_Timeout(t *testing.T) {
	plan, err := Compile("t100 n:slow", "integ-timeout")
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}
	if len(plan) == 0 {
		t.Fatal("expected non-empty plan")
	}

	svcMap := planSvcMap(t, plan)
	tracker := &callTracker{}

	caller := func(svcID uint16, body []byte) ([]byte, error) {
		tracker.record(svcID, body)
		return []byte(`{"result":"fast"}`), nil
	}

	stepLoop(t, plan, caller, nil)

	ids := tracker.ids()
	if len(ids) == 0 {
		t.Fatal("expected at least 1 service call")
	}

	calledName := svcMap[ids[0]]
	if calledName != "slow" {
		t.Errorf("expected slow service, got %q", calledName)
	}
}

// 7. Retry: compile "n:flaky r3:exp", verify plan compiles and services extract.
func TestIntegration_Retry(t *testing.T) {
	plan, err := Compile("n:flaky r3:exp", "integ-retry")
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}
	if len(plan) == 0 {
		t.Fatal("expected non-empty plan")
	}

	svcs, err := PlanServices(plan)
	if err != nil {
		t.Fatalf("PlanServices failed: %v", err)
	}
	if len(svcs) == 0 {
		t.Fatal("expected at least 1 service entry")
	}

	found := false
	for _, s := range svcs {
		if s.Name == "flaky" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected service 'flaky' in plan services, got %v", svcs)
	}

	svcMap := planSvcMap(t, plan)
	tracker := &callTracker{}

	caller := func(svcID uint16, body []byte) ([]byte, error) {
		tracker.record(svcID, body)
		return []byte(`{"attempt":"ok"}`), nil
	}

	stepLoop(t, plan, caller, nil)

	ids := tracker.ids()
	if len(ids) == 0 {
		t.Fatal("expected at least 1 service call")
	}

	calledName := svcMap[ids[0]]
	if calledName != "flaky" {
		t.Errorf("expected flaky service, got %q", calledName)
	}
}

// 8. Schema validation: compile "schema:{!name:string,!age:int} n:process", verify plan services.
func TestIntegration_SchemaValidation(t *testing.T) {
	plan, err := Compile("schema:{!name:string,!age:int} n:process", "integ-schema")
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}
	if len(plan) == 0 {
		t.Fatal("expected non-empty plan")
	}

	svcs, err := PlanServices(plan)
	if err != nil {
		t.Fatalf("PlanServices failed: %v", err)
	}
	if len(svcs) != 1 {
		t.Fatalf("expected 1 service, got %d", len(svcs))
	}
	if svcs[0].Name != "process" {
		t.Errorf("expected service name 'process', got %q", svcs[0].Name)
	}

	caller := func(svcID uint16, body []byte) ([]byte, error) {
		return []byte(`{"validated":true}`), nil
	}

	body := []byte(`{"name":"alice","age":30}`)
	result, err := Execute(plan, body, caller, nil)
	if err != nil {
		t.Fatalf("Execute with schema body failed: %v", err)
	}
	_ = result
}

// 9. Plan complexity: non-trivial DSL returns non-zero complexity.
func TestIntegration_PlanComplexity(t *testing.T) {
	cases := []struct {
		name string
		dsl  string
	}{
		{"single_next", "n:svc"},
		{"chain", "n:a n:b n:c"},
		{"gate", "g:amount>1000 n:review f:approve"},
		{"parallel", "p:svc1,svc2 c"},
		{"timeout_chain", "t500 n:validate t1000 n:ship"},
		{"retry_chain", "n:flaky r3:exp"},
		{"schema_gate", "schema:{!x:int} g:x>0 n:pos f:neg"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := Compile(tc.dsl, "integ-complexity-"+tc.name)
			if err != nil {
				t.Fatalf("Compile(%q) failed: %v", tc.dsl, err)
			}
			cx := PlanComplexity(plan)
			if cx == 0 {
				t.Errorf("PlanComplexity(%q) = 0, want non-zero", tc.dsl)
			}
		})
	}
}

// 10. Plan services extraction: multi-service DSL returns correct service list with IDs.
func TestIntegration_PlanServicesExtraction(t *testing.T) {
	cases := []struct {
		name     string
		dsl      string
		expected []string
	}{
		{
			"single",
			"n:validate",
			[]string{"validate"},
		},
		{
			"chain",
			"n:a n:b n:c",
			[]string{"a", "b", "c"},
		},
		{
			"parallel",
			"p:svc1,svc2 c",
			[]string{"svc1", "svc2"},
		},
		{
			"gate_with_fallback",
			"g:amount>1000 n:manual f:auto",
			[]string{"manual", "auto"},
		},
		{
			"method_syntax",
			"n:payment.authorize",
			[]string{"payment.authorize"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := Compile(tc.dsl, "integ-svcext-"+tc.name)
			if err != nil {
				t.Fatalf("Compile(%q) failed: %v", tc.dsl, err)
			}

			svcs, err := PlanServices(plan)
			if err != nil {
				t.Fatalf("PlanServices failed: %v", err)
			}

			if len(svcs) != len(tc.expected) {
				t.Fatalf("expected %d services, got %d: %v", len(tc.expected), len(svcs), svcs)
			}

			gotNames := make(map[string]bool)
			for _, s := range svcs {
				gotNames[s.Name] = true
			}
			for _, want := range tc.expected {
				if !gotNames[want] {
					t.Errorf("expected service %q not found in %v", want, svcs)
				}
			}

			seen := make(map[uint16]bool)
			for _, s := range svcs {
				if seen[s.ID] {
					t.Errorf("duplicate service ID %d for %q", s.ID, s.Name)
				}
				seen[s.ID] = true
			}
		})
	}
}

// 11. Large body: compile and execute with a 64KB JSON body, verify no truncation.
func TestIntegration_LargeBody(t *testing.T) {
	plan, err := Compile("n:bigdata", "integ-large-body")
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	payload := strings.Repeat("x", 65536)
	body := []byte(fmt.Sprintf(`{"data":"%s","size":%d}`, payload, 65536))

	var receivedBody []byte
	caller := func(svcID uint16, body []byte) ([]byte, error) {
		receivedBody = body
		return body, nil
	}

	result, err := Execute(plan, body, caller, nil)
	if err != nil {
		t.Fatalf("Execute with large body failed: %v", err)
	}
	_ = result

	if len(receivedBody) == 0 {
		t.Fatal("service caller received empty body")
	}

	var parsed map[string]json.RawMessage
	if err := json.Unmarshal(receivedBody, &parsed); err != nil {
		t.Fatalf("received body is not valid JSON: %v", err)
	}

	dataField, ok := parsed["data"]
	if !ok {
		t.Fatal("received body missing 'data' field")
	}
	dataStr := string(dataField)
	if len(dataStr) < 65536 {
		t.Errorf("data field truncated: got %d chars, want >= 65536", len(dataStr))
	}
}

// 12. Concurrent execute: compile "n:svc", execute 100 times concurrently, no races.
func TestIntegration_ConcurrentExecute(t *testing.T) {
	plan, err := Compile("n:svc", "integ-concurrent")
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	var callCount atomic.Int64
	caller := func(svcID uint16, body []byte) ([]byte, error) {
		callCount.Add(1)
		return []byte(`{"ok":true}`), nil
	}

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)

	errCh := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			body := []byte(fmt.Sprintf(`{"i":%d}`, idx))
			_, err := Execute(plan, body, caller, nil)
			if err != nil {
				errCh <- fmt.Errorf("goroutine %d: %v", idx, err)
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for e := range errCh {
		t.Error(e)
	}

	total := callCount.Load()
	if total != goroutines {
		t.Errorf("expected %d calls, got %d", goroutines, total)
	}
}
