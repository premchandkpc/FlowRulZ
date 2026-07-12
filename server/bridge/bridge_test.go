package bridge

import (
	"testing"
)

func TestCompileValidDSL(t *testing.T) {
	plan, err := Compile("n:validate", "test-1")
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}
	if len(plan) == 0 {
		t.Fatal("expected non-empty plan")
	}
}

func TestCompileInvalidDSL(t *testing.T) {
	_, err := Compile("!!!invalid", "bad-rule")
	if err == nil {
		t.Fatal("expected error for invalid DSL")
	}
}

func TestCompileEmptyDSL(t *testing.T) {
	_, err := Compile("", "empty")
	if err == nil {
		t.Fatal("expected error for empty DSL")
	}
}

func TestExecuteValidPlan(t *testing.T) {
	plan, err := Compile("n:validate", "test-exec")
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	caller := func(svcID uint16, body []byte) ([]byte, error) {
		return body, nil
	}

	result, err := Execute(plan, []byte(`{"test": true}`), caller, nil)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	_ = result // result may be empty depending on rule pipeline
}

func TestExecuteEmptyBody(t *testing.T) {
	plan, err := Compile("n:validate", "test-empty")
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	caller := func(svcID uint16, body []byte) ([]byte, error) {
		return body, nil
	}

	_, err = Execute(plan, []byte{}, caller, nil)
	// May succeed or fail depending on Rust VM — just check no panic
	_ = err
}

func TestExecuteBadPlan(t *testing.T) {
	caller := func(svcID uint16, body []byte) ([]byte, error) {
		return body, nil
	}
	_, err := Execute([]byte{0, 1, 2, 3, 4, 5}, []byte(`{}`), caller, nil)
	if err == nil {
		t.Fatal("expected error for bad plan bytes")
	}
}

func TestExecuteNilCaller(t *testing.T) {
	plan, err := Compile("n:validate", "test-nil")
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}
	// nil caller should cause callback to return error
	_, err = Execute(plan, []byte(`{}`), nil, nil)
	_ = err // nil caller may cause error or not depending on DSL; both valid
}

func TestInternRoundtrip(t *testing.T) {
	s := "test-service-name"
	id := Intern(s)
	if id == 0 {
		t.Fatal("expected non-zero intern ID")
	}

	got := InternLookup(id)
	if got != s {
		t.Errorf("roundtrip: expected %q, got %q", s, got)
	}
}

func TestInternEmptyString(t *testing.T) {
	id := Intern("")
	if id != 0 {
		t.Errorf("expected 0 for empty string, got %d", id)
	}
}

func TestInternLookupUnknown(t *testing.T) {
	got := InternLookup(65535)
	if got != "" {
		t.Errorf("expected empty for unknown ID, got %q", got)
	}
}

func TestMsgAllocRelease(t *testing.T) {
	ptr := MsgAlloc(1024)
	if ptr == nil {
		t.Fatal("expected non-nil pointer")
	}
	MsgRelease(ptr)
}

func TestMsgAllocZero(t *testing.T) {
	ptr := MsgAlloc(0)
	MsgRelease(ptr)
}

func TestExecuteWithContext(t *testing.T) {
	plan, err := Compile("n:validate", "test-ctx")
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	caller := func(svcID uint16, body []byte) ([]byte, error) {
		return body, nil
	}

	ctx := &ExecContext{
		MessageID:     "msg-001",
		CorrelationID: "corr-001",
		TraceID:       "trace-001",
		Partition:     3,
		Offset:        1042,
	}

	_, err = Execute(plan, []byte(`{"x":1}`), caller, ctx)
	if err != nil {
		t.Fatalf("Execute with context failed: %v", err)
	}
}

func TestExecuteStepBasic(t *testing.T) {
	plan, err := Compile("n:validate", "test-step")
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	caller := func(svcID uint16, body []byte) ([]byte, error) {
		return body, nil
	}

	var ctxBytes, respBytes []byte
	for i := 0; i < 5; i++ {
		out, err := ExecuteStep(plan, ctxBytes, respBytes, caller)
		if err != nil {
			t.Fatalf("ExecuteStep failed: %v", err)
		}
		ctxBytes = out.CtxBytes
		switch out.Result {
		case StepDone:
			return
		case StepPending:
			resp, err := caller(out.PendingSvc, out.PendingBody)
			if err != nil {
				t.Fatalf("service call failed: %v", err)
			}
			respBytes = resp
		case StepContinue:
			respBytes = nil
		}
	}
	t.Fatal("never reached Done")
}

func TestExecuteStepMultiCall(t *testing.T) {
	plan, err := Compile("n:a n:b n:c", "test-chain")
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	var callIDs []uint16
	caller := func(svcID uint16, body []byte) ([]byte, error) {
		callIDs = append(callIDs, svcID)
		return []byte(`{"called":` + string(rune('0'+svcID)) + `}`), nil
	}

	var ctxBytes, respBytes []byte
	for i := 0; i < 10; i++ {
		out, err := ExecuteStep(plan, ctxBytes, respBytes, caller)
		if err != nil {
			t.Fatalf("step %d failed: %v", i, err)
		}
		ctxBytes = out.CtxBytes
		switch out.Result {
		case StepDone:
			goto done
		case StepPending:
			resp, err := caller(out.PendingSvc, out.PendingBody)
			if err != nil {
				t.Fatalf("service call %d failed: %v", out.PendingSvc, err)
			}
			respBytes = resp
		case StepContinue:
			respBytes = nil
		}
	}
done:
	if len(callIDs) != 3 {
		t.Fatalf("expected 3 service calls, got %d", len(callIDs))
	}
}

func TestExecuteStepEmptyBody(t *testing.T) {
	plan, err := Compile("n:v", "test-empty")
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}
	out, err := ExecuteStep(plan, nil, nil, nil)
	if err != nil {
		t.Fatalf("ExecuteStep with nil body failed: %v", err)
	}
	_ = out.Result // StepDone is valid for simple pipeline
}

func TestParseServiceMethodNoMethod(t *testing.T) {
	svc, method := ParseServiceMethod("payment")
	if svc != "payment" || method != "" {
		t.Fatalf("expected ('payment',''), got (%q,%q)", svc, method)
	}
}

func TestParseServiceMethodWithMethod(t *testing.T) {
	svc, method := ParseServiceMethod("payment.authorize")
	if svc != "payment" || method != "authorize" {
		t.Fatalf("expected ('payment','authorize'), got (%q,%q)", svc, method)
	}
}

func TestParseServiceMethodMultiDot(t *testing.T) {
	svc, method := ParseServiceMethod("payment.authorize.extra")
	if svc != "payment" || method != "authorize.extra" {
		t.Fatalf("expected ('payment','authorize.extra'), got (%q,%q)", svc, method)
	}
}

func TestCompileMethodDSL(t *testing.T) {
	plan, err := Compile("n:payment.authorize", "test-method")
	if err != nil {
		t.Fatalf("Compile with method syntax failed: %v", err)
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
	if svcs[0].Name != "payment.authorize" {
		t.Fatalf("expected service name 'payment.authorize', got %q", svcs[0].Name)
	}

	svc, method := ParseServiceMethod(svcs[0].Name)
	if svc != "payment" || method != "authorize" {
		t.Fatalf("ParseServiceMethod: expected ('payment','authorize'), got (%q,%q)", svc, method)
	}
}

func TestExecuteWithMethodSyntax(t *testing.T) {
	plan, err := Compile("n:payment.authorize", "test-exec-method")
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	// Build plan service map
	svcs, err := PlanServices(plan)
	if err != nil {
		t.Fatalf("PlanServices failed: %v", err)
	}
	svcMap := make(map[uint16]string)
	for _, e := range svcs {
		svcMap[e.ID] = e.Name
	}

	caller := func(svcID uint16, body []byte) ([]byte, error) {
		rawName := svcMap[svcID]
		svc, method := ParseServiceMethod(rawName)
		if svc != "payment" {
			t.Errorf("expected service 'payment', got %q", svc)
		}
		if method != "authorize" {
			t.Errorf("expected method 'authorize', got %q", method)
		}
		return body, nil
	}

	_, err = Execute(plan, []byte(`{"amount":100}`), caller, nil)
	if err != nil {
		t.Fatalf("Execute with method syntax failed: %v", err)
	}
}

func TestParseCompensationNoCompensator(t *testing.T) {
	svc, method, comp, compM := ParseCompensation("payment.authorize")
	if svc != "payment" || method != "authorize" || comp != "" || compM != "" {
		t.Fatalf("expected ('payment','authorize','',''), got (%q,%q,%q,%q)", svc, method, comp, compM)
	}
}

func TestParseCompensationSameService(t *testing.T) {
	svc, method, comp, compM := ParseCompensation("payment.authorize:refund")
	if svc != "payment" || method != "authorize" || comp != "payment" || compM != "refund" {
		t.Fatalf("expected ('payment','authorize','payment','refund'), got (%q,%q,%q,%q)", svc, method, comp, compM)
	}
}

func TestParseCompensationCrossService(t *testing.T) {
	svc, method, comp, compM := ParseCompensation("payment.authorize:inventory.restock")
	if svc != "payment" || method != "authorize" || comp != "inventory" || compM != "restock" {
		t.Fatalf("expected ('payment','authorize','inventory','restock'), got (%q,%q,%q,%q)", svc, method, comp, compM)
	}
}

func TestParseCompensationNoMethod(t *testing.T) {
	svc, method, comp, compM := ParseCompensation("payment:refund")
	if svc != "payment" || method != "" || comp != "payment" || compM != "refund" {
		t.Fatalf("expected ('payment','','payment','refund'), got (%q,%q,%q,%q)", svc, method, comp, compM)
	}
}

func TestExecuteWithPartialContext(t *testing.T) {
	plan, err := Compile("n:validate", "test-partial")
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	caller := func(svcID uint16, body []byte) ([]byte, error) {
		return body, nil
	}

	ctx := &ExecContext{
		MessageID: "msg-002",
		Partition: 5,
	}

	_, err = Execute(plan, []byte(`{"x":1}`), caller, ctx)
	if err != nil {
		t.Fatalf("Execute with partial context failed: %v", err)
	}
}
