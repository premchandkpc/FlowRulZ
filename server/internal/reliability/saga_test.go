package reliability

import (
	"errors"
	"testing"
)

func TestSagaTrackerRegisterCompensate(t *testing.T) {
	var comps []string
	tracker := NewSagaTracker(func(svc, method string, body []byte) error {
		comps = append(comps, svc+"."+method)
		return nil
	})

	tracker.RegisterStep("exec-1", SagaStep{
		ServiceName: "payment", Method: "authorize",
		CompSvc: "payment", CompMethod: "refund",
		Body: []byte(`{"amount":100}`),
	})
	tracker.RegisterStep("exec-1", SagaStep{
		ServiceName: "inventory", Method: "reserve",
		CompSvc: "inventory", CompMethod: "release",
		Body: []byte(`{"sku":"ABC"}`),
	})

	if err := tracker.Compensate("exec-1"); err != nil {
		t.Fatal(err)
	}

	if len(comps) != 2 {
		t.Fatalf("expected 2 compensations, got %d", len(comps))
	}
	if comps[0] != "inventory.release" {
		t.Fatalf("expected inventory.release first (reverse order), got %s", comps[0])
	}
	if comps[1] != "payment.refund" {
		t.Fatalf("expected payment.refund second, got %s", comps[1])
	}
}

func TestSagaTrackerNoCompensator(t *testing.T) {
	var comps []string
	tracker := NewSagaTracker(func(svc, method string, body []byte) error {
		comps = append(comps, svc+"."+method)
		return nil
	})

	// Step without compensator is a read-only step
	tracker.RegisterStep("exec-2", SagaStep{
		ServiceName: "analytics", Method: "track",
	})

	if err := tracker.Compensate("exec-2"); err != nil {
		t.Fatal(err)
	}
	if len(comps) != 0 {
		t.Fatalf("expected 0 compensations, got %d", len(comps))
	}
}

func TestSagaTrackerCompensateError(t *testing.T) {
	tracker := NewSagaTracker(func(svc, method string, body []byte) error {
		return errors.New("compensation failed")
	})

	tracker.RegisterStep("exec-3", SagaStep{
		ServiceName: "payment", Method: "capture",
		CompSvc: "payment", CompMethod: "refund",
	})

	err := tracker.Compensate("exec-3")
	if err == nil {
		t.Fatal("expected error from compensation failure")
	}
}

func TestSagaTrackerClear(t *testing.T) {
	tracker := NewSagaTracker(nil)
	tracker.RegisterStep("exec-4", SagaStep{
		ServiceName: "payment", Method: "authorize",
		CompSvc: "payment", CompMethod: "refund",
	})
	tracker.Clear("exec-4")

	if err := tracker.Compensate("exec-4"); err != nil {
		t.Fatal("expected no error after clear")
	}
}
