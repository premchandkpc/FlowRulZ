package bridge

import (
	"testing"
)

var testDSL = "n:validate n:inventory n:fraud n:payment.authorize n:email"
var cachedPlan []byte

func init() {
	var err error
	cachedPlan, err = Compile(testDSL, "bench-plan")
	if err != nil {
		panic("compile failed: " + err.Error())
	}
}

func BenchmarkCompile(b *testing.B) {
	for b.Loop() {
		_, err := Compile(testDSL, "bench-plan")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCompileCached(b *testing.B) {
	for b.Loop() {
		_, err := Compile(testDSL, "bench-plan-cached")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInitContext(b *testing.B) {
	body := []byte(`{"order_id":"123","amount":99.99}`)
	for b.Loop() {
		_, err := InitContext(body)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExecuteStep(b *testing.B) {
	ctxBytes, err := InitContext([]byte(`{"order_id":"123"}`))
	if err != nil {
		b.Fatal(err)
	}
	for b.Loop() {
		_, err := ExecuteStep(cachedPlan, ctxBytes, nil, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExecuteStepWithResp(b *testing.B) {
	ctxBytes, err := InitContext([]byte(`{"order_id":"123"}`))
	if err != nil {
		b.Fatal(err)
	}
	resp := []byte(`{"status":"ok"}`)
	for b.Loop() {
		_, err := ExecuteStep(cachedPlan, ctxBytes, resp, nil)
		if err != nil {
			b.Fatal(err)
		}
		ctxBytes, _ = InitContext([]byte(`{"order_id":"123"}`))
	}
}

func BenchmarkPlanServices(b *testing.B) {
	for b.Loop() {
		_, err := PlanServices(cachedPlan)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInternLookup(b *testing.B) {
	// First intern some strings
	ids := make([]uint16, 5)
	names := []string{"validate", "inventory", "fraud", "payment", "email"}
	for i, name := range names {
		ids[i] = Intern(name)
	}

	b.ResetTimer()
	for b.Loop() {
		for _, id := range ids {
			_ = InternLookup(id)
		}
	}
}
