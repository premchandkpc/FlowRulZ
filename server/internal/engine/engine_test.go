package engine

import (
	"sync"
	"testing"
	"time"

	"github.com/premchandkpc/FlowRulZ/server/bridge"
)

func TestNewEngine(t *testing.T) {
	e := New("")
	if e == nil {
		t.Fatal("expected non-nil engine")
	}
}

func TestDeployCompile(t *testing.T) {
	e := New("")
	err := e.Deploy("test-1", "n:validate")
	if err != nil {
		t.Fatalf("Deploy failed: %v", err)
	}
	rules := e.Rules()
	if len(rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(rules))
	}
	if len(rules[0].Versions) != 1 {
		t.Fatalf("expected 1 version, got %d", len(rules[0].Versions))
	}
	if rules[0].ActivePlan() == nil {
		t.Fatal("expected non-nil active plan")
	}
}

func TestDeployInvalidDSL(t *testing.T) {
	e := New("")
	err := e.Deploy("bad-rule", "invalid!!!dsl")
	if err == nil {
		t.Fatal("expected error for invalid DSL")
	}
}

func TestRemoveRule(t *testing.T) {
	e := New("")
	e.Deploy("test-1", "n:validate")
	e.Deploy("test-2", "n:validate")

	e.Remove("test-1")
	rules := e.Rules()
	if len(rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(rules))
	}
	if rules[0].ID != "test-2" {
		t.Errorf("expected test-2, got %s", rules[0].ID)
	}
}

func TestVersionPromotion(t *testing.T) {
	e := New("")
	e.Deploy("test-1", "n:validate")
	rules := e.Rules()
	v1 := rules[0].ActivePlan().Version

	e.Deploy("test-1", "n:validate")
	rules = e.Rules()
	v2 := rules[0].ActivePlan().Version
	if v2 <= v1 {
		t.Fatalf("expected version to increase, got v1=%d v2=%d", v1, v2)
	}

	err := e.Promote("test-1", v1)
	if err != nil {
		t.Fatalf("Promote failed: %v", err)
	}
	rules = e.Rules()
	if rules[0].ActivePlan().Version != v1 {
		t.Fatalf("expected active version %d, got %d", v1, rules[0].ActivePlan().Version)
	}
}

func TestDrainRemovesVersion(t *testing.T) {
	e := New("")
	e.Deploy("test-1", "n:validate")
	rules := e.Rules()
	v1 := rules[0].ActivePlan().Version

	e.Deploy("test-1", "n:validate")
	rules = e.Rules()
	if len(rules[0].Versions) != 2 {
		t.Fatalf("expected 2 versions, got %d", len(rules[0].Versions))
	}

	err := e.Drain("test-1", v1)
	if err != nil {
		t.Fatalf("Drain failed: %v", err)
	}
	rules = e.Rules()
	if len(rules[0].Versions) != 1 {
		t.Fatalf("expected 1 version after drain, got %d", len(rules[0].Versions))
	}
}

func TestExecuteAll(t *testing.T) {
	e := New("")
	e.Deploy("test-1", "n:validate")

	results, err := e.ExecuteAll([]byte(`{}`), nil, nil)
	if err != nil {
		t.Fatalf("ExecuteAll failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
}

func TestExecuteAllPinsVersion(t *testing.T) {
	e := New("")
	e.Deploy("test-1", "n:validate")

	ch := make(chan struct{})
	go func() {
		e.ExecuteAll([]byte(`{}`), nil, nil)
		close(ch)
	}()

	rules := e.Rules()
	vp := rules[0].ActivePlan()
	vp.ActiveExec.Wait()
	<-ch
}

func TestAddVersion(t *testing.T) {
	e := New("")
	plan, err := bridge.Compile("n:validate", "dist-test")
	if err != nil {
		t.Fatalf("bridge.Compile failed: %v", err)
	}
	err = e.AddVersion("dist-rule", "n:validate", plan, 42)
	if err != nil {
		t.Fatalf("AddVersion failed: %v", err)
	}
	rules := e.Rules()
	if len(rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(rules))
	}
	if rules[0].ActivePlan() != nil {
		t.Fatal("AddVersion should not auto-activate")
	}
}

func TestAddVersionReplacesExisting(t *testing.T) {
	e := New("")
	plan1, _ := bridge.Compile("n:validate", "dist-test")
	plan2, _ := bridge.Compile("n:other", "dist-test")

	err := e.AddVersion("dist-rule", "n:validate", plan1, 42)
	if err != nil {
		t.Fatalf("first AddVersion failed: %v", err)
	}
	err = e.AddVersion("dist-rule", "n:other", plan2, 42)
	if err != nil {
		t.Fatalf("second AddVersion failed: %v", err)
	}

	rules := e.Rules()
	if len(rules[0].Versions) != 1 {
		t.Fatalf("expected 1 version after replace, got %d", len(rules[0].Versions))
	}
	if rules[0].Versions[0].DSL != "n:other" {
		t.Fatalf("expected replaced DSL, got %s", rules[0].Versions[0].DSL)
	}
}

func TestAddVersionThenPromote(t *testing.T) {
	e := New("")
	plan, _ := bridge.Compile("n:validate", "dist-test")

	e.AddVersion("dist-rule", "n:validate", plan, 42)
	err := e.Promote("dist-rule", 42)
	if err != nil {
		t.Fatalf("Promote after AddVersion failed: %v", err)
	}
	rules := e.Rules()
	if rules[0].ActivePlan() == nil {
		t.Fatal("expected active plan after Promote")
	}
	if rules[0].ActivePlan().Version != 42 {
		t.Fatalf("expected version 42, got %d", rules[0].ActivePlan().Version)
	}
}

func TestRemoveConcurrentWithDeploy(t *testing.T) {
	e := New("")
	e.Deploy("conc-rule", "n:validate")

	var wg sync.WaitGroup
	errs := make(chan error, 20)

	// Concurrent Remove
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.Remove("conc-rule")
		}()
	}

	// Concurrent Deploy
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := e.Deploy("conc-rule", "n:validate"); err != nil {
				errs <- err
			}
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("concurrent deploy error: %v", err)
	}
}

func TestRemoveNonexistent(t *testing.T) {
	e := New("")
	e.Remove("nonexistent") // should not panic
}

func TestRemoveWaitsForActiveExec(t *testing.T) {
	e := New("")
	e.Deploy("wait-rule", "n:validate")

	ch := make(chan struct{})
	go func() {
		e.Remove("wait-rule")
		close(ch)
	}()

	// Remove should complete quickly since no active exec
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("Remove did not complete")
	}

	rules := e.Rules()
	if len(rules) != 0 {
		t.Fatalf("expected 0 rules after remove, got %d", len(rules))
	}
}

func TestRemoveCalledTwice(t *testing.T) {
	e := New("")
	e.Deploy("double-rule", "n:validate")

	e.Remove("double-rule")
	e.Remove("double-rule") // second remove should be no-op

	rules := e.Rules()
	if len(rules) != 0 {
		t.Fatalf("expected 0 rules, got %d", len(rules))
	}
}

func TestRemoveNonexistentRule(t *testing.T) {
	e := New("")
	e.Remove("nonexistent")
	if len(e.Rules()) != 0 {
		t.Fatal("expected no rules after removing nonexistent")
	}
}

func TestDrainConcurrentWithRemove(t *testing.T) {
	e := New("")
	e.Deploy("test-1", "n:validate")
	rules := e.Rules()
	v1 := rules[0].ActivePlan().Version

	e.Deploy("test-1", "n:validate")

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		e.Drain("test-1", v1)
	}()
	go func() {
		defer wg.Done()
		e.Remove("test-1")
	}()
	wg.Wait()
}

func TestPromoteNonexistentVersion(t *testing.T) {
	e := New("")
	e.Deploy("test-1", "n:validate")
	err := e.Promote("test-1", 9999)
	if err == nil {
		t.Fatal("expected error promoting nonexistent version")
	}
}

func TestPromoteNonexistentRule(t *testing.T) {
	e := New("")
	err := e.Promote("nonexistent", 1)
	if err == nil {
		t.Fatal("expected error promoting nonexistent rule")
	}
}

func TestDrainNonexistentRule(t *testing.T) {
	e := New("")
	err := e.Drain("nonexistent", 1)
	if err == nil {
		t.Fatal("expected error draining nonexistent rule")
	}
}

func TestDrainNonexistentVersion(t *testing.T) {
	e := New("")
	e.Deploy("test-1", "n:validate")
	err := e.Drain("test-1", 9999)
	if err == nil {
		t.Fatal("expected error draining nonexistent version")
	}
}

func TestActivePlanBytesEmpty(t *testing.T) {
	e := New("")
	plans := e.ActivePlanBytes()
	if len(plans) != 0 {
		t.Fatal("expected no active plans for empty engine")
	}
}

func TestExecuteAllEmpty(t *testing.T) {
	e := New("")
	results, err := e.ExecuteAll([]byte(`{}`), nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 0 {
		t.Fatal("expected no results for empty engine")
	}
}

func TestLaneForScore(t *testing.T) {
	tests := []struct {
		score    uint32
		expected Lane
	}{
		{0, LaneFast},
		{9, LaneFast},
		{10, LaneNormal},
		{50, LaneNormal},
		{51, LaneHeavy},
		{100, LaneHeavy},
	}
	for _, tt := range tests {
		if got := LaneForScore(tt.score); got != tt.expected {
			t.Errorf("LaneForScore(%d) = %s, want %s", tt.score, got, tt.expected)
		}
	}
}

func TestRulesCopyIsolation(t *testing.T) {
	e := New("")
	e.Deploy("test-1", "n:validate")
	rules := e.Rules()
	rules[0].ID = "mutated"
	if e.Rules()[0].ID == "mutated" {
		t.Fatal("Rules() returned non-isolated copy")
	}
}

func TestSaveRulesInsideLock(t *testing.T) {
	e := New("")
	e.Deploy("save-rule", "n:validate")
	e.Deploy("save-rule", "n:validate") // second version

	// Remove should save rules atomically
	e.Remove("save-rule")
	rules := e.Rules()
	if len(rules) != 0 {
		t.Fatal("expected 0 rules after remove")
	}
}

func TestRemoveWaitsForActiveExecutions(t *testing.T) {
	e := New("")
	e.Deploy("wait-rule", "n:validate")

	rules := e.Rules()
	vp := rules[0].ActivePlan()
	vp.ActiveExec.Add(1)

	done := make(chan struct{})
	go func() {
		e.Remove("wait-rule")
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("Remove returned before ActiveExec.Wait completed")
	case <-time.After(100 * time.Millisecond):
	}

	vp.ActiveExec.Done()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Remove did not complete after ActiveExec.Done")
	}

	if len(e.Rules()) != 0 {
		t.Fatal("expected 0 rules after remove")
	}
}

func TestRemoveMultipleVersions(t *testing.T) {
	e := New("")
	e.Deploy("multi", "n:validate")
	e.Deploy("multi", "n:validate")
	e.Deploy("multi", "n:validate")

	rules := e.Rules()
	if len(rules[0].Versions) != 3 {
		t.Fatalf("expected 3 versions, got %d", len(rules[0].Versions))
	}

	e.Remove("multi")
	if len(e.Rules()) != 0 {
		t.Fatal("expected 0 rules after removing multi-version rule")
	}
}
