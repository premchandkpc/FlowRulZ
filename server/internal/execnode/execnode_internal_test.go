package execnode

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// ─── TermStore tests ────────────────────────────────────────────────────────

func TestTermStoreSaveAndLoad(t *testing.T) {
	dir := t.TempDir()
	ts := NewTermStore(dir)

	if err := ts.Save(42, "leader-1"); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	term, leader := ts.Load()
	if term != 42 {
		t.Fatalf("expected term 42, got %d", term)
	}
	if leader != "leader-1" {
		t.Fatalf("expected leader leader-1, got %s", leader)
	}
}

func TestTermStoreLoadEmpty(t *testing.T) {
	dir := t.TempDir()
	ts := NewTermStore(dir)

	// Remove the file if it somehow exists
	os.Remove(ts.path)

	term, leader := ts.Load()
	if term != 0 {
		t.Fatalf("expected term 0, got %d", term)
	}
	if leader != "" {
		t.Fatalf("expected empty leader, got %s", leader)
	}
}

func TestTermStoreOverwrite(t *testing.T) {
	dir := t.TempDir()
	ts := NewTermStore(dir)

	if err := ts.Save(1, "old-leader"); err != nil {
		t.Fatalf("first Save failed: %v", err)
	}
	if err := ts.Save(99, "new-leader"); err != nil {
		t.Fatalf("second Save failed: %v", err)
	}

	term, leader := ts.Load()
	if term != 99 {
		t.Fatalf("expected term 99, got %d", term)
	}
	if leader != "new-leader" {
		t.Fatalf("expected leader new-leader, got %s", leader)
	}
}

func TestTermStoreLoadFromFile(t *testing.T) {
	dir := t.TempDir()
	// Write the file directly to test Load path
	os.WriteFile(filepath.Join(dir, "cluster-term.json"), []byte(`{"term":7,"leader_id":"direct"}`), 0644)

	ts := NewTermStore(dir)
	term, leader := ts.Load()
	if term != 7 {
		t.Fatalf("expected term 7, got %d", term)
	}
	if leader != "direct" {
		t.Fatalf("expected leader direct, got %s", leader)
	}
}

func TestTermStoreCorruptFile(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "cluster-term.json"), []byte(`not json`), 0644)

	ts := NewTermStore(dir)
	term, leader := ts.Load()
	if term != 0 {
		t.Fatalf("expected term 0 for corrupt file, got %d", term)
	}
	if leader != "" {
		t.Fatalf("expected empty leader for corrupt file, got %s", leader)
	}
}

// ─── ExecRegistry tests ─────────────────────────────────────────────────────

func TestExecRegistryRegister(t *testing.T) {
	er := NewExecRegistry()
	er.Register("exec-1", func() {}, "test")
	if er.Len() != 1 {
		t.Fatalf("expected Len 1, got %d", er.Len())
	}
}

func TestExecRegistryUnregister(t *testing.T) {
	er := NewExecRegistry()
	er.Register("exec-1", func() {}, "test")
	er.Unregister("exec-1")
	if er.Len() != 0 {
		t.Fatalf("expected Len 0, got %d", er.Len())
	}
}

func TestExecRegistryCancel(t *testing.T) {
	er := NewExecRegistry()
	cancelCalled := make(chan struct{}, 1)
	er.Register("exec-1", func() { cancelCalled <- struct{}{} }, "test")

	ok := er.Cancel("exec-1")
	if !ok {
		t.Fatal("Cancel returned false for registered exec")
	}

	select {
	case <-cancelCalled:
	case <-time.After(time.Second):
		t.Fatal("cancel function was not called")
	}
}

func TestExecRegistryCancelNotFound(t *testing.T) {
	er := NewExecRegistry()
	ok := er.Cancel("nonexistent")
	if ok {
		t.Fatal("Cancel should return false for unknown ID")
	}
}

func TestExecRegistryCancelAll(t *testing.T) {
	er := NewExecRegistry()
	cancelCount := 0
	cancelCh := make(chan struct{}, 3)
	cf := func() { cancelCount++; cancelCh <- struct{}{} }

	er.Register("a", cf, "t1")
	er.Register("b", cf, "t2")
	er.Register("c", cf, "t3")

	er.CancelAll()

	// Wait for all 3 cancellations
	for i := 0; i < 3; i++ {
		select {
		case <-cancelCh:
		case <-time.After(time.Second):
			t.Fatalf("only %d of 3 cancel functions called", i)
		}
	}
}

func TestExecRegistryList(t *testing.T) {
	er := NewExecRegistry()
	er.Register("a", func() {}, "t1")
	er.Register("b", func() {}, "t2")

	list := er.List()
	if len(list) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(list))
	}
	if _, ok := list["a"]; !ok {
		t.Fatal("expected 'a' in list")
	}
	if _, ok := list["b"]; !ok {
		t.Fatal("expected 'b' in list")
	}
}

func TestExecRegistryCancelAllEmpty(t *testing.T) {
	er := NewExecRegistry()
	// Should not panic
	er.CancelAll()
}

// ─── ExecutionNode tests ────────────────────────────────────────────────────

func TestIsLeaderWithoutRaft(t *testing.T) {
	cfg := &Config{NodeID: "test-node", HTTPAddr: ":0"}
	en := New(cfg)

	// Without RaftCluster, node assumes single-leader mode
	if !en.IsLeader() {
		t.Fatal("expected IsLeader() == true in single-node mode")
	}
}

func TestCurrentTermFallsBackToPlanDist(t *testing.T) {
	cfg := &Config{NodeID: "test-node", HTTPAddr: ":0"}
	en := New(cfg)

	if en.CurrentTerm() != 0 {
		t.Fatalf("expected CurrentTerm() == 0 initially, got %d", en.CurrentTerm())
	}

	en.PlanDist.SetTerm(5)
	if en.CurrentTerm() != 5 {
		t.Fatalf("expected CurrentTerm() == 5, got %d", en.CurrentTerm())
	}
}

func TestConfigureLifecycleHooks(t *testing.T) {
	cfg := &Config{NodeID: "test-node", HTTPAddr: ":0"}
	en := New(cfg)

	if en.Engine.AfterDeploy == nil {
		t.Fatal("expected AfterDeploy hook to be configured")
	}
	if en.Engine.AfterPromote == nil {
		t.Fatal("expected AfterPromote hook to be configured")
	}

	en.Engine.AfterDeploy("rule-1", "dsl", []byte("plan"), 1)
	if en.PlanDist.CurrentTerm() != 1 {
		t.Fatalf("expected deploy hook to advance term to 1, got %d", en.PlanDist.CurrentTerm())
	}
}

func TestDefaultNodeID(t *testing.T) {
	cfg := &Config{HTTPAddr: ":0"}
	en := New(cfg)
	if en.nodeID != "node-1" {
		t.Fatalf("expected default nodeID node-1, got %s", en.nodeID)
	}
}
