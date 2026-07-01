package execstate

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFileStoreCreateLoad(t *testing.T) {
	dir := t.TempDir()
	fs, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	s := &State{
		ID:        "exec-001",
		RuleID:    "test-rule",
		Version:   1,
		PlanBytes: []byte("plan-data"),
		CtxBytes:  []byte("ctx-data"),
		Status:    StatusRunning,
		CreatedAt: time.Now(),
	}

	if err := fs.Create(context.Background(), s); err != nil {
		t.Fatal(err)
	}

	loaded, err := fs.Load(context.Background(), "exec-001")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.ID != "exec-001" {
		t.Fatalf("expected exec-001, got %s", loaded.ID)
	}
	if loaded.Status != StatusRunning {
		t.Fatalf("expected StatusRunning, got %v", loaded.Status)
	}
	if string(loaded.PlanBytes) != "plan-data" {
		t.Fatalf("expected plan-data, got %s", loaded.PlanBytes)
	}
}

func TestFileStoreList(t *testing.T) {
	dir := t.TempDir()
	fs, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	for i := 0; i < 3; i++ {
		id := "exec-" + string(rune('0'+i))
		fs.Create(context.Background(), &State{
			ID:     id,
			Status: StatusCreated,
		})
	}

	all, err := fs.ListByStatus(context.Background(), StatusCreated, StatusRunning, StatusCompleted, StatusFailed)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Fatalf("expected 3, got %d", len(all))
	}

	running, err := fs.ListByStatus(context.Background(), StatusRunning)
	if err != nil {
		t.Fatal(err)
	}
	if len(running) != 0 {
		t.Fatalf("expected 0 running, got %d", len(running))
	}
}

func TestFileStoreSaveDelete(t *testing.T) {
	dir := t.TempDir()
	fs, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	s := &State{ID: "exec-002", Status: StatusRunning}
	if err := fs.Create(context.Background(), s); err != nil {
		t.Fatal(err)
	}

	s.Status = StatusCompleted
	if err := fs.Save(context.Background(), s); err != nil {
		t.Fatal(err)
	}

	loaded, _ := fs.Load(context.Background(), "exec-002")
	if loaded.Status != StatusCompleted {
		t.Fatalf("expected StatusCompleted, got %v", loaded.Status)
	}

	if err := fs.Delete(context.Background(), "exec-002"); err != nil {
		t.Fatal(err)
	}

	if _, err := fs.Load(context.Background(), "exec-002"); err == nil {
		t.Fatal("expected error after delete")
	}
}

func TestFileStoreDuplicate(t *testing.T) {
	dir := t.TempDir()
	fs, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	s := &State{ID: "exec-003", Status: StatusRunning}
	if err := fs.Create(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if err := fs.Create(context.Background(), s); err == nil {
		t.Fatal("expected error on duplicate create")
	}
}

func TestFileStoreAtomicity(t *testing.T) {
	dir := t.TempDir()
	fs, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	s := &State{
		ID:        "exec-004",
		PlanBytes: make([]byte, 1024),
		CtxBytes:  make([]byte, 512),
		Status:    StatusWaitingForService,
		PendingSvc: 42,
		PendingBody: []byte(`{"hello":"world"}`),
	}

	if err := fs.Save(context.Background(), s); err != nil {
		t.Fatal(err)
	}

	// Verify no .tmp files remain
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".tmp" {
			t.Fatalf("leftover tmp file: %s", e.Name())
		}
	}
}
