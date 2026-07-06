package execstate

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestMemoryStoreCreateLoad(t *testing.T) {
	ms := NewMemoryStore()

	s := &State{
		ID:        "exec-001",
		RuleID:    "test-rule",
		Version:   1,
		PlanBytes: []byte("plan-data"),
		CtxBytes:  []byte("ctx-data"),
		Status:    StatusRunning,
		CreatedAt: time.Now(),
	}

	if err := ms.Create(context.Background(), s); err != nil {
		t.Fatal(err)
	}

	loaded, err := ms.Load(context.Background(), "exec-001")
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

func TestMemoryStoreCreateDuplicate(t *testing.T) {
	ms := NewMemoryStore()

	s := &State{ID: "exec-001", Status: StatusRunning}
	if err := ms.Create(context.Background(), s); err != nil {
		t.Fatal(err)
	}

	s2 := &State{ID: "exec-001", Status: StatusCompleted}
	if err := ms.Create(context.Background(), s2); err == nil {
		t.Fatal("expected error creating duplicate")
	}
}

func TestMemoryStoreList(t *testing.T) {
	ms := NewMemoryStore()

	for i := 0; i < 3; i++ {
		id := "exec-" + string(rune('0'+i))
		ms.Create(context.Background(), &State{
			ID:     id,
			Status: StatusCreated,
		})
	}

	all, err := ms.ListByStatus(context.Background(), StatusCreated, StatusRunning, StatusCompleted, StatusFailed)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Fatalf("expected 3, got %d", len(all))
	}

	running, err := ms.ListByStatus(context.Background(), StatusRunning)
	if err != nil {
		t.Fatal(err)
	}
	if len(running) != 0 {
		t.Fatalf("expected 0 running, got %d", len(running))
	}
}

func TestMemoryStoreSaveDelete(t *testing.T) {
	ms := NewMemoryStore()

	s := &State{ID: "exec-002", Status: StatusRunning}
	if err := ms.Create(context.Background(), s); err != nil {
		t.Fatal(err)
	}

	s.Status = StatusCompleted
	if err := ms.Save(context.Background(), s); err != nil {
		t.Fatal(err)
	}

	loaded, _ := ms.Load(context.Background(), "exec-002")
	if loaded.Status != StatusCompleted {
		t.Fatalf("expected StatusCompleted, got %v", loaded.Status)
	}

	if err := ms.Delete(context.Background(), "exec-002"); err != nil {
		t.Fatal(err)
	}

	_, err := ms.Load(context.Background(), "exec-002")
	if err == nil {
		t.Fatal("expected error loading deleted")
	}
}

func TestMemoryStoreDeleteNonexistent(t *testing.T) {
	ms := NewMemoryStore()

	if err := ms.Delete(context.Background(), "no-such-id"); err == nil {
		t.Fatal("expected error deleting nonexistent")
	}
}

func TestMemoryStoreLoadNonexistent(t *testing.T) {
	ms := NewMemoryStore()

	_, err := ms.Load(context.Background(), "no-such-id")
	if err == nil {
		t.Fatal("expected error loading nonexistent")
	}
}

func TestMemoryStoreConcurrentAccess(t *testing.T) {
	ms := NewMemoryStore()
	var wg sync.WaitGroup

	// Concurrent creates
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := "exec-" + string(rune('A'+i%26)) + string(rune('0'+i/26))
			ms.Create(context.Background(), &State{
				ID:     id,
				Status: StatusRunning,
			})
		}(i)
	}
	wg.Wait()

	// Concurrent reads
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := "exec-" + string(rune('A'+i%26)) + string(rune('0'+i/26))
			ms.Load(context.Background(), id)
		}(i)
	}
	wg.Wait()

	// Verify no corruption
	all, _ := ms.ListByStatus(context.Background())
	if len(all) == 0 {
		t.Fatal("expected some entries")
	}
}

func TestMemoryStoreUpdatedAt(t *testing.T) {
	ms := NewMemoryStore()

	s := &State{ID: "exec-001", Status: StatusRunning}
	ms.Create(context.Background(), s)

	before := time.Now()
	time.Sleep(10 * time.Millisecond)

	s.Status = StatusCompleted
	ms.Save(context.Background(), s)

	loaded, _ := ms.Load(context.Background(), "exec-001")
	if loaded.UpdatedAt.Before(before) {
		t.Fatal("expected UpdatedAt to be set on Save")
	}
}
