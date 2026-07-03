package policy

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMemoryStore_BasicOperations(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	// Set
	policy := &Policy{
		ID:    "test-policy",
		Name:  "Test Policy",
		Level: LevelService,
		Scope: "test-service",
	}
	if err := store.Set(ctx, policy); err != nil {
		t.Fatalf("Failed to set policy: %v", err)
	}

	// Get
	retrieved, err := store.Get(ctx, "test-policy")
	if err != nil {
		t.Fatalf("Failed to get policy: %v", err)
	}
	if retrieved.ID != policy.ID {
		t.Errorf("Expected ID %s, got %s", policy.ID, retrieved.ID)
	}

	// List
	policies, err := store.List(ctx)
	if err != nil {
		t.Fatalf("Failed to list policies: %v", err)
	}
	if len(policies) != 1 {
		t.Errorf("Expected 1 policy, got %d", len(policies))
	}

	// Delete
	if err := store.Delete(ctx, "test-policy"); err != nil {
		t.Fatalf("Failed to delete policy: %v", err)
	}

	// Get after delete
	_, err = store.Get(ctx, "test-policy")
	if err == nil {
		t.Error("Expected error after delete")
	}
}

func TestMemoryStore_ListByLevel(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	// Add policies at different levels
	store.Set(ctx, &Policy{ID: "platform", Level: LevelPlatform})
	store.Set(ctx, &Policy{ID: "env1", Level: LevelEnvironment})
	store.Set(ctx, &Policy{ID: "env2", Level: LevelEnvironment})
	store.Set(ctx, &Policy{ID: "svc1", Level: LevelService})

	// List by level
	envPolicies, err := store.ListByLevel(ctx, LevelEnvironment)
	if err != nil {
		t.Fatalf("Failed to list by level: %v", err)
	}
	if len(envPolicies) != 2 {
		t.Errorf("Expected 2 environment policies, got %d", len(envPolicies))
	}
}

func TestMemoryStore_ListByScope(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	// Add policies with different scopes
	store.Set(ctx, &Policy{ID: "svc1", Level: LevelService, Scope: "payment-service"})
	store.Set(ctx, &Policy{ID: "svc2", Level: LevelService, Scope: "payment-service"})
	store.Set(ctx, &Policy{ID: "svc3", Level: LevelService, Scope: "order-service"})

	// List by scope
	paymentPolicies, err := store.ListByScope(ctx, "payment-service")
	if err != nil {
		t.Fatalf("Failed to list by scope: %v", err)
	}
	if len(paymentPolicies) != 2 {
		t.Errorf("Expected 2 payment-service policies, got %d", len(paymentPolicies))
	}
}

func TestFileStore_BasicOperations(t *testing.T) {
	// Create temp directory
	dir, err := os.MkdirTemp("", "policy-store-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("Failed to create file store: %v", err)
	}

	ctx := context.Background()

	// Set
	policy := &Policy{
		ID:      "test-policy",
		Name:    "Test Policy",
		Level:   LevelService,
		Scope:   "test-service",
		Timeout: &Duration{Duration: 30 * time.Second},
	}
	if err := store.Set(ctx, policy); err != nil {
		t.Fatalf("Failed to set policy: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(filepath.Join(dir, "test-policy.json")); err != nil {
		t.Errorf("Policy file not created: %v", err)
	}

	// Get
	retrieved, err := store.Get(ctx, "test-policy")
	if err != nil {
		t.Fatalf("Failed to get policy: %v", err)
	}
	if retrieved.ID != policy.ID {
		t.Errorf("Expected ID %s, got %s", policy.ID, retrieved.ID)
	}
	if retrieved.Timeout.Duration != 30*time.Second {
		t.Errorf("Expected timeout 30s, got %v", retrieved.Timeout.Duration)
	}

	// List
	policies, err := store.List(ctx)
	if err != nil {
		t.Fatalf("Failed to list policies: %v", err)
	}
	if len(policies) != 1 {
		t.Errorf("Expected 1 policy, got %d", len(policies))
	}

	// Delete
	if err := store.Delete(ctx, "test-policy"); err != nil {
		t.Fatalf("Failed to delete policy: %v", err)
	}

	// Get after delete
	_, err = store.Get(ctx, "test-policy")
	if err == nil {
		t.Error("Expected error after delete")
	}
}

func TestFileStore_Persistence(t *testing.T) {
	// Create temp directory
	dir, err := os.MkdirTemp("", "policy-store-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	ctx := context.Background()

	// Create store and add policy
	store1, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	policy := &Policy{
		ID:    "persistent-policy",
		Name:  "Persistent Policy",
		Level: LevelService,
		Scope: "test-service",
	}
	if err := store1.Set(ctx, policy); err != nil {
		t.Fatalf("Failed to set policy: %v", err)
	}

	// Create new store instance (simulating restart)
	store2, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// Verify policy persists
	retrieved, err := store2.Get(ctx, "persistent-policy")
	if err != nil {
		t.Fatalf("Failed to get policy after restart: %v", err)
	}
	if retrieved.ID != policy.ID {
		t.Errorf("Expected ID %s, got %s", policy.ID, retrieved.ID)
	}
}

func TestFileStore_ListByLevel(t *testing.T) {
	dir, err := os.MkdirTemp("", "policy-store-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	ctx := context.Background()

	// Add policies at different levels
	store.Set(ctx, &Policy{ID: "platform", Level: LevelPlatform})
	store.Set(ctx, &Policy{ID: "env1", Level: LevelEnvironment})
	store.Set(ctx, &Policy{ID: "env2", Level: LevelEnvironment})

	// List by level
	envPolicies, err := store.ListByLevel(ctx, LevelEnvironment)
	if err != nil {
		t.Fatalf("Failed to list by level: %v", err)
	}
	if len(envPolicies) != 2 {
		t.Errorf("Expected 2 environment policies, got %d", len(envPolicies))
	}
}

func TestFileStore_InvalidJSON(t *testing.T) {
	dir, err := os.MkdirTemp("", "policy-store-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Write invalid JSON
	invalidPath := filepath.Join(dir, "invalid.json")
	if err := os.WriteFile(invalidPath, []byte("not json"), 0644); err != nil {
		t.Fatalf("Failed to write invalid file: %v", err)
	}

	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	ctx := context.Background()

	// List should skip invalid files
	policies, err := store.List(ctx)
	if err != nil {
		t.Fatalf("Failed to list policies: %v", err)
	}
	if len(policies) != 0 {
		t.Errorf("Expected 0 valid policies, got %d", len(policies))
	}
}
