package policy

import (
	"context"
	"testing"
	"time"
)

func TestPolicyResolver_BasicResolution(t *testing.T) {
	resolver := NewResolver()
	ctx := context.Background()

	// Set platform-level policy
	platformPolicy := &Policy{
		ID:    "platform-default",
		Level: LevelPlatform,
		Scope: "",
		Timeout: &Duration{Duration: 30 * time.Second},
		Retry: &Retry{
			MaxAttempts:     3,
			InitialDelay:    Duration{Duration: 100 * time.Millisecond},
			BackoffMultiplier: 2.0,
		},
	}
	if err := resolver.SetPolicy(ctx, platformPolicy); err != nil {
		t.Fatalf("Failed to set platform policy: %v", err)
	}

	// Resolve for a basic request
	req := &Request{
		Service: "payment-service",
	}
	resolved, err := resolver.Resolve(ctx, req)
	if err != nil {
		t.Fatalf("Failed to resolve policy: %v", err)
	}

	if resolved.Timeout.Duration != 30*time.Second {
		t.Errorf("Expected timeout 30s, got %v", resolved.Timeout.Duration)
	}
	if resolved.Retry.MaxAttempts != 3 {
		t.Errorf("Expected 3 retry attempts, got %d", resolved.Retry.MaxAttempts)
	}
}

func TestPolicyResolver_HierarchicalOverride(t *testing.T) {
	resolver := NewResolver()
	ctx := context.Background()

	// Platform level
	platformPolicy := &Policy{
		ID:    "platform",
		Level: LevelPlatform,
		Scope: "",
		Timeout: &Duration{Duration: 30 * time.Second},
		Retry: &Retry{
			MaxAttempts: 3,
			InitialDelay: Duration{Duration: 100 * time.Millisecond},
		},
	}
	resolver.SetPolicy(ctx, platformPolicy)

	// Service level override
	servicePolicy := &Policy{
		ID:    "payment-service",
		Level: LevelService,
		Scope: "payment-service",
		Timeout: &Duration{Duration: 60 * time.Second}, // Override timeout
		// Retry not specified, should inherit from platform
	}
	resolver.SetPolicy(ctx, servicePolicy)

	// Resolve
	req := &Request{
		Service: "payment-service",
	}
	resolved, err := resolver.Resolve(ctx, req)
	if err != nil {
		t.Fatalf("Failed to resolve: %v", err)
	}

	// Should use service-level timeout
	if resolved.Timeout.Duration != 60*time.Second {
		t.Errorf("Expected timeout 60s, got %v", resolved.Timeout.Duration)
	}

	// Should inherit platform-level retry
	if resolved.Retry.MaxAttempts != 3 {
		t.Errorf("Expected 3 retry attempts, got %d", resolved.Retry.MaxAttempts)
	}
}

func TestPolicyResolver_FullHierarchy(t *testing.T) {
	resolver := NewResolver()
	ctx := context.Background()

	// Platform
	resolver.SetPolicy(ctx, &Policy{
		ID:    "platform",
		Level: LevelPlatform,
		Scope: "",
		Timeout: &Duration{Duration: 30 * time.Second},
		Retry: &Retry{MaxAttempts: 3},
		RateLimit: &RateLimit{RequestsPerSecond: 1000},
	})

	// Environment
	resolver.SetPolicy(ctx, &Policy{
		ID:    "prod",
		Level: LevelEnvironment,
		Scope: "production",
		Timeout: &Duration{Duration: 20 * time.Second},
	})

	// Tenant
	resolver.SetPolicy(ctx, &Policy{
		ID:    "tenant-acme",
		Level: LevelTenant,
		Scope: "acme-corp",
		RateLimit: &RateLimit{RequestsPerSecond: 5000},
	})

	// Service
	resolver.SetPolicy(ctx, &Policy{
		ID:    "payment-service",
		Level: LevelService,
		Scope: "payment-service",
		Retry: &Retry{MaxAttempts: 5},
	})

	// Method
	resolver.SetPolicy(ctx, &Policy{
		ID:    "payment-create",
		Level: LevelMethod,
		Scope: "POST:/api/payments",
		Timeout: &Duration{Duration: 10 * time.Second},
	})

	// Resolve
	req := &Request{
		Environment: "production",
		Tenant:      "acme-corp",
		Service:     "payment-service",
		Method:      "POST:/api/payments",
	}
	resolved, err := resolver.Resolve(ctx, req)
	if err != nil {
		t.Fatalf("Failed to resolve: %v", err)
	}

	// Method-level timeout (highest precedence)
	if resolved.Timeout.Duration != 10*time.Second {
		t.Errorf("Expected timeout 10s, got %v", resolved.Timeout.Duration)
	}

	// Service-level retry
	if resolved.Retry.MaxAttempts != 5 {
		t.Errorf("Expected 5 retry attempts, got %d", resolved.Retry.MaxAttempts)
	}

	// Tenant-level rate limit
	if resolved.RateLimit.RequestsPerSecond != 5000 {
		t.Errorf("Expected rate limit 5000, got %v", resolved.RateLimit.RequestsPerSecond)
	}
}

func TestPolicyResolver_MapMerging(t *testing.T) {
	resolver := NewResolver()
	ctx := context.Background()

	// Platform
	resolver.SetPolicy(ctx, &Policy{
		ID:    "platform",
		Level: LevelPlatform,
		Scope: "",
		FeatureFlags: map[string]bool{
			"new-ui":      true,
			"dark-mode":   false,
			"beta-access": false,
		},
	})

	// Service
	resolver.SetPolicy(ctx, &Policy{
		ID:    "payment-service",
		Level: LevelService,
		Scope: "payment-service",
		FeatureFlags: map[string]bool{
			"new-ui":     false, // Override
			"beta-access": true, // Override
		},
	})

	req := &Request{Service: "payment-service"}
	resolved, err := resolver.Resolve(ctx, req)
	if err != nil {
		t.Fatalf("Failed to resolve: %v", err)
	}

	// Check merged feature flags
	expected := map[string]bool{
		"new-ui":      false, // Overridden by service
		"dark-mode":   false, // Inherited from platform
		"beta-access": true,  // Overridden by service
	}

	for key, expectedVal := range expected {
		if actual, ok := resolved.FeatureFlags[key]; !ok {
			t.Errorf("Missing feature flag: %s", key)
		} else if actual != expectedVal {
			t.Errorf("Feature flag %s: expected %v, got %v", key, expectedVal, actual)
		}
	}
}

func TestPolicyResolver_Caching(t *testing.T) {
	resolver := NewResolver()
	ctx := context.Background()

	resolver.SetPolicy(ctx, &Policy{
		ID:    "platform",
		Level: LevelPlatform,
		Scope: "",
		Timeout: &Duration{Duration: 30 * time.Second},
	})

	req := &Request{Service: "test-service"}

	// First resolution (cache miss)
	resolved1, err := resolver.Resolve(ctx, req)
	if err != nil {
		t.Fatalf("Failed to resolve: %v", err)
	}

	// Second resolution (cache hit)
	resolved2, err := resolver.Resolve(ctx, req)
	if err != nil {
		t.Fatalf("Failed to resolve: %v", err)
	}

	// Should be the same object (cached)
	if resolved1 != resolved2 {
		t.Error("Expected cached result, got different objects")
	}

	// Update policy (should invalidate cache)
	resolver.SetPolicy(ctx, &Policy{
		ID:    "platform",
		Level: LevelPlatform,
		Scope: "",
		Timeout: &Duration{Duration: 60 * time.Second},
	})

	// Third resolution (cache miss)
	resolved3, err := resolver.Resolve(ctx, req)
	if err != nil {
		t.Fatalf("Failed to resolve: %v", err)
	}

	if resolved3.Timeout.Duration != 60*time.Second {
		t.Errorf("Expected updated timeout 60s, got %v", resolved3.Timeout.Duration)
	}
}

func TestPolicyResolver_DeepCopy(t *testing.T) {
	resolver := NewResolver()
	ctx := context.Background()

	original := &Policy{
		ID:    "test",
		Level: LevelService,
		Scope: "test-service",
		Timeout: &Duration{Duration: 30 * time.Second},
		FeatureFlags: map[string]bool{
			"flag1": true,
		},
	}

	resolver.SetPolicy(ctx, original)

	// Modify original
	original.Timeout.Duration = 60 * time.Second
	original.FeatureFlags["flag1"] = false

	// Retrieve from resolver
	retrieved, err := resolver.GetPolicy(ctx, "test")
	if err != nil {
		t.Fatalf("Failed to get policy: %v", err)
	}

	// Should not be affected by modifications
	if retrieved.Timeout.Duration != 30*time.Second {
		t.Errorf("Deep copy failed: expected 30s, got %v", retrieved.Timeout.Duration)
	}
	if !retrieved.FeatureFlags["flag1"] {
		t.Error("Deep copy failed: flag1 should be true")
	}
}

func TestPolicyResolver_EmptyRequest(t *testing.T) {
	resolver := NewResolver()
	ctx := context.Background()

	_, err := resolver.Resolve(ctx, nil)
	if err == nil {
		t.Error("Expected error for nil request")
	}
}

func TestPolicyResolver_NoPolicies(t *testing.T) {
	resolver := NewResolver()
	ctx := context.Background()

	req := &Request{Service: "test-service"}
	resolved, err := resolver.Resolve(ctx, req)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Should return nil when no policies match
	if resolved != nil {
		t.Error("Expected nil policy when no policies match")
	}
}
