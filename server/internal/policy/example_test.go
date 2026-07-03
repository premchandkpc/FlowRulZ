package policy

import (
	"context"
	"fmt"
	"time"
)

// Example demonstrates how to use the policy resolution system
func Example() {
	// Create resolver and validator
	resolver := NewResolver()
	validator := NewValidator()
	ctx := context.Background()

	// Define platform-level defaults
	platformPolicy := &Policy{
		ID:      "platform-defaults",
		Name:    "Platform Defaults",
		Level:   LevelPlatform,
		Scope:   "",
		Timeout: &Duration{Duration: 30 * time.Second},
		Retry: &Retry{
			MaxAttempts:       3,
			InitialDelay:      Duration{Duration: 100 * time.Millisecond},
			BackoffMultiplier: 2.0,
		},
		RateLimit: &RateLimit{
			RequestsPerSecond: 1000,
			BurstSize:         2000,
		},
		CircuitBreaker: &CircuitBreaker{
			FailureThreshold: 5,
			RecoveryTimeout:  Duration{Duration: 30 * time.Second},
			HalfOpenMaxCalls: 3,
		},
		Tracing: &Tracing{
			Enabled:    true,
			SampleRate: 0.1,
		},
	}

	// Validate and store
	if err := validator.Validate(platformPolicy); err != nil {
		panic(fmt.Sprintf("Invalid platform policy: %v", err))
	}
	if err := resolver.SetPolicy(ctx, platformPolicy); err != nil {
		panic(fmt.Sprintf("Failed to set platform policy: %v", err))
	}

	// Define environment-level overrides (production)
	prodPolicy := &Policy{
		ID:      "production-overrides",
		Name:    "Production Overrides",
		Level:   LevelEnvironment,
		Scope:   "production",
		Timeout: &Duration{Duration: 20 * time.Second}, // Stricter timeout
		Tracing: &Tracing{
			Enabled:    true,
			SampleRate: 0.5, // More tracing in prod
		},
	}
	resolver.SetPolicy(ctx, prodPolicy)

	// Define tenant-level policies
	tenantPolicy := &Policy{
		ID:      "acme-corp-policy",
		Name:    "ACME Corp Policy",
		Level:   LevelTenant,
		Scope:   "acme-corp",
		RateLimit: &RateLimit{
			RequestsPerSecond: 5000, // Higher limit for premium tenant
			BurstSize:         10000,
		},
		FeatureFlags: map[string]bool{
			"beta-features": true,
			"priority-support": true,
		},
	}
	resolver.SetPolicy(ctx, tenantPolicy)

	// Define service-level policies
	servicePolicy := &Policy{
		ID:      "payment-service-policy",
		Name:    "Payment Service Policy",
		Level:   LevelService,
		Scope:   "payment-service",
		Timeout: &Duration{Duration: 10 * time.Second}, // Payment needs quick response
		Retry: &Retry{
			MaxAttempts:       5, // More retries for payments
			InitialDelay:      Duration{Duration: 200 * time.Millisecond},
			BackoffMultiplier: 2.0,
		},
		Authentication: &Authentication{
			Type:     "api_key",
			Required: true,
		},
	}
	resolver.SetPolicy(ctx, servicePolicy)

	// Define method-level policies
	methodPolicy := &Policy{
		ID:      "payment-create-policy",
		Name:    "Payment Create Policy",
		Level:   LevelMethod,
		Scope:   "POST:/api/payments",
		Timeout: &Duration{Duration: 5 * time.Second}, // Even stricter for create
		Authorization: &Authorization{
			Enabled: true,
			Roles:   []string{"payment-creator"},
		},
	}
	resolver.SetPolicy(ctx, methodPolicy)

	// Resolve policy for a specific request
	request := &Request{
		Environment: "production",
		Tenant:      "acme-corp",
		Service:     "payment-service",
		Method:      "POST:/api/payments",
	}

	resolved, err := resolver.Resolve(ctx, request)
	if err != nil {
		panic(fmt.Sprintf("Failed to resolve policy: %v", err))
	}

	// Use the resolved policy
	fmt.Printf("Timeout: %v\n", resolved.Timeout.Duration)
	fmt.Printf("Max Retry Attempts: %d\n", resolved.Retry.MaxAttempts)
	fmt.Printf("Rate Limit: %.0f req/s\n", resolved.RateLimit.RequestsPerSecond)
	fmt.Printf("Auth Required: %v\n", resolved.Authentication.Required)
	fmt.Printf("Auth Roles: %v\n", resolved.Authorization.Roles)
	fmt.Printf("Tracing Sample Rate: %.1f\n", resolved.Tracing.SampleRate)
	fmt.Printf("Beta Features: %v\n", resolved.FeatureFlags["beta-features"])

	// Output:
	// Timeout: 5s
	// Max Retry Attempts: 5
	// Rate Limit: 5000 req/s
	// Auth Required: true
	// Auth Roles: [payment-creator]
	// Tracing Sample Rate: 0.5
	// Beta Features: true
}

// Example_withStore demonstrates using a persistent store
func Example_withStore() {
	ctx := context.Background()

	// Create a file-based store
	store, err := NewFileStore("/tmp/policies")
	if err != nil {
		panic(fmt.Sprintf("Failed to create store: %v", err))
	}

	// Create resolver with store
	resolver := NewResolver()

	// Load policies from store
	policies, err := store.List(ctx)
	if err != nil {
		panic(fmt.Sprintf("Failed to list policies: %v", err))
	}

	for _, p := range policies {
		if err := resolver.SetPolicy(ctx, p); err != nil {
			panic(fmt.Sprintf("Failed to set policy: %v", err))
		}
	}

	// Resolve and use
	request := &Request{
		Service: "payment-service",
	}
	resolved, err := resolver.Resolve(ctx, request)
	if err != nil {
		panic(fmt.Sprintf("Failed to resolve: %v", err))
	}

	fmt.Printf("Resolved policy: %s\n", resolved.Name)
}

// Example_customRules demonstrates adding custom validation rules
func Example_customRules() {
	validator := NewValidator()

	// Add custom rule: require scope for non-platform policies
	validator.AddRule(func(p *Policy) error {
		if p.Level != LevelPlatform && p.Scope == "" {
			return fmt.Errorf("scope is required for %s level policies", p.Level)
		}
		return nil
	})

	// Add custom rule: timeout must be at least 1 second
	validator.AddRule(func(p *Policy) error {
		if p.Timeout != nil && p.Timeout.Duration < time.Second {
			return fmt.Errorf("timeout must be at least 1 second")
		}
		return nil
	})

	// Validate policy
	policy := &Policy{
		ID:      "test",
		Name:    "Test",
		Level:   LevelService,
		Scope:   "my-service",
		Timeout: &Duration{Duration: 5 * time.Second},
	}

	if err := validator.Validate(policy); err != nil {
		fmt.Printf("Validation failed: %v\n", err)
	} else {
		fmt.Println("Policy is valid")
	}
}
