package policy

import (
	"fmt"
	"testing"
	"time"
)

func TestValidator_BasicValidation(t *testing.T) {
	validator := NewValidator()

	// Valid policy
	policy := &Policy{
		ID:   "test-policy",
		Name: "Test Policy",
	}

	if err := validator.Validate(policy); err != nil {
		t.Errorf("Expected valid policy, got error: %v", err)
	}
}

func TestValidator_MissingRequiredFields(t *testing.T) {
	validator := NewValidator()

	tests := []struct {
		name    string
		policy  *Policy
		wantErr bool
	}{
		{
			name:    "missing ID",
			policy:  &Policy{Name: "Test"},
			wantErr: true,
		},
		{
			name:    "missing Name",
			policy:  &Policy{ID: "test"},
			wantErr: true,
		},
		{
			name:    "valid",
			policy:  &Policy{ID: "test", Name: "Test"},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validator.Validate(tt.policy)
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidator_Timeout(t *testing.T) {
	validator := NewValidator()

	tests := []struct {
		name    string
		timeout *Duration
		wantErr bool
	}{
		{
			name:    "valid timeout",
			timeout: &Duration{Duration: 30 * time.Second},
			wantErr: false,
		},
		{
			name:    "zero timeout",
			timeout: &Duration{Duration: 0},
			wantErr: true,
		},
		{
			name:    "negative timeout",
			timeout: &Duration{Duration: -1 * time.Second},
			wantErr: true,
		},
		{
			name:    "too large timeout",
			timeout: &Duration{Duration: 15 * time.Minute},
			wantErr: true,
		},
		{
			name:    "nil timeout",
			timeout: nil,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := &Policy{
				ID:      "test",
				Name:    "Test",
				Timeout: tt.timeout,
			}
			err := validator.Validate(policy)
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidator_Retry(t *testing.T) {
	validator := NewValidator()

	tests := []struct {
		name    string
		retry   *Retry
		wantErr bool
	}{
		{
			name: "valid retry",
			retry: &Retry{
				MaxAttempts:       3,
				InitialDelay:      Duration{Duration: 100 * time.Millisecond},
				BackoffMultiplier: 2.0,
			},
			wantErr: false,
		},
		{
			name: "negative max attempts",
			retry: &Retry{
				MaxAttempts: -1,
			},
			wantErr: true,
		},
		{
			name: "too many attempts",
			retry: &Retry{
				MaxAttempts: 15,
			},
			wantErr: true,
		},
		{
			name: "invalid backoff multiplier",
			retry: &Retry{
				MaxAttempts:       3,
				BackoffMultiplier: 0.5,
			},
			wantErr: true,
		},
		{
			name: "nil retry",
			retry: nil,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := &Policy{
				ID:    "test",
				Name:  "Test",
				Retry: tt.retry,
			}
			err := validator.Validate(policy)
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidator_RateLimit(t *testing.T) {
	validator := NewValidator()

	tests := []struct {
		name      string
		rateLimit *RateLimit
		wantErr   bool
	}{
		{
			name: "valid rate limit",
			rateLimit: &RateLimit{
				RequestsPerSecond: 100,
				BurstSize:         200,
			},
			wantErr: false,
		},
		{
			name: "negative requests per second",
			rateLimit: &RateLimit{
				RequestsPerSecond: -1,
			},
			wantErr: true,
		},
		{
			name: "negative burst size",
			rateLimit: &RateLimit{
				RequestsPerSecond: 100,
				BurstSize:         -1,
			},
			wantErr: true,
		},
		{
			name:      "nil rate limit",
			rateLimit: nil,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := &Policy{
				ID:        "test",
				Name:      "Test",
				RateLimit: tt.rateLimit,
			}
			err := validator.Validate(policy)
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidator_CircuitBreaker(t *testing.T) {
	validator := NewValidator()

	tests := []struct {
		name           string
		circuitBreaker *CircuitBreaker
		wantErr        bool
	}{
		{
			name: "valid circuit breaker",
			circuitBreaker: &CircuitBreaker{
				FailureThreshold: 5,
				RecoveryTimeout:  Duration{Duration: 30 * time.Second},
				HalfOpenMaxCalls: 3,
			},
			wantErr: false,
		},
		{
			name: "zero failure threshold",
			circuitBreaker: &CircuitBreaker{
				FailureThreshold: 0,
			},
			wantErr: true,
		},
		{
			name: "zero recovery timeout",
			circuitBreaker: &CircuitBreaker{
				FailureThreshold: 5,
				RecoveryTimeout:  Duration{Duration: 0},
			},
			wantErr: true,
		},
		{
			name: "nil circuit breaker",
			circuitBreaker: nil,
			wantErr:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := &Policy{
				ID:             "test",
				Name:           "Test",
				CircuitBreaker: tt.circuitBreaker,
			}
			err := validator.Validate(policy)
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidator_Authentication(t *testing.T) {
	validator := NewValidator()

	tests := []struct {
		name           string
		authentication *Authentication
		wantErr        bool
	}{
		{
			name: "valid api_key",
			authentication: &Authentication{
				Type: "api_key",
			},
			wantErr: false,
		},
		{
			name: "valid jwt",
			authentication: &Authentication{
				Type: "jwt",
			},
			wantErr: false,
		},
		{
			name: "invalid type",
			authentication: &Authentication{
				Type: "invalid",
			},
			wantErr: true,
		},
		{
			name:           "nil authentication",
			authentication: nil,
			wantErr:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := &Policy{
				ID:             "test",
				Name:           "Test",
				Authentication: tt.authentication,
			}
			err := validator.Validate(policy)
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidator_Tracing(t *testing.T) {
	validator := NewValidator()

	tests := []struct {
		name    string
		tracing *Tracing
		wantErr bool
	}{
		{
			name: "valid sample rate",
			tracing: &Tracing{
				Enabled:    true,
				SampleRate: 0.5,
			},
			wantErr: false,
		},
		{
			name: "sample rate too high",
			tracing: &Tracing{
				Enabled:    true,
				SampleRate: 1.5,
			},
			wantErr: true,
		},
		{
			name: "negative sample rate",
			tracing: &Tracing{
				Enabled:    true,
				SampleRate: -0.1,
			},
			wantErr: true,
		},
		{
			name:    "nil tracing",
			tracing: nil,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := &Policy{
				ID:      "test",
				Name:    "Test",
				Tracing: tt.tracing,
			}
			err := validator.Validate(policy)
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidator_CustomRule(t *testing.T) {
	validator := NewValidator()

	// Add custom rule
	customRule := func(p *Policy) error {
		if p.Scope == "" && p.Level != LevelPlatform {
			return fmt.Errorf("scope is required for non-platform policies")
		}
		return nil
	}
	validator.AddRule(customRule)

	tests := []struct {
		name    string
		policy  *Policy
		wantErr bool
	}{
		{
			name: "platform without scope",
			policy: &Policy{
				ID:    "test",
				Name:  "Test",
				Level: LevelPlatform,
				Scope: "",
			},
			wantErr: false,
		},
		{
			name: "service without scope",
			policy: &Policy{
				ID:    "test",
				Name:  "Test",
				Level: LevelService,
				Scope: "",
			},
			wantErr: true,
		},
		{
			name: "service with scope",
			policy: &Policy{
				ID:    "test",
				Name:  "Test",
				Level: LevelService,
				Scope: "my-service",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validator.Validate(tt.policy)
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
