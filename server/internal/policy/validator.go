package policy

import (
	"fmt"
	"time"
)

// Validator validates policies
type Validator struct {
	// Custom validation rules
	rules []ValidationRule
}

// ValidationRule is a function that validates a policy
type ValidationRule func(*Policy) error

// NewValidator creates a new policy validator
func NewValidator() *Validator {
	return &Validator{
		rules: []ValidationRule{
			validateRequiredFields,
			validateTimeout,
			validateRetry,
			validateRateLimit,
			validateCircuitBreaker,
			validateBulkhead,
			validateAuthentication,
			validateAuthorization,
			validateTracing,
			validateLogging,
		},
	}
}

// AddRule adds a custom validation rule
func (v *Validator) AddRule(rule ValidationRule) {
	v.rules = append(v.rules, rule)
}

// Validate validates a policy
func (v *Validator) Validate(policy *Policy) error {
	if policy == nil {
		return fmt.Errorf("policy cannot be nil")
	}

	for _, rule := range v.rules {
		if err := rule(policy); err != nil {
			return err
		}
	}

	return nil
}

func validateRequiredFields(p *Policy) error {
	if p.ID == "" {
		return fmt.Errorf("policy ID is required")
	}
	if p.Name == "" {
		return fmt.Errorf("policy name is required")
	}
	return nil
}

func validateTimeout(p *Policy) error {
	if p.Timeout == nil {
		return nil
	}
	if p.Timeout.Duration <= 0 {
		return fmt.Errorf("timeout must be positive")
	}
	if p.Timeout.Duration > 10*time.Minute {
		return fmt.Errorf("timeout cannot exceed 10 minutes")
	}
	return nil
}

func validateRetry(p *Policy) error {
	if p.Retry == nil {
		return nil
	}
	if p.Retry.MaxAttempts < 0 {
		return fmt.Errorf("max attempts cannot be negative")
	}
	if p.Retry.MaxAttempts > 10 {
		return fmt.Errorf("max attempts cannot exceed 10")
	}
	if p.Retry.InitialDelay.Duration < 0 {
		return fmt.Errorf("initial delay cannot be negative")
	}
	if p.Retry.MaxDelay.Duration < 0 {
		return fmt.Errorf("max delay cannot be negative")
	}
	if p.Retry.BackoffMultiplier < 1.0 {
		return fmt.Errorf("backoff multiplier must be >= 1.0")
	}
	if p.Retry.BackoffMultiplier > 10.0 {
		return fmt.Errorf("backoff multiplier cannot exceed 10.0")
	}
	return nil
}

func validateRateLimit(p *Policy) error {
	if p.RateLimit == nil {
		return nil
	}
	if p.RateLimit.RequestsPerSecond < 0 {
		return fmt.Errorf("requests per second cannot be negative")
	}
	if p.RateLimit.BurstSize < 0 {
		return fmt.Errorf("burst size cannot be negative")
	}
	return nil
}

func validateCircuitBreaker(p *Policy) error {
	if p.CircuitBreaker == nil {
		return nil
	}
	if p.CircuitBreaker.FailureThreshold <= 0 {
		return fmt.Errorf("failure threshold must be positive")
	}
	if p.CircuitBreaker.RecoveryTimeout.Duration <= 0 {
		return fmt.Errorf("recovery timeout must be positive")
	}
	if p.CircuitBreaker.HalfOpenMaxCalls <= 0 {
		return fmt.Errorf("half-open max calls must be positive")
	}
	return nil
}

func validateBulkhead(p *Policy) error {
	if p.Bulkhead == nil {
		return nil
	}
	if p.Bulkhead.MaxConcurrent <= 0 {
		return fmt.Errorf("max concurrent must be positive")
	}
	if p.Bulkhead.MaxQueue < 0 {
		return fmt.Errorf("max queue cannot be negative")
	}
	if p.Bulkhead.QueueTimeout.Duration < 0 {
		return fmt.Errorf("queue timeout cannot be negative")
	}
	return nil
}

func validateAuthentication(p *Policy) error {
	if p.Authentication == nil {
		return nil
	}
	validTypes := map[string]bool{
		"api_key": true,
		"jwt":     true,
		"oauth2":  true,
		"mtls":    true,
		"none":    true,
	}
	if !validTypes[p.Authentication.Type] {
		return fmt.Errorf("invalid authentication type: %s", p.Authentication.Type)
	}
	return nil
}

func validateAuthorization(p *Policy) error {
	if p.Authorization == nil {
		return nil
	}
	if p.Authorization.Enabled {
		if len(p.Authorization.Roles) == 0 && len(p.Authorization.Permissions) == 0 {
			return fmt.Errorf("authorization enabled but no roles or permissions specified")
		}
	}
	return nil
}

func validateTracing(p *Policy) error {
	if p.Tracing == nil {
		return nil
	}
	if p.Tracing.SampleRate < 0 || p.Tracing.SampleRate > 1.0 {
		return fmt.Errorf("sample rate must be between 0 and 1.0")
	}
	return nil
}

func validateLogging(p *Policy) error {
	if p.Logging == nil {
		return nil
	}
	validLevels := map[string]bool{
		"debug": true,
		"info":  true,
		"warn":  true,
		"error": true,
		"":      true,
	}
	if !validLevels[p.Logging.Level] {
		return fmt.Errorf("invalid logging level: %s", p.Logging.Level)
	}
	return nil
}
