package policy

import (
	"time"
)

// Level represents the hierarchy level of a policy
type Level int

const (
	LevelPlatform Level = iota
	LevelEnvironment
	LevelTenant
	LevelApplication
	LevelService
	LevelEndpoint
	LevelMethod
	LevelWorkflow
	LevelRuntime
)

// String returns the string representation of a policy level
func (l Level) String() string {
	switch l {
	case LevelPlatform:
		return "platform"
	case LevelEnvironment:
		return "environment"
	case LevelTenant:
		return "tenant"
	case LevelApplication:
		return "application"
	case LevelService:
		return "service"
	case LevelEndpoint:
		return "endpoint"
	case LevelMethod:
		return "method"
	case LevelWorkflow:
		return "workflow"
	case LevelRuntime:
		return "runtime"
	default:
		return "unknown"
	}
}

// Policy represents a complete execution policy
type Policy struct {
	// Identification
	ID    string `json:"id"`
	Name  string `json:"name"`
	Level Level  `json:"level"`
	Scope string `json:"scope"` // e.g., "service:payment", "method:POST:/api/pay"

	// Execution
	Timeout        *Duration       `json:"timeout,omitempty"`
	Retry          *Retry          `json:"retry,omitempty"`
	RateLimit      *RateLimit      `json:"rate_limit,omitempty"`
	CircuitBreaker *CircuitBreaker `json:"circuit_breaker,omitempty"`
	Bulkhead       *Bulkhead       `json:"bulkhead,omitempty"`

	// Security
	Authentication *Authentication `json:"authentication,omitempty"`
	Authorization  *Authorization  `json:"authorization,omitempty"`

	// Observability
	Tracing *Tracing `json:"tracing,omitempty"`
	Metrics *Metrics `json:"metrics,omitempty"`
	Logging *Logging `json:"logging,omitempty"`

	// Validation
	Validation *Validation `json:"validation,omitempty"`

	// Routing
	Routing *Routing `json:"routing,omitempty"`

	// Feature Flags
	FeatureFlags map[string]bool `json:"feature_flags,omitempty"`

	// Custom metadata
	Metadata map[string]interface{} `json:"metadata,omitempty"`

	// Versioning
	Version   int64     `json:"version"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Duration wraps time.Duration for JSON serialization
type Duration struct {
	time.Duration
}

// MarshalJSON implements json.Marshaler
func (d Duration) MarshalJSON() ([]byte, error) {
	return []byte(`"` + d.Duration.String() + `"`), nil
}

// UnmarshalJSON implements json.Unmarshaler
func (d *Duration) UnmarshalJSON(b []byte) error {
	s := string(b)
	if len(s) < 2 {
		return nil
	}
	s = s[1 : len(s)-1] // Remove quotes
	dur, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	d.Duration = dur
	return nil
}

// Retry configuration
type Retry struct {
	MaxAttempts       int      `json:"max_attempts"`
	InitialDelay      Duration `json:"initial_delay"`
	MaxDelay          Duration `json:"max_delay"`
	BackoffMultiplier float64  `json:"backoff_multiplier"`
	RetryableErrors   []string `json:"retryable_errors,omitempty"`
}

// RateLimit configuration
type RateLimit struct {
	RequestsPerSecond float64 `json:"requests_per_second"`
	BurstSize         int     `json:"burst_size"`
}

// CircuitBreaker configuration
type CircuitBreaker struct {
	FailureThreshold int      `json:"failure_threshold"`
	RecoveryTimeout  Duration `json:"recovery_timeout"`
	HalfOpenMaxCalls int      `json:"half_open_max_calls"`
}

// Bulkhead configuration
type Bulkhead struct {
	MaxConcurrent int      `json:"max_concurrent"`
	MaxQueue      int      `json:"max_queue"`
	QueueTimeout  Duration `json:"queue_timeout"`
}

// Authentication configuration
type Authentication struct {
	Type     string   `json:"type"` // "api_key", "jwt", "oauth2", "mtls"
	Required bool     `json:"required"`
	Schemes  []string `json:"schemes,omitempty"`
}

// Authorization configuration
type Authorization struct {
	Enabled     bool     `json:"enabled"`
	Roles       []string `json:"roles,omitempty"`
	Permissions []string `json:"permissions,omitempty"`
}

// Tracing configuration
type Tracing struct {
	Enabled    bool    `json:"enabled"`
	SampleRate float64 `json:"sample_rate"`
}

// Metrics configuration
type Metrics struct {
	Enabled bool `json:"enabled"`
}

// Logging configuration
type Logging struct {
	Enabled bool   `json:"enabled"`
	Level   string `json:"level"` // "debug", "info", "warn", "error"
}

// Validation configuration
type Validation struct {
	Enabled bool   `json:"enabled"`
	Schema  string `json:"schema,omitempty"` // JSON Schema reference
}

// Routing configuration
type Routing struct {
	Strategy string   `json:"strategy"` // "round_robin", "random", "least_loaded", "weighted"
	Targets  []string `json:"targets,omitempty"`
	Weights  []int    `json:"weights,omitempty"`
}

// Clone creates a deep copy of the policy
func (p *Policy) Clone() *Policy {
	if p == nil {
		return nil
	}

	clone := &Policy{
		ID:        p.ID,
		Name:      p.Name,
		Level:     p.Level,
		Scope:     p.Scope,
		Version:   p.Version,
		CreatedAt: p.CreatedAt,
		UpdatedAt: p.UpdatedAt,
	}

	// Deep copy pointers
	if p.Timeout != nil {
		t := *p.Timeout
		clone.Timeout = &t
	}
	if p.Retry != nil {
		r := *p.Retry
		clone.Retry = &r
	}
	if p.RateLimit != nil {
		rl := *p.RateLimit
		clone.RateLimit = &rl
	}
	if p.CircuitBreaker != nil {
		cb := *p.CircuitBreaker
		clone.CircuitBreaker = &cb
	}
	if p.Bulkhead != nil {
		b := *p.Bulkhead
		clone.Bulkhead = &b
	}
	if p.Authentication != nil {
		a := *p.Authentication
		clone.Authentication = &a
	}
	if p.Authorization != nil {
		a := *p.Authorization
		clone.Authorization = &a
	}
	if p.Tracing != nil {
		t := *p.Tracing
		clone.Tracing = &t
	}
	if p.Metrics != nil {
		m := *p.Metrics
		clone.Metrics = &m
	}
	if p.Logging != nil {
		l := *p.Logging
		clone.Logging = &l
	}
	if p.Validation != nil {
		v := *p.Validation
		clone.Validation = &v
	}
	if p.Routing != nil {
		r := *p.Routing
		clone.Routing = &r
	}

	// Deep copy maps
	if p.FeatureFlags != nil {
		clone.FeatureFlags = make(map[string]bool)
		for k, v := range p.FeatureFlags {
			clone.FeatureFlags[k] = v
		}
	}
	if p.Metadata != nil {
		clone.Metadata = make(map[string]interface{})
		for k, v := range p.Metadata {
			clone.Metadata[k] = v
		}
	}

	return clone
}
