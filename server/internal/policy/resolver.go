package policy

import (
	"context"
	"fmt"
	"sync"
)

// Resolver resolves policies by merging hierarchical levels
type Resolver interface {
	// Resolve merges policies from all applicable levels
	Resolve(ctx context.Context, request *Request) (*Policy, error)
	
	// GetPolicy retrieves a policy by ID
	GetPolicy(ctx context.Context, id string) (*Policy, error)
	
	// SetPolicy stores a policy
	SetPolicy(ctx context.Context, policy *Policy) error
	
	// DeletePolicy removes a policy
	DeletePolicy(ctx context.Context, id string) error
	
	// ListPolicies returns all policies
	ListPolicies(ctx context.Context) ([]*Policy, error)
}

// Request contains the context for policy resolution
type Request struct {
	Environment string
	Tenant      string
	Application string
	Service     string
	Endpoint    string
	Method      string
	Workflow    string
	Runtime     string
}

// DefaultResolver implements the Resolver interface
type DefaultResolver struct {
	mu       sync.RWMutex
	policies map[string]*Policy
	cache    *cache
}

// cache stores resolved policies for performance
type cache struct {
	mu      sync.RWMutex
	entries map[string]*Policy
}

// NewResolver creates a new policy resolver
func NewResolver() *DefaultResolver {
	return &DefaultResolver{
		policies: make(map[string]*Policy),
		cache: &cache{
			entries: make(map[string]*Policy),
		},
	}
}

// Resolve merges policies from all applicable levels
func (r *DefaultResolver) Resolve(ctx context.Context, request *Request) (*Policy, error) {
	if request == nil {
		return nil, fmt.Errorf("request cannot be nil")
	}

	// Check cache first
	cacheKey := r.cacheKey(request)
	if cached := r.getFromCache(cacheKey); cached != nil {
		return cached, nil
	}

	// Collect policies from all levels in order of precedence (lowest to highest)
	policies := r.collectPolicies(request)

	// Merge policies
	merged := r.mergePolicies(policies)

	// Cache the result
	r.setCache(cacheKey, merged)

	return merged, nil
}

// collectPolicies gathers all applicable policies for a request
func (r *DefaultResolver) collectPolicies(request *Request) []*Policy {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var policies []*Policy

	// Order matters: lower levels first, higher levels override
	// Platform → Environment → Tenant → Application → Service → Endpoint → Method → Workflow → Runtime

	// Platform level (always applies)
	if p := r.findByLevelAndScope(LevelPlatform, ""); p != nil {
		policies = append(policies, p)
	}

	// Environment level
	if request.Environment != "" {
		if p := r.findByLevelAndScope(LevelEnvironment, request.Environment); p != nil {
			policies = append(policies, p)
		}
	}

	// Tenant level
	if request.Tenant != "" {
		if p := r.findByLevelAndScope(LevelTenant, request.Tenant); p != nil {
			policies = append(policies, p)
		}
	}

	// Application level
	if request.Application != "" {
		if p := r.findByLevelAndScope(LevelApplication, request.Application); p != nil {
			policies = append(policies, p)
		}
	}

	// Service level
	if request.Service != "" {
		if p := r.findByLevelAndScope(LevelService, request.Service); p != nil {
			policies = append(policies, p)
		}
	}

	// Endpoint level
	if request.Endpoint != "" {
		if p := r.findByLevelAndScope(LevelEndpoint, request.Endpoint); p != nil {
			policies = append(policies, p)
		}
	}

	// Method level
	if request.Method != "" {
		if p := r.findByLevelAndScope(LevelMethod, request.Method); p != nil {
			policies = append(policies, p)
		}
	}

	// Workflow level
	if request.Workflow != "" {
		if p := r.findByLevelAndScope(LevelWorkflow, request.Workflow); p != nil {
			policies = append(policies, p)
		}
	}

	// Runtime level (highest precedence)
	if request.Runtime != "" {
		if p := r.findByLevelAndScope(LevelRuntime, request.Runtime); p != nil {
			policies = append(policies, p)
		}
	}

	return policies
}

// findByLevelAndScope finds a policy by level and scope
func (r *DefaultResolver) findByLevelAndScope(level Level, scope string) *Policy {
	for _, p := range r.policies {
		if p.Level == level && p.Scope == scope {
			return p
		}
	}
	return nil
}

// mergePolicies merges multiple policies into one
func (r *DefaultResolver) mergePolicies(policies []*Policy) *Policy {
	if len(policies) == 0 {
		return nil
	}

	// Start with the first policy as base
	result := policies[0].Clone()

	// Merge subsequent policies (higher precedence overrides lower)
	for i := 1; i < len(policies); i++ {
		result = r.mergeTwo(result, policies[i])
	}

	return result
}

// mergeTwo merges two policies, with override taking precedence
func (r *DefaultResolver) mergeTwo(base, override *Policy) *Policy {
	if base == nil {
		return override.Clone()
	}
	if override == nil {
		return base.Clone()
	}

	result := base.Clone()

	// Override non-nil fields
	if override.Timeout != nil {
		result.Timeout = override.Timeout
	}
	if override.Retry != nil {
		result.Retry = override.Retry
	}
	if override.RateLimit != nil {
		result.RateLimit = override.RateLimit
	}
	if override.CircuitBreaker != nil {
		result.CircuitBreaker = override.CircuitBreaker
	}
	if override.Bulkhead != nil {
		result.Bulkhead = override.Bulkhead
	}
	if override.Authentication != nil {
		result.Authentication = override.Authentication
	}
	if override.Authorization != nil {
		result.Authorization = override.Authorization
	}
	if override.Tracing != nil {
		result.Tracing = override.Tracing
	}
	if override.Metrics != nil {
		result.Metrics = override.Metrics
	}
	if override.Logging != nil {
		result.Logging = override.Logging
	}
	if override.Validation != nil {
		result.Validation = override.Validation
	}
	if override.Routing != nil {
		result.Routing = override.Routing
	}

	// Merge maps
	if override.FeatureFlags != nil {
		if result.FeatureFlags == nil {
			result.FeatureFlags = make(map[string]bool)
		}
		for k, v := range override.FeatureFlags {
			result.FeatureFlags[k] = v
		}
	}
	if override.Metadata != nil {
		if result.Metadata == nil {
			result.Metadata = make(map[string]interface{})
		}
		for k, v := range override.Metadata {
			result.Metadata[k] = v
		}
	}

	return result
}

// GetPolicy retrieves a policy by ID
func (r *DefaultResolver) GetPolicy(ctx context.Context, id string) (*Policy, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	p, ok := r.policies[id]
	if !ok {
		return nil, fmt.Errorf("policy not found: %s", id)
	}
	return p.Clone(), nil
}

// SetPolicy stores a policy
func (r *DefaultResolver) SetPolicy(ctx context.Context, policy *Policy) error {
	if policy == nil {
		return fmt.Errorf("policy cannot be nil")
	}
	if policy.ID == "" {
		return fmt.Errorf("policy ID cannot be empty")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.policies[policy.ID] = policy.Clone()
	
	// Invalidate cache
	r.clearCache()

	return nil
}

// DeletePolicy removes a policy
func (r *DefaultResolver) DeletePolicy(ctx context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.policies, id)
	
	// Invalidate cache
	r.clearCache()

	return nil
}

// ListPolicies returns all policies
func (r *DefaultResolver) ListPolicies(ctx context.Context) ([]*Policy, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	policies := make([]*Policy, 0, len(r.policies))
	for _, p := range r.policies {
		policies = append(policies, p.Clone())
	}
	return policies, nil
}

// Cache operations

func (r *DefaultResolver) cacheKey(request *Request) string {
	return fmt.Sprintf("%s:%s:%s:%s:%s:%s:%s:%s",
		request.Environment,
		request.Tenant,
		request.Application,
		request.Service,
		request.Endpoint,
		request.Method,
		request.Workflow,
		request.Runtime,
	)
}

func (r *DefaultResolver) getFromCache(key string) *Policy {
	r.cache.mu.RLock()
	defer r.cache.mu.RUnlock()
	return r.cache.entries[key]
}

func (r *DefaultResolver) setCache(key string, policy *Policy) {
	r.cache.mu.Lock()
	defer r.cache.mu.Unlock()
	r.cache.entries[key] = policy
}

func (r *DefaultResolver) clearCache() {
	r.cache.mu.Lock()
	defer r.cache.mu.Unlock()
	r.cache.entries = make(map[string]*Policy)
}
