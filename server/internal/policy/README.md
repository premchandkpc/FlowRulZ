# Policy Resolution Engine

A hierarchical policy resolution system for FlowRulZ that enables metadata-driven execution with support for policy inheritance, merging, and validation.

## Overview

The Policy Resolution Engine implements a **Policy Resolution Pattern** that allows policies to be defined at multiple levels of a hierarchy:

```
Platform Defaults
    ↓
Environment Policy
    ↓
Tenant Policy
    ↓
Application Policy
    ↓
Service Policy
    ↓
Endpoint Policy
    ↓
Method Policy
    ↓
Workflow Policy
    ↓
Runtime Override
    ↓
Effective Policy
```

Higher levels override lower levels, enabling flexible configuration with sensible defaults.

## Features

- **Hierarchical Resolution**: Policies inherit from parent levels
- **Deep Merging**: Non-nil fields override, nil fields inherit
- **Map Merging**: Feature flags and metadata merge correctly
- **Caching**: Resolved policies are cached for performance
- **Validation**: Comprehensive policy validation with custom rules
- **Persistence**: In-memory and file-based storage backends
- **Thread-Safe**: Safe for concurrent access

## Quick Start

```go
package main

import (
    "context"
    "time"
    "github.com/premchandkpc/FlowRulZ/server/internal/policy"
)

func main() {
    resolver := policy.NewResolver()
    validator := policy.NewValidator()
    ctx := context.Background()

    // Define platform defaults
    platformPolicy := &policy.Policy{
        ID:      "platform-defaults",
        Name:    "Platform Defaults",
        Level:   policy.LevelPlatform,
        Timeout: &policy.Duration{Duration: 30 * time.Second},
        Retry: &policy.Retry{
            MaxAttempts:       3,
            InitialDelay:      policy.Duration{Duration: 100 * time.Millisecond},
            BackoffMultiplier: 2.0,
        },
    }

    // Validate and store
    if err := validator.Validate(platformPolicy); err != nil {
        panic(err)
    }
    resolver.SetPolicy(ctx, platformPolicy)

    // Override at service level
    servicePolicy := &policy.Policy{
        ID:      "payment-service",
        Name:    "Payment Service",
        Level:   policy.LevelService,
        Scope:   "payment-service",
        Timeout: &policy.Duration{Duration: 10 * time.Second}, // Override
        // Retry not specified - inherits from platform
    }
    resolver.SetPolicy(ctx, servicePolicy)

    // Resolve for a request
    request := &policy.Request{
        Service: "payment-service",
    }

    resolved, err := resolver.Resolve(ctx, request)
    if err != nil {
        panic(err)
    }

    // Use resolved policy
    fmt.Printf("Timeout: %v\n", resolved.Timeout.Duration)     // 10s (service level)
    fmt.Printf("Retries: %d\n", resolved.Retry.MaxAttempts)    // 3 (inherited)
}
```

## Policy Levels

| Level | Description | Example Scope |
|-------|-------------|---------------|
| `LevelPlatform` | Global defaults | `""` |
| `LevelEnvironment` | Environment-specific | `"production"`, `"staging"` |
| `LevelTenant` | Tenant-specific | `"acme-corp"`, `"bigco"` |
| `LevelApplication` | Application-specific | `"payment-app"` |
| `LevelService` | Service-specific | `"payment-service"` |
| `LevelEndpoint` | Endpoint-specific | `"/api/payments"` |
| `LevelMethod` | Method-specific | `"POST:/api/payments"` |
| `LevelWorkflow` | Workflow-specific | `"payment-workflow"` |
| `LevelRuntime` | Runtime override | `"instance-1"` |

## Policy Fields

### Execution
- **Timeout**: Request timeout duration
- **Retry**: Retry configuration (max attempts, backoff)
- **RateLimit**: Rate limiting (requests/sec, burst)
- **CircuitBreaker**: Circuit breaker settings
- **Bulkhead**: Concurrency limits

### Security
- **Authentication**: Auth type (api_key, jwt, oauth2, mtls)
- **Authorization**: Roles and permissions

### Observability
- **Tracing**: Distributed tracing (sample rate)
- **Metrics**: Metrics collection
- **Logging**: Log levels

### Validation
- **Validation**: Schema validation

### Routing
- **Routing**: Load balancing strategy

### Custom
- **FeatureFlags**: Boolean feature flags
- **Metadata**: Arbitrary key-value pairs

## Storage Backends

### Memory Store
```go
store := policy.NewMemoryStore()
```

### File Store
```go
store, err := policy.NewFileStore("/path/to/policies")
```

## Validation

The validator ensures policies are well-formed:

```go
validator := policy.NewValidator()

// Built-in validations:
// - Required fields (ID, Name)
// - Timeout bounds (0-10 minutes)
// - Retry bounds (0-10 attempts, multiplier 1.0-10.0)
// - Rate limit (non-negative)
// - Circuit breaker (positive thresholds)
// - Authentication types
// - Tracing sample rate (0-1.0)
// - Logging levels

// Add custom rules
validator.AddRule(func(p *policy.Policy) error {
    if p.Level != policy.LevelPlatform && p.Scope == "" {
        return fmt.Errorf("scope required for non-platform policies")
    }
    return nil
})

if err := validator.Validate(myPolicy); err != nil {
    // Handle validation error
}
```

## Caching

The resolver caches resolved policies for performance:

```go
// First resolution (cache miss)
resolved1, _ := resolver.Resolve(ctx, request)

// Second resolution (cache hit)
resolved2, _ := resolver.Resolve(ctx, request)

// Cache is invalidated when policies change
resolver.SetPolicy(ctx, updatedPolicy) // Clears cache
```

## Thread Safety

All components are thread-safe:
- Resolver uses `sync.RWMutex` for policy storage
- Cache uses separate `sync.RWMutex`
- Stores use `sync.RWMutex` for operations

## Testing

```bash
cd server/internal/policy
go test -v
```

## Performance

- **Resolution**: O(n) where n = number of hierarchy levels (typically < 10)
- **Caching**: O(1) for cache hits
- **Storage**: O(1) for Get/Set operations
- **Memory**: ~1KB per policy (varies by fields)

## Integration with Execution Pipeline

```go
func (n *ProdNode) handleIncomingMessage(ctx context.Context, msg []byte) ([]byte, error) {
    // Resolve policy for this request
    request := &policy.Request{
        Service:  "payment-service",
        Method:   "POST:/api/payments",
        Tenant:   "acme-corp",
    }
    
    resolved, err := n.PolicyResolver.Resolve(ctx, request)
    if err != nil {
        return nil, err
    }
    
    // Use resolved policy
    if resolved.RateLimit != nil {
        // Apply rate limiting
    }
    
    if resolved.Timeout != nil {
        // Apply timeout
        ctx, cancel = context.WithTimeout(ctx, resolved.Timeout.Duration)
        defer cancel()
    }
    
    // Execute with policy
    return n.executeWithPolicy(ctx, msg, resolved)
}
```

## Future Enhancements

- [ ] Policy versioning and rollback
- [ ] Policy diff and compatibility checks
- [ ] Staged rollouts
- [ ] Policy migration tools
- [ ] Policy templates
- [ ] Policy composition operators (AND, OR, NOT)
- [ ] Policy audit logging
- [ ] Policy conflict detection
- [ ] Policy simulation/testing
- [ ] Policy import/export (JSON, YAML, HCL)

## References

- [Policy Resolution Pattern](https://docs.flowrulz.io/patterns/policy-resolution)
- [Hierarchical Configuration](https://docs.flowrulz.io/architecture/hierarchical-config)
- [Metadata-Driven Execution](https://docs.flowrulz.io/architecture/metadata-driven)
