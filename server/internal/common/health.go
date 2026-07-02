package common

import (
	"context"
	"sync"
)

type HealthRegistry struct {
	mu      sync.Mutex
	checks  map[string]HealthChecker
}

func NewHealthRegistry() *HealthRegistry {
	return &HealthRegistry{
		checks: make(map[string]HealthChecker),
	}
}

func (r *HealthRegistry) Register(name string, hc HealthChecker) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.checks[name] = hc
}

func (r *HealthRegistry) CheckAll(ctx context.Context) map[string]HealthStatus {
	r.mu.Lock()
	names := make([]string, 0, len(r.checks))
	checkers := make([]HealthChecker, 0, len(r.checks))
	for name, hc := range r.checks {
		names = append(names, name)
		checkers = append(checkers, hc)
	}
	r.mu.Unlock()

	results := make(map[string]HealthStatus, len(names))
	for i, hc := range checkers {
		results[names[i]] = hc.Health(ctx)
	}
	return results
}

func (r *HealthRegistry) Overall(ctx context.Context) HealthStatus {
	checks := r.CheckAll(ctx)
	allAlive := len(checks) > 0
	details := make(map[string]any, len(checks))
	for name, status := range checks {
		details[name] = status.Details
		if !status.Alive {
			allAlive = false
		}
	}
	return HealthStatus{
		Alive:   allAlive,
		Details: details,
	}
}
