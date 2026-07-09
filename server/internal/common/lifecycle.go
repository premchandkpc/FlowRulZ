package common

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

type Service interface {
	Start(ctx context.Context) error
	Stop() error
}

type LifecycleRegistry struct {
	mu       sync.Mutex
	services []Service
	names    map[string]int
}

func NewLifecycleRegistry() *LifecycleRegistry {
	return &LifecycleRegistry{
		names: make(map[string]int),
	}
}

func (r *LifecycleRegistry) Register(name string, svc Service) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.names[name] = len(r.services)
	r.services = append(r.services, svc)
}

func (r *LifecycleRegistry) StartAll(ctx context.Context) error {
	r.mu.Lock()
	svcs := append([]Service(nil), r.services...)
	r.mu.Unlock()

	for i, svc := range svcs {
		name := r.nameForIndex(i)
		slog.Info("lifecycle: starting", "service", name)
		if err := svc.Start(ctx); err != nil {
			return fmt.Errorf("start %s: %w", name, err)
		}
	}
	return nil
}

func (r *LifecycleRegistry) StopAll(ctx context.Context) error {
	r.mu.Lock()
	svcs := make([]Service, len(r.services))
	copy(svcs, r.services)
	r.mu.Unlock()

	var lastErr error
	for i := len(svcs) - 1; i >= 0; i-- {
		name := r.nameForIndex(i)
		slog.Info("lifecycle: stopping", "service", name)
		if err := svcs[i].Stop(); err != nil {
			slog.Error("lifecycle: stop error", "service", name, "error", err)
			lastErr = err
		}
	}
	return lastErr
}

func (r *LifecycleRegistry) nameForIndex(i int) string {
	for name, idx := range r.names {
		if idx == i {
			return name
		}
	}
	return fmt.Sprintf("service-%d", i)
}
