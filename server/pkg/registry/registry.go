package registry

import (
	"context"
	"errors"
)

type Registry interface {
	Register(ctx context.Context, svc *ServiceRegistration) error
	Unregister(ctx context.Context, name string) error
	Lookup(ctx context.Context, name string) (*ServiceInstance, error)
	LookupMultiple(ctx context.Context, names []string) ([]*ServiceInstance, error)
	ListServices(ctx context.Context) ([]*ServiceRegistration, error)
	HealthCheck(ctx context.Context, name string) (bool, error)
	SubscribeChanges(ctx context.Context, pattern string) (<-chan RegistryEvent, error)
}

var (
	ErrServiceNotFound    = errors.New("service not found")
	ErrServiceUnavailable = errors.New("service unavailable")
)
