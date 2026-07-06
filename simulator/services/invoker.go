package services

import (
	"context"
	"fmt"
)

// ServiceInvoker abstracts service call dispatch.
type ServiceInvoker interface {
	Invoke(ctx context.Context, serviceName, method string, body []byte) ([]byte, error)
}

// ServiceRegistryInvoker adapts a ServiceRegistry to the ServiceInvoker interface.
type ServiceRegistryInvoker struct {
	registry *ServiceRegistry
}

// NewServiceRegistryInvoker creates a ServiceRegistryInvoker.
func NewServiceRegistryInvoker(reg *ServiceRegistry) *ServiceRegistryInvoker {
	return &ServiceRegistryInvoker{registry: reg}
}

func (s *ServiceRegistryInvoker) Invoke(ctx context.Context, serviceName, method string, body []byte) ([]byte, error) {
	svc := s.registry.Get(serviceName)
	if svc == nil {
		return nil, fmt.Errorf("service %s not found", serviceName)
	}

	result := svc.Call(ctx, body)
	if result.Error != nil {
		return nil, result.Error
	}
	return result.Body, nil
}
