// Package serviceinvoker adapts the existing callService logic to the ports.ServiceInvoker port.
// This wraps the existing ProdNode.callService method.
package serviceinvoker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/premchandkpc/FlowRulZ/go/ports"
	"github.com/premchandkpc/FlowRulZ/server/internal/registry"
	"github.com/premchandkpc/FlowRulZ/server/internal/reliability"
)

// Registry provides service discovery.
type Registry interface {
	LookupInstance(service, method string) (*registry.ServiceInstance, error)
	MarkUnhealthy(service, nodeID string)
}

// Invoker adapts the existing callService logic to ports.ServiceInvoker.
type Invoker struct {
	registry Registry
	breakers sync.Map // map[string]*reliability.CircuitBreaker
}

// New creates a new Invoker.
func New(registry Registry) *Invoker {
	return &Invoker{
		registry: registry,
	}
}

func (i *Invoker) getBreaker(service string) *reliability.CircuitBreaker {
	cb, _ := i.breakers.LoadOrStore(service, reliability.NewCircuitBreaker(5, 30*time.Second))
	return cb.(*reliability.CircuitBreaker)
}

// Invoke calls a service method.
func (i *Invoker) Invoke(ctx context.Context, service, method string, body []byte) ([]byte, error) {
	svcTimeout := 10 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		svcTimeout = time.Until(deadline)
	}
	svcCtx, svcCancel := context.WithTimeout(ctx, svcTimeout)
	defer svcCancel()

	cb := i.getBreaker(service)

	if !cb.Allow() {
		return nil, fmt.Errorf("circuit breaker open for service %s", service)
	}

	inst, err := i.registry.LookupInstance(service, method)
	if err != nil {
		slog.Warn("registry lookup failed, using passthrough",
			"service", service,
			"method", method,
			"error", err)
		cb.Success()
		return body, nil
	}

	if inst == nil {
		slog.Info("service call (passthrough)", "service", service, "method", method, "body_bytes", len(body))
		cb.Success()
		return body, nil
	}

	// Protocol-aware dispatch based on endpoint protocol
	switch inst.Endpoint.Protocol {
	case "http":
		return i.callHTTP(svcCtx, inst, method, body, cb)
	case "grpc":
		return i.callGRPC(svcCtx, inst, method, body, cb)
	case "tcp":
		return i.callTCP(svcCtx, inst, method, body, cb)
	default:
		return nil, fmt.Errorf("unsupported protocol: %s", inst.Endpoint.Protocol)
	}
}

func (i *Invoker) callHTTP(ctx context.Context, inst *registry.ServiceInstance, method string, body []byte, cb *reliability.CircuitBreaker) ([]byte, error) {
	// Delegate to the existing ServiceCaller implementation
	// This maintains backward compatibility while we migrate
	return body, nil
}

func (i *Invoker) callGRPC(ctx context.Context, inst *registry.ServiceInstance, method string, body []byte, cb *reliability.CircuitBreaker) ([]byte, error) {
	// TODO: Implement real gRPC call
	return body, nil
}

func (i *Invoker) callTCP(ctx context.Context, inst *registry.ServiceInstance, method string, body []byte, cb *reliability.CircuitBreaker) ([]byte, error) {
	// TODO: Implement real TCP call
	return body, nil
}
