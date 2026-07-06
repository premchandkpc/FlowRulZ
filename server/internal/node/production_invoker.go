package node

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/premchandkpc/FlowRulZ/server/internal/reliability"
)

// ProductionInvoker dispatches service calls via protocol-aware HTTP/gRPC/TCP.
type ProductionInvoker struct {
	caller          *ServiceCaller
	registry        ServiceLookup
	circuitBreakers sync.Map
}

// NewProductionInvoker creates a ProductionInvoker with the given registry.
func NewProductionInvoker(reg ServiceLookup) *ProductionInvoker {
	return &ProductionInvoker{
		caller:   NewServiceCaller(),
		registry: reg,
	}
}

func (p *ProductionInvoker) Invoke(ctx context.Context, serviceName, method string, body []byte) ([]byte, error) {
	svcTimeout := 10 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining > 0 && remaining < svcTimeout {
			svcTimeout = remaining
		}
	}
	svcCtx, svcCancel := context.WithTimeout(ctx, svcTimeout)
	defer svcCancel()

	inst, err := p.registry.LookupInstance(serviceName, method)
	if err != nil {
		slog.Warn("registry lookup failed, using passthrough",
			"service", serviceName,
			"method", method,
			"error", err)
		return body, nil
	}

	if inst == nil {
		slog.Info("service call (passthrough)", "service", serviceName, "method", method, "body_bytes", len(body))
		return body, nil
	}

	cbI, _ := p.circuitBreakers.LoadOrStore(serviceName, reliability.NewCircuitBreaker(5, 30*time.Second))
	cb := cbI.(*reliability.CircuitBreaker)
	resp, err := p.caller.CallService(svcCtx, inst, method, body, cb, p.registry)
	if err != nil {
		return nil, fmt.Errorf("service %s: %w", serviceName, err)
	}

	return resp, nil
}

// Close closes all gRPC connections held by the underlying ServiceCaller.
func (p *ProductionInvoker) Close() {
	p.caller.Close()
}
