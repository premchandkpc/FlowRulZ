// Package execution contains the single, canonical runSteps implementation.
// Both ProdNode (internal/node) and any future node type delegate here.
// The domain knows nothing about Kafka, gRPC, HTTP, or disk — it receives
// ports (ServiceInvoker, StateStore) as constructor arguments.
package execution

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/premchandkpc/FlowRulZ/go/ports"
)

const (
	MaxExecutionSteps = 1000
)

// StepExecutor executes a single VM step. This is a port — the domain
// doesn't know whether it's calling a Rust VM via FFI or a mock for tests.
type StepExecutor interface {
	// ExecuteStep runs one step of the VM.
	ExecuteStep(plan, ctxBytes, respBytes []byte) (StepOutput, error)
}

// StepOutput is the result of a single VM step.
type StepOutput struct {
	StepResult  StepResult
	CtxBytes    []byte
	PendingSvc  uint16
	PendingBody []byte
	TimeoutMs   uint64
	Output      []byte
}

type StepResult int

const (
	StepDone     StepResult = iota
	StepPending
	StepContinue
)

// ServiceNames maps service IDs to names from the plan.
type ServiceNames map[uint16]string

// Runner orchestrates the execution of a compiled plan through the VM.
// It depends only on ports — no concrete adapter imports.
type Runner struct {
	invoker    ports.ServiceInvoker
	store      ports.StateStore
	executor   StepExecutor
	saga       ports.SagaCompensator
	compensate func(execID string) // callback to trigger compensation
}

// NewRunner creates an execution Runner.
func NewRunner(
	invoker ports.ServiceInvoker,
	store ports.StateStore,
	executor StepExecutor,
	saga ports.SagaCompensator,
	compensate func(execID string),
) *Runner {
	return &Runner{
		invoker:    invoker,
		store:      store,
		executor:   executor,
		saga:       saga,
		compensate: compensate,
	}
}

// ExecutePlan runs a compiled plan to completion, calling services as needed.
func (r *Runner) ExecutePlan(ctx context.Context, execID string, plan []byte, names ServiceNames) ([]byte, error) {
	return r.runSteps(ctx, execID, plan, names, nil, nil)
}

// runSteps is the core execution loop — the ONE canonical implementation.
func (r *Runner) runSteps(ctx context.Context, execID string, plan []byte, names ServiceNames, startCtx, startResp []byte) ([]byte, error) {
	ctxBytes, respBytes := startCtx, startResp

	for step := 0; step < MaxExecutionSteps; step++ {
		select {
		case <-ctx.Done():
			if r.compensate != nil {
				r.compensate(execID)
			}
			return nil, fmt.Errorf("execution cancelled at step %d: %w", step, ctx.Err())
		default:
		}

		out, err := r.executor.ExecuteStep(plan, ctxBytes, respBytes)
		if err != nil {
			if r.compensate != nil {
				r.compensate(execID)
			}
			return nil, fmt.Errorf("step %d: %w", step, err)
		}

		ctxBytes = out.CtxBytes

		switch out.StepResult {
		case StepDone:
			if r.saga != nil {
				r.saga.Clear(execID)
			}
			return out.Output, nil

		case StepPending:
			if r.store != nil {
				r.store.SavePending(ctx, execID, out.PendingSvc, out.PendingBody, ctxBytes)
			}

			rawName, ok := names[out.PendingSvc]
			if !ok {
				rawName = fmt.Sprintf("svc-%d", out.PendingSvc)
			}
			svcName, method, compSvc, compMethod := ParseCompensation(rawName)

			if r.saga != nil && compSvc != "" {
				r.saga.RegisterStep(execID, ports.SagaStep{
					ServiceName: svcName,
					Method:      method,
					Body:        out.PendingBody,
					CompSvc:     compSvc,
					CompMethod:  compMethod,
				})
			}

			resp, err := r.invoker.Invoke(ctx, svcName, method, out.PendingBody)
			if err != nil {
				if r.compensate != nil {
					r.compensate(execID)
				}
				return nil, fmt.Errorf("service %s: %w", svcName, err)
			}

			if r.store != nil {
				r.store.SaveRunning(ctx, execID, ctxBytes)
			}
			respBytes = resp

		case StepContinue:
			respBytes = nil
			if r.store != nil {
				r.store.SaveRunning(ctx, execID, ctxBytes)
			}
		}
	}

	if r.compensate != nil {
		r.compensate(execID)
	}
	return nil, fmt.Errorf("execution exceeded max steps")
}

// ParseCompensation extracts service name, method, and compensation info
// from a service name string (e.g. "OrderService.CreateOrder[OrderCompensator.CompensateOrder]").
func ParseCompensation(rawName string) (svcName, method, compSvc, compMethod string) {
	start := -1
	end := -1
	for i, c := range rawName {
		if c == '[' {
			start = i
		} else if c == ']' {
			end = i
			break
		}
	}

	svcMethod := rawName
	compFull := ""
	if start >= 0 && end > start {
		svcMethod = rawName[:start]
		compFull = rawName[start+1 : end]
	}

	slash1 := -1
	for i, c := range svcMethod {
		if c == '/' {
			slash1 = i
			break
		}
	}
	if slash1 >= 0 {
		svcName = svcMethod[:slash1]
		method = svcMethod[slash1+1:]
	} else {
		svcName = svcMethod
	}

	if compFull != "" {
		slash2 := -1
		for i, c := range compFull {
			if c == '/' {
				slash2 = i
				break
			}
		}
		if slash2 >= 0 {
			compSvc = compFull[:slash2]
			compMethod = compFull[slash2+1:]
		} else {
			compSvc = compFull
		}
	}

	slog.Debug("parsed compensation",
		"raw", rawName,
		"svc", svcName,
		"method", method,
		"comp_svc", compSvc,
		"comp_method", compMethod)

	return svcName, method, compSvc, compMethod
}
