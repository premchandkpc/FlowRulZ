package bridge

import (
	"context"
	"fmt"

	"github.com/premchandkpc/FlowRulZ/server/pkg/vm"
)

type BridgeVM struct{}

var _ vm.PlanCompiler = (*BridgeVM)(nil)
var _ vm.VMRunner = (*BridgeVM)(nil)

func NewBridgeVM() *BridgeVM {
	return &BridgeVM{}
}

// PlanCompiler interface

func (b *BridgeVM) Compile(ctx context.Context, dsl string, ruleID string) (*vm.CompileResult, error) {
	planBytes, err := Compile(dsl, ruleID)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", vm.ErrCompileFailed, err)
	}

	svcEntries, err := PlanServices(planBytes)
	if err != nil {
		return nil, fmt.Errorf("%w: plan services: %w", vm.ErrCompileFailed, err)
	}

	services := make([]string, 0, len(svcEntries))
	for _, e := range svcEntries {
		services = append(services, e.Name)
	}

	complexity := int(PlanComplexity(planBytes))

	return &vm.CompileResult{
		PlanBytes:    planBytes,
		Complexity:   complexity,
		Services:     services,
	}, nil
}

func (b *BridgeVM) CompileAndCache(ctx context.Context, dsl string, ruleID string) (*vm.CompileResult, error) {
	return b.Compile(ctx, dsl, ruleID)
}

func (b *BridgeVM) InvalidateCache(ruleID string) {}

// VMRunner interface

func (b *BridgeVM) InitContext(ctx context.Context, body []byte) ([]byte, error) {
	return InitContext(body)
}

func (b *BridgeVM) ExecuteStep(ctx context.Context, plan, ctxBytes, respBytes []byte, opts *vm.StepOptions) (*vm.StepResult, error) {
	var caller ServiceCaller
	if opts != nil && opts.ServiceCallback != nil {
		cb := opts.ServiceCallback
		caller = func(svcID uint16, body []byte) ([]byte, error) {
			return cb(svcID, body)
		}
	}

	stepOut, err := ExecuteStep(plan, ctxBytes, respBytes, caller)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", vm.ErrExecFailed, err)
	}

	result := &vm.StepResult{
		CtxBytes:    stepOut.CtxBytes,
		Output:      stepOut.Output,
		Error:       stepOut.Error,
		PendingSvc:  stepOut.PendingSvc,
		PendingBody: stepOut.PendingBody,
	}

	switch stepOut.Result {
	case StepDone:
		result.Result = vm.StepDone
	case StepPending:
		result.Result = vm.StepPending
	case StepContinue:
		result.Result = vm.StepContinue
	default:
		result.Result = vm.StepFailed
	}

	return result, nil
}

func (b *BridgeVM) ParseServiceMethod(raw string) (string, string) {
	return ParseServiceMethod(raw)
}
