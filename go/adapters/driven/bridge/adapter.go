// Package bridgeadapter adapts the bridge FFI to the execution.StepExecutor port.
// This is the adapter that connects the Go domain to the Rust VM via FFI.
package bridgeadapter

import (
	"github.com/premchandkpc/FlowRulZ/go/domain/execution"
	"github.com/premchandkpc/FlowRulZ/server/bridge"
)

// StepExecutor adapts bridge.ExecuteStep to execution.StepExecutor.
type StepExecutor struct{}

// New creates a new StepExecutor adapter.
func New() *StepExecutor {
	return &StepExecutor{}
}

// ExecuteStep runs one VM step via the bridge FFI.
func (e *StepExecutor) ExecuteStep(plan, ctxBytes, respBytes []byte) (execution.StepOutput, error) {
	out, err := bridge.ExecuteStep(plan, ctxBytes, respBytes, nil)
	if err != nil {
		return execution.StepOutput{}, err
	}

	var result execution.StepResult
	switch out.Result {
	case bridge.StepDone:
		result = execution.StepDone
	case bridge.StepPending:
		result = execution.StepPending
	case bridge.StepContinue:
		result = execution.StepContinue
	}

	return execution.StepOutput{
		StepResult:  result,
		CtxBytes:    out.CtxBytes,
		PendingSvc:  out.PendingSvc,
		PendingBody: out.PendingBody,
		TimeoutMs:   out.TimeoutMs,
		Output:      out.Output,
	}, nil
}
