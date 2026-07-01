package vm

import (
	"context"
	"errors"
	"time"
)

type CompileResult struct {
	PlanBytes    []byte
	Instructions int
	Services     []string
	Complexity   int
	Version      uint64
}

type StepResult struct {
	CtxBytes    []byte
	Output      []byte
	Error       string
	Result      StepCode
	PendingSvc  uint16
	PendingBody []byte
}

type StepCode int

const (
	StepDone     StepCode = iota
	StepPending
	StepContinue
	StepFailed
)

type StepOptions struct {
	MaxSteps        int
	Timeout         time.Duration
	ServiceCallback func(svcID uint16, body []byte) ([]byte, error)
}

type PlanCompiler interface {
	Compile(ctx context.Context, dsl string, ruleID string) (*CompileResult, error)
	CompileAndCache(ctx context.Context, dsl string, ruleID string) (*CompileResult, error)
	InvalidateCache(ruleID string)
}

type VMRunner interface {
	InitContext(ctx context.Context, body []byte) ([]byte, error)
	ExecuteStep(ctx context.Context, plan, ctxBytes, respBytes []byte, opts *StepOptions) (*StepResult, error)
	ParseServiceMethod(raw string) (service string, method string)
}

var (
	ErrCompileFailed = errors.New("plan compilation failed")
	ErrExecFailed    = errors.New("vm execution failed")
	ErrStepTimeout   = errors.New("vm step timed out")
)
