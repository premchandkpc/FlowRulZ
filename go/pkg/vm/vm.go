package vm

import "context"

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
