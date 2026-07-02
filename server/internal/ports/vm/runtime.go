package vm

import "context"

type ServiceCaller func(service, method string, body []byte) ([]byte, error)

type ExecContext struct {
	RuleID      string
	Caller      ServiceCaller
	Lane        string
}

type Runtime interface {
	Execute(ctx context.Context, plan []byte, body []byte, caller ServiceCaller, execCtx *ExecContext) ([]byte, error)
	PlanComplexity(plan []byte) uint32
}
