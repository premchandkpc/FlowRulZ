package vm

import "context"

type Compiler interface {
	Compile(ctx context.Context, dsl string, ruleID string) (*CompileResult, error)
}

type CompileResult struct {
	Plan       []byte
	DSL        string
	Version    uint64
	Complexity uint32
}
