package vm

import (
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

var (
	ErrCompileFailed = errors.New("plan compilation failed")
	ErrExecFailed    = errors.New("vm execution failed")
)
