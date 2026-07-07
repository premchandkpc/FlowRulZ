package reliability

import (
	"context"
	"errors"
	"time"
)

type SagaStep struct {
	Name       string
	Execute    func(context.Context) error
	Compensate func(context.Context) error
	Timeout    time.Duration
}

type SagaStatus struct {
	SagaID    string
	State     string
	Completed []string
	Failed    string
	Error     string
}

type SagaOrchestrator interface {
	Begin(ctx context.Context, sagaID string, steps []SagaStep) error
	ExecuteStep(ctx context.Context, sagaID string, stepName string) error
	Compensate(ctx context.Context, sagaID string) error
	Status(ctx context.Context, sagaID string) (*SagaStatus, error)
}

var ErrSagaFailed = errors.New("saga execution failed")
