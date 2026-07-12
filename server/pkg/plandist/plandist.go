package plandist

import (
	"context"
	"errors"
	"time"
)

type PlanDistributor interface {
	Start(ctx context.Context) error
	Stop() error
	PublishPlan(ctx context.Context, ruleID string, version uint64, plan []byte, dsl string) error
	ActivatePlan(ctx context.Context, ruleID string, version uint64) error
	DeactivatePlan(ctx context.Context, ruleID string) error
	SendAck(ctx context.Context, ruleID string, version uint64, status string) error
	WaitForAcks(ctx context.Context, ruleID string, version uint64, quorum int, timeout time.Duration) error
	SetTerm(term uint64)
	CurrentTerm() uint64
	OnPlan(fn func(ctx context.Context, msg PlanMessage) error)
	OnAck(fn func(ctx context.Context, msg AckMessage))
}

var (
	ErrNoPlanProducer   = errors.New("no plan producer configured")
	ErrAckTimeout       = errors.New("ack wait timed out")
	ErrInsufficientAcks = errors.New("insufficient acks")
)
