package plandist

import (
	"context"
	"errors"
	"time"
)

type PlanMessage struct {
	Type    string
	RuleID  string
	Version uint64
	Term    uint64
	Plan    []byte
	DSL     string
	NodeID  string
}

type AckMessage struct {
	NodeID  string
	RuleID  string
	Version uint64
	Status  string
}

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

type QuorumProvider interface {
	AliveCount() int
}

var (
	ErrNoPlanProducer = errors.New("no plan producer configured")
	ErrAckTimeout     = errors.New("ack wait timed out")
	ErrInsufficientAcks = errors.New("insufficient acks")
)
