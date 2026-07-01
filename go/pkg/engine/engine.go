package engine

import (
	"context"
	"errors"
	"time"

	"github.com/premchandkpc/FlowRulZ/go/pkg/scheduler"
)

type Rule struct {
	ID      string
	DSL     string
	Version uint64
	Active  bool
	Lane    scheduler.Lane
}

type ExecuteOptions struct {
	Timeout       time.Duration
	CorrelationID string
	ReplyTo       string
	Metadata      map[string]string
}

type Engine interface {
	Start(ctx context.Context) error
	Stop() error
	AddRule(ctx context.Context, rule *Rule) error
	RemoveRule(ctx context.Context, ruleID string) error
	GetRule(ctx context.Context, ruleID string) (*Rule, error)
	ListRules(ctx context.Context) ([]*Rule, error)
	Execute(ctx context.Context, ruleID string, body []byte, opts *ExecuteOptions) (*scheduler.Result, error)
	CompileRule(ctx context.Context, rule *Rule) error
	InvalidateCompilation(ruleID string)
}

var (
	ErrRuleNotFound = errors.New("rule not found")
	ErrCompileFailed = errors.New("rule compilation failed")
)
