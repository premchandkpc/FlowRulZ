package engine

import (
	"time"

	"github.com/premchandkpc/FlowRulZ/server/pkg/scheduler"
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
