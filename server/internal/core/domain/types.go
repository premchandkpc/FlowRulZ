// Package domain defines pure domain types for the core logic.
// Zero imports from internal/ — only stdlib allowed.
package domain

import "time"

type ExecutionID string

type ExecutionRecord struct {
	ID          ExecutionID
	PlanID      string
	State       string
	Output      []byte
	Error       string
	CreatedAt   time.Time
	CompletedAt time.Time
}

type ServiceInstance struct {
	ID      string
	Name    string
	Address string
	Healthy bool
	Methods []MethodSpec
	Meta    map[string]string
}

type MethodSpec struct {
	Name   string
	Input  string
	Output string
}

type DeadLetterEntry struct {
	ID        string
	Topic     string
	Payload   []byte
	Error     string
	Timestamp time.Time
}

type SagaStep struct {
	StepID      string
	ServiceName string
	Method      string
	Body        []byte
	CompSvc     string
	CompMethod  string
}

type MetricSnapshot struct {
	Counters map[string]float64
	Gauges   map[string]float64
}

type LeadershipToken struct {
	Term     uint64
	LeaderID string
 Valid    bool
}

type AckMessage struct {
	RuleID  string
	Version uint64
	Status  string
	NodeID  string
}
