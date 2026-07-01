package plandist

type PlanMessage struct {
	Type    string `json:"type"`
	RuleID  string `json:"rule_id"`
	Version uint64 `json:"version"`
	Term    uint64 `json:"term"`
	Plan    []byte `json:"plan,omitempty"`
	DSL     string `json:"dsl,omitempty"`
	NodeID  string `json:"node_id,omitempty"`
}

type AckMessage struct {
	NodeID  string `json:"node_id"`
	RuleID  string `json:"rule_id"`
	Version uint64 `json:"version"`
	Status  string `json:"status"`
}

type QuorumProvider interface {
	AliveCount() int
}
