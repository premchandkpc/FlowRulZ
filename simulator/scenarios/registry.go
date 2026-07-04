package scenarios

import (
	"fmt"
	"time"
)

// ScenarioDef defines a simulation scenario (descriptive, not executable).
type ScenarioDef struct {
	Name        string        `json:"name"`
	Category    string        `json:"category"`
	Description string        `json:"description"`
	Mode        string        `json:"mode"` // "simple", "enterprise", "chaos", etc.
	Steps       []Step        `json:"steps"`
	Duration    time.Duration `json:"duration"`
}

// Step represents a single step in a scenario.
type Step struct {
	Name        string        `json:"name"`
	Type        string        `json:"type"` // "publish", "deploy", "kill", "delay", "wait", "observe"
	Service     string        `json:"service,omitempty"`
	Rule        string        `json:"rule,omitempty"`
	Duration    time.Duration `json:"duration,omitempty"`
	Condition   string        `json:"condition,omitempty"` // what to observe
	Expectation string        `json:"expectation,omitempty"`
}

// AllDefs contains every built-in scenario definition (50+).
var AllDefs = buildScenarioDefs()

func buildScenarioDefs() []ScenarioDef {
	return []ScenarioDef{
		// ═══════════════════════════════════════════════════════════════════
		// BUSINESS SCENARIOS (10)
		// ═══════════════════════════════════════════════════════════════════
		{
			Name:        "order-placement",
			Category:    "business",
			Description: "Complete order flow — order → payment → inventory → shipping → notification",
			Mode:        "enterprise",
			Steps: []Step{
				{Name: "Create order", Type: "publish", Service: "order"},
				{Name: "Authorize payment", Type: "publish", Service: "payment"},
				{Name: "Reserve inventory", Type: "publish", Service: "inventory"},
				{Name: "Schedule shipping", Type: "publish", Service: "shipping"},
				{Name: "Send confirmation", Type: "publish", Service: "notification"},
				{Name: "Observe completion", Type: "observe", Duration: 2 * time.Second},
			},
			Duration: 10 * time.Second,
		},
		{
			Name:        "order-cancellation",
			Category:    "business",
			Description: "Cancel order → release inventory → refund payment → notify customer",
			Mode:        "enterprise",
			Steps: []Step{
				{Name: "Cancel order", Type: "publish", Service: "order"},
				{Name: "Release inventory", Type: "publish", Service: "inventory"},
				{Name: "Process refund", Type: "publish", Service: "refund"},
				{Name: "Notify customer", Type: "publish", Service: "notification"},
				{Name: "Observe completion", Type: "observe", Duration: 2 * time.Second},
			},
			Duration: 10 * time.Second,
		},
		{
			Name:        "refund-processing",
			Category:    "business",
			Description: "Refund workflow with approval and audit trail",
			Mode:        "enterprise",
			Steps: []Step{
				{Name: "Request refund", Type: "publish", Service: "refund"},
				{Name: "Check fraud", Type: "publish", Service: "fraud"},
				{Name: "Approve refund", Type: "publish", Service: "refund"},
				{Name: "Process payment refund", Type: "publish", Service: "payment"},
				{Name: "Update inventory", Type: "publish", Service: "inventory"},
				{Name: "Send refund receipt", Type: "publish", Service: "notification"},
				{Name: "Audit trail", Type: "publish", Service: "audit"},
				{Name: "Observe completion", Type: "observe", Duration: 2 * time.Second},
			},
			Duration: 15 * time.Second,
		},
		{
			Name:        "bulk-order",
			Category:    "business",
			Description: "High-volume bulk order processing with batch inventory reservation",
			Mode:        "enterprise",
			Steps: []Step{
				{Name: "Bulk order items", Type: "publish", Service: "order"},
				{Name: "Batch inventory check", Type: "publish", Service: "inventory"},
				{Name: "Bulk pricing calculate", Type: "publish", Service: "pricing"},
				{Name: "Bulk tax calculation", Type: "publish", Service: "tax"},
				{Name: "Batch payment", Type: "publish", Service: "payment"},
				{Name: "Bulk shipping schedule", Type: "publish", Service: "shipping"},
				{Name: "Batch invoice generate", Type: "publish", Service: "invoice"},
				{Name: "Observe completion", Type: "observe", Duration: 3 * time.Second},
			},
			Duration: 20 * time.Second,
		},
		{
			Name:        "flash-sale",
			Category:    "business",
			Description: "Flash sale with 10K concurrent requests hitting inventory",
			Mode:        "performance",
			Steps: []Step{
				{Name: "Launch flash sale", Type: "publish", Service: "pricing"},
				{Name: "High concurrent orders", Type: "publish", Service: "order"},
				{Name: "Race condition on inventory", Type: "publish", Service: "inventory"},
				{Name: "Queue overflow test", Type: "observe", Duration: 5 * time.Second},
				{Name: "Observe backpressure", Type: "observe", Duration: 2 * time.Second},
			},
			Duration: 30 * time.Second,
		},
		{
			Name:        "subscription-renewal",
			Category:    "business",
			Description: "Monthly subscription renewal — billing → payment → invoice → notification",
			Mode:        "enterprise",
			Steps: []Step{
				{Name: "Trigger renewal", Type: "publish", Service: "billing"},
				{Name: "Charge payment", Type: "publish", Service: "payment"},
				{Name: "Generate invoice", Type: "publish", Service: "invoice"},
				{Name: "Update subscription", Type: "publish", Service: "billing"},
				{Name: "Send renewal receipt", Type: "publish", Service: "notification"},
				{Name: "Update analytics", Type: "publish", Service: "analytics"},
				{Name: "Observe completion", Type: "observe", Duration: 2 * time.Second},
			},
			Duration: 15 * time.Second,
		},
		{
			Name:        "payment-failure",
			Category:    "business",
			Description: "Payment fails after 3 retries — triggers compensation workflow",
			Mode:        "enterprise",
			Steps: []Step{
				{Name: "Create order", Type: "publish", Service: "order"},
				{Name: "Attempt payment (fails)", Type: "publish", Service: "payment"},
				{Name: "Retry payment (fails)", Type: "publish", Service: "payment"},
				{Name: "Retry payment (fails)", Type: "publish", Service: "payment"},
				{Name: "Trigger compensation", Type: "publish", Service: "order"},
				{Name: "Release inventory", Type: "publish", Service: "inventory"},
				{Name: "Notify failure", Type: "publish", Service: "notification"},
				{Name: "Observe DLQ entry", Type: "observe", Duration: 2 * time.Second},
			},
			Duration: 15 * time.Second,
		},
		{
			Name:        "inventory-shortage",
			Category:    "business",
			Description: "Inventory unavailable — partial fulfillment with backorder",
			Mode:        "enterprise",
			Steps: []Step{
				{Name: "Create multi-item order", Type: "publish", Service: "order"},
				{Name: "Check item 1 (in stock)", Type: "publish", Service: "inventory"},
				{Name: "Check item 2 (out of stock)", Type: "publish", Service: "inventory"},
				{Name: "Split order", Type: "publish", Service: "order"},
				{Name: "Ship available items", Type: "publish", Service: "shipping"},
				{Name: "Backorder unavailable", Type: "publish", Service: "order"},
				{Name: "Notify customer", Type: "publish", Service: "notification"},
				{Name: "Observe completion", Type: "observe", Duration: 2 * time.Second},
			},
			Duration: 15 * time.Second,
		},
		{
			Name:        "customer-support-ticket",
			Category:    "business",
			Description: "Customer creates support ticket — AI classifies, routes, resolves",
			Mode:        "enterprise",
			Steps: []Step{
				{Name: "Create ticket", Type: "publish", Service: "support"},
				{Name: "AI classify", Type: "publish", Service: "ai"},
				{Name: "Route to agent", Type: "publish", Service: "support"},
				{Name: "Lookup customer", Type: "publish", Service: "customer"},
				{Name: "Resolve ticket", Type: "publish", Service: "support"},
				{Name: "Send resolution email", Type: "publish", Service: "email"},
				{Name: "Update analytics", Type: "publish", Service: "analytics"},
				{Name: "Observe completion", Type: "observe", Duration: 2 * time.Second},
			},
			Duration: 15 * time.Second,
		},
		{
			Name:        "product-recommendation",
			Category:    "business",
			Description: "Personalized recommendation flow — user profile → history → AI → catalog",
			Mode:        "enterprise",
			Steps: []Step{
				{Name: "Get user profile", Type: "publish", Service: "profile"},
				{Name: "Get purchase history", Type: "publish", Service: "order"},
				{Name: "AI personalize", Type: "publish", Service: "ai"},
				{Name: "Query catalog", Type: "publish", Service: "catalog"},
				{Name: "Rank recommendations", Type: "publish", Service: "recommendation"},
				{Name: "Return results", Type: "publish", Service: "search"},
				{Name: "Observe completion", Type: "observe", Duration: 2 * time.Second},
			},
			Duration: 15 * time.Second,
		},

		// ═══════════════════════════════════════════════════════════════════
		// RELIABILITY SCENARIOS (8)
		// ═══════════════════════════════════════════════════════════════════
		{
			Name:        "retry-success",
			Category:    "reliability",
			Description: "Payment fails twice then succeeds on third attempt",
			Mode:        "enterprise",
			Steps: []Step{
				{Name: "First attempt (fails)", Type: "publish", Service: "payment"},
				{Name: "Second attempt (fails)", Type: "publish", Service: "payment"},
				{Name: "Third attempt (succeeds)", Type: "publish", Service: "payment"},
				{Name: "Observe success", Type: "observe", Duration: 2 * time.Second},
			},
			Duration: 10 * time.Second,
		},
		{
			Name:        "retry-exhausted",
			Category:    "reliability",
			Description: "Payment fails all 3 retries — enters DLQ",
			Mode:        "enterprise",
			Steps: []Step{
				{Name: "First attempt (fails)", Type: "publish", Service: "payment"},
				{Name: "Second attempt (fails)", Type: "publish", Service: "payment"},
				{Name: "Third attempt (fails)", Type: "publish", Service: "payment"},
				{Name: "Enter DLQ", Type: "observe", Duration: 2 * time.Second},
			},
			Duration: 10 * time.Second,
		},
		{
			Name:        "circuit-breaker-opens",
			Category:    "reliability",
			Description: "Payment gateway fails repeatedly → circuit opens → fallback activated",
			Mode:        "enterprise",
			Steps: []Step{
				{Name: "Payment fails 5 times", Type: "publish", Service: "payment"},
				{Name: "Circuit opens", Type: "observe", Duration: 1 * time.Second},
				{Name: "All requests fallback", Type: "publish", Service: "payment"},
				{Name: "Observe circuit open state", Type: "observe", Duration: 2 * time.Second},
			},
			Duration: 15 * time.Second,
		},
		{
			Name:        "timeout-handling",
			Category:    "reliability",
			Description: "Service exceeds configured timeout — context cancelled",
			Mode:        "enterprise",
			Steps: []Step{
				{Name: "Call slow service", Type: "publish", Service: "payment"},
				{Name: "Timeout at 2s", Type: "observe", Duration: 2 * time.Second},
				{Name: "Context cancelled", Type: "observe", Duration: 1 * time.Second},
			},
			Duration: 10 * time.Second,
		},
		{
			Name:        "dlq-replay",
			Category:    "reliability",
			Description: "Replay messages from Dead Letter Queue after fixing downstream",
			Mode:        "enterprise",
			Steps: []Step{
				{Name: "Messages enter DLQ", Type: "publish", Service: "payment"},
				{Name: "Fix downstream", Type: "deploy", Service: "payment"},
				{Name: "Replay DLQ", Type: "publish", Service: "payment"},
				{Name: "Observe messages processed", Type: "observe", Duration: 3 * time.Second},
			},
			Duration: 15 * time.Second,
		},
		{
			Name:        "compensation-workflow",
			Category:    "reliability",
			Description: "Saga pattern — compensate on failure, rollback in reverse order",
			Mode:        "enterprise",
			Steps: []Step{
				{Name: "Order created", Type: "publish", Service: "order"},
				{Name: "Payment captured", Type: "publish", Service: "payment"},
				{Name: "Inventory reserved", Type: "publish", Service: "inventory"},
				{Name: "Shipping fails", Type: "publish", Service: "shipping"},
				{Name: "Compensate inventory", Type: "publish", Service: "inventory"},
				{Name: "Compensate payment", Type: "publish", Service: "payment"},
				{Name: "Cancel order", Type: "publish", Service: "order"},
				{Name: "Observe compensation complete", Type: "observe", Duration: 2 * time.Second},
			},
			Duration: 20 * time.Second,
		},
		{
			Name:        "cascading-failure",
			Category:    "reliability",
			Description: "Database failure cascades to all dependent services",
			Mode:        "chaos",
			Steps: []Step{
				{Name: "Kill database", Type: "kill", Service: "database"},
				{Name: "Customer service fails", Type: "observe", Duration: 2 * time.Second},
				{Name: "Order service fails", Type: "observe", Duration: 2 * time.Second},
				{Name: "Payment service fails", Type: "observe", Duration: 2 * time.Second},
				{Name: "Observe circuit breakers trip", Type: "observe", Duration: 3 * time.Second},
			},
			Duration: 20 * time.Second,
		},
		{
			Name:        "rate-limit-throttle",
			Category:    "reliability",
			Description: "Rate limiter triggers at 1K TPS — requests queued",
			Mode:        "performance",
			Steps: []Step{
				{Name: "Send 2K TPS", Type: "publish", Service: "order"},
				{Name: "Rate limiter triggers", Type: "observe", Duration: 2 * time.Second},
				{Name: "Requests queued", Type: "observe", Duration: 3 * time.Second},
				{Name: "Backpressure applied", Type: "observe", Duration: 2 * time.Second},
			},
			Duration: 15 * time.Second,
		},

		// ═══════════════════════════════════════════════════════════════════
		// DISTRIBUTED SYSTEMS SCENARIOS (8)
		// ═══════════════════════════════════════════════════════════════════
		{
			Name:        "leader-election",
			Category:    "distributed",
			Description: "Leader node dies → new leader elected → cluster continues",
			Mode:        "distributed",
			Steps: []Step{
				{Name: "Start 3-node cluster", Type: "publish", Service: "order"},
				{Name: "Kill leader", Type: "kill", Service: "database"},
				{Name: "New leader elected", Type: "observe", Duration: 3 * time.Second},
				{Name: "Cluster continues", Type: "observe", Duration: 5 * time.Second},
			},
			Duration: 30 * time.Second,
		},
		{
			Name:        "node-joins",
			Category:    "distributed",
			Description: "New node joins cluster — plan redistribution",
			Mode:        "distributed",
			Steps: []Step{
				{Name: "Start 2-node cluster", Type: "publish", Service: "order"},
				{Name: "Third node joins", Type: "publish", Service: "order"},
				{Name: "Plans redistributed", Type: "observe", Duration: 3 * time.Second},
				{Name: "All nodes consistent", Type: "observe", Duration: 5 * time.Second},
			},
			Duration: 30 * time.Second,
		},
		{
			Name:        "node-leaves",
			Category:    "distributed",
			Description: "Node gracefully leaves cluster — work redistributed",
			Mode:        "distributed",
			Steps: []Step{
				{Name: "Start 3-node cluster", Type: "publish", Service: "order"},
				{Name: "Node leaves gracefully", Type: "kill", Service: "order"},
				{Name: "Work redistributed", Type: "observe", Duration: 3 * time.Second},
				{Name: "Cluster stable", Type: "observe", Duration: 5 * time.Second},
			},
			Duration: 30 * time.Second,
		},
		{
			Name:        "network-partition",
			Category:    "distributed",
			Description: "Network split — cluster partitions, heals, reconciles",
			Mode:        "distributed",
			Steps: []Step{
				{Name: "Start 3-node cluster", Type: "publish", Service: "order"},
				{Name: "Network partition (2 vs 1)", Type: "kill", Service: "order"},
				{Name: "Minority partition isolated", Type: "observe", Duration: 5 * time.Second},
				{Name: "Partition heals", Type: "observe", Duration: 5 * time.Second},
				{Name: "State reconciled", Type: "observe", Duration: 3 * time.Second},
			},
			Duration: 45 * time.Second,
		},
		{
			Name:        "split-brain",
			Category:    "distributed",
			Description: "Split-brain simulation — two leaders, then resolution",
			Mode:        "distributed",
			Steps: []Step{
				{Name: "Start 3-node cluster", Type: "publish", Service: "order"},
				{Name: "Network partition", Type: "kill", Service: "order"},
				{Name: "Both sides elect leader", Type: "observe", Duration: 5 * time.Second},
				{Name: "Partition heals", Type: "observe", Duration: 3 * time.Second},
				{Name: "One leader resigns", Type: "observe", Duration: 3 * time.Second},
				{Name: "Consensus reached", Type: "observe", Duration: 5 * time.Second},
			},
			Duration: 60 * time.Second,
		},
		{
			Name:        "partition-rebalancing",
			Category:    "distributed",
			Description: "Key-space shard rebalancing across nodes",
			Mode:        "distributed",
			Steps: []Step{
				{Name: "Start 2-node cluster", Type: "publish", Service: "order"},
				{Name: "Add third node", Type: "publish", Service: "order"},
				{Name: "Rebalancing starts", Type: "observe", Duration: 3 * time.Second},
				{Name: "Shards redistributed", Type: "observe", Duration: 10 * time.Second},
				{Name: "All shards assigned", Type: "observe", Duration: 5 * time.Second},
			},
			Duration: 60 * time.Second,
		},
		{
			Name:        "cross-cluster-plan-distribution",
			Category:    "distributed",
			Description: "Deploy rule to one cluster — propagates to others",
			Mode:        "distributed",
			Steps: []Step{
				{Name: "Deploy rule to cluster A", Type: "deploy", Service: "order"},
				{Name: "Rule propagates to cluster B", Type: "observe", Duration: 3 * time.Second},
				{Name: "Rule propagates to cluster C", Type: "observe", Duration: 3 * time.Second},
				{Name: "All clusters execute rule", Type: "observe", Duration: 5 * time.Second},
			},
			Duration: 30 * time.Second,
		},
		{
			Name:        "multi-region-failover",
			Category:    "distributed",
			Description: "US region fails — traffic fails over to Europe",
			Mode:        "multi-region",
			Steps: []Step{
				{Name: "US region healthy", Type: "publish", Service: "order"},
				{Name: "US region fails", Type: "kill", Service: "database"},
				{Name: "Traffic routes to Europe", Type: "observe", Duration: 5 * time.Second},
				{Name: "Europe handles requests", Type: "observe", Duration: 5 * time.Second},
				{Name: "US region recovers", Type: "observe", Duration: 5 * time.Second},
				{Name: "Traffic rebalances", Type: "observe", Duration: 5 * time.Second},
			},
			Duration: 60 * time.Second,
		},

		// ═══════════════════════════════════════════════════════════════════
		// METADATA SCENARIOS (6)
		// ═══════════════════════════════════════════════════════════════════
		{
			Name:        "live-policy-update",
			Category:    "metadata",
			Description: "Update retry policy at runtime — no restart",
			Mode:        "enterprise",
			Steps: []Step{
				{Name: "Current policy: retry 3", Type: "observe", Duration: 2 * time.Second},
				{Name: "Publish new policy: retry 5", Type: "deploy", Service: "payment"},
				{Name: "FlowRulZ updates", Type: "observe", Duration: 2 * time.Second},
				{Name: "Services update", Type: "observe", Duration: 2 * time.Second},
				{Name: "New retry policy active", Type: "observe", Duration: 2 * time.Second},
			},
			Duration: 20 * time.Second,
		},
		{
			Name:        "dto-schema-evolution",
			Category:    "metadata",
			Description: "Add new field to PaymentRequest — backward compatible",
			Mode:        "enterprise",
			Steps: []Step{
				{Name: "Current schema: PaymentRequest v1", Type: "observe", Duration: 2 * time.Second},
				{Name: "Publish schema v2 (add currency field)", Type: "deploy", Service: "payment"},
				{Name: "SDKs regenerate DTOs", Type: "observe", Duration: 3 * time.Second},
				{Name: "v1 clients still work", Type: "observe", Duration: 2 * time.Second},
				{Name: "v2 clients use new field", Type: "observe", Duration: 2 * time.Second},
			},
			Duration: 20 * time.Second,
		},
		{
			Name:        "version-rollback",
			Category:    "metadata",
			Description: "Deploy v2, then rollback to v1",
			Mode:        "enterprise",
			Steps: []Step{
				{Name: "Deploy version 2", Type: "deploy", Service: "payment"},
				{Name: "Version 2 active", Type: "observe", Duration: 3 * time.Second},
				{Name: "Rollback to version 1", Type: "deploy", Service: "payment"},
				{Name: "Version 1 restored", Type: "observe", Duration: 3 * time.Second},
			},
			Duration: 20 * time.Second,
		},
		{
			Name:        "canary-rollout",
			Category:    "metadata",
			Description: "Canary deploy — 10% traffic to v2, then 100%",
			Mode:        "enterprise",
			Steps: []Step{
				{Name: "Deploy v2 to 10% traffic", Type: "deploy", Service: "payment"},
				{Name: "Observe canary metrics", Type: "observe", Duration: 5 * time.Second},
				{Name: "Promote to 100%", Type: "deploy", Service: "payment"},
				{Name: "All traffic on v2", Type: "observe", Duration: 3 * time.Second},
			},
			Duration: 30 * time.Second,
		},
		{
			Name:        "blue-green-deployment",
			Category:    "metadata",
			Description: "Blue-green deploy — instant cutover",
			Mode:        "enterprise",
			Steps: []Step{
				{Name: "Blue environment active", Type: "observe", Duration: 2 * time.Second},
				{Name: "Deploy to green", Type: "deploy", Service: "payment"},
				{Name: "Cutover to green", Type: "deploy", Service: "payment"},
				{Name: "Green active", Type: "observe", Duration: 2 * time.Second},
				{Name: "Blue decommissioned", Type: "observe", Duration: 2 * time.Second},
			},
			Duration: 20 * time.Second,
		},
		{
			Name:        "feature-flag-toggle",
			Category:    "metadata",
			Description: "Toggle feature flag — enable/disable feature at runtime",
			Mode:        "enterprise",
			Steps: []Step{
				{Name: "Feature flag disabled", Type: "observe", Duration: 2 * time.Second},
				{Name: "Enable feature flag", Type: "deploy", Service: "order"},
				{Name: "Feature active", Type: "observe", Duration: 3 * time.Second},
				{Name: "Disable feature flag", Type: "deploy", Service: "order"},
				{Name: "Feature disabled", Type: "observe", Duration: 2 * time.Second},
			},
			Duration: 20 * time.Second,
		},

		// ═══════════════════════════════════════════════════════════════════
		// PERFORMANCE SCENARIOS (6)
		// ═══════════════════════════════════════════════════════════════════
		{
			Name:        "throughput-1k",
			Category:    "performance",
			Description: "1,000 TPS sustained load",
			Mode:        "performance",
			Steps: []Step{
				{Name: "Ramp to 1K TPS", Type: "publish", Service: "order"},
				{Name: "Sustain 1K TPS", Type: "observe", Duration: 30 * time.Second},
				{Name: "Measure latency", Type: "observe", Duration: 5 * time.Second},
			},
			Duration: 60 * time.Second,
		},
		{
			Name:        "throughput-10k",
			Category:    "performance",
			Description: "10,000 TPS sustained load",
			Mode:        "performance",
			Steps: []Step{
				{Name: "Ramp to 10K TPS", Type: "publish", Service: "order"},
				{Name: "Sustain 10K TPS", Type: "observe", Duration: 30 * time.Second},
				{Name: "Measure latency", Type: "observe", Duration: 5 * time.Second},
			},
			Duration: 60 * time.Second,
		},
		{
			Name:        "throughput-100k",
			Category:    "performance",
			Description: "100,000 TPS sustained load",
			Mode:        "performance",
			Steps: []Step{
				{Name: "Ramp to 100K TPS", Type: "publish", Service: "order"},
				{Name: "Sustain 100K TPS", Type: "observe", Duration: 30 * time.Second},
				{Name: "Measure latency", Type: "observe", Duration: 5 * time.Second},
			},
			Duration: 90 * time.Second,
		},
		{
			Name:        "backpressure",
			Category:    "performance",
			Description: "Slow downstream triggers backpressure",
			Mode:        "performance",
			Steps: []Step{
				{Name: "Normal load", Type: "publish", Service: "order"},
				{Name: "Slow payment gateway", Type: "delay", Service: "payment"},
				{Name: "Queue fills", Type: "observe", Duration: 5 * time.Second},
				{Name: "Backpressure applied", Type: "observe", Duration: 5 * time.Second},
			},
			Duration: 30 * time.Second,
		},
		{
			Name:        "queue-saturation",
			Category:    "performance",
			Description: "Scheduler queue fills to capacity",
			Mode:        "performance",
			Steps: []Step{
				{Name: "Send burst", Type: "publish", Service: "order"},
				{Name: "Queue fills", Type: "observe", Duration: 3 * time.Second},
				{Name: "Workers saturated", Type: "observe", Duration: 5 * time.Second},
				{Name: "New requests rejected", Type: "observe", Duration: 3 * time.Second},
			},
			Duration: 30 * time.Second,
		},
		{
			Name:        "worker-starvation",
			Category:    "performance",
			Description: "All workers busy — new tasks starved",
			Mode:        "performance",
			Steps: []Step{
				{Name: "Send long-running tasks", Type: "publish", Service: "payment"},
				{Name: "Workers all busy", Type: "observe", Duration: 3 * time.Second},
				{Name: "New tasks waiting", Type: "observe", Duration: 5 * time.Second},
				{Name: "Work stealing activates", Type: "observe", Duration: 3 * time.Second},
			},
			Duration: 30 * time.Second,
		},

		// ═══════════════════════════════════════════════════════════════════
		// CHAOS SCENARIOS (8)
		// ═══════════════════════════════════════════════════════════════════
		{
			Name:        "kill-runtime",
			Category:    "chaos",
			Description: "Kill FlowRulZ runtime process — observe recovery",
			Mode:        "chaos",
			Steps: []Step{
				{Name: "Runtime running", Type: "observe", Duration: 2 * time.Second},
				{Name: "Kill runtime", Type: "kill", Service: "database"},
				{Name: "Observe failure", Type: "observe", Duration: 3 * time.Second},
				{Name: "Runtime restarts", Type: "observe", Duration: 5 * time.Second},
				{Name: "State recovered", Type: "observe", Duration: 3 * time.Second},
			},
			Duration: 30 * time.Second,
		},
		{
			Name:        "crash-service",
			Category:    "chaos",
			Description: "Crash a single service — observe retry and fallback",
			Mode:        "chaos",
			Steps: []Step{
				{Name: "Normal operation", Type: "observe", Duration: 2 * time.Second},
				{Name: "Crash payment service", Type: "kill", Service: "payment"},
				{Name: "Retries activate", Type: "observe", Duration: 3 * time.Second},
				{Name: "Fallback used", Type: "observe", Duration: 2 * time.Second},
				{Name: "Service recovers", Type: "observe", Duration: 5 * time.Second},
			},
			Duration: 30 * time.Second,
		},
		{
			Name:        "corrupt-metadata",
			Category:    "chaos",
			Description: "Corrupt metadata — observe validation rejection",
			Mode:        "chaos",
			Steps: []Step{
				{Name: "Normal operation", Type: "observe", Duration: 2 * time.Second},
				{Name: "Corrupt metadata", Type: "deploy", Service: "metadata-server"},
				{Name: "Validation rejects", Type: "observe", Duration: 3 * time.Second},
				{Name: "Previous version used", Type: "observe", Duration: 2 * time.Second},
			},
			Duration: 20 * time.Second,
		},
		{
			Name:        "slow-downstream",
			Category:    "chaos",
			Description: "Payment gateway becomes slow — timeout cascade",
			Mode:        "chaos",
			Steps: []Step{
				{Name: "Normal latency", Type: "observe", Duration: 2 * time.Second},
				{Name: "Payment gateway slows", Type: "delay", Service: "payment"},
				{Name: "Timeouts increase", Type: "observe", Duration: 5 * time.Second},
				{Name: "Circuit breaker opens", Type: "observe", Duration: 3 * time.Second},
			},
			Duration: 30 * time.Second,
		},
		{
			Name:        "random-latency",
			Category:    "chaos",
			Description: "Random latency spikes across all services",
			Mode:        "chaos",
			Steps: []Step{
				{Name: "Inject random latency", Type: "delay", Service: "payment"},
				{Name: "Latency variance high", Type: "observe", Duration: 5 * time.Second},
				{Name: "P99 latency spikes", Type: "observe", Duration: 5 * time.Second},
			},
			Duration: 30 * time.Second,
		},
		{
			Name:        "duplicate-messages",
			Category:    "chaos",
			Description: "Duplicate messages — test idempotency",
			Mode:        "chaos",
			Steps: []Step{
				{Name: "Send order", Type: "publish", Service: "order"},
				{Name: "Duplicate message", Type: "publish", Service: "order"},
				{Name: "Duplicate message", Type: "publish", Service: "order"},
				{Name: "Observe dedup", Type: "observe", Duration: 3 * time.Second},
				{Name: "Order processed once", Type: "observe", Duration: 2 * time.Second},
			},
			Duration: 20 * time.Second,
		},
		{
			Name:        "memory-leak",
			Category:    "chaos",
			Description: "Simulate memory leak — observe GC and recovery",
			Mode:        "chaos",
			Steps: []Step{
				{Name: "Normal memory usage", Type: "observe", Duration: 2 * time.Second},
				{Name: "Memory allocation increases", Type: "delay", Service: "order"},
				{Name: "GC triggers", Type: "observe", Duration: 5 * time.Second},
				{Name: "Memory stabilizes", Type: "observe", Duration: 5 * time.Second},
			},
			Duration: 30 * time.Second,
		},
		{
			Name:        "cpu-spike",
			Category:    "chaos",
			Description: "CPU spike on runtime — observe scheduler behavior",
			Mode:        "chaos",
			Steps: []Step{
				{Name: "Normal CPU", Type: "observe", Duration: 2 * time.Second},
				{Name: "CPU spike", Type: "delay", Service: "database"},
				{Name: "Scheduler adapts", Type: "observe", Duration: 5 * time.Second},
				{Name: "CPU returns normal", Type: "observe", Duration: 5 * time.Second},
			},
			Duration: 30 * time.Second,
		},
	}
}

// ScenariosByCategory returns scenarios grouped by category.
func ScenariosByCategory() map[string][]ScenarioDef {
	cats := make(map[string][]ScenarioDef)
	for _, s := range AllDefs {
		cats[s.Category] = append(cats[s.Category], s)
	}
	return cats
}

// ScenariosByMode returns scenarios filtered by mode.
func ScenariosByMode(mode string) []ScenarioDef {
	var result []ScenarioDef
	for _, s := range AllDefs {
		if s.Mode == mode || s.Mode == "" {
			result = append(result, s)
		}
	}
	return result
}

// GetScenarioDef returns a scenario definition by name.
func GetScenarioDef(name string) (*ScenarioDef, bool) {
	for _, s := range AllDefs {
		if s.Name == name {
			return &s, true
		}
	}
	return nil, false
}

// CategoryCount returns the count per category.
func CategoryCount() map[string]int {
	counts := make(map[string]int)
	for _, s := range AllDefs {
		counts[s.Category]++
	}
	return counts
}

func init() {
	fmt.Printf("Loaded %d scenario definitions across %d categories\n", len(AllDefs), len(CategoryCount()))
}
