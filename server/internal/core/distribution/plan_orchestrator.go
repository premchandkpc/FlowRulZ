// Package distribution implements plan distribution orchestration.
// Depends only on ports — no imports from adapters/.
package distribution

import (
	"context"
	"log/slog"
	"time"

	"github.com/premchandkpc/FlowRulZ/server/internal/ports"
)

// PlanOrchestrator handles the plan distribution lifecycle:
// publish → wait for acks → activate.
type PlanOrchestrator struct {
	planDist ports.PlanDistributor
}

// NewPlanOrchestrator creates a PlanOrchestrator.
func NewPlanOrchestrator(planDist ports.PlanDistributor) *PlanOrchestrator {
	return &PlanOrchestrator{planDist: planDist}
}

// DistributePlan publishes a plan, waits for acks, and activates it.
func (o *PlanOrchestrator) DistributePlan(ctx context.Context, id, dsl string, plan []byte, version uint64) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	if err := o.planDist.PublishPlan(ctx, id, version, plan, dsl); err != nil {
		slog.Error("plandist: publish plan error", "id", id, "version", version, "error", err)
		return
	}

	if err := o.planDist.WaitForAcks(ctx, id, version, 0, 10*time.Second); err != nil {
		slog.Error("plandist: ack wait error", "id", id, "version", version, "error", err)
	}

	if err := o.planDist.ActivatePlan(ctx, id, version); err != nil {
		slog.Error("plandist: activate error", "id", id, "version", version, "error", err)
	}
}

// DistributeActivate sends an activation for an already-published plan.
func (o *PlanOrchestrator) DistributeActivate(ctx context.Context, id string, version uint64) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := o.planDist.ActivatePlan(ctx, id, version); err != nil {
		slog.Error("plandist: activate error during promote", "id", id, "version", version, "error", err)
	}
}
