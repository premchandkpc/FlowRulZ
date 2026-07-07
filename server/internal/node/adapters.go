package node

import (
	"context"
	"time"

	"github.com/premchandkpc/FlowRulZ/server/internal/execstate"
	"github.com/premchandkpc/FlowRulZ/server/internal/ports"
	"github.com/premchandkpc/FlowRulZ/server/internal/reliability"
)

// --- Adapter types that bridge port interfaces → concrete types ---

// stateStoreAdapter bridges execstate.Store → ports.StateStore
type stateStoreAdapter struct {
	inner execstate.Store
}

func (a *stateStoreAdapter) Create(ctx context.Context, rec *ports.ExecutionRecord) error {
	st := &execstate.State{
		ID:        string(rec.ID),
		RuleID:    rec.PlanID,
		Status:    execstate.StatusCreated,
		Output:    rec.Output,
		Error:     rec.Error,
		CreatedAt: rec.CreatedAt,
	}
	return a.inner.Create(ctx, st)
}

func (a *stateStoreAdapter) Save(ctx context.Context, rec *ports.ExecutionRecord) error {
	st := &execstate.State{
		ID:        string(rec.ID),
		RuleID:    rec.PlanID,
		Status:    statusFromString(rec.State),
		Output:    rec.Output,
		Error:     rec.Error,
		CreatedAt: rec.CreatedAt,
		UpdatedAt: time.Now(),
	}
	return a.inner.Save(ctx, st)
}

func (a *stateStoreAdapter) Load(ctx context.Context, id ports.ExecutionID) (*ports.ExecutionRecord, error) {
	st, err := a.inner.Load(ctx, string(id))
	if err != nil {
		return nil, err
	}
	return &ports.ExecutionRecord{
		ID:          ports.ExecutionID(st.ID),
		PlanID:      st.RuleID,
		State:       st.Status.String(),
		Output:      st.Output,
		Error:       st.Error,
		CreatedAt:   st.CreatedAt,
		CompletedAt: st.UpdatedAt,
	}, nil
}

func (a *stateStoreAdapter) List(ctx context.Context) ([]*ports.ExecutionRecord, error) {
	states, err := a.inner.ListByStatus(ctx)
	if err != nil {
		return nil, err
	}
	records := make([]*ports.ExecutionRecord, 0, len(states))
	for _, st := range states {
		records = append(records, &ports.ExecutionRecord{
			ID:          ports.ExecutionID(st.ID),
			PlanID:      st.RuleID,
			State:       st.Status.String(),
			Output:      st.Output,
			Error:       st.Error,
			CreatedAt:   st.CreatedAt,
			CompletedAt: st.UpdatedAt,
		})
	}
	return records, nil
}

func (a *stateStoreAdapter) Delete(ctx context.Context, id ports.ExecutionID) error {
	return a.inner.Delete(ctx, string(id))
}

func (a *stateStoreAdapter) Close() error {
	return a.inner.Close()
}

// sagaAdapter bridges NodeSagaTracker → ports.SagaTracker
type sagaAdapter struct {
	inner NodeSagaTracker
}

func (a *sagaAdapter) RegisterStep(execID string, step ports.SagaStep) {
	a.inner.RegisterStep(execID, reliability.SagaStep{
		ServiceName: step.ServiceName,
		Method:      step.Method,
		Body:        step.Body,
		CompSvc:     step.CompSvc,
		CompMethod:  step.CompMethod,
	})
}

func (a *sagaAdapter) Compensate(execID string) error {
	return a.inner.Compensate(execID)
}

func (a *sagaAdapter) Clear(execID string) {
	a.inner.Clear(execID)
}

// metricsAdapter bridges ports.MetricsCollector → ports.MetricsCollector (passthrough for core engine)
type metricsAdapter struct {
	inner ports.MetricsCollector
}

func (a *metricsAdapter) RecordExec(name string) {
	a.inner.RecordExec(name)
}

func (a *metricsAdapter) RecordError(name string) {
	a.inner.RecordError(name)
}

func (a *metricsAdapter) Snapshot() ports.MetricSnapshot {
	return a.inner.Snapshot()
}

// execRegistryAdapter bridges ExecRegistry → ports.ExecTracker
type execRegistryAdapter struct {
	inner execstate.ExecRegistry
}

func (a *execRegistryAdapter) Register(id string, cancel context.CancelFunc, name string) {
	a.inner.Register(id, cancel, name)
}

func (a *execRegistryAdapter) Unregister(id string) {
	a.inner.Unregister(id)
}

func (a *execRegistryAdapter) Cancel(id string) bool {
	return a.inner.Cancel(id)
}

func (a *execRegistryAdapter) CancelAll() {
	a.inner.CancelAll()
}

func (a *execRegistryAdapter) List() map[string]time.Time {
	return a.inner.List()
}

func (a *execRegistryAdapter) Len() int {
	return a.inner.Len()
}

// Compile-time interface compliance checks
var _ ports.StateStore = (*stateStoreAdapter)(nil)
var _ ports.SagaTracker = (*sagaAdapter)(nil)
var _ ports.MetricsCollector = (*metricsAdapter)(nil)
var _ ports.ExecTracker = (*execRegistryAdapter)(nil)

// statusFromString converts a string status to execstate.Status
func statusFromString(s string) execstate.Status {
	switch s {
	case "created":
		return execstate.StatusCreated
	case "running":
		return execstate.StatusRunning
	case "waiting_for_service":
		return execstate.StatusWaitingForService
	case "completed":
		return execstate.StatusCompleted
	case "failed":
		return execstate.StatusFailed
	default:
		return execstate.StatusCreated
	}
}

// --- Leadership token conversion (delegated to cluster package) ---
