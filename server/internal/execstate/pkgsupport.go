package execstate

import (
	"context"
	"time"

	pkgstore "github.com/premchandkpc/FlowRulZ/server/pkg/store"
)

var _ pkgstore.Store = (*ExecutionStore)(nil)

type ExecutionStore struct {
	inner *FileStore
}

func NewExecutionStore(dir string) (*ExecutionStore, error) {
	fs, err := NewFileStore(dir)
	if err != nil {
		return nil, err
	}
	return &ExecutionStore{inner: fs}, nil
}

func (s *ExecutionStore) Create(ctx context.Context, record *pkgstore.ExecutionRecord) error {
	st := &State{
		ID:        string(record.ID),
		RuleID:    record.PlanID,
		Status:    statusFromString(record.State),
		Output:    record.Output,
		Error:     record.Error,
		CreatedAt: record.CreatedAt,
	}
	if !record.CompletedAt.IsZero() {
		st.UpdatedAt = record.CompletedAt
	} else {
		st.UpdatedAt = time.Now()
	}
	return s.inner.Create(ctx, st)
}

func (s *ExecutionStore) Save(ctx context.Context, record *pkgstore.ExecutionRecord) error {
	st := &State{
		ID:        string(record.ID),
		RuleID:    record.PlanID,
		Status:    statusFromString(record.State),
		Output:    record.Output,
		Error:     record.Error,
		CreatedAt: record.CreatedAt,
		UpdatedAt: time.Now(),
	}
	return s.inner.Save(ctx, st)
}

func (s *ExecutionStore) Load(ctx context.Context, id pkgstore.ExecutionID) (*pkgstore.ExecutionRecord, error) {
	st, err := s.inner.Load(ctx, string(id))
	if err != nil {
		return nil, err
	}
	return stateToRecord(st), nil
}

func (s *ExecutionStore) List(ctx context.Context) ([]*pkgstore.ExecutionRecord, error) {
	states, err := s.inner.ListByStatus(ctx)
	if err != nil {
		return nil, err
	}
	records := make([]*pkgstore.ExecutionRecord, 0, len(states))
	for _, st := range states {
		records = append(records, stateToRecord(st))
	}
	return records, nil
}

func (s *ExecutionStore) ListByPlan(ctx context.Context, planID string) ([]*pkgstore.ExecutionRecord, error) {
	all, err := s.List(ctx)
	if err != nil {
		return nil, err
	}
	filtered := make([]*pkgstore.ExecutionRecord, 0, len(all))
	for _, r := range all {
		if r.PlanID == planID {
			filtered = append(filtered, r)
		}
	}
	return filtered, nil
}

func (s *ExecutionStore) Delete(ctx context.Context, id pkgstore.ExecutionID) error {
	return s.inner.Delete(ctx, string(id))
}

func (s *ExecutionStore) Close() error {
	return s.inner.Close()
}

func stateToRecord(st *State) *pkgstore.ExecutionRecord {
	rec := &pkgstore.ExecutionRecord{
		ID:        pkgstore.ExecutionID(st.ID),
		PlanID:    st.RuleID,
		State:     st.Status.String(),
		Output:    st.Output,
		Error:     st.Error,
		CreatedAt: st.CreatedAt,
	}
	if !st.UpdatedAt.IsZero() {
		rec.CompletedAt = st.UpdatedAt
	}
	return rec
}

func statusFromString(s string) Status {
	switch s {
	case "created":
		return StatusCreated
	case "running":
		return StatusRunning
	case "waiting_for_service":
		return StatusWaitingForService
	case "completed":
		return StatusCompleted
	case "failed":
		return StatusFailed
	default:
		return StatusCreated
	}
}
