// Package statestore adapts the existing execstate.FileStore to the ports.StateStore port.
package statestore

import (
	"context"

	"github.com/premchandkpc/FlowRulZ/go/ports"
	"github.com/premchandkpc/FlowRulZ/server/internal/execstate"
)

// FileStoreAdapter wraps execstate.FileStore to implement ports.StateStore.
type FileStoreAdapter struct {
	inner *execstate.FileStore
}

// NewFileStoreAdapter creates a new FileStoreAdapter.
func NewFileStoreAdapter(inner *execstate.FileStore) *FileStoreAdapter {
	return &FileStoreAdapter{inner: inner}
}

func toPortState(s *execstate.State) *ports.ExecutionState {
	return &ports.ExecutionState{
		ID:          s.ID,
		RuleID:      s.RuleID,
		Version:     s.Version,
		PlanBytes:   s.PlanBytes,
		CtxBytes:    s.CtxBytes,
		Status:      ports.ExecutionStatus(s.Status),
		PendingSvc:  s.PendingSvc,
		PendingBody: s.PendingBody,
		Error:       s.Error,
		Output:      s.Output,
		CreatedAt:   s.CreatedAt,
		UpdatedAt:   s.UpdatedAt,
	}
}

func toInternalState(s *ports.ExecutionState) *execstate.State {
	return &execstate.State{
		ID:          s.ID,
		RuleID:      s.RuleID,
		Version:     s.Version,
		PlanBytes:   s.PlanBytes,
		CtxBytes:    s.CtxBytes,
		Status:      execstate.Status(s.Status),
		PendingSvc:  s.PendingSvc,
		PendingBody: s.PendingBody,
		Error:       s.Error,
		Output:      s.Output,
		CreatedAt:   s.CreatedAt,
		UpdatedAt:   s.UpdatedAt,
	}
}

func (a *FileStoreAdapter) Create(ctx context.Context, state *ports.ExecutionState) error {
	return a.inner.Create(ctx, toInternalState(state))
}

func (a *FileStoreAdapter) Save(ctx context.Context, state *ports.ExecutionState) error {
	return a.inner.Save(ctx, toInternalState(state))
}

func (a *FileStoreAdapter) Load(ctx context.Context, id string) (*ports.ExecutionState, error) {
	s, err := a.inner.Load(ctx, id)
	if err != nil {
		return nil, err
	}
	return toPortState(s), nil
}

func (a *FileStoreAdapter) ListByStatus(ctx context.Context, statuses ...ports.ExecutionStatus) ([]*ports.ExecutionState, error) {
	internalStatuses := make([]execstate.Status, len(statuses))
	for i, s := range statuses {
		internalStatuses[i] = execstate.Status(s)
	}
	states, err := a.inner.ListByStatus(ctx, internalStatuses...)
	if err != nil {
		return nil, err
	}
	result := make([]*ports.ExecutionState, len(states))
	for i, s := range states {
		result[i] = toPortState(s)
	}
	return result, nil
}

func (a *FileStoreAdapter) Delete(ctx context.Context, id string) error {
	return a.inner.Delete(ctx, id)
}

func (a *FileStoreAdapter) Close() error {
	return a.inner.Close()
}

func (a *FileStoreAdapter) SavePending(ctx context.Context, execID string, pendingSvc uint16, pendingBody, ctxBytes []byte) error {
	s, err := a.inner.Load(ctx, execID)
	if err != nil {
		return err
	}
	s.Status = execstate.StatusWaitingForService
	s.PendingSvc = pendingSvc
	s.PendingBody = pendingBody
	s.CtxBytes = ctxBytes
	return a.inner.Save(ctx, s)
}

func (a *FileStoreAdapter) SaveRunning(ctx context.Context, execID string, ctxBytes []byte) error {
	s, err := a.inner.Load(ctx, execID)
	if err != nil {
		return err
	}
	s.Status = execstate.StatusRunning
	s.PendingSvc = 0
	s.PendingBody = nil
	s.CtxBytes = ctxBytes
	return a.inner.Save(ctx, s)
}

func (a *FileStoreAdapter) SaveCompleted(ctx context.Context, execID string, output []byte) error {
	s, err := a.inner.Load(ctx, execID)
	if err != nil {
		return err
	}
	s.Status = execstate.StatusCompleted
	s.Output = output
	return a.inner.Save(ctx, s)
}

func (a *FileStoreAdapter) SaveFailed(ctx context.Context, execID string, errMsg string) error {
	s, err := a.inner.Load(ctx, execID)
	if err != nil {
		return err
	}
	s.Status = execstate.StatusFailed
	s.Error = errMsg
	return a.inner.Save(ctx, s)
}
