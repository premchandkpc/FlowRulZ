// Package statestore implements ports.StateStore with pluggable backends.
// FileStore is the local-dev default; Postgres is for production HPA scaling.
package statestore

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/premchandkpc/FlowRulZ/go/ports"
)

// FileStore implements ports.StateStore using the local filesystem.
// This is the existing implementation, demoted to "one adapter among many".
type FileStore struct {
	dir string
	mu  sync.RWMutex
	// index caches loaded states for SavePending/SaveRunning convenience methods.
	index map[string]*ports.ExecutionState
}

// NewFileStore creates a new FileStore.
func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("statestore: mkdir: %w", err)
	}
	return &FileStore{dir: dir, index: make(map[string]*ports.ExecutionState)}, nil
}

func (fs *FileStore) path(id string) string {
	return filepath.Join(fs.dir, id+".json")
}

func (fs *FileStore) Create(_ context.Context, s *ports.ExecutionState) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	p := fs.path(s.ID)
	if _, err := os.Stat(p); err == nil {
		return fmt.Errorf("statestore: %s already exists", s.ID)
	}
	s.CreatedAt = time.Now().UTC()
	s.UpdatedAt = s.CreatedAt
	if err := fs.writeLocked(p, s); err != nil {
		return err
	}
	fs.index[s.ID] = s
	return nil
}

func (fs *FileStore) Save(_ context.Context, s *ports.ExecutionState) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	s.UpdatedAt = time.Now().UTC()
	if err := fs.writeLocked(fs.path(s.ID), s); err != nil {
		return err
	}
	fs.index[s.ID] = s
	return nil
}

func (fs *FileStore) Load(_ context.Context, id string) (*ports.ExecutionState, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	return fs.readLocked(fs.path(id))
}

func (fs *FileStore) ListByStatus(_ context.Context, statuses ...ports.ExecutionStatus) ([]*ports.ExecutionState, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	entries, err := os.ReadDir(fs.dir)
	if err != nil {
		return nil, fmt.Errorf("statestore: read dir: %w", err)
	}

	want := make(map[ports.ExecutionStatus]bool)
	for _, s := range statuses {
		want[s] = true
	}

	var result []*ports.ExecutionState
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		s, err := fs.readLocked(filepath.Join(fs.dir, e.Name()))
		if err != nil {
			slog.Warn("statestore: skip", "file", e.Name(), "error", err)
			continue
		}
		if len(want) > 0 && !want[s.Status] {
			continue
		}
		result = append(result, s)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].CreatedAt.Before(result[j].CreatedAt)
	})
	return result, nil
}

func (fs *FileStore) Delete(_ context.Context, id string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	p := fs.path(id)
	if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("statestore: remove %s: %w", id, err)
	}
	delete(fs.index, id)
	return nil
}

func (fs *FileStore) Close() error {
	return nil
}

// SavePending marks an execution as waiting for a service response.
func (fs *FileStore) SavePending(ctx context.Context, execID string, pendingSvc uint16, pendingBody, ctxBytes []byte) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	st, ok := fs.index[execID]
	if !ok {
		// Try loading from disk
		var err error
		st, err = fs.readLocked(fs.path(execID))
		if err != nil {
			return err
		}
	}

	st.Status = ports.StatusWaitingForService
	st.PendingSvc = pendingSvc
	st.PendingBody = pendingBody
	st.CtxBytes = ctxBytes
	st.UpdatedAt = time.Now().UTC()

	if err := fs.writeLocked(fs.path(execID), st); err != nil {
		return err
	}
	fs.index[execID] = st
	return nil
}

// SaveRunning marks an execution as running with updated context bytes.
func (fs *FileStore) SaveRunning(ctx context.Context, execID string, ctxBytes []byte) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	st, ok := fs.index[execID]
	if !ok {
		var err error
		st, err = fs.readLocked(fs.path(execID))
		if err != nil {
			return err
		}
	}

	st.Status = ports.StatusRunning
	st.PendingSvc = 0
	st.PendingBody = nil
	st.CtxBytes = ctxBytes
	st.UpdatedAt = time.Now().UTC()

	if err := fs.writeLocked(fs.path(execID), st); err != nil {
		return err
	}
	fs.index[execID] = st
	return nil
}

// SaveCompleted marks an execution as completed with output.
func (fs *FileStore) SaveCompleted(ctx context.Context, execID string, output []byte) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	st, ok := fs.index[execID]
	if !ok {
		var err error
		st, err = fs.readLocked(fs.path(execID))
		if err != nil {
			return err
		}
	}

	st.Status = ports.StatusCompleted
	st.Output = output
	st.UpdatedAt = time.Now().UTC()

	if err := fs.writeLocked(fs.path(execID), st); err != nil {
		return err
	}
	fs.index[execID] = st
	return nil
}

// SaveFailed marks an execution as failed with error.
func (fs *FileStore) SaveFailed(ctx context.Context, execID string, errMsg string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	st, ok := fs.index[execID]
	if !ok {
		var err error
		st, err = fs.readLocked(fs.path(execID))
		if err != nil {
			return err
		}
	}

	st.Status = ports.StatusFailed
	st.Error = errMsg
	st.UpdatedAt = time.Now().UTC()

	if err := fs.writeLocked(fs.path(execID), st); err != nil {
		return err
	}
	fs.index[execID] = st
	return nil
}

func (fs *FileStore) writeLocked(p string, s *ports.ExecutionState) error {
	data, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("statestore: marshal: %w", err)
	}
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return fmt.Errorf("statestore: write tmp: %w", err)
	}
	if err := os.Rename(tmp, p); err != nil {
		return fmt.Errorf("statestore: rename: %w", err)
	}
	return nil
}

func (fs *FileStore) readLocked(p string) (*ports.ExecutionState, error) {
	data, err := os.ReadFile(p)
	if err != nil {
		return nil, fmt.Errorf("statestore: read: %w", err)
	}
	var s ports.ExecutionState
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("statestore: unmarshal: %w", err)
	}
	return &s, nil
}
