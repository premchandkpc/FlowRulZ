package execstate

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
)

type FileStore struct {
	dir   string
	mu    sync.RWMutex
	index map[string]Status // id → status (in-memory index for O(1) ListByStatus)
}

func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("execstate: mkdir: %w", err)
	}
	fs := &FileStore{
		dir:   dir,
		index: make(map[string]Status),
	}
	// Build index from existing files
	if err := fs.buildIndex(); err != nil {
		return nil, err
	}
	return fs, nil
}

func (fs *FileStore) buildIndex() error {
	entries, err := os.ReadDir(fs.dir)
	if err != nil {
		return fmt.Errorf("execstate: read dir: %w", err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		id := strings.TrimSuffix(e.Name(), ".json")
		s, err := fs.readLocked(filepath.Join(fs.dir, e.Name()))
		if err != nil {
			slog.Warn("execstate: skip bad file during index build", "file", e.Name(), "error", err)
			continue
		}
		fs.index[id] = s.Status
	}
	slog.Info("execstate: index built", "entries", len(fs.index))
	return nil
}

func (fs *FileStore) path(id string) string {
	return filepath.Join(fs.dir, id+".json")
}

func (fs *FileStore) Create(_ context.Context, s *State) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	p := fs.path(s.ID)
	if _, exists := fs.index[s.ID]; exists {
		return fmt.Errorf("execstate: %s already exists", s.ID)
	}
	if err := fs.writeLocked(p, s); err != nil {
		return err
	}
	fs.index[s.ID] = s.Status
	return nil
}

func (fs *FileStore) Save(_ context.Context, s *State) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	s.UpdatedAt = makeTimestamp()
	if err := fs.writeLocked(fs.path(s.ID), s); err != nil {
		return err
	}
	fs.index[s.ID] = s.Status
	return nil
}

func (fs *FileStore) Load(_ context.Context, id string) (*State, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	return fs.readLocked(fs.path(id))
}

func (fs *FileStore) ListByStatus(_ context.Context, statuses ...Status) ([]*State, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	want := make(map[Status]bool)
	for _, s := range statuses {
		want[s] = true
	}

	// Collect IDs matching the requested statuses from index
	var ids []string
	for id, status := range fs.index {
		if len(want) > 0 && !want[status] {
			continue
		}
		ids = append(ids, id)
	}

	// Read only the matching files
	var result []*State
	for _, id := range ids {
		s, err := fs.readLocked(filepath.Join(fs.dir, id+".json"))
		if err != nil {
			slog.Warn("execstate: skip", "id", id, "error", err)
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
		return fmt.Errorf("execstate: remove %s: %w", id, err)
	}
	delete(fs.index, id)
	return nil
}

func (fs *FileStore) Close() error {
	return nil
}

func (fs *FileStore) writeLocked(p string, s *State) error {
	data, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("execstate: marshal: %w", err)
	}
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return fmt.Errorf("execstate: write tmp: %w", err)
	}
	if err := os.Rename(tmp, p); err != nil {
		return fmt.Errorf("execstate: rename: %w", err)
	}
	return nil
}

func (fs *FileStore) readLocked(p string) (*State, error) {
	data, err := os.ReadFile(p)
	if err != nil {
		return nil, fmt.Errorf("execstate: read: %w", err)
	}
	var s State
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("execstate: unmarshal: %w", err)
	}
	return &s, nil
}

func makeTimestamp() time.Time {
	return time.Now().UTC()
}
