package execstate

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

type FileStore struct {
	dir string
	mu  sync.RWMutex
}

func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("execstate: mkdir: %w", err)
	}
	return &FileStore{dir: dir}, nil
}

func (fs *FileStore) path(id string) string {
	return filepath.Join(fs.dir, id+".json")
}

func (fs *FileStore) Create(_ context.Context, s *State) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	p := fs.path(s.ID)
	if _, err := os.Stat(p); err == nil {
		return fmt.Errorf("execstate: %s already exists", s.ID)
	}
	return fs.writeLocked(p, s)
}

func (fs *FileStore) Save(_ context.Context, s *State) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	s.UpdatedAt = makeTimestamp()
	return fs.writeLocked(fs.path(s.ID), s)
}

func (fs *FileStore) Load(_ context.Context, id string) (*State, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	return fs.readLocked(fs.path(id))
}

func (fs *FileStore) List(_ context.Context, statuses ...Status) ([]*State, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	entries, err := os.ReadDir(fs.dir)
	if err != nil {
		return nil, fmt.Errorf("execstate: read dir: %w", err)
	}

	want := make(map[Status]bool)
	for _, s := range statuses {
		want[s] = true
	}

	var result []*State
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		s, err := fs.readLocked(filepath.Join(fs.dir, e.Name()))
		if err != nil {
			log.Printf("execstate: skip %s: %v", e.Name(), err)
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
		return fmt.Errorf("execstate: remove %s: %w", id, err)
	}
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
