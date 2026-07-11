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

const shardCount = 16

type shard struct {
	mu sync.RWMutex
}

type FileStore struct {
	dir         string
	shards      [shardCount]shard
	statusIndex map[Status]map[string]bool // status → set of exec IDs
	indexMu     sync.RWMutex
}

func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("execstate: mkdir: %w", err)
	}
	fs := &FileStore{
		dir:         dir,
		statusIndex: make(map[Status]map[string]bool),
	}
	fs.buildIndex()
	return fs, nil
}

func (fs *FileStore) shardFor(id string) *shard {
	h := fnv32(id)
	return &fs.shards[h%shardCount]
}

func fnv32(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

func (fs *FileStore) path(id string) string {
	return filepath.Join(fs.dir, id+".json")
}

func (fs *FileStore) Create(_ context.Context, s *State) error {
	sh := fs.shardFor(s.ID)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	p := fs.path(s.ID)
	if _, err := os.Stat(p); err == nil {
		return fmt.Errorf("execstate: %s already exists", s.ID)
	}
	if err := fs.writeLocked(p, s); err != nil {
		return err
	}
	fs.addToIndex(s.ID, s.Status)
	return nil
}

func (fs *FileStore) Save(_ context.Context, s *State) error {
	sh := fs.shardFor(s.ID)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	// Capture old status for index update.
	oldStatus := fs.getStatusFromIndex(s.ID)
	s.UpdatedAt = makeTimestamp()
	if err := fs.writeLocked(fs.path(s.ID), s); err != nil {
		return err
	}
	fs.updateIndex(s.ID, oldStatus, s.Status)
	return nil
}

func (fs *FileStore) Load(_ context.Context, id string) (*State, error) {
	sh := fs.shardFor(id)
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	return fs.readLocked(fs.path(id))
}

func (fs *FileStore) ListByStatus(_ context.Context, statuses ...Status) ([]*State, error) {
	fs.indexMu.RLock()
	// Collect candidate IDs from index.
	var candidates []string
	if len(statuses) == 0 {
		// No filter — all IDs.
		for _, ids := range fs.statusIndex {
			for id := range ids {
				candidates = append(candidates, id)
			}
		}
	} else {
		for _, s := range statuses {
			for id := range fs.statusIndex[s] {
				candidates = append(candidates, id)
			}
		}
	}
	fs.indexMu.RUnlock()

	var result []*State
	for _, id := range candidates {
		sh := fs.shardFor(id)
		sh.mu.RLock()
		s, err := fs.readLocked(filepath.Join(fs.dir, id+".json"))
		sh.mu.RUnlock()
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
	sh := fs.shardFor(id)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	p := fs.path(id)
	if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("execstate: remove %s: %w", id, err)
	}
	fs.removeFromIndex(id)
	return nil
}

func (fs *FileStore) Close() error {
	return nil
}

// buildIndex scans the directory once at startup to populate the status index.
func (fs *FileStore) buildIndex() {
	entries, err := os.ReadDir(fs.dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		id := strings.TrimSuffix(e.Name(), ".json")
		data, err := os.ReadFile(filepath.Join(fs.dir, e.Name()))
		if err != nil {
			continue
		}
		var s State
		if err := json.Unmarshal(data, &s); err != nil {
			continue
		}
		fs.addToIndex(id, s.Status)
	}
}

func (fs *FileStore) addToIndex(id string, status Status) {
	fs.indexMu.Lock()
	defer fs.indexMu.Unlock()
	if fs.statusIndex[status] == nil {
		fs.statusIndex[status] = make(map[string]bool)
	}
	fs.statusIndex[status][id] = true
}

func (fs *FileStore) removeFromIndex(id string) {
	fs.indexMu.Lock()
	defer fs.indexMu.Unlock()
	for _, ids := range fs.statusIndex {
		delete(ids, id)
	}
}

func (fs *FileStore) updateIndex(id string, oldStatus, newStatus Status) {
	if oldStatus == newStatus {
		return
	}
	fs.indexMu.Lock()
	defer fs.indexMu.Unlock()
	if oldStatus != 0 && fs.statusIndex[oldStatus] != nil {
		delete(fs.statusIndex[oldStatus], id)
	}
	if fs.statusIndex[newStatus] == nil {
		fs.statusIndex[newStatus] = make(map[string]bool)
	}
	fs.statusIndex[newStatus][id] = true
}

func (fs *FileStore) getStatusFromIndex(id string) Status {
	fs.indexMu.RLock()
	defer fs.indexMu.RUnlock()
	for status, ids := range fs.statusIndex {
		if ids[id] {
			return status
		}
	}
	return 0
}

func (fs *FileStore) writeLocked(p string, s *State) error {
	data, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("execstate: marshal: %w", err)
	}
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
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
