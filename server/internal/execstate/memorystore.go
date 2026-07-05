package execstate

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

type MemoryStore struct {
	mu      sync.RWMutex
	entries map[string]*State
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		entries: make(map[string]*State),
	}
}

func (ms *MemoryStore) Create(_ context.Context, s *State) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if _, exists := ms.entries[s.ID]; exists {
		return fmt.Errorf("execstate: %s already exists", s.ID)
	}
	ms.entries[s.ID] = s
	return nil
}

func (ms *MemoryStore) Save(_ context.Context, s *State) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	s.UpdatedAt = time.Now().UTC()
	ms.entries[s.ID] = s
	return nil
}

func (ms *MemoryStore) Load(_ context.Context, id string) (*State, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	s, ok := ms.entries[id]
	if !ok {
		return nil, fmt.Errorf("execstate: %s not found", id)
	}
	return s, nil
}

func (ms *MemoryStore) ListByStatus(_ context.Context, statuses ...Status) ([]*State, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	want := make(map[Status]bool)
	for _, s := range statuses {
		want[s] = true
	}

	var result []*State
	for _, s := range ms.entries {
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

func (ms *MemoryStore) Delete(_ context.Context, id string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if _, exists := ms.entries[id]; !exists {
		return fmt.Errorf("execstate: %s not found", id)
	}
	delete(ms.entries, id)
	return nil
}

func (ms *MemoryStore) Close() error {
	return nil
}
