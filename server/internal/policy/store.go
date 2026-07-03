package policy

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Store defines the interface for policy persistence
type Store interface {
	// Get retrieves a policy by ID
	Get(ctx context.Context, id string) (*Policy, error)
	
	// Set stores a policy
	Set(ctx context.Context, policy *Policy) error
	
	// Delete removes a policy
	Delete(ctx context.Context, id string) error
	
	// List returns all policies
	List(ctx context.Context) ([]*Policy, error)
	
	// ListByLevel returns policies by level
	ListByLevel(ctx context.Context, level Level) ([]*Policy, error)
	
	// ListByScope returns policies by scope
	ListByScope(ctx context.Context, scope string) ([]*Policy, error)
}

// MemoryStore implements Store with in-memory storage
type MemoryStore struct {
	mu       sync.RWMutex
	policies map[string]*Policy
}

// NewMemoryStore creates a new in-memory policy store
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		policies: make(map[string]*Policy),
	}
}

// Get retrieves a policy by ID
func (s *MemoryStore) Get(ctx context.Context, id string) (*Policy, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	p, ok := s.policies[id]
	if !ok {
		return nil, fmt.Errorf("policy not found: %s", id)
	}
	return p.Clone(), nil
}

// Set stores a policy
func (s *MemoryStore) Set(ctx context.Context, policy *Policy) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.policies[policy.ID] = policy.Clone()
	return nil
}

// Delete removes a policy
func (s *MemoryStore) Delete(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.policies, id)
	return nil
}

// List returns all policies
func (s *MemoryStore) List(ctx context.Context) ([]*Policy, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	policies := make([]*Policy, 0, len(s.policies))
	for _, p := range s.policies {
		policies = append(policies, p.Clone())
	}
	return policies, nil
}

// ListByLevel returns policies by level
func (s *MemoryStore) ListByLevel(ctx context.Context, level Level) ([]*Policy, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var policies []*Policy
	for _, p := range s.policies {
		if p.Level == level {
			policies = append(policies, p.Clone())
		}
	}
	return policies, nil
}

// ListByScope returns policies by scope
func (s *MemoryStore) ListByScope(ctx context.Context, scope string) ([]*Policy, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var policies []*Policy
	for _, p := range s.policies {
		if p.Scope == scope {
			policies = append(policies, p.Clone())
		}
	}
	return policies, nil
}

// FileStore implements Store with file-based persistence
type FileStore struct {
	mu   sync.RWMutex
	dir  string
}

// NewFileStore creates a new file-based policy store
func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create policy directory: %w", err)
	}
	return &FileStore{dir: dir}, nil
}

// Get retrieves a policy by ID
func (s *FileStore) Get(ctx context.Context, id string) (*Policy, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	path := s.policyPath(id)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("policy not found: %s", id)
		}
		return nil, fmt.Errorf("failed to read policy: %w", err)
	}

	var policy Policy
	if err := json.Unmarshal(data, &policy); err != nil {
		return nil, fmt.Errorf("failed to unmarshal policy: %w", err)
	}

	return &policy, nil
}

// Set stores a policy
func (s *FileStore) Set(ctx context.Context, policy *Policy) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	policy.UpdatedAt = time.Now()
	
	data, err := json.MarshalIndent(policy, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal policy: %w", err)
	}

	path := s.policyPath(policy.ID)
	tmpPath := path + ".tmp"

	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write policy: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("failed to rename policy file: %w", err)
	}

	return nil
}

// Delete removes a policy
func (s *FileStore) Delete(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.policyPath(id)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete policy: %w", err)
	}
	return nil
}

// List returns all policies
func (s *FileStore) List(ctx context.Context) ([]*Policy, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read policy directory: %w", err)
	}

	var policies []*Policy
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		path := filepath.Join(s.dir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}

		var policy Policy
		if err := json.Unmarshal(data, &policy); err != nil {
			continue
		}

		policies = append(policies, &policy)
	}

	return policies, nil
}

// ListByLevel returns policies by level
func (s *FileStore) ListByLevel(ctx context.Context, level Level) ([]*Policy, error) {
	all, err := s.List(ctx)
	if err != nil {
		return nil, err
	}

	var policies []*Policy
	for _, p := range all {
		if p.Level == level {
			policies = append(policies, p)
		}
	}
	return policies, nil
}

// ListByScope returns policies by scope
func (s *FileStore) ListByScope(ctx context.Context, scope string) ([]*Policy, error) {
	all, err := s.List(ctx)
	if err != nil {
		return nil, err
	}

	var policies []*Policy
	for _, p := range all {
		if p.Scope == scope {
			policies = append(policies, p)
		}
	}
	return policies, nil
}

func (s *FileStore) policyPath(id string) string {
	return filepath.Join(s.dir, id+".json")
}
