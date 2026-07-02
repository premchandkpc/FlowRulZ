package execnode

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type termData struct {
	Term     uint64 `json:"term"`
	LeaderID string `json:"leader_id"`
}

type TermStore struct {
	path string
	mu   sync.RWMutex
}

func NewTermStore(dir string) *TermStore {
	return &TermStore{path: filepath.Join(dir, "cluster-term.json")}
}

func (ts *TermStore) Load() (uint64, string) {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	data, err := os.ReadFile(ts.path)
	if err != nil {
		return 0, ""
	}
	var td termData
	if err := json.Unmarshal(data, &td); err != nil {
		return 0, ""
	}
	return td.Term, td.LeaderID
}

func (ts *TermStore) Save(term uint64, leaderID string) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	td := termData{Term: term, LeaderID: leaderID}
	data, err := json.Marshal(td)
	if err != nil {
		return fmt.Errorf("term marshal: %w", err)
	}
	tmp := ts.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return fmt.Errorf("term write: %w", err)
	}
	return os.Rename(tmp, ts.path)
}
