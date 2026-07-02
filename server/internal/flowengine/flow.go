package flowengine

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sync"
)

type FlowState int

const (
	StatePending FlowState = iota
	StateRunning
	StateCompleted
	StateFailed
)

func (s FlowState) String() string {
	switch s {
	case StatePending:
		return "pending"
	case StateRunning:
		return "running"
	case StateCompleted:
		return "completed"
	case StateFailed:
		return "failed"
	default:
		return "unknown"
	}
}

type Flow struct {
	ID        string            `json:"id"`
	RuleID    string            `json:"rule_id"`
	Input     []byte            `json:"input"`
	Output    []byte            `json:"output"`
	State     FlowState         `json:"state"`
	Error     string            `json:"error,omitempty"`
	responses map[uint16][]byte `json:"-"`
	dir       string            `json:"-"`
	mu        sync.Mutex        `json:"-"`
}

type flowPersistence struct {
	ID     string    `json:"id"`
	RuleID string    `json:"rule_id"`
	Input  []byte    `json:"input"`
	Output []byte    `json:"output"`
	State  FlowState `json:"state"`
	Error  string    `json:"error,omitempty"`
}

type Orchestrator struct {
	mu    sync.Mutex
	flows map[string]*Flow
	dir   string
}

func NewOrchestrator() *Orchestrator {
	return &Orchestrator{flows: make(map[string]*Flow)}
}

func NewOrchestratorWithCheckpointDir(dir string) *Orchestrator {
	o := &Orchestrator{flows: make(map[string]*Flow), dir: dir}
	o.loadCheckpoints()
	return o
}

func (o *Orchestrator) Start(ctx context.Context, id, ruleID string, input []byte) *Flow {
	f := &Flow{
		ID:        id,
		RuleID:    ruleID,
		Input:     input,
		State:     StatePending,
		responses: make(map[uint16][]byte),
		dir:       o.dir,
	}
	o.mu.Lock()
	o.flows[id] = f
	o.mu.Unlock()
	f.checkpoint()
	return f
}

func (o *Orchestrator) Get(id string) (*Flow, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	f, ok := o.flows[id]
	return f, ok
}

func (o *Orchestrator) StoreResponse(flowID string, svcID uint16, resp []byte) {
	o.mu.Lock()
	f, ok := o.flows[flowID]
	o.mu.Unlock()
	if !ok {
		return
	}
	f.mu.Lock()
	f.responses[svcID] = resp
	f.State = StateRunning
	f.mu.Unlock()
	f.checkpoint()
}

func (o *Orchestrator) Complete(id string) {
	o.mu.Lock()
	f, ok := o.flows[id]
	o.mu.Unlock()
	if !ok {
		return
	}
	f.mu.Lock()
	f.State = StateCompleted
	f.mu.Unlock()
	f.checkpoint()
}

func (o *Orchestrator) Fail(id string, err string) {
	o.mu.Lock()
	f, ok := o.flows[id]
	o.mu.Unlock()
	if !ok {
		return
	}
	f.mu.Lock()
	f.State = StateFailed
	f.Error = err
	f.mu.Unlock()
	f.checkpoint()
}

func (o *Orchestrator) Remove(id string) {
	o.mu.Lock()
	delete(o.flows, id)
	o.mu.Unlock()
	o.removeCheckpoint(id)
}

func (o *Orchestrator) List() []*Flow {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]*Flow, 0, len(o.flows))
	for _, f := range o.flows {
		out = append(out, f)
	}
	return out
}

func (f *Flow) checkpoint() {
	if f.dir == "" {
		return
	}
	data, err := json.Marshal(flowPersistence{
		ID:     f.ID,
		RuleID: f.RuleID,
		Input:  f.Input,
		Output: f.Output,
		State:  f.State,
		Error:  f.Error,
	})
	if err != nil {
		log.Printf("flow checkpoint marshal error: %v", err)
		return
	}
	path := filepath.Join(f.dir, f.ID+".json")
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		log.Printf("flow checkpoint write error: %v", err)
		return
	}
	if err := os.Rename(tmpPath, path); err != nil {
		log.Printf("flow checkpoint rename error: %v", err)
	}
}

func (o *Orchestrator) loadCheckpoints() {
	if o.dir == "" {
		return
	}
	entries, err := os.ReadDir(o.dir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(o.dir, entry.Name()))
		if err != nil {
			continue
		}
		var fp flowPersistence
		if err := json.Unmarshal(data, &fp); err != nil {
			continue
		}
		f := &Flow{
			ID:        fp.ID,
			RuleID:    fp.RuleID,
			Input:     fp.Input,
			Output:    fp.Output,
			State:     fp.State,
			Error:     fp.Error,
			responses: make(map[uint16][]byte),
			dir:       o.dir,
		}
		o.flows[fp.ID] = f
	}
}

func (o *Orchestrator) removeCheckpoint(id string) {
	if o.dir == "" {
		return
	}
	path := filepath.Join(o.dir, id+".json")
	os.Remove(path)
	tmpPath := path + ".tmp"
	os.Remove(tmpPath)
}
