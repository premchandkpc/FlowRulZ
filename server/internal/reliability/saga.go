package reliability

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type SagaStep struct {
	ServiceName string `json:"service_name"`
	Method      string `json:"method"`
	Body        []byte `json:"body"`
	CompSvc     string `json:"comp_svc"`
	CompMethod  string `json:"comp_method"`
}

type CompensatorFunc func(svcName, method string, body []byte) error

type SagaTracker struct {
	mu    sync.Mutex
	steps map[string][]SagaStep
	call  CompensatorFunc
	dir   string
}

func NewSagaTracker(call CompensatorFunc) *SagaTracker {
	if call == nil {
		call = func(_, _ string, _ []byte) error { return nil }
	}
	return &SagaTracker{
		steps: make(map[string][]SagaStep),
		call:  call,
	}
}

func NewSagaTrackerWithDir(call CompensatorFunc, dir string) *SagaTracker {
	if call == nil {
		call = func(_, _ string, _ []byte) error { return nil }
	}
	st := &SagaTracker{
		steps: make(map[string][]SagaStep),
		call:  call,
		dir:   dir,
	}
	st.load()
	return st
}

func (st *SagaTracker) SetDir(dir string) {
	st.mu.Lock()
	st.dir = dir
	st.mu.Unlock()
	st.load()
}

func (st *SagaTracker) RegisterStep(execID string, step SagaStep) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.steps[execID] = append(st.steps[execID], step)
	st.persistLocked(execID)
}

func (st *SagaTracker) Compensate(execID string) error {
	st.mu.Lock()
	steps := st.steps[execID]
	delete(st.steps, execID)
	st.mu.Unlock()

	if len(steps) == 0 {
		return nil
	}

	var errs []error
	for i := len(steps) - 1; i >= 0; i-- {
		s := steps[i]
		if s.CompSvc == "" && s.CompMethod == "" {
			continue
		}
		log.Printf("saga: compensating %s/%s via %s/%s", s.ServiceName, s.Method, s.CompSvc, s.CompMethod)
		if err := st.call(s.CompSvc, s.CompMethod, s.Body); err != nil {
			errs = append(errs, fmt.Errorf("compensate %s/%s: %w", s.ServiceName, s.Method, err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("saga compensation errors: %v", errs)
	}
	return nil
}

func (st *SagaTracker) StepsFor(execID string) []SagaStep {
	st.mu.Lock()
	defer st.mu.Unlock()
	steps := st.steps[execID]
	out := make([]SagaStep, len(steps))
	copy(out, steps)
	return out
}

func (st *SagaTracker) Clear(execID string) {
	st.mu.Lock()
	delete(st.steps, execID)
	path := st.stepPath(execID)
	st.mu.Unlock()
	if path != "" {
		os.Remove(path)
		os.Remove(path + ".tmp")
	}
}

func (st *SagaTracker) stepPath(execID string) string {
	if st.dir == "" {
		return ""
	}
	return filepath.Join(st.dir, execID+"-saga.json")
}

func (st *SagaTracker) persistLocked(execID string) {
	path := st.stepPath(execID)
	if path == "" {
		return
	}
	steps := st.steps[execID]
	data, err := json.Marshal(steps)
	if err != nil {
		log.Printf("saga: marshal error for %s: %v", execID, err)
		return
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		log.Printf("saga: write error for %s: %v", execID, err)
		return
	}
	os.Rename(tmp, path)
}

func (st *SagaTracker) load() {
	if st.dir == "" {
		return
	}
	entries, err := os.ReadDir(st.dir)
	if err != nil {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" || !strings.HasSuffix(e.Name(), "-saga.json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(st.dir, e.Name()))
		if err != nil {
			continue
		}
		var steps []SagaStep
		if err := json.Unmarshal(data, &steps); err != nil {
			continue
		}
		execID := strings.TrimSuffix(e.Name(), "-saga.json")
		st.steps[execID] = steps
	}
}
