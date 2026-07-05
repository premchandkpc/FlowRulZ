package reliability

import (
	"fmt"
	"log/slog"
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

func (st *SagaTracker) RegisterStep(execID string, step SagaStep) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.steps[execID] = append(st.steps[execID], step)
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
		slog.Info("saga: compensating", "service", s.ServiceName, "method", s.Method, "comp_svc", s.CompSvc, "comp_method", s.CompMethod)
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
	st.mu.Unlock()
}
