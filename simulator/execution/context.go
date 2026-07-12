package execution

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type Result struct {
	Body  []byte
	Error error
}

type State int

const (
	StateCreated State = iota
	StateReady
	StateRunning
	StateWaitingForService
	StateCompleted
	StateFailed
)

func (s State) String() string {
	switch s {
	case StateCreated:
		return "created"
	case StateReady:
		return "ready"
	case StateRunning:
		return "running"
	case StateWaitingForService:
		return "waiting"
	case StateCompleted:
		return "completed"
	case StateFailed:
		return "failed"
	default:
		return "unknown"
	}
}

var nextExecID atomic.Uint64

type ExecutionContext struct {
	mu   sync.Mutex
	ID   string
	Plan *Plan
	IP   int

	state            State
	variables        map[string]any
	IncomingBody     []byte
	Output           []byte
	WaitingService   string
	WaitingStartTime time.Time
	ResultCh         chan *Result
	OnDone           func()

	StateChanges []StateChange

	CreatedAt time.Time
	UpdatedAt time.Time
	Duration  time.Duration
}

type StateChange struct {
	From State
	To   State
	At   time.Time
	Meta string
}

func NewContext(plan *Plan, body []byte) *ExecutionContext {
	id := fmt.Sprintf("exec-%d", nextExecID.Add(1))
	return &ExecutionContext{
		ID:           id,
		Plan:         plan,
		IP:           0,
		state:        StateCreated,
		variables:    make(map[string]any),
		IncomingBody: body,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}
}

func (ec *ExecutionContext) State() State {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	return ec.state
}

func (ec *ExecutionContext) SetVariable(key string, val any) {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	ec.variables[key] = val
}

func (ec *ExecutionContext) Variable(key string) any {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	return ec.variables[key]
}

func (ec *ExecutionContext) VariablesMap() map[string]any {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	m := make(map[string]any, len(ec.variables))
	for k, v := range ec.variables {
		m[k] = v
	}
	return m
}

func (ec *ExecutionContext) Transition(to State, meta string) {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	ec.StateChanges = append(ec.StateChanges, StateChange{
		From: ec.state,
		To:   to,
		At:   time.Now(),
		Meta: meta,
	})
	ec.state = to
	ec.UpdatedAt = time.Now()
}

func (ec *ExecutionContext) MarkDone() {
	ec.markFinal(StateCompleted, "execution completed")
}

func (ec *ExecutionContext) MarkFailed(err error) {
	ec.markFinal(StateFailed, err.Error())
}

func (ec *ExecutionContext) markFinal(to State, meta string) {
	ec.mu.Lock()
	ec.Duration = time.Since(ec.CreatedAt)
	prev := ec.state
	ec.state = to
	ec.StateChanges = append(ec.StateChanges, StateChange{
		From: prev,
		To:   to,
		At:   time.Now(),
		Meta: meta,
	})
	ec.UpdatedAt = time.Now()
	onDone := ec.OnDone
	ec.mu.Unlock()
	if onDone != nil {
		onDone()
	}
}
