package execution

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/premchandkpc/FlowRulZ/go/simulator/timeline"
)

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
	ID               string
	Plan             *Plan
	IP               int
	State            State
	Variables        map[string]any
	IncomingBody     []byte
	WaitingService   string
	WaitingStartTime time.Time

	StateChanges []StateChange
	Events       []timeline.Event

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
		State:        StateCreated,
		Variables:    make(map[string]any),
		IncomingBody: body,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}
}

func (ec *ExecutionContext) Transition(to State, meta string) {
	ec.StateChanges = append(ec.StateChanges, StateChange{
		From: ec.State,
		To:   to,
		At:   time.Now(),
		Meta: meta,
	})
	ec.State = to
	ec.UpdatedAt = time.Now()
}

func (ec *ExecutionContext) AddEvent(event timeline.Event) {
	ec.Events = append(ec.Events, event)
}

func (ec *ExecutionContext) MarkDone() {
	ec.Duration = time.Since(ec.CreatedAt)
	ec.Transition(StateCompleted, "execution completed")
}

func (ec *ExecutionContext) MarkFailed(err error) {
	ec.Duration = time.Since(ec.CreatedAt)
	ec.Transition(StateFailed, err.Error())
}
