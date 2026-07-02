package scheduler

import "time"

type ExecutionID string

type State int

const (
	StateCreated State = iota
	StateReady
	StateRunning
	StateWaitingForService
	StateCompleted
	StateFailed
	StateCancelled
)

type Plan struct {
	ID           string
	Instructions []Instruction
	PlanBytes    []byte
	ServiceNames map[uint16]string
}

type Instruction struct {
	Op      OpCode
	Service string
	Args    []string
}

type OpCode int

const (
	OpCallService OpCode = iota
	OpValidate
	OpBranch
	OpPublish
	OpReturn
)

type ExecutionContext struct {
	ID               ExecutionID
	Plan             *Plan
	State            State
	Variables        map[string]any
	IncomingBody     []byte
	Output           []byte
	Duration         time.Duration
	Lane             Lane
	ResultCh         chan *Result
	CreatedAt        time.Time
	WaitingService   string
	WaitingStartTime time.Time
	IP               int
	StateChanges     []StateChange
}

type StateChange struct {
	From State
	To   State
	Meta string
}

type Result struct {
	Body  []byte
	Error error
}

type SchedulerSnapshot struct {
	ReadyQueueLen   int
	WaitingQueueLen int
	ActiveWorkers   int
	LaneCounts      map[Lane]int
}
