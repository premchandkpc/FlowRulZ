package scheduler

import "time"

type ExecutionContext struct {
	ID            string
	Plan          *Plan
	PlanBytes     []byte
	Body          []byte
	IncomingBody  []byte
	State         int
	Lane          Lane
	ResultCh      chan *Result
	CreatedAt     time.Time
	WaitingService   string
	WaitingStartTime time.Time
	IP               int
	StateChanges     []StateChange
}

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

type StateChange struct {
	From int
	To   int
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
