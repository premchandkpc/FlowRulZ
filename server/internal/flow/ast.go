package flow

// Node is the base interface for all AST nodes.
type Node interface {
	nodeType() string
}

// Flow is the root AST node.
type Flow struct {
	Metadata   Metadata
	Imports    []Import
	Variables  []Variable
	Constants  []Constant
	Services   []Service
	Events     []Event
	Trigger    *Trigger
	Workflow   *Workflow
	Outputs    []Output
	Retry      *RetryPolicy
	Breaker    *CircuitBreaker
	Timeout    string
	Errors     *ErrorBlock
	Compensate []CompensateStep
}

func (f *Flow) nodeType() string { return "Flow" }

// Trigger defines what starts the flow.
type Trigger struct {
	Type   string // message, http, cron, webhook
	Topic  string
	Schema map[string]string
	Path   string
	Cron   string
	Method string
}

// Metadata describes the flow.
type Metadata struct {
	Name        string
	Description string
	Tags        []string
}

// Import represents an import statement.
type Import struct {
	Path string
	As   string // optional alias
	Line int
}

// Variable declares a flow variable.
type Variable struct {
	Name string
	Type string
	Line int
}

// Constant declares a flow constant.
type Constant struct {
	Name  string
	Value string
	Line  int
}

// Service declares an external service.
type Service struct {
	Name    string
	Type    ServiceType
	Options []ServiceOption
	Line    int
}

// ServiceType is the type of service connection.
type ServiceType string

const (
	ServiceGRPC     ServiceType = "grpc"
	ServiceHTTP     ServiceType = "http"
	ServiceKafka    ServiceType = "kafka"
	ServiceRedis    ServiceType = "redis"
	ServicePostgres ServiceType = "postgres"
	ServiceTCP      ServiceType = "tcp"
)

// ServiceOption is a key-value pair in a service definition.
type ServiceOption struct {
	Key   string
	Value interface{} // string, []string, map[string]string, bool
	Line  int
}

// Event declares a flow event.
type Event struct {
	Name    string
	Payload string // optional payload type
	Line    int
}

// Workflow is the main execution logic.
type Workflow struct {
	Steps []WorkflowStep
}

// WorkflowStep is a single step in the workflow.
type WorkflowStep interface {
	nodeType() string
	workflowStep()
}

// StepRef is a reference to a named step or service call.
type StepRef struct {
	Name string
	Line int
}

func (s *StepRef) nodeType() string { return "StepRef" }
func (s *StepRef) workflowStep()    {}

// IfBlock is a conditional branch.
type IfBlock struct {
	Condition string
	Then      []WorkflowStep
	Else      []WorkflowStep
	Line      int
}

func (i *IfBlock) nodeType() string { return "IfBlock" }
func (i *IfBlock) workflowStep()    {}

// SwitchBlock is a multi-way branch.
type SwitchBlock struct {
	Variable string
	Cases    []CaseBlock
	Default  []WorkflowStep
	Line     int
}

func (s *SwitchBlock) nodeType() string { return "SwitchBlock" }
func (s *SwitchBlock) workflowStep()    {}

// CaseBlock is a case in a switch.
type CaseBlock struct {
	Value string
	Steps []WorkflowStep
	Line  int
}

// ParallelBlock runs steps concurrently.
type ParallelBlock struct {
	Steps []WorkflowStep
	Line  int
}

func (p *ParallelBlock) nodeType() string { return "ParallelBlock" }
func (p *ParallelBlock) workflowStep()    {}

// WaitBlock waits for an event.
type WaitBlock struct {
	Event   string
	Timeout string
	Line    int
}

func (w *WaitBlock) nodeType() string { return "WaitBlock" }
func (w *WaitBlock) workflowStep()    {}

// ForeachLoop iterates over a collection.
type ForeachLoop struct {
	Variable string
	Steps    []WorkflowStep
	Line     int
}

func (f *ForeachLoop) nodeType() string { return "ForeachLoop" }
func (f *ForeachLoop) workflowStep()    {}

// WhileLoop loops while a condition is true.
type WhileLoop struct {
	Condition string
	Steps     []WorkflowStep
	Line      int
}

func (w *WhileLoop) nodeType() string { return "WhileLoop" }
func (w *WhileLoop) workflowStep()    {}

// EmitEvent emits an event.
type EmitEvent struct {
	Event string
	Line  int
}

func (e *EmitEvent) nodeType() string { return "EmitEvent" }
func (e *EmitEvent) workflowStep()    {}

// ReturnStep returns a value from the workflow.
type ReturnStep struct {
	Value string
	Line  int
}

func (r *ReturnStep) nodeType() string { return "ReturnStep" }
func (r *ReturnStep) workflowStep()    {}

// Output declares a flow output.
type Output struct {
	Name string
	Line int
}

// RetryPolicy configures retry behavior.
type RetryPolicy struct {
	Attempts int
	Backoff  string // linear, exponential, fixed
	Delay    string
	MaxDelay string
	Line     int
}

// CircuitBreaker configures circuit breaker.
type CircuitBreaker struct {
	FailureRate int
	Window      int
	Cooldown    string
	Line        int
}

// ErrorBlock defines error handling.
type ErrorBlock struct {
	Cases    []ErrorCase
	Fallback []WorkflowStep
	Line     int
}

// ErrorCase is a specific error handler.
type ErrorCase struct {
	ErrorType string
	Steps     []WorkflowStep
	Line      int
}

// CompensateStep maps a step to its compensation.
type CompensateStep struct {
	Step         string
	Compensation string
	Line         int
}
