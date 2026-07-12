package flow

import (
	"encoding/json"
	"fmt"
)

// IR is the intermediate representation of a flow.
type IR struct {
	Name      string        `json:"name"`
	Version   int           `json:"version"`
	Nodes     []IRNode      `json:"nodes"`
	Edges     []IREdge      `json:"edges"`
	Services  []IRService   `json:"services"`
	Events    []IREvent     `json:"events"`
	Variables []IRVariable  `json:"variables"`
	Constants []IRConstant  `json:"constants"`
	Outputs   []string      `json:"outputs"`
	Retry     *IRRetry      `json:"retry,omitempty"`
	Breaker   *IRBreaker    `json:"breaker,omitempty"`
	Timeout   string        `json:"timeout,omitempty"`
	Errors    *IRErrorBlock `json:"errors,omitempty"`
}

// IRNode is a node in the execution graph.
type IRNode struct {
	ID        string `json:"id"`
	Type      string `json:"type"` // step, if, parallel, switch, wait, foreach, while, emit, return
	Name      string `json:"name,omitempty"`
	Target    string `json:"target,omitempty"` // service call target
	Timeout   string `json:"timeout,omitempty"`
	Async     bool   `json:"async,omitempty"`
	Condition string `json:"condition,omitempty"`
}

// IREdge connects two nodes.
type IREdge struct {
	From      string `json:"from"`
	To        string `json:"to"`
	Condition string `json:"condition,omitempty"` // for if/switch edges
}

// IRService is a service declaration.
type IRService struct {
	Name    string                 `json:"name"`
	Type    string                 `json:"type"`
	Address string                 `json:"address,omitempty"`
	URL     string                 `json:"url,omitempty"`
	Options map[string]interface{} `json:"options,omitempty"`
}

// IREvent is an event declaration.
type IREvent struct {
	Name    string `json:"name"`
	Payload string `json:"payload,omitempty"`
}

// IRVariable is a variable declaration.
type IRVariable struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// IRConstant is a constant declaration.
type IRConstant struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// IRRetry is retry configuration.
type IRRetry struct {
	Attempts int    `json:"attempts"`
	Backoff  string `json:"backoff"`
	Delay    string `json:"delay,omitempty"`
	MaxDelay string `json:"maxDelay,omitempty"`
}

// IRBreaker is circuit breaker configuration.
type IRBreaker struct {
	FailureRate int    `json:"failureRate"`
	Window      int    `json:"window"`
	Cooldown    string `json:"cooldown"`
}

// IRErrorBlock is error handling configuration.
type IRErrorBlock struct {
	Cases    []IRErrorCase `json:"cases,omitempty"`
	Fallback []string      `json:"fallback,omitempty"`
}

// IRErrorCase is a specific error handler.
type IRErrorCase struct {
	ErrorType string   `json:"errorType"`
	Targets   []string `json:"targets"`
}

// Compiler converts AST to IR.
type Compiler struct {
	nodes  []IRNode
	edges  []IREdge
	nextID int
}

// NewCompiler creates a new IR compiler.
func NewCompiler() *Compiler {
	return &Compiler{}
}

// Compile converts a Flow AST to IR.
func (c *Compiler) Compile(flow *Flow) (*IR, error) {
	c.nodes = nil
	c.edges = nil
	c.nextID = 0

	ir := &IR{
		Name:    flow.Metadata.Name,
		Version: 1,
	}

	// Convert services
	for _, svc := range flow.Services {
		irSvc := IRService{
			Name:    svc.Name,
			Type:    string(svc.Type),
			Options: make(map[string]interface{}),
		}
		for _, opt := range svc.Options {
			irSvc.Options[opt.Key] = opt.Value
		}
		ir.Services = append(ir.Services, irSvc)
	}

	// Convert events
	for _, evt := range flow.Events {
		ir.Events = append(ir.Events, IREvent{Name: evt.Name, Payload: evt.Payload})
	}

	// Convert variables
	for _, v := range flow.Variables {
		ir.Variables = append(ir.Variables, IRVariable{Name: v.Name, Type: v.Type})
	}

	// Convert constants
	for _, k := range flow.Constants {
		ir.Constants = append(ir.Constants, IRConstant{Name: k.Name, Value: k.Value})
	}

	// Convert outputs
	for _, o := range flow.Outputs {
		ir.Outputs = append(ir.Outputs, o.Name)
	}

	// Convert retry
	if flow.Retry != nil {
		ir.Retry = &IRRetry{
			Attempts: flow.Retry.Attempts,
			Backoff:  flow.Retry.Backoff,
			Delay:    flow.Retry.Delay,
			MaxDelay: flow.Retry.MaxDelay,
		}
	}

	// Convert breaker
	if flow.Breaker != nil {
		ir.Breaker = &IRBreaker{
			FailureRate: flow.Breaker.FailureRate,
			Window:      flow.Breaker.Window,
			Cooldown:    flow.Breaker.Cooldown,
		}
	}

	ir.Timeout = flow.Timeout

	// Convert workflow to graph
	if flow.Workflow != nil {
		startID := c.emitNode(IRNode{Type: "start", Name: "Start"})
		endID := c.emitNode(IRNode{Type: "end", Name: "End"})

		lastID := startID
		for _, step := range flow.Workflow.Steps {
			stepID, err := c.compileStep(step)
			if err != nil {
				return nil, err
			}
			c.edges = append(c.edges, IREdge{From: lastID, To: stepID})
			lastID = stepID
		}
		c.edges = append(c.edges, IREdge{From: lastID, To: endID})
	}

	ir.Nodes = c.nodes
	ir.Edges = c.edges

	return ir, nil
}

func (c *Compiler) emitNode(node IRNode) string {
	id := fmt.Sprintf("n%d", c.nextID)
	c.nextID++
	node.ID = id
	c.nodes = append(c.nodes, node)
	return id
}

func (c *Compiler) compileStep(step WorkflowStep) (string, error) {
	switch s := step.(type) {
	case *StepRef:
		return c.emitNode(IRNode{
			Type:   "step",
			Name:   s.Name,
			Target: s.Name,
		}), nil

	case *IfBlock:
		ifID := c.emitNode(IRNode{
			Type:      "if",
			Name:      "if",
			Condition: s.Condition,
		})

		// Compile then branch
		var thenEnd string
		if len(s.Then) > 0 {
			lastID := ifID
			for _, step := range s.Then {
				stepID, err := c.compileStep(step)
				if err != nil {
					return "", err
				}
				c.edges = append(c.edges, IREdge{From: lastID, To: stepID, Condition: "success"})
				lastID = stepID
			}
			thenEnd = lastID
		} else {
			thenEnd = ifID
		}

		// Compile else branch
		var elseEnd string
		if len(s.Else) > 0 {
			lastID := ifID
			for _, step := range s.Else {
				stepID, err := c.compileStep(step)
				if err != nil {
					return "", err
				}
				c.edges = append(c.edges, IREdge{From: lastID, To: stepID, Condition: "failure"})
				lastID = stepID
			}
			elseEnd = lastID
		} else {
			elseEnd = ifID
		}

		// Merge point
		mergeID := c.emitNode(IRNode{Type: "merge", Name: "merge"})
		if thenEnd != ifID {
			c.edges = append(c.edges, IREdge{From: thenEnd, To: mergeID})
		}
		if elseEnd != ifID {
			c.edges = append(c.edges, IREdge{From: elseEnd, To: mergeID})
		}

		return mergeID, nil

	case *ParallelBlock:
		parID := c.emitNode(IRNode{Type: "parallel", Name: "parallel"})

		var branchEnds []string
		for _, step := range s.Steps {
			stepID, err := c.compileStep(step)
			if err != nil {
				return "", err
			}
			c.edges = append(c.edges, IREdge{From: parID, To: stepID})
			branchEnds = append(branchEnds, stepID)
		}

		// Join point
		joinID := c.emitNode(IRNode{Type: "join", Name: "join"})
		for _, end := range branchEnds {
			c.edges = append(c.edges, IREdge{From: end, To: joinID})
		}

		return joinID, nil

	case *WaitBlock:
		return c.emitNode(IRNode{
			Type:    "wait",
			Name:    s.Event,
			Timeout: s.Timeout,
		}), nil

	case *EmitEvent:
		return c.emitNode(IRNode{
			Type:   "emit",
			Name:   s.Event,
			Target: s.Event,
		}), nil

	case *ReturnStep:
		return c.emitNode(IRNode{
			Type:   "return",
			Name:   s.Value,
			Target: s.Value,
		}), nil

	default:
		return "", fmt.Errorf("flow: unknown step type %T", step)
	}
}

// MarshalIR serializes IR to JSON.
func MarshalIR(ir *IR) ([]byte, error) {
	return json.Marshal(ir)
}

// UnmarshalIR deserializes IR from JSON.
func UnmarshalIR(data []byte) (*IR, error) {
	var ir IR
	if err := json.Unmarshal(data, &ir); err != nil {
		return nil, err
	}
	return &ir, nil
}
