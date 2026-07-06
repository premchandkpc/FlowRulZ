package flow

import (
	"fmt"
)

// SemanticError represents a validation error.
type SemanticError struct {
	Message string
	Line    int
}

func (e SemanticError) Error() string {
	if e.Line > 0 {
		return fmt.Sprintf("flow: line %d: %s", e.Line, e.Message)
	}
	return fmt.Sprintf("flow: %s", e.Message)
}

// Analyzer performs semantic analysis on a parsed flow.
type Analyzer struct {
	errors []SemanticError
	services map[string]Service
	events   map[string]Event
	vars     map[string]Variable
	consts   map[string]Constant
}

// NewAnalyzer creates a new semantic analyzer.
func NewAnalyzer() *Analyzer {
	return &Analyzer{
		services: make(map[string]Service),
		events:   make(map[string]Event),
		vars:     make(map[string]Variable),
		consts:   make(map[string]Constant),
	}
}

// Analyze validates a flow AST.
func (a *Analyzer) Analyze(flow *Flow) []SemanticError {
	a.errors = nil

	// Register all declarations
	a.registerServices(flow.Services)
	a.registerEvents(flow.Events)
	a.registerVariables(flow.Variables)
	a.registerConstants(flow.Constants)

	// Validate workflow
	if flow.Workflow != nil {
		a.validateWorkflow(flow.Workflow)
	}

	// Validate error handlers
	if flow.Errors != nil {
		a.validateErrorBlock(flow.Errors)
	}

	// Validate compensation
	a.validateCompensation(flow.Compensate, flow.Workflow)

	return a.errors
}

func (a *Analyzer) registerServices(services []Service) {
	for _, svc := range services {
		if _, exists := a.services[svc.Name]; exists {
			a.errors = append(a.errors, SemanticError{
				Message: fmt.Sprintf("duplicate service %q", svc.Name),
			})
		}
		a.services[svc.Name] = svc
	}
}

func (a *Analyzer) registerEvents(events []Event) {
	for _, evt := range events {
		if _, exists := a.events[evt.Name]; exists {
			a.errors = append(a.errors, SemanticError{
				Message: fmt.Sprintf("duplicate event %q", evt.Name),
			})
		}
		a.events[evt.Name] = evt
	}
}

func (a *Analyzer) registerVariables(vars []Variable) {
	for _, v := range vars {
		a.vars[v.Name] = v
	}
}

func (a *Analyzer) registerConstants(consts []Constant) {
	for _, c := range consts {
		a.consts[c.Name] = c
	}
}

func (a *Analyzer) validateWorkflow(wf *Workflow) {
	for _, step := range wf.Steps {
		a.validateStep(step)
	}
}

func (a *Analyzer) validateStep(step WorkflowStep) {
	switch s := step.(type) {
	case *StepRef:
		// Check if referencing a service
		parts := splitRef(s.Name)
		if len(parts) > 1 {
			svcName := parts[0]
			if _, exists := a.services[svcName]; !exists {
				a.errors = append(a.errors, SemanticError{
					Message: fmt.Sprintf("unknown service %q in step %q", svcName, s.Name),
					Line:    s.Line,
				})
			}
		}

	case *IfBlock:
		for _, then := range s.Then {
			a.validateStep(then)
		}
		for _, els := range s.Else {
			a.validateStep(els)
		}

	case *SwitchBlock:
		for _, c := range s.Cases {
			for _, step := range c.Steps {
				a.validateStep(step)
			}
		}
		for _, step := range s.Default {
			a.validateStep(step)
		}

	case *ParallelBlock:
		for _, step := range s.Steps {
			a.validateStep(step)
		}

	case *ForeachLoop:
		for _, step := range s.Steps {
			a.validateStep(step)
		}

	case *WhileLoop:
		for _, step := range s.Steps {
			a.validateStep(step)
		}

	case *EmitEvent:
		if _, exists := a.events[s.Event]; !exists {
			a.errors = append(a.errors, SemanticError{
				Message: fmt.Sprintf("unknown event %q in emit", s.Event),
				Line:    s.Line,
			})
		}
	}
}

func (a *Analyzer) validateErrorBlock(block *ErrorBlock) {
	for _, c := range block.Cases {
		for _, step := range c.Steps {
			a.validateStep(step)
		}
	}
	for _, step := range block.Fallback {
		a.validateStep(step)
	}
}

func (a *Analyzer) validateCompensation(steps []CompensateStep, wf *Workflow) {
	if wf == nil {
		return
	}

	// Build set of workflow steps
	stepNames := make(map[string]bool)
	collectStepNames(wf.Steps, stepNames)

	for _, comp := range steps {
		if !stepNames[comp.Step] {
			a.errors = append(a.errors, SemanticError{
				Message: fmt.Sprintf("compensation for unknown step %q", comp.Step),
			})
		}
	}
}

func collectStepNames(steps []WorkflowStep, names map[string]bool) {
	for _, step := range steps {
		switch s := step.(type) {
		case *StepRef:
			names[s.Name] = true
		case *IfBlock:
			collectStepNames(s.Then, names)
			collectStepNames(s.Else, names)
		case *SwitchBlock:
			for _, c := range s.Cases {
				collectStepNames(c.Steps, names)
			}
			collectStepNames(s.Default, names)
		case *ParallelBlock:
			collectStepNames(s.Steps, names)
		case *ForeachLoop:
			collectStepNames(s.Steps, names)
		case *WhileLoop:
			collectStepNames(s.Steps, names)
		}
	}
}

func splitRef(name string) []string {
	var parts []string
	current := ""
	for _, ch := range name {
		if ch == '.' {
			if current != "" {
				parts = append(parts, current)
				current = ""
			}
		} else {
			current += string(ch)
		}
	}
	if current != "" {
		parts = append(parts, current)
	}
	return parts
}
