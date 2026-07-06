package flow

import (
	"bytes"
	"fmt"
	"strings"
)

// Formatter produces canonical .flow output.
type Formatter struct {
	buf    bytes.Buffer
	indent int
}

// NewFormatter creates a new formatter.
func NewFormatter() *Formatter {
	return &Formatter{}
}

// Format produces canonical .flow output from an AST.
func (f *Formatter) Format(flow *Flow) string {
	f.buf.Reset()
	f.indent = 0

	// Version
	f.line("version 1")

	// Flow declaration
	f.line("")
	f.line(fmt.Sprintf("flow %s", flow.Metadata.Name))

	// Description
	if flow.Metadata.Description != "" || len(flow.Metadata.Tags) > 0 {
		f.line("")
		f.line("description")
		f.indent++
		if flow.Metadata.Description != "" {
			f.line(flow.Metadata.Description)
		}
		if len(flow.Metadata.Tags) > 0 {
			f.line("tags")
			f.indent++
			for _, tag := range flow.Metadata.Tags {
				f.line(tag)
			}
			f.indent--
		}
		f.indent--
	}

	// Imports
	if len(flow.Imports) > 0 {
		f.line("")
		for _, imp := range flow.Imports {
			if imp.As != "" {
				f.line(fmt.Sprintf("import %s as %s", imp.Path, imp.As))
			} else if strings.HasSuffix(imp.Path, ".flow") {
				f.line(fmt.Sprintf("import %s", imp.Path))
			} else {
				f.line(fmt.Sprintf("include %s", imp.Path))
			}
		}
	}

	// Variables
	if len(flow.Variables) > 0 {
		f.line("")
		f.line("variables")
		f.indent++
		for _, v := range flow.Variables {
			f.line(fmt.Sprintf("%s %s", v.Name, v.Type))
		}
		f.indent--
	}

	// Constants
	if len(flow.Constants) > 0 {
		f.line("")
		f.line("constants")
		f.indent++
		for _, c := range flow.Constants {
			f.line(fmt.Sprintf("%s = %s", c.Name, c.Value))
		}
		f.indent--
	}

	// Services
	for _, svc := range flow.Services {
		f.line("")
		f.formatService(svc)
	}

	// Events
	for _, evt := range flow.Events {
		f.line("")
		f.line(fmt.Sprintf("event %s", evt.Name))
		if evt.Payload != "" {
			f.indent++
			f.line(fmt.Sprintf("payload %s", evt.Payload))
			f.indent--
		}
	}

	// Retry
	if flow.Retry != nil {
		f.line("")
		f.line("retry")
		f.indent++
		if flow.Retry.Attempts > 0 {
			f.line(fmt.Sprintf("attempts %d", flow.Retry.Attempts))
		}
		if flow.Retry.Backoff != "" {
			f.line(fmt.Sprintf("backoff %s", flow.Retry.Backoff))
		}
		if flow.Retry.Delay != "" {
			f.line(fmt.Sprintf("delay %s", flow.Retry.Delay))
		}
		if flow.Retry.MaxDelay != "" {
			f.line(fmt.Sprintf("maxDelay %s", flow.Retry.MaxDelay))
		}
		f.indent--
	}

	// Breaker
	if flow.Breaker != nil {
		f.line("")
		f.line("breaker")
		f.indent++
		if flow.Breaker.FailureRate > 0 {
			f.line(fmt.Sprintf("failureRate %d", flow.Breaker.FailureRate))
		}
		if flow.Breaker.Window > 0 {
			f.line(fmt.Sprintf("window %d", flow.Breaker.Window))
		}
		if flow.Breaker.Cooldown != "" {
			f.line(fmt.Sprintf("cooldown %s", flow.Breaker.Cooldown))
		}
		f.indent--
	}

	// Timeout
	if flow.Timeout != "" {
		f.line("")
		f.line("timeout")
		f.indent++
		f.line(flow.Timeout)
		f.indent--
	}

	// Workflow
	if flow.Workflow != nil {
		f.line("")
		f.line("workflow")
		f.indent++
		f.formatWorkflow(flow.Workflow.Steps)
		f.indent--
	}

	// Error handling
	if flow.Errors != nil {
		f.line("")
		f.formatErrorBlock(flow.Errors)
	}

	// Compensation
	if len(flow.Compensate) > 0 {
		f.line("")
		f.line("compensate")
		f.indent++
		for _, comp := range flow.Compensate {
			f.line(fmt.Sprintf("%s %s", comp.Step, comp.Compensation))
		}
		f.indent--
	}

	// Outputs
	if len(flow.Outputs) > 0 {
		f.line("")
		f.line("output")
		f.indent++
		for _, o := range flow.Outputs {
			f.line(o.Name)
		}
		f.indent--
	}

	f.line("")
	return f.buf.String()
}

func (f *Formatter) formatService(svc Service) {
	f.line(fmt.Sprintf("service %s", svc.Name))
	f.indent++

	f.line(fmt.Sprintf("type %s", svc.Type))

	for _, opt := range svc.Options {
		switch v := opt.Value.(type) {
		case bool:
			f.line(fmt.Sprintf("%s %t", opt.Key, v))
		case []string:
			f.line(opt.Key)
			f.indent++
			for _, item := range v {
				f.line(item)
			}
			f.indent--
		case map[string]string:
			f.line(opt.Key)
			f.indent++
			for k, val := range v {
				f.line(fmt.Sprintf("%s %s", k, val))
			}
			f.indent--
		default:
			f.line(fmt.Sprintf("%s %v", opt.Key, v))
		}
	}

	f.indent--
}

func (f *Formatter) formatWorkflow(steps []WorkflowStep) {
	for i, step := range steps {
		if i > 0 {
			f.line("")
		}
		f.formatStep(step)
	}
}

func (f *Formatter) formatStep(step WorkflowStep) {
	switch s := step.(type) {
	case *StepRef:
		f.line(fmt.Sprintf("-> %s", s.Name))

	case *IfBlock:
		f.line(fmt.Sprintf("if %s", s.Condition))
		f.indent++
		f.formatWorkflow(s.Then)
		f.indent--
		if len(s.Else) > 0 {
			f.line("else")
			f.indent++
			f.formatWorkflow(s.Else)
			f.indent--
		}

	case *SwitchBlock:
		f.line(fmt.Sprintf("switch %s", s.Variable))
		f.indent++
		for _, c := range s.Cases {
			f.line(fmt.Sprintf("case %s", c.Value))
			f.indent++
			f.formatWorkflow(c.Steps)
			f.indent--
		}
		if len(s.Default) > 0 {
			f.line("default")
			f.indent++
			f.formatWorkflow(s.Default)
			f.indent--
		}
		f.indent--

	case *ParallelBlock:
		f.line("parallel")
		f.indent++
		f.formatWorkflow(s.Steps)
		f.indent--
		f.line("join")

	case *WaitBlock:
		f.line(fmt.Sprintf("wait %s", s.Event))
		if s.Timeout != "" {
			f.indent++
			f.line(fmt.Sprintf("timeout %s", s.Timeout))
			f.indent--
		}

	case *ForeachLoop:
		f.line(fmt.Sprintf("foreach %s", s.Variable))
		f.indent++
		f.formatWorkflow(s.Steps)
		f.indent--

	case *WhileLoop:
		f.line(fmt.Sprintf("while %s", s.Condition))
		f.indent++
		f.formatWorkflow(s.Steps)
		f.indent--

	case *EmitEvent:
		f.line(fmt.Sprintf("emit %s", s.Event))

	case *ReturnStep:
		if s.Value != "" {
			f.line(fmt.Sprintf("Return %s", s.Value))
		} else {
			f.line("Return")
		}
	}
}

func (f *Formatter) formatErrorBlock(block *ErrorBlock) {
	f.line("onError")
	f.indent++
	for _, c := range block.Cases {
		f.line(c.ErrorType)
		f.indent++
		f.formatWorkflow(c.Steps)
		f.indent--
	}
	if len(block.Fallback) > 0 {
		f.line("Default")
		f.indent++
		f.formatWorkflow(block.Fallback)
		f.indent--
	}
	f.indent--
}

func (f *Formatter) line(s string) {
	indent := strings.Repeat("    ", f.indent)
	f.buf.WriteString(indent)
	f.buf.WriteString(s)
	f.buf.WriteString("\n")
}
