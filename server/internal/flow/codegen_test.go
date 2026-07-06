package flow

import (
	"strings"
	"testing"
)

func TestGraphGeneratorDOT(t *testing.T) {
	input := `version 1

flow TestFlow

service auth
    type grpc
    address auth:50051

workflow

Start

-> auth.CreateUser

-> End
`
	parser := NewParser()
	flow, err := parser.ParseString(input)
	if err != nil {
		t.Fatalf("parser error: %v", err)
	}

	compiler := NewCompiler()
	ir, err := compiler.Compile(flow)
	if err != nil {
		t.Fatalf("compiler error: %v", err)
	}

	gen := NewGraphGenerator()
	dot := gen.DOT(ir)

	if !strings.Contains(dot, "digraph flow") {
		t.Error("expected DOT digraph header")
	}
	if !strings.Contains(dot, "auth") {
		t.Error("expected auth service in DOT")
	}
}

func TestGraphGeneratorMermaid(t *testing.T) {
	input := `version 1

flow TestFlow

service auth
    type grpc
    address auth:50051

workflow

Start

-> auth.CreateUser

-> End
`
	parser := NewParser()
	flow, err := parser.ParseString(input)
	if err != nil {
		t.Fatalf("parser error: %v", err)
	}

	compiler := NewCompiler()
	ir, err := compiler.Compile(flow)
	if err != nil {
		t.Fatalf("compiler error: %v", err)
	}

	gen := NewGraphGenerator()
	mermaid := gen.Mermaid(ir)

	if !strings.Contains(mermaid, "flowchart TD") {
		t.Error("expected Mermaid flowchart header")
	}
	if !strings.Contains(mermaid, "auth") {
		t.Error("expected auth service in Mermaid")
	}
}

func TestCodeGeneratorGo(t *testing.T) {
	input := `version 1

flow TestFlow

variables
    userId string

service auth
    type grpc
    address auth:50051

workflow

Start

-> auth.CreateUser

-> End
`
	parser := NewParser()
	flow, err := parser.ParseString(input)
	if err != nil {
		t.Fatalf("parser error: %v", err)
	}

	compiler := NewCompiler()
	ir, err := compiler.Compile(flow)
	if err != nil {
		t.Fatalf("compiler error: %v", err)
	}

	gen := NewCodeGenerator(TargetGo)
	code, err := gen.Generate(ir)
	if err != nil {
		t.Fatalf("codegen error: %v", err)
	}

	if !strings.Contains(code, "package main") {
		t.Error("expected Go package declaration")
	}
	if !strings.Contains(code, "type TestFlow struct") {
		t.Error("expected Go struct")
	}
}

func TestCodeGeneratorRust(t *testing.T) {
	input := `version 1

flow TestFlow

variables
    userId string

service auth
    type grpc
    address auth:50051

workflow

Start

-> auth.CreateUser

-> End
`
	parser := NewParser()
	flow, err := parser.ParseString(input)
	if err != nil {
		t.Fatalf("parser error: %v", err)
	}

	compiler := NewCompiler()
	ir, err := compiler.Compile(flow)
	if err != nil {
		t.Fatalf("compiler error: %v", err)
	}

	gen := NewCodeGenerator(TargetRust)
	code, err := gen.Generate(ir)
	if err != nil {
		t.Fatalf("codegen error: %v", err)
	}

	if !strings.Contains(code, "pub struct TestFlow") {
		t.Error("expected Rust struct")
	}
}

func TestLSPServer(t *testing.T) {
	server := NewLSPServer()

	content := `version 1

flow TestFlow

service auth
    type grpc
    address auth:50051

workflow

Start

-> auth.CreateUser

-> End
`

	diagnostics := server.OpenDocument("file:///test.flow", content)
	if len(diagnostics) > 0 {
		for _, d := range diagnostics {
			t.Errorf("unexpected diagnostic: %s", d.Message)
		}
	}

	// Test completion
	items := server.Completion("file:///test.flow", Position{Line: 10, Character: 5})
	if len(items) == 0 {
		t.Error("expected completion items")
	}

	// Test hover
	hover := server.Hover("file:///test.flow", Position{Line: 5, Character: 5})
	if hover == nil {
		t.Log("no hover at this position (expected)")
	}

	// Test format
	formatted, err := server.FormatDocument("file:///test.flow")
	if err != nil {
		t.Fatalf("format error: %v", err)
	}
	if !strings.Contains(formatted, "flow TestFlow") {
		t.Error("expected formatted output to contain flow name")
	}
}

func TestLSPDiagnostics(t *testing.T) {
	server := NewLSPServer()

	content := `version 1

flow TestFlow

workflow

Start

-> unknown.CallMethod

-> End
`

	diagnostics := server.OpenDocument("file:///test.flow", content)
	if len(diagnostics) == 0 {
		t.Error("expected diagnostic for unknown service")
	}

	found := false
	for _, d := range diagnostics {
		if strings.Contains(d.Message, "unknown service") {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'unknown service' diagnostic")
	}
}

func TestCLI(t *testing.T) {
	cli := NewCLI()

	// Test help
	err := cli.Run([]string{"flow", "help"})
	if err != nil {
		t.Errorf("help command failed: %v", err)
	}
}
