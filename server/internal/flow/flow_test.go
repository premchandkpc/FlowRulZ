package flow

import (
	"strings"
	"testing"
)

func TestLexerSimpleFlow(t *testing.T) {
	input := `version 1

flow UserSignup

description
    Complete signup workflow

tags
    auth
    production

service auth
    type grpc
    address auth:50051

service email
    type http
    url https://email.internal

workflow

Start

-> ValidateRequest

-> auth.CreateUser

-> email.SendWelcome

-> End
`
	lexer := NewLexer(input)
	tokens, err := lexer.Tokenize()
	if err != nil {
		t.Fatalf("lexer error: %v", err)
	}

	if len(tokens) < 20 {
		t.Fatalf("expected at least 20 tokens, got %d", len(tokens))
	}

	// Check first tokens
	if tokens[0].Type != TokenVersion {
		t.Errorf("expected version, got %v", tokens[0].Type)
	}
	if tokens[1].Type != TokenNumber {
		t.Errorf("expected number, got %v", tokens[1].Type)
	}

	// Check flow keyword
	found := false
	for _, tok := range tokens {
		if tok.Type == TokenFlow {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected flow keyword")
	}
}

func TestLexerOperators(t *testing.T) {
	input := `-> != <= >= && || !`
	lexer := NewLexer(input)
	tokens, err := lexer.Tokenize()
	if err != nil {
		t.Fatalf("lexer error: %v", err)
	}

	expected := []TokenType{TokenArrow, TokenNotEquals, TokenLE, TokenGE, TokenAnd, TokenOr, TokenNot}
	for i, exp := range expected {
		if tokens[i].Type != exp {
			t.Errorf("token %d: expected %v, got %v", i, exp, tokens[i].Type)
		}
	}
}

func TestLexerComments(t *testing.T) {
	input := `# this is a comment
flow Test
// another comment
service auth
/* block
   comment */
`
	lexer := NewLexer(input)
	tokens, err := lexer.Tokenize()
	if err != nil {
		t.Fatalf("lexer error: %v", err)
	}

	// Comments should be filtered out
	for _, tok := range tokens {
		if tok.Type == TokenIdent && (tok.Value == "this" || tok.Value == "another" || tok.Value == "block") {
			t.Errorf("comment not filtered: %q", tok.Value)
		}
	}
}

func TestLexerDuration(t *testing.T) {
	input := `timeout 2s
delay 500ms
cooldown 30s`
	lexer := NewLexer(input)
	tokens, err := lexer.Tokenize()
	if err != nil {
		t.Fatalf("lexer error: %v", err)
	}

	durations := []string{"2s", "500ms", "30s"}
	i := 0
	for _, tok := range tokens {
		if tok.Type == TokenDuration {
			if i >= len(durations) {
				t.Fatal("too many durations")
			}
			if tok.Value != durations[i] {
				t.Errorf("duration %d: expected %q, got %q", i, durations[i], tok.Value)
			}
			i++
		}
	}
	if i != len(durations) {
		t.Errorf("expected %d durations, found %d", len(durations), i)
	}
}

func TestParserSimpleFlow(t *testing.T) {
	input := `version 1

flow UserSignup

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

	if flow.Metadata.Name != "UserSignup" {
		t.Errorf("expected name UserSignup, got %q", flow.Metadata.Name)
	}

	if len(flow.Services) != 1 {
		t.Fatalf("expected 1 service, got %d", len(flow.Services))
	}

	if flow.Services[0].Name != "auth" {
		t.Errorf("expected service name auth, got %q", flow.Services[0].Name)
	}

	if flow.Services[0].Type != ServiceGRPC {
		t.Errorf("expected service type grpc, got %q", flow.Services[0].Type)
	}

	if flow.Workflow == nil {
		t.Fatal("expected workflow")
	}

	if len(flow.Workflow.Steps) != 3 {
		t.Errorf("expected 3 steps, got %d", len(flow.Workflow.Steps))
	}
}

func TestParserWithRetry(t *testing.T) {
	input := `version 1

flow RetryFlow

retry
    attempts 3
    backoff exponential
    delay 500ms

workflow

Start

-> CallService

-> End
`
	parser := NewParser()
	flow, err := parser.ParseString(input)
	if err != nil {
		t.Fatalf("parser error: %v", err)
	}

	if flow.Retry == nil {
		t.Fatal("expected retry policy")
	}

	if flow.Retry.Attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", flow.Retry.Attempts)
	}

	if flow.Retry.Backoff != "exponential" {
		t.Errorf("expected exponential backoff, got %q", flow.Retry.Backoff)
	}
}

func TestParserWithParallel(t *testing.T) {
	input := `version 1

flow ParallelFlow

service db
    type postgres
    connection host db

service kafka
    type kafka
    brokers kafka1:9092

workflow

Start

parallel

    -> db.Save

    -> kafka.Publish

join

-> End
`
	parser := NewParser()
	flow, err := parser.ParseString(input)
	if err != nil {
		t.Fatalf("parser error: %v", err)
	}

	if flow.Workflow == nil {
		t.Fatal("expected workflow")
	}

	// Find parallel block
	found := false
	for _, step := range flow.Workflow.Steps {
		if _, ok := step.(*ParallelBlock); ok {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected parallel block")
	}
}

func TestFormatterRoundtrip(t *testing.T) {
	input := `version 1

flow UserSignup

description
    Complete signup workflow

service auth
    type grpc
    address auth:50051

service email
    type http
    url https://email.internal

workflow

Start

-> ValidateRequest

-> auth.CreateUser

-> email.SendWelcome

-> End
`
	parser := NewParser()
	flow, err := parser.ParseString(input)
	if err != nil {
		t.Fatalf("parser error: %v", err)
	}

	formatter := NewFormatter()
	output := formatter.Format(flow)

	// Re-parse the output
	flow2, err := parser.ParseString(output)
	if err != nil {
		t.Fatalf("re-parse error: %v\nOutput:\n%s", err, output)
	}

	if flow.Metadata.Name != flow2.Metadata.Name {
		t.Errorf("name mismatch: %q vs %q", flow.Metadata.Name, flow2.Metadata.Name)
	}

	if len(flow.Services) != len(flow2.Services) {
		t.Errorf("service count mismatch: %d vs %d", len(flow.Services), len(flow2.Services))
	}
}

func TestSemanticAnalyzer(t *testing.T) {
	input := `version 1

flow TestFlow

service auth
    type grpc
    address auth:50051

event UserCreated

workflow

Start

-> auth.CreateUser

-> emit UserCreated

-> End
`
	parser := NewParser()
	flow, err := parser.ParseString(input)
	if err != nil {
		t.Fatalf("parser error: %v", err)
	}

	analyzer := NewAnalyzer()
	errs := analyzer.Analyze(flow)

	if len(errs) > 0 {
		for _, e := range errs {
			t.Errorf("semantic error: %v", e)
		}
	}
}

func TestSemanticAnalyzerUnknownService(t *testing.T) {
	input := `version 1

flow TestFlow

workflow

Start

-> unknown.CallMethod

-> End
`
	parser := NewParser()
	flow, err := parser.ParseString(input)
	if err != nil {
		t.Fatalf("parser error: %v", err)
	}

	analyzer := NewAnalyzer()
	errs := analyzer.Analyze(flow)

	if len(errs) == 0 {
		t.Error("expected semantic error for unknown service")
	}
}

func TestIRCompiler(t *testing.T) {
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

	if ir.Name != "TestFlow" {
		t.Errorf("expected name TestFlow, got %q", ir.Name)
	}

	if len(ir.Nodes) == 0 {
		t.Error("expected nodes in IR")
	}

	if len(ir.Edges) == 0 {
		t.Error("expected edges in IR")
	}

	if len(ir.Services) != 1 {
		t.Errorf("expected 1 service, got %d", len(ir.Services))
	}
}

func TestLexerEdgeCases(t *testing.T) {
	tests := []struct {
		name  string
		input string
		valid bool
	}{
		{"empty", "", true},
		{"single keyword", "flow", true},
		{"nested parens", "flow (a (b))", true},
		{"unclosed string", `flow "test`, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			_, err := lexer.Tokenize()
			if tt.valid && err != nil {
				t.Errorf("expected valid, got error: %v", err)
			}
			if !tt.valid && err == nil {
				t.Error("expected error, got valid")
			}
		})
	}
}

func TestParserErrorRecovery(t *testing.T) {
	input := `version 1

flow BadFlow

workflow

Start

->

-> End
`
	parser := NewParser()
	_, err := parser.ParseString(input)
	if err == nil {
		t.Error("expected parser error")
	}
	if !strings.Contains(err.Error(), "expected identifier") {
		t.Errorf("unexpected error message: %v", err)
	}
}
