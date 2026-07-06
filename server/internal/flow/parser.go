package flow

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Parser parses .flow tokens into an AST.
type Parser struct {
	tokens []Token
	pos    int
}

// NewParser creates a new parser.
func NewParser() *Parser {
	return &Parser{}
}

// ParseFile reads and parses a .flow file.
func (p *Parser) ParseFile(path string) (*Flow, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("flow: read file %s: %w", path, err)
	}
	return p.Parse(data)
}

// Parse parses .flow source bytes into an AST.
func (p *Parser) Parse(data []byte) (*Flow, error) {
	lexer := NewLexer(string(data))
	tokens, err := lexer.Tokenize()
	if err != nil {
		return nil, err
	}

	p.tokens = FilterNewlines(tokens)
	p.pos = 0

	return p.parseFlow()
}

// ParseString parses a .flow string.
func (p *Parser) ParseString(src string) (*Flow, error) {
	return p.Parse([]byte(src))
}

func (p *Parser) parseFlow() (*Flow, error) {
	flow := &Flow{}

	// Parse version (optional, ignored for now)
	if p.match(TokenVersion) {
		p.advance()
		// Skip version number
		if p.match(TokenNumber) {
			p.advance()
		}
	}

	// Parse flow name
	if p.match(TokenFlow) {
		p.advance()
		name, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		flow.Metadata.Name = name
	}

	// Parse sections
	for !p.eof() {
		tok := p.peek()
		switch tok.Type {
		case TokenIdent:
			switch tok.Value {
			case "description":
				meta, err := p.parseDescription()
				if err != nil {
					return nil, err
				}
				flow.Metadata.Description = meta.Description
				flow.Metadata.Tags = meta.Tags
			case "variables":
				vars, err := p.parseVariables()
				if err != nil {
					return nil, err
				}
				flow.Variables = vars
			case "constants":
				consts, err := p.parseConstants()
				if err != nil {
					return nil, err
				}
				flow.Constants = consts
			case "output":
				outputs, err := p.parseOutputs()
				if err != nil {
					return nil, err
				}
				flow.Outputs = outputs
			case "import":
				imp, err := p.parseImport()
				if err != nil {
					return nil, err
				}
				flow.Imports = append(flow.Imports, imp)
			case "include":
				imp, err := p.parseInclude()
				if err != nil {
					return nil, err
				}
				flow.Imports = append(flow.Imports, imp)
			default:
				return nil, fmt.Errorf("flow: unexpected section %q at line %d", tok.Value, tok.Line)
			}
		case TokenRetry:
			retry, err := p.parseRetry()
			if err != nil {
				return nil, err
			}
			flow.Retry = retry
		case TokenBreaker:
			breaker, err := p.parseBreaker()
			if err != nil {
				return nil, err
			}
			flow.Breaker = breaker
		case TokenTimeout:
			p.advance()
			timeout, err := p.expectValue()
			if err != nil {
				return nil, err
			}
			flow.Timeout = timeout
		case TokenOnError:
			errors, err := p.parseOnError()
			if err != nil {
				return nil, err
			}
			flow.Errors = errors
		case TokenCompensate:
			comp, err := p.parseCompensate()
			if err != nil {
				return nil, err
			}
			flow.Compensate = comp
		case TokenService:
			svc, err := p.parseService()
			if err != nil {
				return nil, err
			}
			flow.Services = append(flow.Services, svc)
		case TokenEvent:
			evt, err := p.parseEvent()
			if err != nil {
				return nil, err
			}
			flow.Events = append(flow.Events, evt)
		case TokenWorkflow:
			wf, err := p.parseWorkflow()
			if err != nil {
				return nil, err
			}
			flow.Workflow = wf
		default:
			return nil, fmt.Errorf("flow: unexpected token %q at line %d", tok.Value, tok.Line)
		}
	}

	return flow, nil
}

func (p *Parser) parseDescription() (Metadata, error) {
	meta := Metadata{}
	p.advance() // consume 'description'

	// Parse indented content
	for !p.eof() && p.peek().Type == TokenIdent {
		line := p.parseIndentedLine()
		if strings.HasPrefix(line, "tags") {
			// Parse tags
			for !p.eof() && p.peek().Type == TokenIdent {
				tag := p.advance().Value
				meta.Tags = append(meta.Tags, tag)
			}
		} else {
			if meta.Description != "" {
				meta.Description += " "
			}
			meta.Description += line
		}
	}

	return meta, nil
}

func (p *Parser) parseIndentedLine() string {
	var words []string
	for !p.eof() && p.peek().Type != TokenNewline {
		words = append(words, p.advance().Value)
	}
	if !p.eof() && p.peek().Type == TokenNewline {
		p.advance()
	}
	return strings.Join(words, " ")
}

func (p *Parser) parseVariables() ([]Variable, error) {
	p.advance() // consume 'variables'
	var vars []Variable

	for !p.eof() {
		tok := p.peek()
		if tok.Type != TokenIdent {
			break
		}
		// Check if next token is a type
		if p.pos+1 < len(p.tokens) && (p.tokens[p.pos+1].Type == TokenIdent || p.tokens[p.pos+1].Type == TokenGRPC || p.tokens[p.pos+1].Type == TokenHTTP) {
			name := p.advance().Value
			typ, err := p.expectIdent()
			if err != nil {
				return nil, err
			}
			vars = append(vars, Variable{Name: name, Type: typ, Line: tok.Line})
		} else {
			break
		}
	}

	return vars, nil
}

func (p *Parser) parseConstants() ([]Constant, error) {
	p.advance() // consume 'constants'
	var consts []Constant

	for !p.eof() {
		tok := p.peek()
		if tok.Type != TokenIdent {
			break
		}
		name := p.advance().Value
		if p.match(TokenEquals) {
			p.advance()
		}
		value := p.advance().Value
		consts = append(consts, Constant{Name: name, Value: value, Line: tok.Line})
	}

	return consts, nil
}

func (p *Parser) parseOutputs() ([]Output, error) {
	p.advance() // consume 'output'
	var outputs []Output

	for !p.eof() {
		tok := p.peek()
		if tok.Type != TokenIdent {
			break
		}
		name := p.advance().Value
		outputs = append(outputs, Output{Name: name, Line: tok.Line})
	}

	return outputs, nil
}

func (p *Parser) parseImport() (Import, error) {
	p.advance() // consume 'import'
	path, err := p.expectValue()
	if err != nil {
		return Import{}, err
	}

	// Optional alias
	imp := Import{Path: path}
	if p.match(TokenIdent) && p.peek().Value == "as" {
		p.advance()
		alias, err := p.expectIdent()
		if err != nil {
			return Import{}, err
		}
		imp.As = alias
	}

	return imp, nil
}

func (p *Parser) parseInclude() (Import, error) {
	p.advance() // consume 'include'
	path, err := p.expectValue()
	if err != nil {
		return Import{}, err
	}
	return Import{Path: path}, nil
}

func (p *Parser) parseService() (Service, error) {
	p.advance() // consume 'service'
	name, err := p.expectIdent()
	if err != nil {
		return Service{}, err
	}

	svc := Service{Name: name}

	// Parse service options (indented block)
	for !p.eof() {
		tok := p.peek()
		if tok.Type != TokenIdent {
			break
		}
		// Check if this is a new top-level keyword
		if tok.Value == "service" || tok.Value == "event" || tok.Value == "workflow" || tok.Value == "description" || tok.Value == "variables" || tok.Value == "constants" || tok.Value == "output" || tok.Value == "import" || tok.Value == "include" || tok.Value == "retry" || tok.Value == "breaker" || tok.Value == "timeout" || tok.Value == "onError" || tok.Value == "compensate" {
			break
		}

		key := p.advance().Value

		// Special handling for type
		if key == "type" {
			typeTok := p.peek()
			switch typeTok.Type {
			case TokenGRPC:
				svc.Type = ServiceGRPC
				p.advance()
			case TokenHTTP:
				svc.Type = ServiceHTTP
				p.advance()
			case TokenKafka:
				svc.Type = ServiceKafka
				p.advance()
			case TokenRedis:
				svc.Type = ServiceRedis
				p.advance()
			case TokenPostgres:
				svc.Type = ServicePostgres
				p.advance()
			case TokenTCP:
				svc.Type = ServiceTCP
				p.advance()
			default:
				svc.Type = ServiceType(p.advance().Value)
			}
		} else if key == "tls" || key == "idempotent" || key == "enabled" {
			// Boolean options
			val := p.advance().Value
			svc.Options = append(svc.Options, ServiceOption{Key: key, Value: val == "true"})
		} else if key == "brokers" || key == "headers" {
			// List options - handle values with colons (e.g., kafka1:9092)
			var items []string
			for !p.eof() {
				tok := p.peek()
				if tok.Type == TokenIdent || tok.Type == TokenNumber {
					item := p.advance().Value
					// Handle values with colons (e.g., kafka1:9092)
					for p.match(TokenColon) {
						p.advance() // skip colon
						item += ":" + p.advance().Value
					}
					items = append(items, item)
				} else {
					break
				}
			}
			svc.Options = append(svc.Options, ServiceOption{Key: key, Value: items})
		} else if key == "connection" {
			// Map options
			conn := make(map[string]string)
			for !p.eof() && p.peek().Type == TokenIdent {
				connKey := p.advance().Value
				connVal := p.advance().Value
				conn[connKey] = connVal
			}
			svc.Options = append(svc.Options, ServiceOption{Key: key, Value: conn})
		} else {
			// Simple key-value - skip colon if present
			if p.match(TokenColon) {
				p.advance()
			}
			// Handle values that may contain colons (e.g., auth:50051)
			val := p.advance().Value
			for p.match(TokenColon) {
				p.advance() // skip colon
				val += ":" + p.advance().Value
			}
			svc.Options = append(svc.Options, ServiceOption{Key: key, Value: val})
		}
	}

	return svc, nil
}

func (p *Parser) parseEvent() (Event, error) {
	p.advance() // consume 'event'
	name, err := p.expectIdent()
	if err != nil {
		return Event{}, err
	}

	evt := Event{Name: name}

	// Optional payload
	if p.match(TokenIdent) && p.peek().Value == "payload" {
		p.advance()
		payload, err := p.expectIdent()
		if err != nil {
			return Event{}, err
		}
		evt.Payload = payload
	}

	return evt, nil
}

func (p *Parser) parseWorkflow() (*Workflow, error) {
	p.advance() // consume 'workflow'

	wf := &Workflow{}
	steps, err := p.parseWorkflowSteps()
	if err != nil {
		return nil, err
	}
	wf.Steps = steps
	return wf, nil
}

func (p *Parser) parseWorkflowSteps() ([]WorkflowStep, error) {
	var steps []WorkflowStep

	for !p.eof() {
		tok := p.peek()

		// End of indented block
		if tok.Type == TokenIdent {
			// Check if this is a new section keyword
			if tok.Value == "service" || tok.Value == "event" || tok.Value == "workflow" || tok.Value == "description" || tok.Value == "variables" || tok.Value == "constants" || tok.Value == "output" || tok.Value == "import" || tok.Value == "include" {
				break
			}
		}

		// Also break on section keywords
		if tok.Type == TokenRetry || tok.Type == TokenBreaker || tok.Type == TokenTimeout || tok.Type == TokenOnError || tok.Type == TokenCompensate || tok.Type == TokenService || tok.Type == TokenEvent || tok.Type == TokenWorkflow {
			break
		}

		if tok.Type == TokenEOF {
			break
		}

		step, err := p.parseWorkflowStep()
		if err != nil {
			return nil, err
		}
		if step != nil {
			steps = append(steps, step)
		}
	}

	return steps, nil
}

func (p *Parser) parseWorkflowStep() (WorkflowStep, error) {
	tok := p.peek()

	switch tok.Type {
	case TokenArrow:
		p.advance()
		// -> StepName
		name, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		return &StepRef{Name: name, Line: tok.Line}, nil

	case TokenIf:
		return p.parseIfBlock()

	case TokenSwitch:
		return p.parseSwitchBlock()

	case TokenParallel:
		return p.parseParallelBlock()

	case TokenWait:
		return p.parseWaitBlock()

	case TokenForeach:
		return p.parseForeachLoop()

	case TokenWhile:
		return p.parseWhileLoop()

	case TokenEmit:
		p.advance()
		event, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		return &EmitEvent{Event: event, Line: tok.Line}, nil

	case TokenReturn:
		p.advance()
		value := ""
		if !p.eof() && p.peek().Type != TokenNewline {
			value = p.advance().Value
		}
		return &ReturnStep{Value: value, Line: tok.Line}, nil

	case TokenStart, TokenEnd:
		p.advance()
		return &StepRef{Name: tok.Value, Line: tok.Line}, nil

	case TokenIdent:
		// Could be a step reference or label
		name := p.advance().Value
		return &StepRef{Name: name, Line: tok.Line}, nil

	case TokenNewline:
		p.advance()
		return nil, nil

	default:
		p.advance()
		return nil, nil
	}
}

func (p *Parser) parseIfBlock() (*IfBlock, error) {
	p.advance() // consume 'if'
	cond, err := p.parseCondition()
	if err != nil {
		return nil, err
	}

	block := &IfBlock{Condition: cond}

	// Parse 'then' steps
	block.Then, err = p.parseWorkflowSteps()
	if err != nil {
		return nil, err
	}

	// Check for 'else'
	if p.match(TokenElse) {
		p.advance()
		block.Else, err = p.parseWorkflowSteps()
		if err != nil {
			return nil, err
		}
	}

	return block, nil
}

func (p *Parser) parseCondition() (string, error) {
	var parts []string
	for !p.eof() && p.peek().Type != TokenNewline && p.peek().Type != TokenEOF {
		parts = append(parts, p.advance().Value)
	}
	return strings.Join(parts, " "), nil
}

func (p *Parser) parseSwitchBlock() (*SwitchBlock, error) {
	p.advance() // consume 'switch'
	variable, err := p.expectIdent()
	if err != nil {
		return nil, err
	}

	block := &SwitchBlock{Variable: variable}

	for !p.eof() {
		tok := p.peek()
		if tok.Type == TokenCase {
			p.advance()
			value := p.advance().Value
			steps, err := p.parseWorkflowSteps()
			if err != nil {
				return nil, err
			}
			block.Cases = append(block.Cases, CaseBlock{Value: value, Steps: steps})
		} else if tok.Type == TokenDefault {
			p.advance()
			steps, err := p.parseWorkflowSteps()
			if err != nil {
				return nil, err
			}
			block.Default = steps
		} else {
			break
		}
	}

	return block, nil
}

func (p *Parser) parseParallelBlock() (*ParallelBlock, error) {
	p.advance() // consume 'parallel'

	block := &ParallelBlock{}
	steps, err := p.parseWorkflowSteps()
	if err != nil {
		return nil, err
	}
	block.Steps = steps

	// Consume 'join'
	if p.match(TokenJoin) {
		p.advance()
	}

	return block, nil
}

func (p *Parser) parseWaitBlock() (*WaitBlock, error) {
	p.advance() // consume 'wait'

	event, err := p.expectIdent()
	if err != nil {
		return nil, err
	}

	block := &WaitBlock{Event: event}

	// Optional timeout
	if !p.eof() && p.peek().Type == TokenIdent && p.peek().Value == "timeout" {
		p.advance()
		timeout, err := p.expectValue()
		if err != nil {
			return nil, err
		}
		block.Timeout = timeout
	}

	return block, nil
}

func (p *Parser) parseForeachLoop() (*ForeachLoop, error) {
	p.advance() // consume 'foreach'

	variable, err := p.expectIdent()
	if err != nil {
		return nil, err
	}

	loop := &ForeachLoop{Variable: variable}
	steps, err := p.parseWorkflowSteps()
	if err != nil {
		return nil, err
	}
	loop.Steps = steps

	return loop, nil
}

func (p *Parser) parseWhileLoop() (*WhileLoop, error) {
	p.advance() // consume 'while'

	cond, err := p.parseCondition()
	if err != nil {
		return nil, err
	}

	loop := &WhileLoop{Condition: cond}
	steps, err := p.parseWorkflowSteps()
	if err != nil {
		return nil, err
	}
	loop.Steps = steps

	return loop, nil
}

func (p *Parser) parseRetry() (*RetryPolicy, error) {
	p.advance() // consume 'retry'

	retry := &RetryPolicy{}

	for !p.eof() {
		tok := p.peek()
		if tok.Type != TokenIdent {
			break
		}

		switch tok.Value {
		case "attempts":
			p.advance()
			val, err := p.expectValue()
			if err != nil {
				return nil, err
			}
			retry.Attempts, _ = strconv.Atoi(val)
		case "backoff":
			p.advance()
			retry.Backoff = p.advance().Value
		case "delay":
			p.advance()
			retry.Delay = p.advance().Value
		case "maxDelay":
			p.advance()
			retry.MaxDelay = p.advance().Value
		default:
			return nil, fmt.Errorf("flow: unknown retry option %q at line %d", tok.Value, tok.Line)
		}
	}

	return retry, nil
}

func (p *Parser) parseBreaker() (*CircuitBreaker, error) {
	p.advance() // consume 'breaker'

	breaker := &CircuitBreaker{}

	for !p.eof() {
		tok := p.peek()
		if tok.Type != TokenIdent {
			break
		}

		switch tok.Value {
		case "failureRate":
			p.advance()
			val, err := p.expectValue()
			if err != nil {
				return nil, err
			}
			breaker.FailureRate, _ = strconv.Atoi(val)
		case "window":
			p.advance()
			val, err := p.expectValue()
			if err != nil {
				return nil, err
			}
			breaker.Window, _ = strconv.Atoi(val)
		case "cooldown":
			p.advance()
			breaker.Cooldown = p.advance().Value
		default:
			return nil, fmt.Errorf("flow: unknown breaker option %q at line %d", tok.Value, tok.Line)
		}
	}

	return breaker, nil
}

func (p *Parser) parseOnError() (*ErrorBlock, error) {
	p.advance() // consume 'onError'

	block := &ErrorBlock{}

	for !p.eof() {
		tok := p.peek()
		if tok.Type != TokenIdent {
			break
		}

		if tok.Value == "Default" {
			p.advance()
			steps, err := p.parseWorkflowSteps()
			if err != nil {
				return nil, err
			}
			block.Fallback = steps
		} else {
			errorType := p.advance().Value
			steps, err := p.parseWorkflowSteps()
			if err != nil {
				return nil, err
			}
			block.Cases = append(block.Cases, ErrorCase{ErrorType: errorType, Steps: steps})
		}
	}

	return block, nil
}

func (p *Parser) parseCompensate() ([]CompensateStep, error) {
	p.advance() // consume 'compensate'

	var steps []CompensateStep
	for !p.eof() {
		tok := p.peek()
		if tok.Type != TokenIdent {
			break
		}
		step := p.advance().Value
		comp := p.advance().Value
		steps = append(steps, CompensateStep{Step: step, Compensation: comp})
	}

	return steps, nil
}

// Helper methods

func (p *Parser) peek() Token {
	if p.pos >= len(p.tokens) {
		return Token{Type: TokenEOF}
	}
	return p.tokens[p.pos]
}

func (p *Parser) advance() Token {
	tok := p.tokens[p.pos]
	p.pos++
	return tok
}

func (p *Parser) match(typ TokenType) bool {
	return p.peek().Type == typ
}

func (p *Parser) eof() bool {
	return p.pos >= len(p.tokens) || p.tokens[p.pos].Type == TokenEOF
}

func (p *Parser) expectIdent() (string, error) {
	tok := p.peek()
	if tok.Type != TokenIdent {
		// Allow keywords as identifiers in step references
		if tok.Type >= TokenFlow && tok.Type <= TokenTCP {
			return p.advance().Value, nil
		}
		return "", fmt.Errorf("flow: expected identifier, got %q at line %d", tok.Value, tok.Line)
	}
	return p.advance().Value, nil
}

func (p *Parser) expectValue() (string, error) {
	tok := p.peek()
	switch tok.Type {
	case TokenString, TokenNumber, TokenDuration, TokenBool, TokenIdent:
		return p.advance().Value, nil
	default:
		// Allow keywords as values
		if tok.Type >= TokenFlow && tok.Type <= TokenTCP {
			return p.advance().Value, nil
		}
		return "", fmt.Errorf("flow: expected value, got %q at line %d", tok.Value, tok.Line)
	}
}
