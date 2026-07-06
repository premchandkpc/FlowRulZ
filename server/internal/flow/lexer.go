package flow

import (
	"fmt"
	"unicode"
)

// TokenType represents the type of a token.
type TokenType int

const (
	// Literals
	TokenIdent TokenType = iota
	TokenString
	TokenNumber
	TokenDuration
	TokenBool
	TokenVersion

	// Keywords
	TokenFlow
	TokenService
	TokenEvent
	TokenWorkflow
	TokenIf
	TokenElse
	TokenSwitch
	TokenCase
	TokenDefault
	TokenParallel
	TokenJoin
	TokenRetry
	TokenBreaker
	TokenTimeout
	TokenWait
	TokenForeach
	TokenWhile
	TokenOutput
	TokenImport
	TokenInclude
	TokenCompensate
	TokenOnError
	TokenEmit
	TokenStart
	TokenEnd
	TokenReturn
	TokenSuccess
	TokenFailure

	// Types
	TokenGRPC
	TokenHTTP
	TokenKafka
	TokenRedis
	TokenPostgres
	TokenTCP

	// Operators
	TokenArrow     // ->
	TokenEquals    // =
	TokenNotEquals // !=
	TokenLess      // <
	TokenGreater   // >
	TokenLE        // <=
	TokenGE        // >=
	TokenAnd       // &&
	TokenOr        // ||
	TokenNot       // !

	// Delimiters
	TokenLParen   // (
	TokenRParen   // )
	TokenLBrace   // {
	TokenRBrace   // }
	TokenLBracket // [
	TokenRBracket // ]
	TokenColon    // :
	TokenComma    // ,
	TokenDot      // .
	TokenDollar   // $
	TokenAt       // @
	TokenHash     // #
	TokenNewline

	// Special
	TokenEOF
	TokenError
)

var keywords = map[string]TokenType{
	"flow":        TokenFlow,
	"service":     TokenService,
	"event":       TokenEvent,
	"workflow":    TokenWorkflow,
	"if":          TokenIf,
	"else":        TokenElse,
	"switch":      TokenSwitch,
	"case":        TokenCase,
	"default":     TokenDefault,
	"parallel":    TokenParallel,
	"join":        TokenJoin,
	"retry":       TokenRetry,
	"breaker":     TokenBreaker,
	"timeout":     TokenTimeout,
	"wait":        TokenWait,
	"foreach":     TokenForeach,
	"while":       TokenWhile,
	"output":      TokenOutput,
	"import":      TokenImport,
	"include":     TokenInclude,
	"compensate":  TokenCompensate,
	"onError":     TokenOnError,
	"emit":        TokenEmit,
	"Start":       TokenStart,
	"End":         TokenEnd,
	"Return":      TokenReturn,
	"success":     TokenSuccess,
	"failure":     TokenFailure,
	"grpc":        TokenGRPC,
	"http":        TokenHTTP,
	"kafka":       TokenKafka,
	"redis":       TokenRedis,
	"postgres":    TokenPostgres,
	"tcp":         TokenTCP,
	"version":     TokenVersion,
	"true":        TokenBool,
	"false":       TokenBool,
}

// Token represents a lexical token.
type Token struct {
	Type    TokenType
	Value   string
	Line    int
	Column  int
}

func (t Token) String() string {
	return fmt.Sprintf("%s(%q)", TokenTypeName(t.Type), t.Value)
}

// Lexer tokenizes .flow source code.
type Lexer struct {
	input  string
	pos    int
	line   int
	col    int
	tokens []Token
}

// NewLexer creates a new lexer.
func NewLexer(input string) *Lexer {
	return &Lexer{
		input: input,
		pos:   0,
		line:  1,
		col:   1,
	}
}

// Tokenize returns all tokens from the input.
func (l *Lexer) Tokenize() ([]Token, error) {
	for l.pos < len(l.input) {
		l.skipWhitespace()
		if l.pos >= len(l.input) {
			break
		}

		ch := l.peek()

		// Comments
		if ch == '#' || (ch == '/' && l.peekAt(1) == '/') {
			l.skipComment()
			continue
		}
		if ch == '/' && l.peekAt(1) == '*' {
			l.skipBlockComment()
			continue
		}

		// Newline
		if ch == '\n' {
			l.tokens = append(l.tokens, Token{Type: TokenNewline, Value: "\n", Line: l.line, Column: l.col})
			l.advance()
			l.line++
			l.col = 1
			continue
		}

		// Arrow
		if ch == '-' && l.peekAt(1) == '>' {
			l.tokens = append(l.tokens, Token{Type: TokenArrow, Value: "->", Line: l.line, Column: l.col})
			l.advance()
			l.advance()
			l.col += 2
			continue
		}

		// Operators
		if ch == '=' && l.peekAt(1) != '=' {
			l.tokens = append(l.tokens, Token{Type: TokenEquals, Value: "=", Line: l.line, Column: l.col})
			l.advance()
			l.col++
			continue
		}
		if ch == '!' && l.peekAt(1) == '=' {
			l.tokens = append(l.tokens, Token{Type: TokenNotEquals, Value: "!=", Line: l.line, Column: l.col})
			l.advance()
			l.advance()
			l.col += 2
			continue
		}
		if ch == '<' && l.peekAt(1) == '=' {
			l.tokens = append(l.tokens, Token{Type: TokenLE, Value: "<=", Line: l.line, Column: l.col})
			l.advance()
			l.advance()
			l.col += 2
			continue
		}
		if ch == '>' && l.peekAt(1) == '=' {
			l.tokens = append(l.tokens, Token{Type: TokenGE, Value: ">=", Line: l.line, Column: l.col})
			l.advance()
			l.advance()
			l.col += 2
			continue
		}
		if ch == '<' {
			l.tokens = append(l.tokens, Token{Type: TokenLess, Value: "<", Line: l.line, Column: l.col})
			l.advance()
			l.col++
			continue
		}
		if ch == '>' {
			l.tokens = append(l.tokens, Token{Type: TokenGreater, Value: ">", Line: l.line, Column: l.col})
			l.advance()
			l.col++
			continue
		}
		if ch == '&' && l.peekAt(1) == '&' {
			l.tokens = append(l.tokens, Token{Type: TokenAnd, Value: "&&", Line: l.line, Column: l.col})
			l.advance()
			l.advance()
			l.col += 2
			continue
		}
		if ch == '|' && l.peekAt(1) == '|' {
			l.tokens = append(l.tokens, Token{Type: TokenOr, Value: "||", Line: l.line, Column: l.col})
			l.advance()
			l.advance()
			l.col += 2
			continue
		}
		if ch == '!' {
			l.tokens = append(l.tokens, Token{Type: TokenNot, Value: "!", Line: l.line, Column: l.col})
			l.advance()
			l.col++
			continue
		}

		// Delimiters
		switch ch {
		case '(':
			l.addToken(TokenLParen, "(")
		case ')':
			l.addToken(TokenRParen, ")")
		case '{':
			l.addToken(TokenLBrace, "{")
		case '}':
			l.addToken(TokenRBrace, "}")
		case '[':
			l.addToken(TokenLBracket, "[")
		case ']':
			l.addToken(TokenRBracket, "]")
		case ':':
			l.addToken(TokenColon, ":")
		case ',':
			l.addToken(TokenComma, ",")
		case '.':
			l.addToken(TokenDot, ".")
		case '$':
			l.addToken(TokenDollar, "$")
		case '@':
			l.addToken(TokenAt, "@")
		default:
			if ch == '"' || ch == '\'' {
				l.readString()
			} else if unicode.IsDigit(ch) || (ch == '-' && unicode.IsDigit(l.peekAt(1))) {
				l.readNumber()
			} else if ch == 'v' && l.peekAt(1) == '.' {
				l.readVersion()
			} else if isIdentStart(ch) {
				l.readIdent()
			} else {
				return nil, fmt.Errorf("flow: unexpected character %q at line %d col %d", ch, l.line, l.col)
			}
		}
	}

	l.tokens = append(l.tokens, Token{Type: TokenEOF, Line: l.line, Column: l.col})

	// Check for any error tokens
	for _, tok := range l.tokens {
		if tok.Type == TokenError {
			return l.tokens, fmt.Errorf("flow: %s at line %d col %d", tok.Value, tok.Line, tok.Column)
		}
	}

	return l.tokens, nil
}

func (l *Lexer) peek() rune {
	if l.pos >= len(l.input) {
		return 0
	}
	return rune(l.input[l.pos])
}

func (l *Lexer) peekAt(offset int) rune {
	pos := l.pos + offset
	if pos >= len(l.input) {
		return 0
	}
	return rune(l.input[pos])
}

func (l *Lexer) advance() rune {
	ch := rune(l.input[l.pos])
	l.pos++
	return ch
}

func (l *Lexer) addToken(typ TokenType, value string) {
	l.tokens = append(l.tokens, Token{Type: typ, Value: value, Line: l.line, Column: l.col})
	l.advance()
	l.col++
}

func (l *Lexer) skipWhitespace() {
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == ' ' || ch == '\t' || ch == '\r' {
			l.advance()
			l.col++
		} else {
			break
		}
	}
}

func (l *Lexer) skipComment() {
	for l.pos < len(l.input) && l.input[l.pos] != '\n' {
		l.advance()
	}
}

func (l *Lexer) skipBlockComment() {
	l.advance() // skip /
	l.advance() // skip *
	for l.pos < len(l.input)-1 {
		if l.input[l.pos] == '*' && l.input[l.pos+1] == '/' {
			l.advance()
			l.advance()
			return
		}
		if l.input[l.pos] == '\n' {
			l.line++
			l.col = 1
		}
		l.advance()
	}
}

func (l *Lexer) readString() {
	quote := l.advance()
	start := l.pos
	for l.pos < len(l.input) && rune(l.input[l.pos]) != quote {
		if l.input[l.pos] == '\\' {
			l.advance()
		}
		l.advance()
	}
	// Check for unclosed string
	if l.pos >= len(l.input) {
		l.tokens = append(l.tokens, Token{Type: TokenError, Value: "unclosed string", Line: l.line, Column: l.col})
		// Set pos to trigger error on next iteration
		l.pos = len(l.input)
		return
	}
	value := l.input[start:l.pos]
	l.advance() // closing quote
	l.tokens = append(l.tokens, Token{Type: TokenString, Value: value, Line: l.line, Column: l.col})
	l.col += l.pos - start + 2
}

func (l *Lexer) readNumber() {
	start := l.pos
	if l.input[l.pos] == '-' {
		l.advance()
	}
	for l.pos < len(l.input) && (unicode.IsDigit(rune(l.input[l.pos])) || l.input[l.pos] == '.') {
		l.advance()
	}
	// Check for duration suffix (ms, s, m, h, d, w, M, y)
	if l.pos < len(l.input) {
		ch := l.input[l.pos]
		ch2 := byte(0)
		if l.pos+1 < len(l.input) {
			ch2 = l.input[l.pos+1]
		}
		// Two-char suffixes first: ms
		if ch == 'm' && ch2 == 's' {
			l.advance() // m
			l.advance() // s
			value := l.input[start:l.pos]
			l.tokens = append(l.tokens, Token{Type: TokenDuration, Value: value, Line: l.line, Column: l.col})
			l.col += l.pos - start
			return
		}
		// Single-char suffixes
		if ch == 's' || ch == 'm' || ch == 'h' || ch == 'd' || ch == 'w' || ch == 'M' || ch == 'y' {
			// Not followed by alphanumeric (e.g., "service")
			if l.pos+1 >= len(l.input) || !isIdentChar(rune(l.input[l.pos+1])) {
				l.advance()
				value := l.input[start:l.pos]
				l.tokens = append(l.tokens, Token{Type: TokenDuration, Value: value, Line: l.line, Column: l.col})
				l.col += l.pos - start
				return
			}
		}
	}
	value := l.input[start:l.pos]
	l.tokens = append(l.tokens, Token{Type: TokenNumber, Value: value, Line: l.line, Column: l.col})
	l.col += l.pos - start
}

func (l *Lexer) readVersion() {
	start := l.pos
	for l.pos < len(l.input) && (l.input[l.pos] == '.' || unicode.IsDigit(rune(l.input[l.pos]))) {
		l.advance()
	}
	value := l.input[start:l.pos]
	l.tokens = append(l.tokens, Token{Type: TokenVersion, Value: value, Line: l.line, Column: l.col})
	l.col += l.pos - start
}

func (l *Lexer) readIdent() {
	start := l.pos
	for l.pos < len(l.input) && isIdentChar(rune(l.input[l.pos])) {
		l.advance()
	}
	value := l.input[start:l.pos]

	// Check if it's a keyword
	if typ, ok := keywords[value]; ok {
		l.tokens = append(l.tokens, Token{Type: typ, Value: value, Line: l.line, Column: l.col})
	} else {
		l.tokens = append(l.tokens, Token{Type: TokenIdent, Value: value, Line: l.line, Column: l.col})
	}
	l.col += l.pos - start
}

func isIdentStart(ch rune) bool {
	return unicode.IsLetter(ch) || ch == '_'
}

func isIdentChar(ch rune) bool {
	return unicode.IsLetter(ch) || unicode.IsDigit(ch) || ch == '_' || ch == '.' || ch == '-'
}

// FilterNewlines removes newline tokens (useful for某些 parsing contexts).
func FilterNewlines(tokens []Token) []Token {
	var result []Token
	for _, t := range tokens {
		if t.Type != TokenNewline {
			result = append(result, t)
		}
	}
	return result
}

// TokenTypeName returns the string name of a token type.
func TokenTypeName(typ TokenType) string {
	names := map[TokenType]string{
		TokenIdent: "ident", TokenString: "string", TokenNumber: "number",
		TokenDuration: "duration", TokenBool: "bool", TokenVersion: "version",
		TokenFlow: "flow", TokenService: "service", TokenEvent: "event",
		TokenWorkflow: "workflow", TokenIf: "if", TokenElse: "else",
		TokenSwitch: "switch", TokenCase: "case", TokenDefault: "default",
		TokenParallel: "parallel", TokenJoin: "join", TokenRetry: "retry",
		TokenBreaker: "breaker", TokenTimeout: "timeout", TokenWait: "wait",
		TokenForeach: "foreach", TokenWhile: "while", TokenOutput: "output",
		TokenImport: "import", TokenInclude: "include", TokenCompensate: "compensate",
		TokenOnError: "onError", TokenEmit: "emit", TokenStart: "Start",
		TokenEnd: "End", TokenReturn: "Return", TokenSuccess: "success",
		TokenFailure: "failure", TokenGRPC: "grpc", TokenHTTP: "http",
		TokenKafka: "kafka", TokenRedis: "redis", TokenPostgres: "postgres",
		TokenTCP: "tcp", TokenArrow: "->", TokenEquals: "=",
		TokenNotEquals: "!=", TokenLess: "<", TokenGreater: ">",
		TokenLE: "<=", TokenGE: ">=", TokenAnd: "&&", TokenOr: "||",
		TokenNot: "!", TokenLParen: "(", TokenRParen: ")",
		TokenLBrace: "{", TokenRBrace: "}", TokenLBracket: "[",
		TokenRBracket: "]", TokenColon: ":", TokenComma: ",",
		TokenDot: ".", TokenDollar: "$", TokenAt: "@", TokenHash: "#",
		TokenNewline: "newline", TokenEOF: "eof", TokenError: "error",
	}
	if name, ok := names[typ]; ok {
		return name
	}
	return fmt.Sprintf("token(%d)", int(typ))
}

// Position returns current position info.
func (l *Lexer) Position() (line, col int) {
	return l.line, l.col
}

// Remaining returns remaining input.
func (l *Lexer) Remaining() string {
	if l.pos >= len(l.input) {
		return ""
	}
	return l.input[l.pos:]
}

// PeekToken returns the next non-newline token without consuming it.
func (l *Lexer) PeekToken(tokens []Token, pos int) Token {
	for i := pos; i < len(tokens); i++ {
		if tokens[i].Type != TokenNewline {
			return tokens[i]
		}
	}
	return Token{Type: TokenEOF}
}
