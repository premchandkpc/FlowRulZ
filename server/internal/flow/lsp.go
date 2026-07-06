package flow

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// LSPServer is a Language Server Protocol implementation for .flow files.
type LSPServer struct {
	parser    *Parser
	analyzer  *Analyzer
	compiler  *Compiler
	formatter *Formatter
	documents map[string]*Document
}

// Document represents an open document.
type Document struct {
	URI     string
	Content string
	Flow    *Flow
	Errors  []Diagnostic
}

// Diagnostic represents a LSP diagnostic.
type Diagnostic struct {
	Range    Range    `json:"range"`
	Severity int      `json:"severity"`
	Source   string   `json:"source"`
	Message  string   `json:"message"`
}

// Range represents a text range.
type Range struct {
	Start Position `json:"start"`
	End   Position `json:"end"`
}

// Position represents a cursor position.
type Position struct {
	Line      int `json:"line"`
	Character int `json:"character"`
}

// CompletionItem represents an auto-complete suggestion.
type CompletionItem struct {
	Label  string `json:"label"`
	Kind   int    `json:"kind"`
	Detail string `json:"detail"`
}

// HoverInfo represents hover information.
type HoverInfo struct {
	Contents string `json:"contents"`
}

// NewLSPServer creates a new LSP server.
func NewLSPServer() *LSPServer {
	return &LSPServer{
		parser:    NewParser(),
		analyzer:  NewAnalyzer(),
		compiler:  NewCompiler(),
		formatter: NewFormatter(),
		documents: make(map[string]*Document),
	}
}

// OpenDocument handles textDocument/didOpen.
func (s *LSPServer) OpenDocument(uri, content string) []Diagnostic {
	doc := &Document{
		URI:     uri,
		Content: content,
	}

	flow, err := s.parser.ParseString(content)
	if err != nil {
		doc.Errors = []Diagnostic{{
			Range: Range{
				Start: Position{Line: 0, Character: 0},
				End:   Position{Line: 0, Character: 0},
			},
			Severity: 1,
			Source:   "flow",
			Message:  err.Error(),
		}}
		s.documents[uri] = doc
		return doc.Errors
	}

	doc.Flow = flow

	// Semantic analysis
	errs := s.analyzer.Analyze(flow)
	diagnostics := make([]Diagnostic, len(errs))
	for i, e := range errs {
		diagnostics[i] = Diagnostic{
			Range: Range{
				Start: Position{Line: e.Line - 1, Character: 0},
				End:   Position{Line: e.Line - 1, Character: 100},
			},
			Severity: 1,
			Source:   "flow",
			Message:  e.Message,
		}
	}

	doc.Errors = diagnostics
	s.documents[uri] = doc
	return diagnostics
}

// UpdateDocument handles textDocument/didChange.
func (s *LSPServer) UpdateDocument(uri, content string) []Diagnostic {
	return s.OpenDocument(uri, content)
}

// CloseDocument handles textDocument/didClose.
func (s *LSPServer) CloseDocument(uri string) {
	delete(s.documents, uri)
}

// FormatDocument handles textDocument/formatting.
func (s *LSPServer) FormatDocument(uri string) (string, error) {
	doc, ok := s.documents[uri]
	if !ok || doc.Flow == nil {
		return "", fmt.Errorf("document not found")
	}

	return s.formatter.Format(doc.Flow), nil
}

// Completion handles textDocument/completion.
func (s *LSPServer) Completion(uri string, pos Position) []CompletionItem {
	doc, ok := s.documents[uri]
	if !ok {
		return nil
	}

	// Get current line
	lines := strings.Split(doc.Content, "\n")
	if pos.Line >= len(lines) {
		return nil
	}

	line := lines[pos.Line]
	word := getWordAtCursor(line, pos.Character)

	var items []CompletionItem

	// Service completions
	for _, svc := range doc.Flow.Services {
		items = append(items, CompletionItem{
			Label:  svc.Name,
			Kind:   12, // Module
			Detail: string(svc.Type),
		})
	}

	// Keyword completions
	keywords := []string{"workflow", "service", "event", "parallel", "join", "if", "else", "switch", "case", "retry", "breaker", "timeout", "emit", "wait", "foreach", "while", "output", "import"}
	for _, kw := range keywords {
		if word == "" || strings.HasPrefix(kw, word) {
			items = append(items, CompletionItem{
				Label:  kw,
				Kind:   14, // Keyword
				Detail: "keyword",
			})
		}
	}

	return items
}

// Hover handles textDocument/hover.
func (s *LSPServer) Hover(uri string, pos Position) *HoverInfo {
	doc, ok := s.documents[uri]
	if !ok || doc.Flow == nil {
		return nil
	}

	lines := strings.Split(doc.Content, "\n")
	if pos.Line >= len(lines) {
		return nil
	}

	line := lines[pos.Line]
	word := getWordAtCursor(line, pos.Character)

	// Check if hovering over a service
	for _, svc := range doc.Flow.Services {
		if svc.Name == word {
			return &HoverInfo{
				Contents: fmt.Sprintf("**%s** (%s)", svc.Name, svc.Type),
			}
		}
	}

	return nil
}

// Diagnostics returns all diagnostics for a document.
func (s *LSPServer) Diagnostics(uri string) []Diagnostic {
	doc, ok := s.documents[uri]
	if !ok {
		return nil
	}
	return doc.Errors
}

// Graph returns the flow graph for visualization.
func (s *LSPServer) Graph(uri string) (string, error) {
	doc, ok := s.documents[uri]
	if !ok || doc.Flow == nil {
		return "", fmt.Errorf("document not found")
	}

	ir, err := s.compiler.Compile(doc.Flow)
	if err != nil {
		return "", err
	}

	gen := NewGraphGenerator()
	return gen.Mermaid(ir), nil
}

func getWordAtCursor(line string, pos int) string {
	if pos > len(line) {
		pos = len(line)
	}

	start := pos
	for start > 0 && line[start-1] != ' ' && line[start-1] != '\t' {
		start--
	}

	end := pos
	for end < len(line) && line[end] != ' ' && line[end] != '\t' {
		end++
	}

	return line[start:end]
}

// HandleRequest processes a JSON-RPC request.
func (s *LSPServer) HandleRequest(request []byte) ([]byte, error) {
	var req struct {
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
	}

	if err := json.Unmarshal(request, &req); err != nil {
		return nil, err
	}

	switch req.Method {
	case "initialize":
		return s.handleInitialize(req.Params)
	case "textDocument/didOpen":
		return s.handleDidOpen(req.Params)
	case "textDocument/didChange":
		return s.handleDidChange(req.Params)
	case "textDocument/didClose":
		return s.handleDidClose(req.Params)
	case "textDocument/formatting":
		return s.handleFormatting(req.Params)
	case "textDocument/completion":
		return s.handleCompletion(req.Params)
	case "textDocument/hover":
		return s.handleHover(req.Params)
	default:
		return nil, fmt.Errorf("unknown method: %s", req.Method)
	}
}

func (s *LSPServer) handleInitialize(params json.RawMessage) ([]byte, error) {
	result := map[string]interface{}{
		"capabilities": map[string]interface{}{
			"textDocumentSync": 1,
			"completionProvider": map[string]interface{}{
				"triggerCharacters": []string{".", ":"},
			},
			"hoverProvider": true,
			"documentFormattingProvider": true,
		},
	}
	return json.Marshal(result)
}

func (s *LSPServer) handleDidOpen(params json.RawMessage) ([]byte, error) {
	var p struct {
		TextDocument struct {
			URI     string `json:"uri"`
			Text    string `json:"text"`
		} `json:"textDocument"`
	}
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, err
	}

	s.OpenDocument(p.TextDocument.URI, p.TextDocument.Text)
	return nil, nil
}

func (s *LSPServer) handleDidChange(params json.RawMessage) ([]byte, error) {
	var p struct {
		TextDocument struct {
			URI string `json:"uri"`
		} `json:"textDocument"`
		ContentChanges []struct {
			Text string `json:"text"`
		} `json:"contentChanges"`
	}
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, err
	}

	if len(p.ContentChanges) > 0 {
		s.UpdateDocument(p.TextDocument.URI, p.ContentChanges[0].Text)
	}
	return nil, nil
}

func (s *LSPServer) handleDidClose(params json.RawMessage) ([]byte, error) {
	var p struct {
		TextDocument struct {
			URI string `json:"uri"`
		} `json:"textDocument"`
	}
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, err
	}

	s.CloseDocument(p.TextDocument.URI)
	return nil, nil
}

func (s *LSPServer) handleFormatting(params json.RawMessage) ([]byte, error) {
	var p struct {
		TextDocument struct {
			URI string `json:"uri"`
		} `json:"textDocument"`
	}
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, err
	}

	content, err := s.FormatDocument(p.TextDocument.URI)
	if err != nil {
		return nil, err
	}

	result := []struct {
		Range   Range  `json:"range"`
		NewText string `json:"newText"`
	}{
		{
			Range: Range{
				Start: Position{Line: 0, Character: 0},
				End:   Position{Line: 999999, Character: 0},
			},
			NewText: content,
		},
	}

	return json.Marshal(result)
}

func (s *LSPServer) handleCompletion(params json.RawMessage) ([]byte, error) {
	var p struct {
		TextDocument struct {
			URI string `json:"uri"`
		} `json:"textDocument"`
		Position Position `json:"position"`
	}
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, err
	}

	items := s.Completion(p.TextDocument.URI, p.Position)
	return json.Marshal(items)
}

func (s *LSPServer) handleHover(params json.RawMessage) ([]byte, error) {
	var p struct {
		TextDocument struct {
			URI string `json:"uri"`
		} `json:"textDocument"`
		Position Position `json:"position"`
	}
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, err
	}

	info := s.Hover(p.TextDocument.URI, p.Position)
	if info == nil {
		return nil, nil
	}

	return json.Marshal(info)
}

func init() {
	// Ensure os is imported for LSP file operations
	_ = os.ReadFile
}
