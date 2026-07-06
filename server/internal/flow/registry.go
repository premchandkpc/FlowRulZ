package flow

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/premchandkpc/FlowRulZ/server/internal/cache"
)

// FlowState represents a flow with its runtime state.
type FlowState struct {
	Name     string   `json:"name"`
	Hash     string   `json:"hash"`
	AST      *Flow    `json:"-"`
	IR       *IR      `json:"ir,omitempty"`
	Status   string   `json:"status"`
	Error    string   `json:"error,omitempty"`
}

// Registry manages flow definitions with caching.
type Registry struct {
	mu      sync.RWMutex
	flows   map[string]*FlowState
	cache   cache.Cache
	parser  *Parser
	analyzer *Analyzer
	compiler *Compiler
	ttl     time.Duration
	stop    chan struct{}
}

// NewRegistry creates a new flow registry.
func NewRegistry(c cache.Cache) *Registry {
	return &Registry{
		flows:    make(map[string]*FlowState),
		cache:    c,
		parser:   NewParser(),
		analyzer: NewAnalyzer(),
		compiler: NewCompiler(),
		ttl:      5 * time.Minute,
		stop:     make(chan struct{}),
	}
}

// LoadFile loads a .flow file into the registry.
func (r *Registry) LoadFile(ctx context.Context, path string) error {
	ast, err := r.parser.ParseFile(path)
	if err != nil {
		return err
	}
	return r.Register(ctx, ast)
}

// LoadDirectory loads all .flow files from a directory.
func (r *Registry) LoadDirectory(ctx context.Context, dir string) error {
	entries, err := readDir(dir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if !entry.IsDir() && hasFlowExtension(entry.Name()) {
			path := joinPath(dir, entry.Name())
			if err := r.LoadFile(ctx, path); err != nil {
				return fmt.Errorf("flow: load %s: %w", path, err)
			}
		}
	}
	return nil
}

// Register adds or updates a flow in the registry.
func (r *Registry) Register(ctx context.Context, ast *Flow) error {
	name := ast.Metadata.Name
	if name == "" {
		return fmt.Errorf("flow: name is required")
	}

	// Semantic analysis
	if errs := r.analyzer.Analyze(ast); len(errs) > 0 {
		state := &FlowState{
			Name:   name,
			Status: "error",
			Error:  errs[0].Error(),
		}
		r.mu.Lock()
		r.flows[name] = state
		r.mu.Unlock()
		return errs[0]
	}

	// Compile to IR
	ir, err := r.compiler.Compile(ast)
	if err != nil {
		return fmt.Errorf("flow: compile %s: %w", name, err)
	}

	// Hash for cache invalidation
	hash := hashAST(ast)

	state := &FlowState{
		Name:   name,
		Hash:   hash,
		AST:    ast,
		IR:     ir,
		Status: "active",
	}

	// Cache the IR
	cacheKey := fmt.Sprintf("flow:%s:ir", name)
	data, _ := json.Marshal(ir)
	_ = r.cache.Set(ctx, cacheKey, data, r.ttl)

	// Update route cache
	if ast.Trigger.Topic != "" {
		routeKey := fmt.Sprintf("flow:route:%s", ast.Trigger.Topic)
		_ = r.cache.Set(ctx, routeKey, []byte(name), r.ttl)
	}

	r.mu.Lock()
	r.flows[name] = state
	r.mu.Unlock()

	return nil
}

// Get returns a flow by name.
func (r *Registry) Get(ctx context.Context, name string) (*FlowState, error) {
	r.mu.RLock()
	state, ok := r.flows[name]
	r.mu.RUnlock()

	if ok {
		return state, nil
	}

	// Try cache
	cacheKey := fmt.Sprintf("flow:%s:ir", name)
	data, err := r.cache.Get(ctx, cacheKey)
	if err != nil || data == nil {
		return nil, fmt.Errorf("flow: %s not found", name)
	}

	var ir IR
	if err := json.Unmarshal(data, &ir); err != nil {
		return nil, fmt.Errorf("flow: unmarshal %s: %w", name, err)
	}

	return &FlowState{
		Name:   name,
		IR:     &ir,
		Status: "active",
	}, nil
}

// GetByTopic returns the flow for a given topic.
func (r *Registry) GetByTopic(ctx context.Context, topic string) (*FlowState, error) {
	// Check route cache
	routeKey := fmt.Sprintf("flow:route:%s", topic)
	data, err := r.cache.Get(ctx, routeKey)
	if err == nil && data != nil {
		return r.Get(ctx, string(data))
	}

	// Linear search
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, state := range r.flows {
		if state.AST != nil && state.AST.Trigger.Topic == topic {
			return state, nil
		}
	}

	return nil, fmt.Errorf("flow: no flow for topic %s", topic)
}

// List returns all registered flows.
func (r *Registry) List(ctx context.Context) []*FlowState {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*FlowState, 0, len(r.flows))
	for _, state := range r.flows {
		result = append(result, state)
	}
	return result
}

// Delete removes a flow from the registry.
func (r *Registry) Delete(ctx context.Context, name string) error {
	r.mu.Lock()
	_, ok := r.flows[name]
	if ok {
		delete(r.flows, name)
	}
	r.mu.Unlock()

	if !ok {
		return fmt.Errorf("flow: %s not found", name)
	}

	// Remove from cache
	cacheKey := fmt.Sprintf("flow:%s:ir", name)
	_ = r.cache.Delete(ctx, cacheKey)

	return nil
}

// Format returns the canonical .flow representation.
func (r *Registry) Format(name string) (string, error) {
	r.mu.RLock()
	state, ok := r.flows[name]
	r.mu.RUnlock()

	if !ok || state.AST == nil {
		return "", fmt.Errorf("flow: %s not found", name)
	}

	formatter := NewFormatter()
	return formatter.Format(state.AST), nil
}

// Close stops the registry.
func (r *Registry) Close() error {
	close(r.stop)
	return nil
}

func hashAST(ast *Flow) string {
	data, _ := json.Marshal(ast)
	h := sha256.Sum256(data)
	return fmt.Sprintf("%x", h[:8])
}

func readDir(dir string) ([]os.DirEntry, error) {
	return os.ReadDir(dir)
}

func hasFlowExtension(name string) bool {
	return len(name) > 5 && name[len(name)-5:] == ".flow"
}

func joinPath(elem ...string) string {
	return filepath.Join(elem...)
}
