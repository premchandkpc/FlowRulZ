package pipeline

import (
	"context"
)

// Handler represents a single stage in the execution pipeline.
// Each handler processes the request and optionally passes it to the next handler.
type Handler interface {
	// Execute processes the request. Returns the result or an error.
	// Call next() to continue the chain, or return early to short-circuit.
	Execute(ctx context.Context, req *Request, next func() (*Response, error)) (*Response, error)
}

// HandlerFunc is an adapter to allow the use of ordinary functions as handlers.
type HandlerFunc func(ctx context.Context, req *Request, next func() (*Response, error)) (*Response, error)

// Execute implements Handler.
func (f HandlerFunc) Execute(ctx context.Context, req *Request, next func() (*Response, error)) (*Response, error) {
	return f(ctx, req, next)
}

// Request represents an incoming execution request.
type Request struct {
	Body    []byte
	Headers map[string]string
	Meta    map[string]any
}

// Response represents the result of execution.
type Response struct {
	Body    []byte
	Headers map[string]string
	Error   error
}

// Chain builds and executes a handler chain.
type Chain struct {
	handlers []Handler
}

// NewChain creates a new handler chain.
func NewChain(handlers ...Handler) *Chain {
	return &Chain{handlers: handlers}
}

// Execute runs the chain starting from the first handler.
func (c *Chain) Execute(ctx context.Context, req *Request) (*Response, error) {
	return c.execute(ctx, req, 0)
}

func (c *Chain) execute(ctx context.Context, req *Request, index int) (*Response, error) {
	if index >= len(c.handlers) {
		// End of chain - return empty response
		return &Response{}, nil
	}

	handler := c.handlers[index]
	return handler.Execute(ctx, req, func() (*Response, error) {
		return c.execute(ctx, req, index+1)
	})
}

// Builder provides a fluent API for constructing handler chains.
type Builder struct {
	handlers []Handler
}

// NewBuilder creates a new chain builder.
func NewBuilder() *Builder {
	return &Builder{handlers: make([]Handler, 0)}
}

// Use adds a handler to the chain.
func (b *Builder) Use(handler Handler) *Builder {
	b.handlers = append(b.handlers, handler)
	return b
}

// UseFunc adds a handler function to the chain.
func (b *Builder) UseFunc(fn HandlerFunc) *Builder {
	b.handlers = append(b.handlers, fn)
	return b
}

// Build constructs the chain.
func (b *Builder) Build() *Chain {
	return NewChain(b.handlers...)
}
