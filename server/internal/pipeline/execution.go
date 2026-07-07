package pipeline

import (
	"context"
	"fmt"

	"github.com/premchandkpc/FlowRulZ/server/internal/observability"
	"github.com/premchandkpc/FlowRulZ/server/internal/reliability"
)

// ExecutionFunc is the function signature for the actual execution logic.
type ExecutionFunc func(ctx context.Context, body []byte) ([][]byte, error)

// ExecutionHandler runs the core execution logic.
type ExecutionHandler struct {
	ExecuteFn ExecutionFunc
}

// Execute implements Handler.
func (h *ExecutionHandler) Execute(ctx context.Context, req *Request, next func() (*Response, error)) (*Response, error) {
	if h.ExecuteFn == nil {
		return &Response{
			Error: fmt.Errorf("execution function not configured"),
		}, nil
	}

	results, err := h.ExecuteFn(ctx, req.Body)
	if err != nil {
		observability.RecordError("exec")
		return &Response{
			Error: err,
		}, nil
	}

	observability.RecordExec("msg")

	// Return first result if available
	if len(results) > 0 {
		return &Response{
			Body: results[0],
		}, nil
	}

	return &Response{}, nil
}

// DLQHandler sends failed requests to the dead letter queue.
type DLQHandler struct {
	DLQ *reliability.DLQ
}

// Execute implements Handler.
func (h *DLQHandler) Execute(ctx context.Context, req *Request, next func() (*Response, error)) (*Response, error) {
	resp, err := next()

	// If there's an error and DLQ is configured, send to DLQ
	if err != nil && h.DLQ != nil {
		h.DLQ.Send(&reliability.DeadLetterEntry{
			ID:    fmt.Sprintf("dlq-%d", ctx.Value("exec_id")),
			Body:  req.Body,
			Error: err.Error(),
		})
	}

	return resp, err
}
