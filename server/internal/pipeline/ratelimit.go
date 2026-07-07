package pipeline

import (
	"context"
	"fmt"
	"hash/fnv"

	"github.com/premchandkpc/FlowRulZ/server/internal/observability"
	"github.com/premchandkpc/FlowRulZ/server/internal/reliability"
)

// RateLimitHandler enforces rate limiting on incoming requests.
type RateLimitHandler struct {
	Limiter *reliability.RateLimiter
	Key     string
}

// Execute implements Handler.
func (h *RateLimitHandler) Execute(ctx context.Context, req *Request, next func() (*Response, error)) (*Response, error) {
	if h.Limiter != nil && !h.Limiter.Allow(h.Key) {
		observability.RecordError("rate_limited")
		return &Response{
			Error: fmt.Errorf("rate limit exceeded for key: %s", h.Key),
		}, nil
	}
	return next()
}

// DedupHandler prevents duplicate message processing.
type DedupHandler struct {
	Dedup *reliability.DedupTracker
}

// Execute implements Handler.
func (h *DedupHandler) Execute(ctx context.Context, req *Request, next func() (*Response, error)) (*Response, error) {
	if h.Dedup == nil {
		return next()
	}

	// Generate message ID from body hash
	hash := fnv.New128a()
	hash.Write(req.Body)
	msgID := fmt.Sprintf("%x", hash.Sum(nil))

	if h.Dedup.Seen(msgID) {
		observability.RecordExec("dedup_skipped")
		return &Response{
			Body: nil, // Duplicate - no response
		}, nil
	}

	h.Dedup.Mark(msgID)
	return next()
}
