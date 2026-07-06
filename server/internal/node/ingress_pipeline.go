package node

import (
	"context"
	"fmt"
	"hash/fnv"

	"github.com/premchandkpc/FlowRulZ/server/internal/observability"
	"github.com/premchandkpc/FlowRulZ/server/internal/reliability"
)

// IngressPipeline handles the reliability pipeline for inbound messages:
// rate limiting -> dedup -> execution -> DLQ on failure.
type IngressPipeline struct {
	rateLimiter RateLimiter
	dedup       DedupChecker
	dlq         NodeDLQ
	executor    *ExecutionEngine
}

// NewIngressPipeline creates an IngressPipeline with the given dependencies.
func NewIngressPipeline(
	rateLimiter RateLimiter,
	dedup DedupChecker,
	dlq NodeDLQ,
	executor *ExecutionEngine,
) *IngressPipeline {
	return &IngressPipeline{
		rateLimiter: rateLimiter,
		dedup:       dedup,
		dlq:         dlq,
		executor:    executor,
	}
}

// HandleMessage processes an inbound message through the reliability pipeline.
func (p *IngressPipeline) HandleMessage(ctx context.Context, msg []byte) ([]byte, error) {
	if !p.rateLimiter.Allow("ingress") {
		observability.RecordError("rate_limited")
		p.dlq.Send(&reliability.DeadLetterEntry{
			ID:    "ratelimited",
			Body:  msg,
			Error: "rate limited",
		})
		return nil, nil
	}

	h := fnv.New128a()
	h.Write(msg)
	msgIDStr := fmt.Sprintf("%x", h.Sum(nil))

	if p.dedup.CheckAndMark(msgIDStr) {
		observability.RecordExec("dedup_skipped")
		return nil, nil
	}

	results, err := p.executor.ExecuteAll(ctx, msg)
	if err != nil {
		observability.RecordError("exec")
		p.dlq.Send(&reliability.DeadLetterEntry{
			ID:    "exec-error",
			Body:  msg,
			Error: err.Error(),
		})
		return nil, err
	}
	if len(results) == 0 {
		return nil, nil
	}
	observability.RecordExec("msg")
	return results[0], nil
}
