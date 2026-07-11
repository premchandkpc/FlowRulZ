package node

import (
	"context"
	"testing"
)

func TestContextWithTraceID(t *testing.T) {
	ctx := ContextWithTraceID(context.Background())
	id := TraceIDFromContext(ctx)
	if id == "" {
		t.Fatal("expected non-empty trace ID")
	}
}

func TestTraceIDFromContextEmpty(t *testing.T) {
	id := TraceIDFromContext(context.Background())
	if id != "" {
		t.Fatalf("expected empty trace ID, got %q", id)
	}
}

func TestContextWithTraceIDIdempotent(t *testing.T) {
	ctx := ContextWithTraceID(context.Background())
	first := TraceIDFromContext(ctx)

	ctx = ContextWithTraceID(ctx)
	second := TraceIDFromContext(ctx)

	if first != second {
		t.Fatalf("expected same trace ID, got %q and %q", first, second)
	}
}
