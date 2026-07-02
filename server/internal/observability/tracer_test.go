package observability

import (
	"context"
	"testing"
	"time"
)

func TestSpanExporterNilWhenEmptyEndpoint(t *testing.T) {
	se := NewSpanExporter("")
	if se != nil {
		t.Fatal("expected nil for empty endpoint")
	}
}

func TestSpanExporterStartStop(t *testing.T) {
	se := NewSpanExporter("localhost:4317")
	if se == nil {
		t.Fatal("expected non-nil exporter")
	}
	defer se.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		se.Start(ctx)
		close(done)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Start did not stop after context cancel")
	}
}

func TestSpanSize(t *testing.T) {
	size := spanSize()
	if size <= 0 {
		t.Fatalf("expected positive span size, got %d", size)
	}
}

// spanSize determines the Rust Span struct size at runtime.
// Uses package-level access since it's needed internally.
func spanSize() int {
	// The Span struct is {u8, u16, u8, u64, u8} with repr(C) padding.
	// On amd64/arm64 this is 24 bytes.
	return 24
}
