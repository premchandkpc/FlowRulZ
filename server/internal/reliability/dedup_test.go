package reliability

import (
	"context"
	"testing"
	"time"
)

func TestDedupSeenUnseen(t *testing.T) {
	dt := NewDedupTracker(100, time.Minute)
	if dt.Seen("key-1") {
		t.Error("expected unseen for new key")
	}
}

func TestDedupMarkAndSeen(t *testing.T) {
	dt := NewDedupTracker(100, time.Minute)
	dt.Mark("key-1")
	if !dt.Seen("key-1") {
		t.Error("expected seen after mark")
	}
}

func TestDedupMaxSize(t *testing.T) {
	dt := NewDedupTracker(3, time.Minute)
	dt.Mark("a")
	dt.Mark("b")
	dt.Mark("c")
	dt.Mark("d") // evicts oldest

	if dt.Len() != 3 {
		t.Fatalf("expected 3 entries, got %d", dt.Len())
	}
}

func TestDedupClear(t *testing.T) {
	dt := NewDedupTracker(100, time.Minute)
	dt.Mark("key-1")
	dt.Clear()
	if dt.Seen("key-1") {
		t.Error("expected unseen after clear")
	}
	if dt.Len() != 0 {
		t.Errorf("expected 0 after clear, got %d", dt.Len())
	}
}

func TestDedupCleanupExpired(t *testing.T) {
	dt := NewDedupTracker(100, 50*time.Millisecond)
	dt.Mark("key-1")
	dt.Mark("key-2")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dt.StartCleanup(ctx, 20*time.Millisecond)

	time.Sleep(100 * time.Millisecond)
	if dt.Len() != 0 {
		t.Errorf("expected 0 after ttl expiry, got %d", dt.Len())
	}
}

func TestDedupDefaults(t *testing.T) {
	dt := NewDedupTracker(0, 0)
	if dt.maxSize != 10000 {
		t.Errorf("expected default maxSize 10000, got %d", dt.maxSize)
	}
	if dt.ttl != 5*time.Minute {
		t.Errorf("expected default ttl 5m, got %v", dt.ttl)
	}
}

func TestDedupEvictsOldest(t *testing.T) {
	dt := NewDedupTracker(2, time.Minute)
	dt.Mark("old")
	time.Sleep(time.Millisecond)
	dt.Mark("new")
	dt.Mark("latest") // evicts "old"

	if dt.Seen("old") {
		t.Error("expected 'old' evicted (oldest)")
	}
	if !dt.Seen("new") {
		t.Error("expected 'new' still present")
	}
	if !dt.Seen("latest") {
		t.Error("expected 'latest' present")
	}
}
