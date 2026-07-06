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
	// Use small total size (16 = 1 per shard) to test per-shard eviction
	dt := NewDedupTracker(16, time.Minute)
	for i := 0; i < 100; i++ {
		dt.Mark("key-" + string(rune('a'+i%26)) + "-" + string(rune('0'+i/26)))
	}
	// Total entries <= numShards (16)
	if dt.Len() > 16 {
		t.Fatalf("expected <= 16 entries, got %d", dt.Len())
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
	// Per-shard maxSize = 10000/16 = 625
	if dt.maxSize != 625 {
		t.Errorf("expected default per-shard maxSize 625, got %d", dt.maxSize)
	}
	if dt.ttl != 5*time.Minute {
		t.Errorf("expected default ttl 5m, got %v", dt.ttl)
	}
}

func TestDedupEvictsOldest(t *testing.T) {
	// With sharding, use many keys to ensure same-shard collision and eviction
	dt := NewDedupTracker(32, time.Minute) // 2 per shard
	for i := 0; i < 100; i++ {
		dt.Mark("key-" + string(rune('a'+i%26)) + "-" + string(rune('0'+i/26)))
	}
	// At least some eviction must have occurred
	if dt.Len() > 32 {
		t.Errorf("expected <= 32 entries, got %d", dt.Len())
	}
}
