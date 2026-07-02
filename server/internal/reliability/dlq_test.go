package reliability

import (
	"context"
	"sync/atomic"
	"testing"
)

func TestDLQSendAndList(t *testing.T) {
	d := NewDLQ(100)

	d.Send(&DeadLetterEntry{ID: "1", RuleID: "rule-1", Body: []byte("data"), Error: "timeout"})
	d.Send(&DeadLetterEntry{ID: "2", RuleID: "rule-2", Body: []byte("data2"), Error: "error"})

	entries := d.List()
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	if d.Len() != 2 {
		t.Fatalf("expected Len()=2, got %d", d.Len())
	}
}

func TestDLQMaxSize(t *testing.T) {
	d := NewDLQ(3)

	for i := 0; i < 5; i++ {
		d.Send(&DeadLetterEntry{ID: string(rune('0' + i)), Error: "err"})
	}

	if d.Len() != 3 {
		t.Fatalf("expected 3 entries (max), got %d", d.Len())
	}
}

func TestDLQReplay(t *testing.T) {
	d := NewDLQ(100)

	var replayCount atomic.Int32
	d.SetReplayFn(func(ctx context.Context, entry *DeadLetterEntry) error {
		replayCount.Add(1)
		return nil
	})

	d.Send(&DeadLetterEntry{ID: "1", RuleID: "rule-1", Body: []byte("data"), Error: "timeout"})

	if d.Len() != 1 {
		t.Fatalf("expected 1 entry before replay")
	}

	err := d.Replay(context.Background(), "1")
	if err != nil {
		t.Fatal(err)
	}

	if replayCount.Load() != 1 {
		t.Fatalf("expected 1 replay, got %d", replayCount.Load())
	}

	if d.Len() != 0 {
		t.Fatalf("expected 0 entries after replay, got %d", d.Len())
	}
}

func TestDLQReplayAll(t *testing.T) {
	d := NewDLQ(100)

	var replayCount atomic.Int32
	d.SetReplayFn(func(ctx context.Context, entry *DeadLetterEntry) error {
		replayCount.Add(1)
		return nil
	})

	for i := 0; i < 5; i++ {
		d.Send(&DeadLetterEntry{ID: string(rune('0' + i)), Error: "err"})
	}

	count := d.ReplayAll(context.Background())
	if count != 5 {
		t.Fatalf("expected 5 replays, got %d", count)
	}
	if replayCount.Load() != 5 {
		t.Fatalf("expected replay count 5, got %d", replayCount.Load())
	}
	if d.Len() != 0 {
		t.Fatalf("expected 0 entries after replay all, got %d", d.Len())
	}
}

func TestDLQClear(t *testing.T) {
	d := NewDLQ(100)

	d.Send(&DeadLetterEntry{ID: "1", Error: "err"})
	d.Send(&DeadLetterEntry{ID: "2", Error: "err"})
	d.Clear()

	if d.Len() != 0 {
		t.Fatalf("expected 0 after clear, got %d", d.Len())
	}
}

func TestDLQToJSON(t *testing.T) {
	d := NewDLQ(100)

	d.Send(&DeadLetterEntry{ID: "1", RuleID: "rule-1", Body: []byte("data"), Error: "timeout"})

	data, err := d.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Fatal("expected non-empty JSON")
	}
}
