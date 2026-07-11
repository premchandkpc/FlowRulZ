package reliability

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
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

func TestDLQUniqueIDs(t *testing.T) {
	d := NewDLQ(100)

	ids := make(map[string]bool)
	for i := 0; i < 20; i++ {
		body := []byte{byte(i), byte(i + 1), byte(i + 2)}
		msgID := fmt.Sprintf("rl-%d-%x", i, body)
		d.Send(&DeadLetterEntry{ID: msgID, Body: body, Error: "rate limited"})
		ids[msgID] = true
	}

	entries := d.List()
	if len(entries) != 20 {
		t.Fatalf("expected 20 entries, got %d", len(entries))
	}

	if len(ids) != 20 {
		t.Fatalf("expected 20 unique IDs, got %d", len(ids))
	}
}

func TestDLQReplayFailureReQueues(t *testing.T) {
	d := NewDLQ(100)

	var callCount int32
	d.SetReplayFn(func(ctx context.Context, entry *DeadLetterEntry) error {
		atomic.AddInt32(&callCount, 1)
		return fmt.Errorf("service unavailable")
	})

	d.Send(&DeadLetterEntry{ID: "fail-1", Body: []byte("data"), Error: "err"})

	count := d.ReplayAll(context.Background())
	if count != 0 {
		t.Fatalf("expected 0 successful replays, got %d", count)
	}

	if d.Len() != 1 {
		t.Fatalf("expected 1 entry re-queued after failure, got %d", d.Len())
	}

	if atomic.LoadInt32(&callCount) != 1 {
		t.Fatalf("expected 1 call, got %d", atomic.LoadInt32(&callCount))
	}
}

func TestDLQConcurrentSendAndList(t *testing.T) {
	d := NewDLQ(1000)
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			d.Send(&DeadLetterEntry{
				ID:    fmt.Sprintf("concurrent-%d", n),
				Body:  []byte("data"),
				Error: "err",
			})
		}(i)
	}
	wg.Wait()

	entries := d.List()
	if len(entries) != 50 {
		t.Fatalf("expected 50 entries, got %d", len(entries))
	}
}

func TestDLQReplayAllPartialFailure(t *testing.T) {
	d := NewDLQ(100)

	var callCount int32
	d.SetReplayFn(func(ctx context.Context, entry *DeadLetterEntry) error {
		n := atomic.AddInt32(&callCount, 1)
		if n == 2 {
			return fmt.Errorf("fail on second")
		}
		return nil
	})

	for i := 0; i < 3; i++ {
		d.Send(&DeadLetterEntry{ID: fmt.Sprintf("entry-%d", i), Body: []byte("data"), Error: "err"})
	}

	count := d.ReplayAll(context.Background())
	if count != 2 {
		t.Fatalf("expected 2 successful replays, got %d", count)
	}

	if d.Len() != 1 {
		t.Fatalf("expected 1 failed entry re-queued, got %d", d.Len())
	}
}

func TestDLQLoadFromMessages(t *testing.T) {
	d := NewDLQ(100)

	msgs := [][]byte{
		mustMarshal(t, &DeadLetterEntry{ID: "load-1", RuleID: "rule-a", Body: []byte("hello"), Error: "err1"}),
		mustMarshal(t, &DeadLetterEntry{ID: "load-2", RuleID: "rule-b", Body: []byte("world"), Error: "err2"}),
		mustMarshal(t, &DeadLetterEntry{ID: "load-3", RuleID: "rule-c", Body: []byte("foo"), Error: "err3"}),
	}

	added := d.LoadFromMessages(context.Background(), msgs)
	if added != 3 {
		t.Fatalf("expected 3 added, got %d", added)
	}

	entries := d.List()
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	ids := make(map[string]bool)
	for _, e := range entries {
		ids[e.ID] = true
	}
	for _, id := range []string{"load-1", "load-2", "load-3"} {
		if !ids[id] {
			t.Fatalf("expected entry with ID %q", id)
		}
	}
}

func TestDLQLoadFromMessagesIdempotent(t *testing.T) {
	d := NewDLQ(100)

	msgs := [][]byte{
		mustMarshal(t, &DeadLetterEntry{ID: "idem-1", RuleID: "rule-x", Error: "err"}),
		mustMarshal(t, &DeadLetterEntry{ID: "idem-2", RuleID: "rule-y", Error: "err"}),
	}

	added1 := d.LoadFromMessages(context.Background(), msgs)
	if added1 != 2 {
		t.Fatalf("expected 2 added on first load, got %d", added1)
	}

	added2 := d.LoadFromMessages(context.Background(), msgs)
	if added2 != 0 {
		t.Fatalf("expected 0 added on second load (idempotent), got %d", added2)
	}

	if d.Len() != 2 {
		t.Fatalf("expected 2 entries after idempotent load, got %d", d.Len())
	}
}

func TestDLQLoadFromMessagesEmpty(t *testing.T) {
	d := NewDLQ(100)

	added := d.LoadFromMessages(context.Background(), nil)
	if added != 0 {
		t.Fatalf("expected 0 added for nil messages, got %d", added)
	}

	added = d.LoadFromMessages(context.Background(), [][]byte{})
	if added != 0 {
		t.Fatalf("expected 0 added for empty messages, got %d", added)
	}
}

func TestDLQLoadFromMessagesMalformed(t *testing.T) {
	d := NewDLQ(100)

	msgs := [][]byte{
		[]byte("not valid json"),
		mustMarshal(t, &DeadLetterEntry{ID: "good-1", RuleID: "rule-a", Error: "err"}),
		[]byte("{invalid"),
		mustMarshal(t, &DeadLetterEntry{ID: "good-2", RuleID: "rule-b", Error: "err"}),
	}

	added := d.LoadFromMessages(context.Background(), msgs)
	if added != 2 {
		t.Fatalf("expected 2 added (valid only), got %d", added)
	}

	entries := d.List()
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	ids := make(map[string]bool)
	for _, e := range entries {
		ids[e.ID] = true
	}
	if !ids["good-1"] || !ids["good-2"] {
		t.Fatalf("expected only valid entries, got IDs: %v", ids)
	}
}

func TestDLQLoadFromMessagesMaxSize(t *testing.T) {
	d := NewDLQ(3)

	for i := 0; i < 3; i++ {
		d.Send(&DeadLetterEntry{ID: fmt.Sprintf("existing-%d", i), Error: "err"})
	}

	if d.Len() != 3 {
		t.Fatalf("expected 3 existing entries, got %d", d.Len())
	}

	msgs := [][]byte{
		mustMarshal(t, &DeadLetterEntry{ID: "new-1", RuleID: "r", Error: "err"}),
		mustMarshal(t, &DeadLetterEntry{ID: "new-2", RuleID: "r", Error: "err"}),
	}

	added := d.LoadFromMessages(context.Background(), msgs)
	if added != 2 {
		t.Fatalf("expected 2 added, got %d", added)
	}

	if d.Len() != 3 {
		t.Fatalf("expected 3 entries (max size enforced), got %d", d.Len())
	}

	entries := d.List()
	for _, e := range entries {
		if e.ID == "existing-0" {
			t.Fatalf("oldest entry should have been evicted")
		}
	}
}

func TestDLQLoadFromMessagesPreservesExisting(t *testing.T) {
	d := NewDLQ(100)

	d.Send(&DeadLetterEntry{ID: "pre-1", RuleID: "rule-p", Body: []byte("existing"), Error: "err"})
	d.Send(&DeadLetterEntry{ID: "pre-2", RuleID: "rule-q", Body: []byte("existing"), Error: "err"})

	if d.Len() != 2 {
		t.Fatalf("expected 2 existing entries, got %d", d.Len())
	}

	msgs := [][]byte{
		mustMarshal(t, &DeadLetterEntry{ID: "new-1", RuleID: "rule-n", Body: []byte("new"), Error: "err"}),
	}

	added := d.LoadFromMessages(context.Background(), msgs)
	if added != 1 {
		t.Fatalf("expected 1 added, got %d", added)
	}

	entries := d.List()
	if len(entries) != 3 {
		t.Fatalf("expected 3 total entries, got %d", len(entries))
	}

	ids := make(map[string]bool)
	for _, e := range entries {
		ids[e.ID] = true
	}
	for _, id := range []string{"pre-1", "pre-2", "new-1"} {
		if !ids[id] {
			t.Fatalf("expected entry with ID %q to be present", id)
		}
	}
}

func TestDLQSendInvalidID(t *testing.T) {
	d := NewDLQ(100, WithDLQDir(t.TempDir()))

	err := d.Send(&DeadLetterEntry{ID: "../etc/passwd", RuleID: "r", Body: []byte("data"), Error: "err"})
	if err == nil {
		t.Fatal("expected error for path-traversal ID")
	}
	var idErr *InvalidEntryIDError
	if !errors.As(err, &idErr) {
		t.Fatalf("expected InvalidEntryIDError, got %T", err)
	}
}

func TestDLQSendEmptyID(t *testing.T) {
	d := NewDLQ(100, WithDLQDir(t.TempDir()))

	err := d.Send(&DeadLetterEntry{ID: "", RuleID: "r", Body: []byte("data"), Error: "err"})
	if err == nil {
		t.Fatal("expected error for empty ID")
	}
	var idErr *InvalidEntryIDError
	if !errors.As(err, &idErr) {
		t.Fatalf("expected InvalidEntryIDError, got %T", err)
	}
}

func TestDLQLoadFromMessagesInvalidID(t *testing.T) {
	d := NewDLQ(100)

	msgs := [][]byte{
		mustMarshal(t, &DeadLetterEntry{ID: "good-1", RuleID: "r", Body: []byte("ok"), Error: "err"}),
		mustMarshal(t, &DeadLetterEntry{ID: "bad/id", RuleID: "r", Body: []byte("bad"), Error: "err"}),
		mustMarshal(t, &DeadLetterEntry{ID: "good-2", RuleID: "r", Body: []byte("ok"), Error: "err"}),
	}

	added := d.LoadFromMessages(context.Background(), msgs)
	if added != 2 {
		t.Fatalf("expected 2 added (invalid ID skipped), got %d", added)
	}

	entries := d.List()
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	ids := make(map[string]bool)
	for _, e := range entries {
		ids[e.ID] = true
	}
	if !ids["good-1"] || !ids["good-2"] {
		t.Fatalf("expected only valid entries, got IDs: %v", ids)
	}
	if ids["bad/id"] {
		t.Fatal("expected invalid ID entry to be rejected")
	}
}

func TestDLQSendValidIDs(t *testing.T) {
	d := NewDLQ(100, WithDLQDir(t.TempDir()))

	validIDs := []string{"abc-123_def", "node-42", "a", "X_9-z"}
	for _, id := range validIDs {
		if err := d.Send(&DeadLetterEntry{ID: id, RuleID: "r", Body: []byte("data"), Error: "err"}); err != nil {
			t.Fatalf("unexpected error for valid ID %q: %v", id, err)
		}
	}

	if d.Len() != len(validIDs) {
		t.Fatalf("expected %d entries, got %d", len(validIDs), d.Len())
	}
}

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("mustMarshal: %v", err)
	}
	return data
}
