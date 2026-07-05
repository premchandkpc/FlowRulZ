package reliability

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/premchandkpc/FlowRulZ/server/internal/transport"
)

const DefaultDLQTopic = "_flowrulz_dlq"

type DeadLetterEntry struct {
	ID         string    `json:"id"`
	RuleID     string    `json:"rule_id"`
	Topic      string    `json:"topic"`
	Partition  int32     `json:"partition"`
	Offset     int64     `json:"offset"`
	Body       []byte    `json:"body"`
	Error      string    `json:"error"`
	FailedAt   time.Time `json:"failed_at"`
	RetryCount int       `json:"retry_count"`
}

type DLQ struct {
	mu       sync.RWMutex
	entries  []*DeadLetterEntry
	maxSize  int
	replayFn func(ctx context.Context, entry *DeadLetterEntry) error
	producer transport.MessageProducer
	topic    string
}

type DLQOption func(*DLQ)

func WithDLQProducer(p transport.MessageProducer) DLQOption {
	return func(d *DLQ) { d.producer = p }
}

func WithDLQTopic(t string) DLQOption {
	return func(d *DLQ) { d.topic = t }
}

func NewDLQ(maxSize int, opts ...DLQOption) *DLQ {
	if maxSize <= 0 {
		maxSize = 10000
	}
	d := &DLQ{
		entries: make([]*DeadLetterEntry, 0),
		maxSize: maxSize,
		topic:   DefaultDLQTopic,
	}
	for _, o := range opts {
		o(d)
	}
	return d
}

func (d *DLQ) SetReplayFn(fn func(ctx context.Context, entry *DeadLetterEntry) error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.replayFn = fn
}

func (d *DLQ) Send(entry *DeadLetterEntry) error {
	d.mu.Lock()
	if len(d.entries) >= d.maxSize {
		d.entries = d.entries[1:]
	}
	entry.FailedAt = time.Now()
	d.entries = append(d.entries, entry)
	entryCopy := *entry
	d.mu.Unlock()

	slog.Warn("dlq: message", "rule", entry.RuleID, "id", entry.ID, "error", entry.Error)

	if d.producer != nil {
		data, err := json.Marshal(&entryCopy)
		if err != nil {
			slog.Error("dlq: marshal error for kafka", "error", err)
			return nil
		}
		if err := d.producer.Send(context.Background(), []byte(entry.ID), data); err != nil {
			slog.Error("dlq: kafka produce error", "error", err)
			return err
		}
	}
	return nil
}

func (d *DLQ) Replay(ctx context.Context, id string) error {
	d.mu.Lock()
	var entry *DeadLetterEntry
	var idx int = -1
	for i, e := range d.entries {
		if e.ID == id {
			entry = e
			idx = i
			break
		}
	}
	if entry == nil {
		d.mu.Unlock()
		return nil
	}
	d.entries = append(d.entries[:idx], d.entries[idx+1:]...)
	d.mu.Unlock()

	if d.replayFn != nil {
		entry.RetryCount++
		return d.replayFn(ctx, entry)
	}
	return nil
}

func (d *DLQ) ReplayAll(ctx context.Context) int {
	d.mu.Lock()
	entries := make([]*DeadLetterEntry, len(d.entries))
	copy(entries, d.entries)
	d.entries = d.entries[:0]
	d.mu.Unlock()

	count := 0
	for _, entry := range entries {
		if d.replayFn != nil {
			entry.RetryCount++
			replayErr := func() (err error) {
				defer func() {
					if r := recover(); r != nil {
						err = &replayPanicError{value: r}
					}
				}()
				return d.replayFn(ctx, entry)
			}()
			if replayErr != nil {
				if pe, ok := replayErr.(*replayPanicError); ok {
					slog.Error("dlq: replay panic, re-queuing", "id", entry.ID, "panic", pe.value)
				}
				d.Send(entry)
				continue
			}
			count++
		}
	}
	return count
}

type replayPanicError struct {
	value any
}

func (e *replayPanicError) Error() string {
	return "replay panic"
}

func (d *DLQ) List() []*DeadLetterEntry {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make([]*DeadLetterEntry, len(d.entries))
	copy(out, d.entries)
	return out
}

func (d *DLQ) Len() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.entries)
}

func (d *DLQ) Clear() {
	d.mu.Lock()
	d.entries = d.entries[:0]
	d.mu.Unlock()
}

func (d *DLQ) ToJSON() ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return json.Marshal(d.entries)
}
