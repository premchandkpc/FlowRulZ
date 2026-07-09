package reliability

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
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
	dir      string
}

type DLQOption func(*DLQ)

func WithDLQProducer(p transport.MessageProducer) DLQOption {
	return func(d *DLQ) { d.producer = p }
}

func WithDLQDir(dir string) DLQOption {
	return func(d *DLQ) { d.dir = dir }
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
	if d.dir != "" {
		d.loadFromDir()
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
		oldest := d.entries[0]
		d.entries = d.entries[1:]
		d.removePersisted(oldest.ID)
	}
	entry.FailedAt = time.Now()
	d.entries = append(d.entries, entry)
	entryCopy := *entry
	d.mu.Unlock()

	d.persistEntry(&entryCopy)

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

func (d *DLQ) loadFromDir() {
	entries, err := os.ReadDir(d.dir)
	if err != nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(d.dir, e.Name()))
		if err != nil {
			continue
		}
		var entry DeadLetterEntry
		if err := json.Unmarshal(data, &entry); err != nil {
			continue
		}
		d.entries = append(d.entries, &entry)
	}
	slog.Info("dlq: restored entries", "count", len(d.entries), "dir", d.dir)
}

func (d *DLQ) persistEntry(entry *DeadLetterEntry) {
	if d.dir == "" {
		return
	}
	path := filepath.Join(d.dir, entry.ID+".json")
	data, err := json.Marshal(entry)
	if err != nil {
		slog.Error("dlq: marshal error", "error", err)
		return
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		slog.Error("dlq: write error", "error", err)
		return
	}
	os.Rename(tmp, path)
}

func (d *DLQ) removePersisted(id string) {
	if d.dir == "" {
		return
	}
	path := filepath.Join(d.dir, id+".json")
	os.Remove(path)
	os.Remove(path + ".tmp")
}

func (d *DLQ) LoadFromTopic(ctx context.Context) {
	slog.Warn("dlq: rebuild from topic not implemented")
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

	d.removePersisted(id)

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
	entries := d.entries
	d.entries = d.entries[:0]
	d.mu.Unlock()

	if d.dir != "" {
		for _, e := range entries {
			d.removePersisted(e.ID)
		}
	}
}

func (d *DLQ) ToJSON() ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return json.Marshal(d.entries)
}
