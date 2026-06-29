package reliability

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/premchandkpc/FlowRulZ/go/internal/transport"
)

const DefaultDLQTopic = "_flowrulz_dlq"

type DeadLetterEntry struct {
	ID          string    `json:"id"`
	RuleID      string    `json:"rule_id"`
	Topic       string    `json:"topic"`
	Partition   int32     `json:"partition"`
	Offset      int64     `json:"offset"`
	Body        []byte    `json:"body"`
	Error       string    `json:"error"`
	FailedAt    time.Time `json:"failed_at"`
	RetryCount  int       `json:"retry_count"`
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
	producer := d.producer
	d.mu.Unlock()

	log.Printf("dlq: rule=%s id=%s error=%s", entry.RuleID, entry.ID, entry.Error)

	if producer != nil {
		data, err := json.Marshal(entry)
		if err != nil {
			log.Printf("dlq: marshal error for kafka: %v", err)
			return nil
		}
		if err := producer.Send(context.Background(), []byte(entry.ID), data); err != nil {
			log.Printf("dlq: kafka produce error: %v", err)
		}
	}
	return nil
}

func (d *DLQ) LoadFromTopic(ctx context.Context) {
	d.mu.Lock()
	topic := d.topic
	d.mu.Unlock()

	log.Printf("dlq: rebuild from topic %s (placeholder — real Kafka consumer needed)", topic)
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
			if err := d.replayFn(ctx, entry); err != nil {
				d.Send(entry)
				continue
			}
			count++
		}
	}
	return count
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
	defer d.mu.Unlock()
	d.entries = d.entries[:0]
}

func (d *DLQ) ToJSON() ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return json.Marshal(d.entries)
}
