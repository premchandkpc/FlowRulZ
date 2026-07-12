package reliability

import (
	"container/list"
	"context"
	"sync"
	"time"
)

type dedupEntry struct {
	key       string
	timestamp time.Time
	elem      *list.Element
}

type DedupTracker struct {
	mu      sync.RWMutex
	entries map[string]dedupEntry
	order   *list.List
	maxSize int
	ttl     time.Duration
}

func NewDedupTracker(maxSize int, ttl time.Duration) *DedupTracker {
	if maxSize <= 0 {
		maxSize = 10000
	}
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return &DedupTracker{
		entries: make(map[string]dedupEntry),
		order:   list.New(),
		maxSize: maxSize,
		ttl:     ttl,
	}
}

func (dt *DedupTracker) Seen(key string) bool {
	dt.mu.RLock()
	_, ok := dt.entries[key]
	dt.mu.RUnlock()
	return ok
}

func (dt *DedupTracker) Mark(key string) {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	dt.markLocked(key)
}

func (dt *DedupTracker) markLocked(key string) {
	if existing, ok := dt.entries[key]; ok {
		existing.timestamp = time.Now()
		dt.order.MoveToFront(existing.elem)
		dt.entries[key] = existing
		return
	}
	if len(dt.entries) >= dt.maxSize {
		oldest := dt.order.Back()
		if oldest != nil {
			oldestKey := dt.order.Remove(oldest).(string)
			delete(dt.entries, oldestKey)
		}
	}
	elem := dt.order.PushFront(key)
	dt.entries[key] = dedupEntry{key: key, timestamp: time.Now(), elem: elem}
}

// CheckAndMark atomically checks if a key has been seen and marks it if not.
// Returns true if the key was already seen (duplicate), false if it's new.
// This eliminates the TOCTOU race between Seen() and Mark().
func (dt *DedupTracker) CheckAndMark(key string) bool {
	dt.mu.Lock()
	defer dt.mu.Unlock()

	if _, ok := dt.entries[key]; ok {
		// Already seen - update timestamp and move to front
		if existing, ok := dt.entries[key]; ok {
			existing.timestamp = time.Now()
			dt.order.MoveToFront(existing.elem)
			dt.entries[key] = existing
		}
		return true
	}

	// Not seen - mark it
	dt.markLocked(key)
	return false
}

func (dt *DedupTracker) StartCleanup(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				dt.mu.Lock()
				now := time.Now()
				for k, v := range dt.entries {
					if now.Sub(v.timestamp) > dt.ttl {
						dt.order.Remove(v.elem)
						delete(dt.entries, k)
					}
				}
				dt.mu.Unlock()
			}
		}
	}()
}

func (dt *DedupTracker) Len() int {
	dt.mu.RLock()
	defer dt.mu.RUnlock()
	return len(dt.entries)
}

func (dt *DedupTracker) Clear() {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	dt.entries = make(map[string]dedupEntry)
	dt.order.Init()
}
