package reliability

import (
	"context"
	"sync"
	"time"
)

type DedupEntry struct {
	timestamp time.Time
}

type DedupTracker struct {
	mu      sync.RWMutex
	entries map[string]DedupEntry
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
		entries: make(map[string]DedupEntry),
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
	if len(dt.entries) >= dt.maxSize {
		var oldestKey string
		var oldestTime time.Time
		for k, v := range dt.entries {
			if oldestTime.IsZero() || v.timestamp.Before(oldestTime) {
				oldestKey = k
				oldestTime = v.timestamp
			}
		}
		delete(dt.entries, oldestKey)
	}
	dt.entries[key] = DedupEntry{timestamp: time.Now()}
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
	dt.entries = make(map[string]DedupEntry)
}
