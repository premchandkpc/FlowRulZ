package reliability

import (
	"container/list"
	"context"
	"hash/maphash"
	"sync"
	"time"
)

const numShards = 16

type dedupEntry struct {
	key       string
	timestamp time.Time
	elem      *list.Element
}

type dedupShard struct {
	mu      sync.Mutex
	entries map[string]dedupEntry
	order   *list.List
}

type DedupTracker struct {
	shards  [numShards]dedupShard
	maxSize int
	ttl     time.Duration
	hasher  maphash.Hash
}

func NewDedupTracker(maxSize int, ttl time.Duration) *DedupTracker {
	if maxSize <= 0 {
		maxSize = 10000
	}
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	dt := &DedupTracker{
		maxSize: maxSize / numShards,
		ttl:     ttl,
	}
	for i := range dt.shards {
		dt.shards[i].entries = make(map[string]dedupEntry)
		dt.shards[i].order = list.New()
	}
	return dt
}

func (dt *DedupTracker) shard(key string) *dedupShard {
	dt.hasher.Reset()
	dt.hasher.WriteString(key)
	return &dt.shards[dt.hasher.Sum64()%numShards]
}

func (dt *DedupTracker) Seen(key string) bool {
	s := dt.shard(key)
	s.mu.Lock()
	e, ok := s.entries[key]
	if ok && time.Since(e.timestamp) > dt.ttl {
		s.order.Remove(e.elem)
		delete(s.entries, key)
		s.mu.Unlock()
		return false
	}
	s.mu.Unlock()
	return ok
}

func (dt *DedupTracker) Mark(key string) {
	s := dt.shard(key)
	s.mu.Lock()
	dt.markLocked(s, key)
	s.mu.Unlock()
}

func (dt *DedupTracker) markLocked(s *dedupShard, key string) {
	now := time.Now()
	if existing, ok := s.entries[key]; ok {
		existing.timestamp = now
		s.order.MoveToFront(existing.elem)
		return
	}
	if len(s.entries) >= dt.maxSize {
		oldest := s.order.Back()
		if oldest != nil {
			delete(s.entries, oldest.Value.(string))
			s.order.Remove(oldest)
		}
	}
	elem := s.order.PushFront(key)
	s.entries[key] = dedupEntry{key: key, timestamp: now, elem: elem}
}

// CheckAndMark atomically checks if a key has been seen and marks it if not.
// Returns true if the key was already seen (duplicate), false if it's new.
func (dt *DedupTracker) CheckAndMark(key string) bool {
	s := dt.shard(key)
	s.mu.Lock()

	if e, ok := s.entries[key]; ok {
		if time.Since(e.timestamp) > dt.ttl {
			s.order.Remove(e.elem)
			delete(s.entries, key)
			s.mu.Unlock()
			dt.markLocked(s, key)
			return false
		}
		e.timestamp = time.Now()
		s.order.MoveToFront(e.elem)
		s.mu.Unlock()
		return true
	}

	dt.markLocked(s, key)
	s.mu.Unlock()
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
				now := time.Now()
				for i := range dt.shards {
					s := &dt.shards[i]
					s.mu.Lock()
				// Walk from back (oldest), stop at first non-expired
				for e := s.order.Back(); e != nil; {
					key := e.Value.(string)
					entry, ok := s.entries[key]
					if !ok {
						prev := e.Prev()
						s.order.Remove(e)
						e = prev
						continue
					}
					if now.Sub(entry.timestamp) > dt.ttl {
						prev := e.Prev()
						s.order.Remove(e)
						delete(s.entries, key)
						e = prev
					} else {
						break // rest are newer
					}
				}
					s.mu.Unlock()
				}
			}
		}
	}()
}

func (dt *DedupTracker) Len() int {
	n := 0
	for i := range dt.shards {
		s := &dt.shards[i]
		s.mu.Lock()
		n += len(s.entries)
		s.mu.Unlock()
	}
	return n
}

func (dt *DedupTracker) Clear() {
	for i := range dt.shards {
		s := &dt.shards[i]
		s.mu.Lock()
		s.entries = make(map[string]dedupEntry)
		s.order.Init()
		s.mu.Unlock()
	}
}
