package cache

import (
	"context"
	"sync"
	"time"
)

type entry struct {
	value     []byte
	expiresAt time.Time
}

// MemoryCache is an in-memory cache implementation.
type MemoryCache struct {
	mu      sync.RWMutex
	entries map[string]entry
	stop    chan struct{}
}

// NewMemoryCache creates a new in-memory cache.
func NewMemoryCache() *MemoryCache {
	c := &MemoryCache{
		entries: make(map[string]entry),
		stop:    make(chan struct{}),
	}
	go c.cleanup()
	return c
}

func (c *MemoryCache) Get(_ context.Context, key string) ([]byte, error) {
	c.mu.RLock()
	e, ok := c.entries[key]
	c.mu.RUnlock()

	if !ok {
		return nil, nil
	}

	if !e.expiresAt.IsZero() && time.Now().After(e.expiresAt) {
		_ = c.Delete(context.Background(), key)
		return nil, nil
	}

	result := make([]byte, len(e.value))
	copy(result, e.value)
	return result, nil
}

func (c *MemoryCache) Set(_ context.Context, key string, value []byte, ttl time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}

	// Store copy
	stored := make([]byte, len(value))
	copy(stored, value)

	c.entries[key] = entry{
		value:     stored,
		expiresAt: expiresAt,
	}
	return nil
}

func (c *MemoryCache) Delete(_ context.Context, key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, key)
	return nil
}

func (c *MemoryCache) Exists(_ context.Context, key string) (bool, error) {
	c.mu.RLock()
	_, ok := c.entries[key]
	c.mu.RUnlock()
	return ok, nil
}

func (c *MemoryCache) Clear(_ context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[string]entry)
	return nil
}

func (c *MemoryCache) Close() error {
	close(c.stop)
	return nil
}

func (c *MemoryCache) cleanup() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.stop:
			return
		case <-ticker.C:
			c.evictExpired()
		}
	}
}

func (c *MemoryCache) evictExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for k, e := range c.entries {
		if !e.expiresAt.IsZero() && now.After(e.expiresAt) {
			delete(c.entries, k)
		}
	}
}

// Len returns the number of entries.
func (c *MemoryCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

// MemoryProvider is a factory for MemoryCache.
type MemoryProvider struct{}

func (p *MemoryProvider) Name() string { return "memory" }

func (p *MemoryProvider) New(_ map[string]interface{}) (Cache, error) {
	return NewMemoryCache(), nil
}

func init() {
	RegisterProvider(&MemoryProvider{})
}

// Registry holds registered cache providers.
var registry = map[string]CacheProvider{}

// RegisterProvider registers a cache provider.
func RegisterProvider(p CacheProvider) {
	registry[p.Name()] = p
}

// GetProvider returns a registered cache provider.
func GetProvider(name string) (CacheProvider, bool) {
	p, ok := registry[name]
	return p, ok
}

// NewFromConfig creates a cache from configuration.
func NewFromConfig(cfg Config) (Cache, error) {
	p, ok := GetProvider(cfg.Provider)
	if !ok {
		p = &MemoryProvider{}
	}
	return p.New(cfg.Options)
}
