package cache

import (
	"context"
	"time"
)

// Cache is a generic key-value cache interface.
type Cache interface {
	// Get retrieves a value by key. Returns nil if not found.
	Get(ctx context.Context, key string) ([]byte, error)

	// Set stores a value with TTL. 0 means no expiry.
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error

	// Delete removes a key.
	Delete(ctx context.Context, key string) error

	// Exists checks if a key exists.
	Exists(ctx context.Context, key string) (bool, error)

	// Clear removes all keys.
	Clear(ctx context.Context) error

	// Close closes the cache connection.
	Close() error
}

// CacheProvider is a factory for creating caches.
type CacheProvider interface {
	// Name returns the provider name (e.g., "redis", "memory").
	Name() string

	// New creates a new cache instance.
	New(config map[string]interface{}) (Cache, error)
}

// Config holds cache configuration.
type Config struct {
	Provider string                 `yaml:"provider" json:"provider"` // redis, memory
	Options  map[string]interface{} `yaml:"options" json:"options"`
}

// DefaultConfig returns default cache configuration.
func DefaultConfig() Config {
	return Config{
		Provider: "memory",
		Options:  map[string]interface{}{},
	}
}
