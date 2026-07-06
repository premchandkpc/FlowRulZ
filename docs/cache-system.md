# Cache System

**Status:** Implemented. A pluggable, provider-based key-value cache for the Go control plane.

## Overview

The cache abstraction provides a simple `Get`/`Set`/`Delete`/`Exists`/`Clear` interface with TTL support. Two backends: in-memory (default) and Redis.

## Interface

```go
type Cache interface {
    Get(ctx context.Context, key string) ([]byte, error)
    Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
    Delete(ctx context.Context, key string) error
    Exists(ctx context.Context, key string) (bool, error)
    Clear(ctx context.Context) error
}
```

Values are `[]byte` — not typed, making this a low-level building block.

## Backends

### Memory (`cache/memory.go`)

- `sync.RWMutex`-protected `map[string]entry`
- Entries store `[]byte` value + optional `expiresAt` time
- Background cleanup goroutine ticks every 1 second, evicts expired entries
- Values defensively copied on `Get` and `Set` to prevent aliasing
- Self-registers via `init()`

### Redis (`cache/redis.go`)

- Wraps `github.com/redis/go-redis/v9`
- `Get` returns `nil` on `redis.Nil`
- `Clear` calls `FlushDB`
- Adds `Ping` health-check method beyond the interface
- Self-registers via `init()`

## Provider Registry

```go
cache.RegisterProvider(provider)   // register a CacheProvider
cache.GetProvider(name)            // look up by name
cache.NewFromConfig(config)        // create from Config (falls back to memory)
```

## Current Usage

The Flow DSL registry uses the cache for IR caching:

```go
flowCache := cache.NewMemoryCache()
flowRegistry := flow.NewRegistry(flowCache)
```

- IR cached under `flow:<name>:ir` with 5-minute TTL
- Topic-to-flow routing cached under `flow:route:<topic>`

The Redis backend exists but is not yet wired into production.

## Files

| File | Purpose |
|---|---|
| `cache/cache.go` | `Cache` + `CacheProvider` interfaces, `Config`, provider registry |
| `cache/memory.go` | In-memory backend with TTL + background cleanup |
| `cache/redis.go` | Redis backend |
