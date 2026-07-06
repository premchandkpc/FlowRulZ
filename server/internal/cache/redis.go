package cache

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisCache is a Redis-backed cache implementation.
type RedisCache struct {
	client *redis.Client
}

// NewRedisCache creates a new Redis cache.
func NewRedisCache(addr, password string, db int) *RedisCache {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})
	return &RedisCache{client: client}
}

func (c *RedisCache) Get(ctx context.Context, key string) ([]byte, error) {
	val, err := c.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("redis get: %w", err)
	}
	return val, nil
}

func (c *RedisCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	err := c.client.Set(ctx, key, value, ttl).Err()
	if err != nil {
		return fmt.Errorf("redis set: %w", err)
	}
	return nil
}

func (c *RedisCache) Delete(ctx context.Context, key string) error {
	err := c.client.Del(ctx, key).Err()
	if err != nil {
		return fmt.Errorf("redis del: %w", err)
	}
	return nil
}

func (c *RedisCache) Exists(ctx context.Context, key string) (bool, error) {
	n, err := c.client.Exists(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("redis exists: %w", err)
	}
	return n > 0, nil
}

func (c *RedisCache) Clear(ctx context.Context) error {
	err := c.client.FlushDB(ctx).Err()
	if err != nil {
		return fmt.Errorf("redis flushdb: %w", err)
	}
	return nil
}

func (c *RedisCache) Close() error {
	return c.client.Close()
}

// Ping checks Redis connection.
func (c *RedisCache) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

// RedisProvider is a factory for RedisCache.
type RedisProvider struct{}

func (p *RedisProvider) Name() string { return "redis" }

func (p *RedisProvider) New(opts map[string]interface{}) (Cache, error) {
	addr, _ := opts["addr"].(string)
	if addr == "" {
		addr = "localhost:6379"
	}
	password, _ := opts["password"].(string)
	db := 0
	if v, ok := opts["db"].(string); ok {
		db, _ = strconv.Atoi(v)
	}
	return NewRedisCache(addr, password, db), nil
}

func init() {
	RegisterProvider(&RedisProvider{})
}
