package reliability

import (
	"sync"
	"time"
)

type TokenBucket struct {
	mu         sync.Mutex
	rate       float64
	burst      int
	tokens     float64
	lastRefill time.Time
}

type RateLimiter struct {
	mu     sync.RWMutex
	buckets map[string]*TokenBucket
}

func NewRateLimiter() *RateLimiter {
	return &RateLimiter{
		buckets: make(map[string]*TokenBucket),
	}
}

func NewTokenBucket(rate float64, burst int) *TokenBucket {
	return &TokenBucket{
		rate:       rate,
		burst:      burst,
		tokens:     float64(burst),
		lastRefill: time.Now(),
	}
}

func (tb *TokenBucket) Allow() bool {
	return tb.AllowN(1)
}

func (tb *TokenBucket) AllowN(n int) bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.refill()

	if tb.tokens >= float64(n) {
		tb.tokens -= float64(n)
		return true
	}
	return false
}

func (tb *TokenBucket) refill() {
	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.lastRefill = now

	tb.tokens += elapsed * tb.rate
	if tb.tokens > float64(tb.burst) {
		tb.tokens = float64(tb.burst)
	}
}

func (rl *RateLimiter) Bucket(name string) *TokenBucket {
	rl.mu.RLock()
	b, ok := rl.buckets[name]
	rl.mu.RUnlock()
	if ok {
		return b
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()
	if b, ok := rl.buckets[name]; ok {
		return b
	}
	b = NewTokenBucket(100, 100)
	rl.buckets[name] = b
	return b
}

func (rl *RateLimiter) SetBucket(name string, rate float64, burst int) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.buckets[name] = NewTokenBucket(rate, burst)
}

func (rl *RateLimiter) Allow(name string) bool {
	return rl.Bucket(name).Allow()
}

func (rl *RateLimiter) AllowN(name string, n int) bool {
	return rl.Bucket(name).AllowN(n)
}
