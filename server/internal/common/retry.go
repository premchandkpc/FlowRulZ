package common

import (
	"context"
	"math"
	"math/rand"
	"time"
)

type BackoffStrategy interface {
	Duration(attempt int) time.Duration
}

type ExponentialBackoff struct {
	Base   time.Duration
	Max    time.Duration
	Factor float64
	Jitter float64
}

func (b *ExponentialBackoff) Duration(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	dur := float64(b.Base) * math.Pow(b.Factor, float64(attempt))
	if dur > float64(b.Max) {
		dur = float64(b.Max)
	}
	if b.Jitter > 0 {
		jitter := 1 + b.Jitter*(2*rand.Float64()-1)
		dur *= jitter
	}
	return time.Duration(dur)
}

type RetryConfig struct {
	MaxAttempts int
	Backoff     BackoffStrategy
	Retryable   func(error) bool
}

type RetryOption func(*RetryConfig)

func WithMaxAttempts(n int) RetryOption {
	return func(c *RetryConfig) { c.MaxAttempts = n }
}

func WithBackoff(b BackoffStrategy) RetryOption {
	return func(c *RetryConfig) { c.Backoff = b }
}

func WithRetryable(fn func(error) bool) RetryOption {
	return func(c *RetryConfig) { c.Retryable = fn }
}

func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts: 3,
		Backoff:     &ExponentialBackoff{Base: 100 * time.Millisecond, Max: 10 * time.Second, Factor: 2, Jitter: 0.2},
		Retryable:   IsTransient,
	}
}

func NewRetryConfig(opts ...RetryOption) RetryConfig {
	cfg := DefaultRetryConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

func DoWithRetry(ctx context.Context, fn func(context.Context) error, opts ...RetryOption) error {
	cfg := NewRetryConfig(opts...)
	var lastErr error
	for attempt := 0; attempt < cfg.MaxAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(cfg.Backoff.Duration(attempt)):
			}
		}
		lastErr = fn(ctx)
		if lastErr == nil {
			return nil
		}
		if cfg.Retryable != nil && !cfg.Retryable(lastErr) {
			return lastErr
		}
	}
	return lastErr
}
