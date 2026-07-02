package reliability

import (
	"context"
	"errors"
)

type RateLimiter interface {
	Allow(ctx context.Context, key string) bool
	Wait(ctx context.Context, key string) error
	SetRate(key string, rate float64, burst int)
}

var ErrRateLimited = errors.New("rate limit exceeded")
