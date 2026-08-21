package reliability

import (
	"testing"
	"time"
)

func BenchmarkTokenBucket_Allow(b *testing.B) {
	tb := NewTokenBucket(1000000, 1000000) // high rate to avoid blocking
	for b.Loop() {
		tb.Allow()
	}
}

func BenchmarkTokenBucket_AllowN(b *testing.B) {
	tb := NewTokenBucket(1000000, 1000000)
	for b.Loop() {
		tb.AllowN(1)
	}
}

func BenchmarkTokenBucket_AllowContention(b *testing.B) {
	tb := NewTokenBucket(1000000, 1000000)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tb.Allow()
		}
	})
}

func BenchmarkRateLimiter_Allow(b *testing.B) {
	rl := NewRateLimiter()
	rl.SetBucket("svc-a", 1000000, 1000000)
	for b.Loop() {
		rl.Allow("svc-a")
	}
}

func BenchmarkRateLimiter_AllowNewKey(b *testing.B) {
	rl := NewRateLimiter()
	for b.Loop() {
		rl.Allow("svc-new")
	}
}

func BenchmarkRateLimiter_AllowParallel(b *testing.B) {
	rl := NewRateLimiter()
	rl.SetBucket("svc-parallel", 1000000, 1000000)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rl.Allow("svc-parallel")
		}
	})
}

func BenchmarkCircuitBreaker_Allow_Closed(b *testing.B) {
	cb := NewCircuitBreaker(100, time.Hour) // never trips
	for b.Loop() {
		cb.Allow()
	}
}

func BenchmarkCircuitBreaker_Allow_Parallel(b *testing.B) {
	cb := NewCircuitBreaker(1000000, time.Hour)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cb.Allow()
		}
	})
}

func BenchmarkCircuitBreaker_Success(b *testing.B) {
	cb := NewCircuitBreaker(100, time.Hour)
	for b.Loop() {
		cb.Success()
	}
}

func BenchmarkCircuitBreaker_Failure(b *testing.B) {
	cb := NewCircuitBreaker(100, time.Hour)
	for b.Loop() {
		cb.Failure()
	}
}
