package reliability

import (
	"testing"
	"time"
)

func TestTokenBucketBasic(t *testing.T) {
	tb := NewTokenBucket(10, 5)

	for i := 0; i < 5; i++ {
		if !tb.Allow() {
			t.Fatalf("expected allow at %d", i)
		}
	}

	if tb.Allow() {
		t.Fatal("expected deny after burst exhausted")
	}
}

func TestTokenBucketRefill(t *testing.T) {
	tb := NewTokenBucket(100, 1)

	if !tb.Allow() {
		t.Fatal("expected allow")
	}
	if tb.Allow() {
		t.Fatal("expected deny after single burst")
	}

	time.Sleep(20 * time.Millisecond)

	if !tb.Allow() {
		t.Fatal("expected allow after refill")
	}
}

func TestAllowN(t *testing.T) {
	tb := NewTokenBucket(10, 10)

	if !tb.AllowN(5) {
		t.Fatal("expected allow 5")
	}
	if !tb.AllowN(5) {
		t.Fatal("expected allow 5 more")
	}
	if tb.AllowN(1) {
		t.Fatal("expected deny after burst exhausted")
	}
}

func TestRateLimiter(t *testing.T) {
	rl := NewRateLimiter()

	rl.SetBucket("test", 100, 10)
	for i := 0; i < 10; i++ {
		if !rl.Allow("test") {
			t.Fatalf("expected allow at %d", i)
		}
	}
	if rl.Allow("test") {
		t.Fatal("expected deny")
	}
}

func TestRateLimiterDefaultBucket(t *testing.T) {
	rl := NewRateLimiter()
	b := rl.Bucket("auto")
	if b == nil {
		t.Fatal("expected default bucket")
	}
}

func TestRateLimiterIsolation(t *testing.T) {
	rl := NewRateLimiter()

	rl.SetBucket("a", 100, 5)
	rl.SetBucket("b", 100, 5)

	for i := 0; i < 5; i++ {
		rl.Allow("a")
		rl.Allow("b")
	}

	if rl.Allow("a") {
		t.Fatal("expected a to be limited")
	}
	if rl.Allow("b") {
		t.Fatal("expected b to be limited")
	}
}
