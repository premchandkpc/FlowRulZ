package fabric

import (
	"math/rand"
	"sync"
	"time"
)

// Thread-safe random helpers for the fabric.
// Uses a dedicated rand source to avoid contention with global rand.

var (
	fabricRand   = rand.New(rand.NewSource(time.Now().UnixNano()))
	fabricRandMu sync.Mutex
)

func randFloat64() float64 {
	fabricRandMu.Lock()
	v := fabricRand.Float64()
	fabricRandMu.Unlock()
	return v
}

func randInt63n(n int64) int64 {
	if n <= 0 {
		return 0
	}
	fabricRandMu.Lock()
	v := fabricRand.Int63n(n)
	fabricRandMu.Unlock()
	return v
}
