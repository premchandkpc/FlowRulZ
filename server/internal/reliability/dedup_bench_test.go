package reliability

import (
	"fmt"
	"testing"
)

// BenchmarkDedupTracker_CheckAndMark_NewKeys benchmarks checking new keys (no duplicates).
func BenchmarkDedupTracker_CheckAndMark_NewKeys(b *testing.B) {
	dt := NewDedupTracker(10000, 0) // no cleanup during benchmark

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("msg-%d", i)
		dt.CheckAndMark(key)
	}
}

// BenchmarkDedupTracker_CheckAndMark_DuplicateKeys benchmarks checking duplicate keys.
func BenchmarkDedupTracker_CheckAndMark_DuplicateKeys(b *testing.B) {
	dt := NewDedupTracker(10000, 0)

	// Pre-populate with keys
	for i := 0; i < 1000; i++ {
		dt.Mark(fmt.Sprintf("msg-%d", i))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("msg-%d", i%1000) // cycle through pre-populated keys
		dt.CheckAndMark(key)
	}
}

// BenchmarkDedupTracker_CheckAndMark_AtCapacity benchmarks eviction at capacity.
func BenchmarkDedupTracker_CheckAndMark_AtCapacity(b *testing.B) {
	dt := NewDedupTracker(1000, 0) // small capacity to force eviction

	// Fill to capacity
	for i := 0; i < 1000; i++ {
		dt.Mark(fmt.Sprintf("msg-%d", i))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("new-msg-%d", i) // all new keys, forces eviction
		dt.CheckAndMark(key)
	}
}

// BenchmarkDedupTracker_Mark benchmarks the Mark function.
func BenchmarkDedupTracker_Mark(b *testing.B) {
	dt := NewDedupTracker(10000, 0)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("msg-%d", i)
		dt.Mark(key)
	}
}

// BenchmarkDedupTracker_Seen benchmarks the Seen function (read-only).
func BenchmarkDedupTracker_Seen(b *testing.B) {
	dt := NewDedupTracker(10000, 0)

	// Pre-populate
	for i := 0; i < 5000; i++ {
		dt.Mark(fmt.Sprintf("msg-%d", i))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("msg-%d", i%5000)
		dt.Seen(key)
	}
}

// BenchmarkDedupTracker_Concurrent benchmarks concurrent access patterns.
func BenchmarkDedupTracker_Concurrent(b *testing.B) {
	for _, parallelism := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("parallel-%d", parallelism), func(b *testing.B) {
			dt := NewDedupTracker(10000, 0)

			b.ResetTimer()
			b.ReportAllocs()
			b.SetParallelism(parallelism)

			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					key := fmt.Sprintf("msg-%d", i)
					if i%3 == 0 {
						// 33% duplicates
						dt.CheckAndMark(key)
					} else {
						// 67% new
						dt.CheckAndMark(key)
					}
					i++
				}
			})
		})
	}
}
