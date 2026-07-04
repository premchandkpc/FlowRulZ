package bridge

import (
	"fmt"
	"testing"
)

// BenchmarkExecuteStep_BufferAllocation benchmarks the buffer allocation pattern in ExecuteStep.
// This measures the cost of pool checkout + errBuf allocation + copyBytes.
func BenchmarkExecuteStep_BufferAllocation(b *testing.B) {
	// Create a minimal plan for benchmarking
	plan := []byte{0x01, 0x00, 0x00, 0x00} // minimal valid plan header

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// This will fail at the FFI boundary, but we're measuring allocation overhead
		ExecuteStep(plan, nil, nil, nil)
	}
}

// BenchmarkOutputBufPool benchmarks the sync.Pool operations.
func BenchmarkOutputBufPool(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		buf := *outputBufPool.Get().(*[]byte)
		outputBufPool.Put(&buf)
	}
}

// BenchmarkCopyBytes benchmarks the copyBytes function.
func BenchmarkCopyBytes(b *testing.B) {
	src := make([]byte, 1024)
	for i := range src {
		src[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = copyBytes(src, len(src))
	}
}

// BenchmarkCopyBytes_VaryingSizes benchmarks copyBytes with different data sizes.
func BenchmarkCopyBytes_VaryingSizes(b *testing.B) {
	sizes := []int{64, 256, 1024, 4096, 16384}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size-%d", size), func(b *testing.B) {
			src := make([]byte, size)
			for i := range src {
				src[i] = byte(i % 256)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				_ = copyBytes(src, size)
			}
		})
	}
}

// BenchmarkCallerMap benchmarks the sync.Map operations for caller tracking.
func BenchmarkCallerMap(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		callerMap.Store(i, nil)
		callerMap.Delete(i)
	}
}
