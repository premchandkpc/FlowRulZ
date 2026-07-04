package execstate

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// BenchmarkFileStore_Save benchmarks the per-step Save call.
// This is the hot path: json.Marshal + write + rename under a global mutex.
func BenchmarkFileStore_Save(b *testing.B) {
	dir := b.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()

	// Create a state with realistic PlanBytes and CtxBytes sizes
	planBytes := make([]byte, 4096)   // 4KB plan
	rand.Read(planBytes)
	ctxBytes := make([]byte, 1024)    // 1KB context
	rand.Read(ctxBytes)

	st := &State{
		ID:        "bench-exec-1",
		PlanBytes: planBytes,
		CtxBytes:  ctxBytes,
		Status:    StatusRunning,
	}
	if err := store.Create(ctx, st); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		st.CtxBytes = ctxBytes // simulate context change per step
		if err := store.Save(ctx, st); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFileStore_Save_Concurrent benchmarks concurrent Saves from different executions.
// This reveals the global mutex bottleneck.
func BenchmarkFileStore_Save_Concurrent(b *testing.B) {
	for _, parallelism := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("parallel-%d", parallelism), func(b *testing.B) {
			dir := b.TempDir()
			store, err := NewFileStore(dir)
			if err != nil {
				b.Fatal(err)
			}
			defer store.Close()

			ctx := context.Background()

			planBytes := make([]byte, 4096)
			rand.Read(planBytes)
			ctxBytes := make([]byte, 1024)
			rand.Read(ctxBytes)

			// Pre-create N states (one per goroutine)
			states := make([]*State, parallelism)
			for i := 0; i < parallelism; i++ {
				st := &State{
					ID:        fmt.Sprintf("bench-exec-%d", i),
					PlanBytes: planBytes,
					CtxBytes:  ctxBytes,
					Status:    StatusRunning,
				}
				if err := store.Create(ctx, st); err != nil {
					b.Fatal(err)
				}
				states[i] = st
			}

			b.ResetTimer()
			b.ReportAllocs()
			b.SetParallelism(parallelism)

			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					st := states[i%parallelism]
					st.CtxBytes = ctxBytes
					if err := store.Save(ctx, st); err != nil {
						b.Error(err)
					}
					i++
				}
			})
		})
	}
}

// BenchmarkFileStore_Create benchmarks the initial Create call.
func BenchmarkFileStore_Create(b *testing.B) {
	dir := b.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	planBytes := make([]byte, 4096)
	rand.Read(planBytes)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		st := &State{
			ID:        fmt.Sprintf("bench-exec-%d", i),
			PlanBytes: planBytes,
			Status:    StatusCreated,
		}
		if err := store.Create(ctx, st); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFileStore_ListByStatus benchmarks the indexed lookup with mixed statuses.
func BenchmarkFileStore_ListByStatus(b *testing.B) {
	for _, count := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("execs-%d", count), func(b *testing.B) {
			dir := b.TempDir()

			// Pre-create files with mixed statuses (80% completed, 20% running)
			planBytes := make([]byte, 4096)
			for i := 0; i < count; i++ {
				status := StatusCompleted
				if i%5 == 0 {
					status = StatusRunning // 20% running
				}
				st := &State{
					ID:        fmt.Sprintf("exec-%d", i),
					PlanBytes: planBytes,
					Status:    status,
				}
				data, _ := json.Marshal(st)
				os.WriteFile(filepath.Join(dir, st.ID+".json"), data, 0644)
			}

			// Open store — index is built from existing files
			store, err := NewFileStore(dir)
			if err != nil {
				b.Fatal(err)
			}
			defer store.Close()

			ctx := context.Background()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// ListByStatus for running (20% of total)
				_, err := store.ListByStatus(ctx, StatusRunning)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkFileStore_Delete benchmarks Delete (called after successful recovery).
func BenchmarkFileStore_Delete(b *testing.B) {
	dir := b.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	planBytes := make([]byte, 4096)

	// Pre-create all states
	states := make([]*State, b.N)
	for i := 0; i < b.N; i++ {
		st := &State{
			ID:        fmt.Sprintf("bench-exec-%d", i),
			PlanBytes: planBytes,
			Status:    StatusCompleted,
		}
		if err := store.Create(ctx, st); err != nil {
			b.Fatal(err)
		}
		states[i] = st
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := store.Delete(ctx, states[i].ID); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFileStore_JsonMarshal benchmarks just the JSON marshal step.
func BenchmarkFileStore_JsonMarshal(b *testing.B) {
	planBytes := make([]byte, 4096)
	rand.Read(planBytes)
	ctxBytes := make([]byte, 1024)
	rand.Read(ctxBytes)

	st := &State{
		ID:        "bench-exec-1",
		PlanBytes: planBytes,
		CtxBytes:  ctxBytes,
		Status:    StatusRunning,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(st)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFileStore_DirScan benchmarks os.ReadDir (used by ListByStatus).
func BenchmarkFileStore_DirScan(b *testing.B) {
	for _, count := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("files-%d", count), func(b *testing.B) {
			dir := b.TempDir()

			// Create N files
			for i := 0; i < count; i++ {
				data := []byte(fmt.Sprintf(`{"id":"exec-%d","status":1}`, i))
				os.WriteFile(fmt.Sprintf("%s/exec-%d.json", dir, i), data, 0644)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				entries, err := os.ReadDir(dir)
				if err != nil {
					b.Fatal(err)
				}
				_ = entries
			}
		})
	}
}
