# Performance Tuning

Benchmarks, optimization, profiling, capacity planning.

## 1. Benchmarking

### Go Benchmarks

```bash
cd server
go test -bench=. -benchmem ./bridge/...
go test -bench=. -benchmem ./internal/scheduler/...
go test -bench=. -benchmem ./internal/engine/...
```

### Rust Benchmarks

```bash
cd runtime
cargo bench
```

### Benchmark Examples

```rust
// runtime/benches/flowrulz_bench.rs
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn bench_pipeline(c: &mut Criterion) {
    let plan = compile("n:validate | n:process | n:store");
    let event = create_test_event();
    
    c.bench_function("simple_pipeline", |b| {
        b.iter(|| execute(black_box(&plan), black_box(&event)))
    });
}

fn bench_parallel(c: &mut Criterion) {
    let plan = compile("p:svc1,svc2,svc3 | c");
    let event = create_test_event();
    
    c.bench_function("parallel_3_services", |b| {
        b.iter(|| execute(black_box(&plan), black_box(&event)))
    });
}

criterion_group!(benches, bench_pipeline, bench_parallel);
criterion_main!(benches);
```

## 2. Optimization Patterns

### Reduce Allocations

```
# Before
m:{data: @, metadata: {source: "api", timestamp: now(), version: "1.0"}}

# After (fewer allocations)
m:{data: @, ts: now()}
```

### Use Gate Instead of Map

```
# Before (Map always executes)
m:{result: @.status == "active" ? "yes" : "no"}

# After (Gate skips if false)
g:status==active n:process-active n:done
```

### Batch Operations

```
# Before (individual calls)
n:store-item-1 | n:store-item-2 | n:store-item-3

# After (batch call)
b100 n:bulk-store
```

### Parallel Where Possible

```
# Before (sequential)
n:check-a | n:check-b | n:check-c

# After (parallel)
p:check-a,check-b,check-c | c
```

## 3. Profiling

### CPU Profiling

```bash
# Go
go test -cpuprofile=cpu.prof ./...
go tool pprof cpu.prof

# Rust
cargo bench -- --profile-time 10
```

### Memory Profiling

```bash
# Go
go test -memprofile=mem.prof ./...
go tool pprof mem.prof

# Rust
valgrind --tool=massif ./target/release/flowrulz
```

### Trace Profiling

```bash
# Go
go test -trace=trace.prof ./...
go tool trace trace.prof
```

## 4. Capacity Planning

### Complexity Scoring

| Operation | Score | Lane | Concurrency |
|-----------|-------|------|-------------|
| Gate | 5 | Fast | 50 |
| Map | 3 | Fast | 50 |
| Next | 10 | Normal | 20 |
| Emit | 8 | Normal | 20 |
| Buffer | 15 | Heavy | 5 |
| Parallel | 20 | Heavy | 5 |
| DAG | 20 | Heavy | 5 |
| Chunk | 25 | Heavy | 5 |

### Lane Sizing

| Metric | Fast | Normal | Heavy |
|--------|------|--------|-------|
| Concurrency | 50 | 20 | 5 |
| Queue depth | 1000 | 500 | 100 |
| Avg latency | <1ms | <10ms | <100ms |

### Sizing Formula

```
Required workers = (requests_per_second * avg_latency_seconds) / target_utilization

Example:
- 1000 rps
- 10ms avg latency
- 80% target utilization

Workers = (1000 * 0.01) / 0.8 = 12.5 → 13 workers
```

## 5. Memory Management

### Arena Allocator

The VM uses arena allocation for fast, bulk deallocation:

- Blocks: 64KB each
- Allocation: O(1) bump pointer
- Deallocation: bulk drop

### String Interning

Deduplicate strings to reduce memory:

- Constant pool: indexed by u16
- Service table: indexed by u16
- Reduces heap pressure for repeated strings

### GC Tuning (Go)

```bash
# Increase GC frequency for low-memory
GOGC=200 ./flowrulz

# Disable GC for benchmarks
GOGC=off go test -bench=.
```

## 6. Network Optimization

### Connection Pooling

```go
// gRPC connection pool
type ConnPool struct {
    conns []*grpc.ClientConn
    mu    sync.Map
}
```

### Batch Publishing

```go
// Kafka batch producer
producer, _ := sarama.NewSyncProducer(brokers, config)
producer.Config.Producer.Flush.Messages = 100
producer.Config.Producer.Flush.Frequency = 100 * time.Millisecond
```

### Compression

```
# Enable gzip for large payloads
Content-Encoding: gzip
```

## 7. Caching Strategies

### Read-Through Cache

```
n:get-from-cache g:cache_hit==true n:return-cached n:fetch-from-source n:store-in-cache
```

### Write-Through Cache

```
n:write-to-source n:write-to-cache
```

### Cache Invalidation

```
n:write-to-source n:invalidate-cache
```

## 8. Load Testing

### Artillery Config

```yaml
config:
  target: "http://localhost:8080"
  phases:
    - duration: 60
      arrivalRate: 10
    - duration: 120
      arrivalRate: 100
    - duration: 60
      arrivalRate: 1000

scenarios:
  - flow:
    - post:
        url: "/event"
        json:
          topic: "test"
          payload:
            key: "value"
          mode: 0
```

### k6 Script

```javascript
import http from 'k6/http';
import { check, sleep } from 'k6';

export let options = {
  stages: [
    { duration: '30s', target: 100 },
    { duration: '1m', target: 500 },
    { duration: '30s', target: 0 },
  ],
};

export default function () {
  let payload = JSON.stringify({
    topic: 'test',
    payload: { key: 'value' },
    mode: 0,
  });

  let res = http.post('http://localhost:8080/event', payload, {
    headers: { 'Content-Type': 'application/json' },
  });

  check(res, { 'status was 200': (r) => r.status == 200 });
  sleep(0.1);
}
```

## 9. Monitoring Key Metrics

| Metric | Warning | Critical |
|--------|---------|----------|
| Event latency p99 | >100ms | >500ms |
| Queue depth | >1000 | >5000 |
| Error rate | >1% | >5% |
| Memory usage | >70% | >90% |
| CPU usage | >70% | >90% |
| GC pause | >10ms | >100ms |

## 10. Performance Gotchas

### 1. Gate Skip Cost
Gate that evaluates to false skips the next instruction — but the instruction is still *decoded*:
```
g:always_false n:expensive_operation
# Gate evaluates: false
# VM skips expensive_operation
# But decoding the Next instruction still costs ~100ns
```

### 2. Map Allocation Pressure
```
m:{a: @.x, b: @.y, c: @.z}
# Creates new JSON object on every execution
# High-frequency flows: consider caching or reducing map complexity
```

### 3. Parallel Overhead
```
p:svc1,svc2,svc3 | c
# Overhead: 3 goroutine spawns + channel communication
# For fast services (<1ms), sequential might be faster
# Parallel shines when services take >10ms each
```

### 4. Buffer Memory Usage
```
b10000 n:process
# Buffers 10,000 messages in memory before processing
# If each message is 1KB: 10MB buffer
# Monitor buffer size under load
```

### 5. Schema Validation Cost
```
schema:{!a:string,!b:int,!c:float,!d:object,!e:array}
# Validates 5 fields on every execution
# For high-throughput flows: move schema to edge (API gateway)
# Let internal flows skip validation
```

### 6. String Interning Impact
```
# First occurrence of "validate": interned (allocation)
# All subsequent occurrences: reused (no allocation)
# High-cardinality strings (UUIDs) don't benefit from interning
```

### 7. Arena Fragmentation
```
# Arena allocates in 64KB blocks
# If average allocation is 100 bytes: 640 allocations per block
# If average allocation is 10KB: 6 allocations per block
# Small allocations are more efficient
```

### 8. Work Stealing Contention
```
# Too many steal attempts → lock contention
# Solution: randomize steal targets
# Each worker tries random other lane first
```
