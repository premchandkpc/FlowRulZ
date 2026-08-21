# Chunking & Buffering

Message batching, chunk splitting, buffer accumulation.

## Bytecode DSL

### Buffer

```
b10
```

Accumulate 10 messages before processing. The buffer collects incoming messages and emits them as a batch when the count is reached.

### Buffer + Process

```
b100 n:bulk-insert
```

Buffer 100 messages, then call `bulk-insert` with all 100 as an array.

### Buffer + Transform

```
b50 m:{batch: @, count: length(@), timestamp: now()} n:analytics
```

Buffer 50 events, wrap them in a batch envelope, then send to analytics.

### Chunk Sequential

```
chunk:10:seq n:storage
```

Split the input array into chunks of 10, process each chunk sequentially (one after another).

### Chunk Parallel

```
chunk:4:par n:processor
```

Split the input array into chunks of 4, process all chunks concurrently.

### Chunk + Buffer Combo

```
b1000 chunk:100:par n:storage
```

Buffer 1000 messages, then split into chunks of 100 and process in parallel.

## Flow DSL

### Batch Processing

```
version 1

flow BatchIngestion

service storage
    type postgres
    connection postgres://db:5432/events

service kafka
    type kafka
    brokers kafka:9092
    topic raw-events

workflow

Start

-> kafka.Consume

-> storage.BatchInsert

-> End
```

### Chunked Processing

```
version 1

flow ChunkedETL

service extract
    type http
    url https://api.internal/data

service process
    type grpc
    address processor:50051

service load
    type postgres
    connection postgres://db:5432/warehouse

workflow

Start

-> extract.FetchAll

-> process.Transform

-> load.Store

-> End
```

## Use Cases

### Log Aggregation

```
b100 m:{logs: @, count: length(@)} n:log-storage
```

Buffer 100 log entries, then store them as a batch. Reduces I/O overhead.

### Event Streaming

```
b500 m:{events: @, window_start: @0.timestamp, window_end: @499.timestamp} n:analytics
```

Buffer 500 events into a time window for aggregate analytics.

### Batch Database Writes

```
b100 n:bulk-insert
```

Instead of 100 individual INSERT statements, one bulk INSERT.

### Parallel File Processing

```
chunk:10:par n:file-processor
```

Split a large file into 10-line chunks and process them concurrently.

### Rate-Limited API Calls

```
chunk:5:seq n:external-api
```

Process 5 items at a time, sequentially, to respect API rate limits.

### Large Dataset ETL

```
b10000 chunk:500:par n:transform | n:load
```

Buffer 10,000 records, split into 500-record chunks, process in parallel, then load.

## Buffer vs Chunk

| Feature | Buffer (`b`) | Chunk (`chunk`) |
|---------|-------------|-----------------|
| Trigger | Count threshold | Array size |
| Input | Individual messages | Single array |
| Output | Batch as array | Multiple smaller arrays |
| Use case | Aggregation | Parallelization |
| Ordering | Preserved | Preserved within chunk |

## Performance Tuning

| Parameter | Effect |
|-----------|--------|
| Small buffer (10-50) | Low latency, more overhead |
| Medium buffer (100-500) | Balanced |
| Large buffer (1000+) | High throughput, higher latency |
| Small chunk (5-10) | More parallelism, more scheduling |
| Large chunk (100+) | Less overhead, less parallelism |

## Use Cases

| Pattern | Configuration |
|---------|--------------|
| Log batching | `b100` — batch 100 log entries |
| Bulk inserts | `b50 n:bulk-insert` — batch DB writes |
| Parallel ETL | `chunk:10:par` — concurrent chunk processing |
| Rate limiting | `chunk:5:seq` — sequential rate-limited calls |
| Windowed analytics | `b1000 m:{...}` — time-window aggregation |
