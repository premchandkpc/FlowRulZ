# Ingress Pipeline

**Status:** Implemented. The `IngressPipeline` (`server/internal/node/ingress_pipeline.go`) implements a reliability pipeline for inbound messages.

## Pipeline Stages

```
Transport consumer
       │
       ▼
┌──────────────┐
│ Rate Limiter │ ── denied → DLQ + silent return
└──────┬───────┘
       │ allowed
       ▼
┌──────────────┐
│    Dedup     │ ── duplicate → silent return
└──────┬───────┘
       │ new
       ▼
┌──────────────┐
│   Execute    │ ── failure → DLQ + error
└──────┬───────┘
       │ success
       ▼
    Response
```

### 1. Rate Limiting

Checks `rateLimiter.Allow("ingress")`. If denied:
- Records a `rate_limited` error
- Sends message to DLQ
- Returns silently (no error to caller)

### 2. Deduplication

Computes FNV-128a hash of the message body. Calls `dedup.CheckAndMark(hash)`. If the message was already seen:
- Records a `dedup_skipped` event
- Returns silently

`CheckAndMark` is atomic — prevents TOCTOU race between `Seen` and `Mark` that existed in the older `pipeline.DedupHandler`.

### 3. Execution

Delegates to `executor.ExecuteAll(ctx, msg)` which runs all active bytecode plans against the message body.

### 4. DLQ on Failure

If execution fails:
- Records an `exec` error
- Sends message to DLQ with the error message
- Returns the error

## Wiring

```go
// In ProdNode.Start()
n.msgRouter.StartConsumers(ctx, n.ingress.HandleMessage)
```

The `MessageRouter` creates 5 consumers and routes the user-topic messages to `IngressPipeline.HandleMessage`.

## Dedup Implementation

The `DedupTracker` uses 16 shards with per-shard `sync.Mutex`, `map[string]dedupEntry`, and `*list.List` for LRU eviction.

- Shard key: `maphash.Hash(key) % 16`
- Max size divided across shards: `maxSize / 16` per shard (default: 625)
- LRU within each shard — oldest evicted at capacity
- `CheckAndMark(key) bool` — atomic check-and-mark (returns `true` if duplicate)
- Background cleanup walks each shard from back (oldest) to front

## Files

| File | Purpose |
|---|---|
| `node/ingress_pipeline.go` | Pipeline stages (rate limit, dedup, execute, DLQ) |
| `reliability/dedup.go` | 16-shard LRU dedup tracker |
| `reliability/ratelimit.go` | Token bucket rate limiter |
