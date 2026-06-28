# Kafka Semantics Specification

**Status:** Reference for transport implementation. The Go transport layer is responsible for all Kafka concerns; the Rust VM is transport-agnostic.

## Consumer Groups

```
flowrulz
└── consumer-group
    ├── partition-0 ── worker-0 ── rule-vm
    ├── partition-1 ── worker-1 ── rule-vm
    └── partition-2 ── worker-2 ── rule-vm
```

- One consumer group per deployment
- Each partition assigned to exactly one worker goroutine
- Workers are long-lived (no per-message goroutine churn)
- Rebalance listener pauses/resumes partition workers

### Lane Routing

Rules are assigned to lanes based on `complexity_score` at deploy time:

| Score | Lane | BatchSize | PollTimeout |
|-------|------|-----------|-------------|
| < 10 | fast | 500 | 10ms |
| ≤ 50 | normal | 100 | 50ms |
| > 50 | heavy | 10 | 500ms |

Lane assignment is done in `engine.Deploy()` via `flowrulz_plan_complexity` FFI.

## Offset Commit

### Commit Strategies

| Mode | Behavior |
|------|----------|
| `at-least-once` | Commit after VM execution succeeds |
| `manual` | User controls commit via admin API |

### Commit Timing

```
Message Received → Execute Rule → Produce Output → Commit Offset
```

## Batch Poll

```
Consumer.PollBatch(N) → ExecuteBatch (Rust FFI N times)
```

Batch size configurable. Backpressure: if processing is slow, reduce batch size.

## Exactly-Once & Idempotency

### Producer Idempotency

- Enable `enable.idempotence=true` on Kafka producer
- On retry, same sequence number → broker dedup

## Dead Letter Queue (DLQ)

### Poison Message Handling

```
Message → VM → Error
    ↓
Retry count < max? → Yes → Retry (with backoff)
                    → No  → DLQ
```

### DLQ Topic Format

```json
{
  "original_topic": "input",
  "original_partition": 3,
  "original_offset": 1042,
  "body": {"original": "payload"},
  "error": "FieldNotFound: path segment 'address' not found",
  "retry_count": 3,
  "timestamp": "..."
}
```

## Backpressure

| Level | Mechanism | Trigger |
|-------|-----------|---------|
| Go channel | Block send to full channel | Channel at capacity |
| Kafka consumer | `pause()` partition | Memory threshold exceeded |

## Retry Topics (Planned)

```
Main Topic → Worker (VM error + retryable)
           → Retry Topic (with backoff)
           → Worker (VM error + exhausted)
           → DLQ
```

Retry topic naming: `{input-topic}-retry-{delay}s`

## Ordering & Partition Affinity

- Messages from the same partition processed sequentially by the same worker
- No reordering within a partition
- Output produced with same key as input to preserve partition affinity

## Health Checks

```
GET /health
{
  "status": "ok"
}
```

## Admin API

See `docs/specs/admin-api.md` or the Admin section in `CLAUDE.md`.
