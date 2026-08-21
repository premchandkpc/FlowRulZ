# Retry & Resilience

Exponential/linear/fixed retry, timeouts, circuit breakers, DLQ.

## Bytecode DSL

### Retry Strategies

#### Exponential Backoff

```
n:flaky-service r3:exp
```

Retries 3 times with exponential backoff:
- Attempt 1: 100ms delay
- Attempt 2: 200ms delay
- Attempt 3: 400ms delay

#### Linear Backoff

```
n:flaky-service r5:lin
```

Retries 5 times with linear backoff:
- Attempt 1: 100ms
- Attempt 2: 200ms
- Attempt 3: 300ms
- Attempt 4: 400ms
- Attempt 5: 500ms

#### Fixed Delay

```
n:flaky-service r3:fixed:200
```

Retries 3 times with 200ms fixed delay between attempts.

### Timeout

```
n:slow-service t5000
```

5-second timeout. If the service doesn't respond, the step fails.

### Retry + Timeout

```
n:slow-flaky t3000 r3:exp
```

3-second timeout per attempt, 3 retries with exponential backoff.

### Retry + Fallback

```
n:primary r3:exp f:secondary
```

Try `primary` 3 times. If all fail, fall back to `secondary`.

### Retry + Timeout + Fallback

```
n:primary t2000 r3:exp f:cached
```

Complete resilience chain: timeout → retry → fallback.

## Flow DSL

### Flow-Wide Retry

```
version 1

flow ResilientFlow

service api
    type http
    url https://external.api.com/data

retry
    attempts 3
    backoff exponential
    delay 500ms
    maxDelay 10s

timeout 30s

workflow

Start

-> api.Fetch

-> End
```

The retry policy applies to all service calls in the flow.

### Flow-Wide Circuit Breaker

```
version 1

flow CircuitProtected

service payment
    type http
    url https://payment.internal/charge

breaker
    failureRate 50
    window 60s
    cooldown 30s

workflow

Start

-> payment.Charge

-> End
```

If 50% of calls fail within 60 seconds, the circuit opens for 30 seconds.

### Combined Resilience

```
version 1

flow FullResilience

service api
    type http
    url https://external.api.com

service cache
    type redis
    connection redis:6379

retry
    attempts 3
    backoff exponential
    delay 1s
    maxDelay 30s

breaker
    failureRate 50
    window 60s
    cooldown 30s

timeout 10s

workflow

Start

-> api.Fetch

-> cache.Store

-> End

onError
    CircuitOpenError
        -> cache.GetCached
    TimeoutError
        -> cache.GetCached
    Default
        -> dlq.Send
```

### Per-Service Retry Override

```
service unreliable
    type http
    url https://unreliable.api.com
    retries 5
    timeout 2s

service reliable
    type grpc
    address reliable:50051
    retries 1
    timeout 30s
```

## Timeout Behavior

| Timeout | Behavior |
|---------|----------|
| Per-step | `t5000` on a specific call |
| Flow-wide | `timeout 30s` for entire flow |
| Per-service | `timeout 30s` in service declaration |

Timeouts cascade:
1. If step timeout fires, the step fails
2. Retry logic kicks in (if configured)
3. If all retries fail, fallback runs (if configured)
4. If no fallback, error handler runs

## Circuit Breaker States

```
Closed (normal) → Open (blocking) → Half-Open (testing)
       ↑                                |
       └────────────────────────────────┘
```

| State | Behavior |
|-------|----------|
| Closed | Normal operation, failures counted |
| Open | All calls rejected immediately |
| Half-Open | One test call allowed through |

### Configuration

```
breaker
    failureRate 50    # 50% failure threshold
    window 60s        # 60-second evaluation window
    cooldown 30s      # 30 seconds in open state
```

## DLQ Integration

```
n:process f:dlq
```

Failed events are sent to the DLQ with:

```json
{
  "entry_id": "unique-id",
  "payload": "...",
  "error": "connection refused",
  "step": "process",
  "retry_count": 3,
  "timestamp": "2026-08-21T10:00:00Z",
  "trace_id": "abc-123"
}
```

DLQ features:
- **Replay**: reprocess failed events
- **Deduplication**: prevent duplicate processing
- **Persistence**: disk-backed with 0600 permissions
- **Validation**: entry IDs validated against `^[a-zA-Z0-9_\-]+$`

## Use Cases

| Pattern | Configuration |
|---------|--------------|
| Transient failures | `r3:exp` — retry with exponential backoff |
| Slow services | `t5000` — timeout protection |
| Cascading failures | `breaker` — circuit breaker |
| Unrecoverable errors | `f:dlq` — dead letter queue |
| Full resilience | Retry + timeout + breaker + DLQ |
