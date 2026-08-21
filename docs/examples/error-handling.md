# Error Handling

Fallbacks, DLQ, typed error cases, circuit breakers.

## Bytecode DSL

### Fallback on Failure

```
n:primary-service f:fallback-service
```

If `primary-service` fails, route to `fallback-service` instead of halting.

### Fallback to DLQ

```
n:validate | n:process f:dlq
```

If `process` fails, send the payload to `dlq` (dead letter queue).

### Retry + Fallback

```
n:flaky-service r3:exp f:dlq
```

Retry 3 times with exponential backoff. If all retries fail, send to DLQ.

### Timeout + Fallback

```
n:slow-api t2000 f:cached-response
```

2-second timeout on `slow-api`. On timeout, fall back to `cached-response`.

### Gate + Fallback

```
g:status==error f:handle-error n:continue
```

If `status` field equals `"error"`, route to `handle-error`. Otherwise continue.

## Flow DSL

### Typed Error Cases

```
version 1

flow ResilientOrder

service payment
    type http
    url https://payment.internal/charge

service inventory
    type grpc
    address inventory:50051

service fulfillment
    type grpc
    address fulfillment:50051

service dlq
    type kafka
    brokers kafka1:9092
    topic dead-letter-queue

workflow

Start

-> payment.Charge

-> inventory.Reserve

-> fulfillment.Ship

-> End

onError
    TimeoutError
        -> dlq.SendTimeout
    PaymentDeclined
        -> payment.ReverseHold
        -> email.NotifyFailure
    InsufficientInventory
        -> payment.Refund
        -> email.NotifyStock
    Default
        -> dlq.SendGeneric
```

### Circuit Breaker

```
version 1

flow ExternalAPICall

service external-api
    type http
    url https://external.api.com/data

service cache
    type redis
    connection redis:6379

breaker
    failureRate 50
    window 60s
    cooldown 30s

retry
    attempts 3
    backoff exponential
    delay 1s

timeout 10s

workflow

Start

-> external-api.Fetch

-> cache.Store

-> End

onError
    CircuitOpenError
        -> cache.GetCached
    Default
        -> dlq.Send
```

### Compensation (Saga Pattern)

```
version 1

flow OrderWithSaga

service payment
    type http
    url https://payment.internal/charge

service inventory
    type grpc
    address inventory:50051

service fulfillment
    type grpc
    address fulfillment:50051

workflow

Start

-> payment.Charge

-> inventory.Reserve

-> fulfillment.Ship

-> End

onError
    Default
        -> dlq.Send

compensate
    payment.Charge -> payment.Refund
    inventory.Reserve -> inventory.Release
    fulfillment.Ship -> fulfillment.CancelShipment
```

If `fulfillment.Ship` fails, the saga automatically:
1. Releases inventory hold
2. Refunds the payment

## Error Types Reference

| Error Type | When It Triggers |
|------------|-----------------|
| `TimeoutError` | Service call exceeds configured timeout |
| `ConnectionError` | Cannot establish connection to service |
| `PaymentDeclined` | Payment service returns decline |
| `InsufficientInventory` | Inventory check fails |
| `CircuitOpenError` | Circuit breaker is in open state |
| `Default` | Any unhandled error |

## DLQ Pattern

The dead letter queue captures failed events for later inspection:

```
n:process f:dlq
```

Events land in the DLQ topic with:
- Original payload
- Error metadata (which step failed, error message)
- Timestamp
- Retry count

The DLQ supports:
- **Replay**: reprocess failed events
- **Deduplication**: prevent duplicate processing
- **Persistence**: disk-backed storage with 0600 permissions

## Resilience Patterns Summary

| Pattern | Bytecode | Flow DSL | Use Case |
|---------|----------|----------|----------|
| Fallback | `f:svc` | `onError` | Degrade gracefully |
| Retry | `r3:exp` | `retry` block | Transient failures |
| Timeout | `t5000` | `timeout` | Slow services |
| Circuit Breaker | — | `breaker` block | Cascading failures |
| DLQ | `f:dlq` | `onError Default` | Dead letter capture |
| Saga | — | `compensate` block | Multi-step rollback |

## Edge Cases

### 1. Fallback Only Catches Preceding Failure
```
# WRONG — f:dlq catches the VALIDATE failure, not process
n:validate f:dlq | n:process

# RIGHT — f:dlq catches process failure
n:validate | n:process f:dlq
```

### 2. Retry Is Per-Attempt, Not Total
```
n:svc t1000 r3:exp
# Attempt 1: 1s timeout
# Attempt 2: 1s timeout (after 100ms delay)
# Attempt 3: 1s timeout (after 200ms delay)
# Total worst case: 1s + 100ms + 1s + 200ms + 1s = 3.3s
```

### 3. Circuit Breaker States
```
Closed (normal) → Open (blocking) → Half-Open (testing)
       ↑                                |
       └────────────────────────────────┘
```

When Open:
- All calls fail immediately with `CircuitOpenError`
- No actual service call is made
- After cooldown: one test call is allowed through

### 4. DLQ Entry Validation
DLQ entry IDs validated against `^[a-zA-Z0-9_\-]+$` to prevent path traversal:
```
# Valid
entry_id: "order-123_abc"

# Invalid (rejected)
entry_id: "../../etc/passwd"
entry_id: "order/123"
```

### 5. Saga Compensator Wiring
Saga compensators are wired post-construction via `SetCompensator()`:
```go
// ServiceCaller is created after SagaTracker
saga.SetCompensator(step, compensator)
```

### 6. Error Propagation Chain
```
VM error
  → StepFailed
  → Go bridge checks f: fallback
  → If fallback: route to fallback service
  → If no fallback: check onError handler
  → If handler: execute handler steps
  → If no handler: write to DLQ (if configured)
  → If no DLQ: error propagates to caller
```
