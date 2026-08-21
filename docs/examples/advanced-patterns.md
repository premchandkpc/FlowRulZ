# Advanced Patterns

Saga, choreography, CQRS, event sourcing, outbox pattern, circuit breaker chains.

## 1. Saga Pattern (Orchestration)

### Flow DSL

```
version 1

flow OrderSaga

service order
    type grpc
    address order:50051

service inventory
    type grpc
    address inventory:50051

service payment
    type http
    url https://payment.internal/charge

service shipping
    type grpc
    address shipping:50051

service notifications
    type kafka
    brokers kafka:9092
    topic saga-events

timeout 30s

workflow

Start

-> order.Create

-> inventory.Reserve

-> payment.Charge

-> shipping.Schedule

-> order.Confirm

-> notifications.SendConfirmation

-> Return {order_id: order.id, status: "confirmed"}

onError
    Default
        -> notifications.SendSagaFailed

compensate
    shipping.Schedule -> shipping.Cancel
    payment.Charge -> payment.Refund
    inventory.Reserve -> inventory.Release
    order.Create -> order.Cancel
```

## 2. Choreography (Event-Driven)

### Flow DSL

```
version 1

flow OrderChoreography

service kafka
    type kafka
    brokers kafka:9092
    topic order-events

service order-handler
    type grpc
    address order:50051

service inventory-handler
    type grpc
    address inventory:50051

service payment-handler
    type grpc
    address payment:50051

service shipping-handler
    type grpc
    address shipping:50051

timeout 10s

workflow

Start

-> kafka.Consume("order.created")

-> order-handler.Process

-> kafka.Publish("order.validated")

-> kafka.Consume("order.validated")

-> inventory-handler.Reserve

-> kafka.Publish("inventory.reserved")

-> kafka.Consume("inventory.reserved")

-> payment-handler.Charge

-> kafka.Publish("payment.charged")

-> kafka.Consume("payment.charged")

-> shipping-handler.Schedule

-> kafka.Publish("order.completed")

-> Return {order_id: order.id, status: "completed"}
```

## 3. Outbox Pattern

### Flow DSL

```
version 1

flow OutboxPattern

service business-logic
    type grpc
    address logic:50051

service outbox-store
    type grpc
    address outbox:50051

service kafka-producer
    type kafka
    brokers kafka:9092
    topic outbox-events

service notifications
    type kafka
    brokers kafka:9092
    topic domain-events

timeout 10s

workflow

Start

-> business-logic.Execute

-> outbox-store.SaveToOutbox

-> kafka-producer.Publish

-> outbox-store.MarkPublished

-> notifications.SendDomainEvent

-> Return {status: "published", event_id: outbox-store.event_id}
```

## 4. Circuit Breaker Chain

### Flow DSL

```
version 1

flow CircuitBreakerChain

service primary-api
    type http
    url https://primary.internal/api

service secondary-api
    type http
    url https://secondary.internal/api

service fallback-api
    type http
    url https://fallback.internal/api

service cache
    type redis
    connection redis:6379

breaker
    failureRate 50
    window 60s
    cooldown 30s

timeout 5s

workflow

Start

-> if cache.has(@.key)
    then
        -> Return cache.get(@.key)

-> primary-api.Call

-> if primary-api.status == "circuit_open"
    then
        -> secondary-api.Call
        -> if secondary-api.status == "circuit_open"
            then
                -> fallback-api.Call
            else
                -> cache.Store
                -> Return secondary-api.result
    else
        -> cache.Store
        -> Return primary-api.result
```

## 5. CQRS (Command Query Responsibility Segregation)

### Flow DSL

```
version 1

flow CQRSCommandSide

service auth
    type grpc
    address auth:50051

service validator
    type grpc
    address validator:50051

service event-store
    type grpc
    address eventstore:50051

service projections
    type grpc
    address projections:50051

service notifications
    type kafka
    brokers kafka:9092
    topic cqrs-events

timeout 5s

workflow

Start

-> auth.AuthenticateCommand

-> validator.ValidateCommand

-> event-store.AppendEvent

-> projections.UpdateReadModel

-> notifications.BroadcastEvent

-> Return {event_id: event-store.event_id, version: event-store.version}
```

```
version 1

flow CQRSQuerySide

service cache
    type redis
    connection redis:6379

service read-model
    type grpc
    address readmodel:50051

service query-handler
    type grpc
    address queryhandler:50051

timeout 5s

workflow

Start

-> cache.GetCached

-> if cache.hit == true
    then
        -> Return cache.result
    else
        -> read-model.Query
        -> query-handler.Format
        -> cache.Store
        -> Return query-handler.result
```

## 6. Event Sourcing with Snapshots

### Flow DSL

```
version 1

flow EventSourcingWithSnapshots

service event-store
    type grpc
    address eventstore:50051

service snapshot-store
    type grpc
    address snapshots:50051

service projection
    type grpc
    address projections:50051

service query
    type grpc
    address query:50051

constants
    SNAPSHOT_INTERVAL int = 100

timeout 10s

workflow

Start

-> event-store.AppendEvent

-> projection.UpdateProjection

-> if event-store.version % SNAPSHOT_INTERVAL == 0
    then
        -> snapshot-store.CreateSnapshot

-> query.UpdateReadModel

-> Return {version: event-store.version}
```

## 7. Idempotent Processing

### Bytecode DSL

```
schema:{!idempotency_key:string,!payload:object} | n:check-duplicate | g:is_duplicate==true n:return-cached | n:process | n:store-result | e:processed
```

### Flow DSL

```
version 1

flow IdempotentProcessing

service dedup
    type grpc
    address dedup:50051

service processor
    type grpc
    address processor:50051

service result-store
    type grpc
    address results:50051

timeout 5s

workflow

Start

-> dedup.CheckDuplicate

-> if dedup.is_duplicate == true
    then
        -> result-store.GetCachedResult
        -> Return dedup.cached_result
    else
        -> processor.Process
        -> result-store.StoreResult
        -> dedup.RecordProcessed

-> Return processor.result
```

## Advanced Pattern Edge Cases

### 1. Saga Compensation Failure
What if a compensator itself fails?
```
compensate
    payment.Charge -> payment.Refund  # Refund fails!

# Solution: compensators have their own retry policy
# If refund fails after retries:
# 1. Log the failure
# 2. Alert operations team
# 3. Manual intervention required
# The saga is in a "partially compensated" state
```

### 2. Choreography Event Loss
```
# Kafka topic "order.created" loses a message
# Order handler never processes the event
# Solution: use Outbox pattern
# Write event to outbox table in same transaction as business logic
# Background poller publishes from outbox to Kafka
```

### 3. CQRS Read Model Staleness
```
# Command side updated, read model not yet updated
# User reads stale data
# Solution: read-your-writes consistency
# After command, return version number
# On read, wait until read model reaches that version
```

### 4. Circuit Breaker Half-Open Storm
```
# All half-open test calls succeed
# Circuit closes immediately
# Underlying issue might be intermittent
# Solution: gradual half-open (allow 10% traffic through)
```

### 5. Outbox Poller Latency
```
# Outbox poller runs every 5 seconds
# Event sits in outbox for up to 5 seconds
# Solution: use CDC (Change Data Capture) instead of polling
# Or: reduce poll interval (but increase DB load)
```

### 6. Event Sourcing Snapshot Corruption
```
# Snapshot is corrupted
# Solution: rebuild from event log
# Keep enough events to rebuild any snapshot
# Or: keep multiple snapshot versions
```

### 7. Idempotency Key Expiry
```
# Idempotency key TTL = 24 hours
# Request arrives after 24 hours with same key
# Treated as new request (not duplicate)
# Solution: extend TTL for critical operations
```
