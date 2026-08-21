# Complex Workflows

End-to-end production patterns combining all features.

## E-Commerce Order Processing

### Bytecode DSL

```
schema:{!order_id:string,!items:array,!user_id:string} | t500 n:auth | p:fraud-check,inventory-reserve | c | g:fraud_score>0.8 f:dlq | g:inventory_status==available n:charge-payment r3:exp f:refund | n:fulfill | e:notify-user,analytics,audit-log
```

**Step-by-step:**
1. Validate schema (order_id, items, user_id required)
2. Authenticate with 500ms timeout
3. Parallel: fraud check + inventory reservation
4. Collect results
5. Gate: if fraud score > 0.8, send to DLQ
6. Gate: if inventory available, charge payment (3 retries, exponential backoff)
7. If payment fails, refund
8. Fulfill order
9. Emit to notify, analytics, audit-log

### Flow DSL

```
version 1

flow OrderProcessing

description
    Complete e-commerce order processing with fraud detection,
    inventory management, payment, and fulfillment.

service auth
    type grpc
    address auth:50051

service fraud
    type grpc
    address fraud:50051

service inventory
    type grpc
    address inventory:50051

service payment
    type http
    url https://payment.internal/charge
    method POST

service fulfillment
    type grpc
    address fulfillment:50051

service email
    type http
    url https://email.internal/send

service analytics
    type kafka
    brokers kafka1:9092,kafka2:9092
    topic order-events

service dlq
    type kafka
    brokers kafka1:9092
    topic dead-letter-queue

constants
    MAX_RETRY int = 3
    PAYMENT_TIMEOUT string = "10s"

variables
    order_total float = 0.0
    fraud_score float = 0.0

retry
    attempts 3
    backoff exponential
    delay 500ms
    maxDelay 10s

breaker
    failureRate 50
    window 60s
    cooldown 30s

timeout 30s

event OrderReceived
event OrderFulfilled
event OrderFailed

workflow

Start

-> auth.ValidateToken

parallel
    -> fraud.Check
    -> inventory.Reserve
join

-> if fraud.result == "pass"
    then
        -> if inventory.status == "available"
            then
                -> payment.Charge
                -> fulfillment.Ship
                -> emit OrderFulfilled
            else
                -> email.NotifyOutOfStock
                -> emit OrderFailed
    else
        -> payment.Refund
        -> email.NotifyFraud
        -> emit OrderFailed

-> email.SendConfirmation

-> analytics.Record

-> End

onError
    TimeoutError
        -> fulfillment.Cancel
        -> email.NotifyTimeout
    PaymentDeclined
        -> inventory.Release
        -> email.NotifyPaymentFailed
    Default
        -> dlq.Send

compensate
    payment.Charge -> payment.Refund
    inventory.Reserve -> inventory.Release
    fulfillment.Ship -> fulfillment.CancelShipment
```

## User Onboarding Flow

```
version 1

flow UserOnboarding

service auth
    type grpc
    address auth:50051

service profile
    type grpc
    address profile:50051

service email
    type http
    url https://email.internal/send

service analytics
    type kafka
    brokers kafka:9092
    topic user-events

workflow

Start

-> auth.CreateUser

-> profile.InitProfile

parallel
    -> email.SendWelcome
    -> email.SendGettingStarted
    -> analytics.TrackSignup
join

-> if user.plan == "premium"
    then
        -> profile.SetupPremium
        -> email.SendPremiumGuide
    else
        -> profile.SetupFree

-> End
```

## Data Pipeline (ETL)

```
version 1

flow DataPipeline

service extractor
    type http
    url https://source.internal/extract

service transformer
    type grpc
    address transformer:50051

service validator
    type grpc
    address validator:50051

service warehouse
    type postgres
    connection postgres://db:5432/warehouse

service monitor
    type http
    url https://monitor.internal/alert

retry
    attempts 3
    backoff exponential
    delay 1m

timeout 5m

workflow

Start

-> extractor.Pull

parallel
    -> transformer.Normalize
    -> transformer.Enrich
join

-> validator.Check

-> if validator.errors.length > 0
    then
        -> monitor.SendAlert
        -> warehouse.StoreQuarantine
    else
        -> warehouse.Store

-> monitor.ReportSuccess

-> End

onError
    TimeoutError
        -> monitor.SendAlert
    Default
        -> monitor.SendAlert
```

## Multi-Tenant API Gateway

```
version 1

flow APIGateway

service auth
    type grpc
    address auth:50051

service rate-limiter
    type redis
    connection redis:6379

service router
    type grpc
    address router:50051

service cache
    type redis
    connection redis:6379

constants
    RATE_LIMIT int = 100
    CACHE_TTL string = "5m"

workflow

Start

-> auth.Authenticate

-> rate-limiter.Check

-> if rate-limiter.allowed == false
    then
        -> Return 429
    else
        -> if cache.has(@.path)
            then
                -> Return cache.get(@.path)
            else
                -> router.Route
                -> cache.Set

-> Return result

onError
    TimeoutError
        -> Return 504
    Default
        -> Return 500
```

## Microservice Orchestration

```
version 1

flow ServiceMesh

service gateway
    type http
    url https://gateway.internal/route

service auth
    type grpc
    address auth:50051

service user-svc
    type grpc
    address user:50051

service order-svc
    type grpc
    address order:50051

service inventory-svc
    type grpc
    address inventory:50051

service payment-svc
    type http
    url https://payment.internal/charge

service notification-svc
    type kafka
    brokers kafka:9092
    topic notifications

retry
    attempts 2
    backoff linear
    delay 1s

timeout 15s

workflow

Start

-> auth.Validate

-> user-svc.GetProfile

-> order-svc.Create

parallel
    -> inventory-svc.Reserve
    -> payment-svc.Charge
join

-> order-svc.Confirm

-> notification-svc.Send

-> End

onError
    Default
        -> order-svc.Cancel
        -> notification-svc.SendFailure
```

## Feature Flags + A/B Testing

```
version 1

flow ABTesting

service flags
    type redis
    connection redis:6379

service variant-a
    type grpc
    address variant-a:50051

service variant-b
    type grpc
    address variant-b:50051

service analytics
    type kafka
    brokers kafka:9092
    topic ab-test-events

workflow

Start

-> flags.GetVariant

-> if flags.variant == "A"
    then
        -> variant-a.Process
    else
        -> variant-b.Process

-> analytics.Track

-> End
```

## Common Patterns Summary

| Pattern | Bytecode | Flow DSL |
|---------|----------|----------|
| Sequential | `n:a \| n:b \| n:c` | `-> A -> B -> C` |
| Parallel | `p:a,b \| c` | `parallel -> A -> B join` |
| Conditional | `g:field==val` | `if/else` |
| Fallback | `f:svc` | `onError` |
| Retry | `r3:exp` | `retry` block |
| Timeout | `t5000` | `timeout` |
| Circuit Breaker | — | `breaker` block |
| Schema Guard | `schema:{...}` | Variable types |
| Data Transform | `m:{...}` | Implicit in steps |
| Emit | `e:svc1,svc2` | `-> emit Event` |
| Buffer | `b100` | — |
| Chunk | `chunk:10:par` | — |
| DAG | `dag:{...}` | `parallel/join` |
| WASM | `w:plugin.func` | — |
| Delay | `delay:5000` | — |
| Jump | `j:label` | — |
| Label | `name:` | — |
| Saga | — | `compensate` block |
