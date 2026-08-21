# Basic Pipelines

Sequential service calls — the simplest flow pattern.

## Bytecode DSL

### Single Service Call

```
n:auth
```

Call `auth` synchronously. Wait for response. The response payload becomes the input to the next operation.

### Sequential Chain

```
n:validate | n:enrich | n:store
```

Three calls in sequence. Each receives the output of the previous.

**Execution flow:**
1. Call `validate` with original payload
2. Call `enrich` with `validate`'s response
3. Call `store` with `enrich`'s response

### Request/Reply

```
n:my-service
```

The VM pauses execution, sends the payload to `my-service` via HTTP/gRPC, waits for the response, and continues with the response as the new payload.

**What happens under the hood:**
1. VM emits `StepPending` with `svc_id` and body
2. Go control plane calls the actual service
3. Response is fed back into the VM
4. VM continues to next instruction

### Fire-and-Forget

```
a:audit-log
```

Call `audit-log` without waiting. Execution continues immediately. Useful for side effects that shouldn't block the main flow.

### Multi-Service Emit

```
e:notify,email-service
```

Publish to both `notify` and `email-service` simultaneously. Neither response is awaited.

### With Timeout

```
n:slow-service t5000
```

Call `slow-service` with a 5-second timeout. If the service doesn't respond in time, the step fails.

### Visual Separator

```
n:validate | n:enrich | n:store
```

`|` is a no-op separator — removed by the optimizer. Use it to group related operations visually.

## Flow DSL

### Simple Sequential

```
version 1

flow UserSignup

service auth
    type grpc
    address auth:50051

service email
    type http
    url https://email.internal/send

workflow

Start

-> auth.CreateUser

-> email.SendWelcome

-> End
```

### With Service Options

```
version 1

flow DataPipeline

service ingress
    type kafka
    brokers kafka1:9092,kafka2:9092
    topic raw-events

service processor
    type grpc
    address processor:50051
    tls true

service warehouse
    type postgres
    connection postgres://db:5432/warehouse

workflow

Start

-> ingress.Consume

-> processor.Transform

-> warehouse.Store

-> End
```

### With Variables and Constants

```
version 1

flow OrderProcessing

constants
    MAX_ITEMS int = 100
    TAX_RATE float = 0.08

variables
    subtotal float = 0.0
    order_id string = ""

service pricing
    type grpc
    address pricing:50051

service db
    type postgres
    connection postgres://db:5432/orders

workflow

Start

-> pricing.Calculate

-> db.InsertOrder

-> Return order_id
```

## Use Cases

| Pattern | Example |
|---------|---------|
| ETL pipeline | Extract, Transform, Load |
| CRUD operations | Validate, Create, Respond |
| Data enrichment | Fetch, Enrich, Store |
| User flows | Authenticate, Authorize, Execute |
| Notifications | Process, Notify, Log |
