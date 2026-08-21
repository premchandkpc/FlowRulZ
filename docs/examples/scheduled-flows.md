# Scheduled Flows

Cron triggers, delayed execution, timers, time-based workflows.

## Bytecode DSL

### Delay

```
delay:5000 n:svc
```

Wait 5 seconds before calling `svc`.

### Delayed Pipeline

```
n:validate | delay:1000 n:process | delay:5000 n:notify
```

Validate immediately, process after 1 second, notify after 5 more seconds.

## Flow DSL

### Cron Trigger

```
version 1

flow ScheduledReport

service db
    type postgres
    connection postgres://db:5432/analytics

service email
    type http
    url https://email.internal/send

trigger cron
    schedule "0 6 * * *"

workflow

Start

-> db.QueryMetrics

-> email.SendReport

-> End
```

Runs every day at 6:00 AM.

### Cron Schedules

| Schedule | Description |
|----------|-------------|
| `"0 * * * *"` | Every hour |
| `"0 */5 * * *"` | Every 5 minutes |
| `"0 9 * * 1-5"` | Weekdays at 9 AM |
| `"0 0 1 * *"` | First day of month |
| `"30 18 * * 5"` | Fridays at 6:30 PM |
| `"0 6,12,18 * * *"` | 6 AM, 12 PM, 6 PM |

### Message Trigger

```
version 1

flow EventDriven

service processor
    type grpc
    address processor:50051

trigger message
    topic orders

workflow

Start

-> processor.Handle

-> End
```

Triggered by incoming messages on the `orders` topic.

### HTTP Trigger

```
version 1

flow APIEndpoint

service backend
    type grpc
    address backend:50051

trigger http
    path /api/orders
    method POST

workflow

Start

-> backend.ProcessOrder

-> End
```

Triggered by HTTP POST to `/api/orders`.

### Webhook Trigger

```
version 1

flow PaymentWebhook

service payment
    type http
    url https://payment.internal/verify

trigger webhook
    path /webhook/stripe

workflow

Start

-> payment.Verify

-> fulfillment.Ship

-> End
```

Triggered by webhook callback from Stripe.

## Scheduled Patterns

### Periodic Cleanup

```
version 1

flow Cleanup

service db
    type postgres
    connection postgres://db:5432/app

trigger cron
    schedule "0 2 * * *"

workflow

Start

-> db.CleanupOldRecords

-> db.Vacuum

-> End
```

Daily at 2 AM.

### Health Check

```
version 1

flow HealthCheck

service api-gateway
    type http
    url https://gateway.internal/health

service alerting
    type http
    url https://alerts.internal/send

trigger cron
    schedule "*/5 * * * *"

workflow

Start

-> api-gateway.Check

-> if api-gateway.status != "healthy"
    then
        -> alerting.SendAlert
    else
        -> Return "ok"

-> End
```

Every 5 minutes.

### Scheduled Report with Retry

```
version 1

flow WeeklyReport

service db
    type postgres
    connection postgres://db:5432/analytics

service report
    type http
    url https://reports.internal/generate

service email
    type http
    url https://email.internal/send

retry
    attempts 3
    backoff exponential
    delay 1m

trigger cron
    schedule "0 8 * * 1"

workflow

Start

-> db.QueryWeeklyMetrics

-> report.Generate

-> email.Send

-> End
```

Mondays at 8 AM with retry.

## Time-Based Flow Patterns

| Pattern | Trigger | Example |
|---------|---------|---------|
| Periodic cleanup | Cron | Daily at 2 AM |
| Health monitoring | Cron | Every 5 minutes |
| Report generation | Cron | Weekly on Monday |
| Event-driven processing | Message | On order received |
| API endpoint | HTTP | On POST request |
| Webhook callback | Webhook | On payment notification |
| Delayed action | Delay | Wait then process |
| Staggered execution | Delay | Sequential delays |
