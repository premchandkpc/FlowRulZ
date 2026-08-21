# Flow DSL Reference

The `.flow` file format is a block-based orchestration language for defining multi-step workflows with services, events, error handling, and compensation. It compiles to IR then bytecode for execution on the Rust VM.

## File Structure

A `.flow` file follows this order:

```
version 1

flow <name>

description
    <text>
    tags
        <tag1>
        <tag2>

import "<path>"
include "<path>"

variables
    <name> <type>

constants
    <name> = <value>

service <name>
    type <grpc|http|kafka|redis|postgres|tcp>
    <key> <value>

event <name>
    payload <type>

retry
    attempts <N>
    backoff <linear|exponential|fixed>
    delay <duration>
    maxDelay <duration>

breaker
    failureRate <percent>
    window <seconds>
    cooldown <duration>

timeout <duration>

workflow
    -> <step>
    if <condition>
        -> <step>
    else
        -> <step>
    switch <variable>
        case <value>
            -> <step>
        default
            -> <step>
    parallel
        -> <step1>
        -> <step2>
    join
    wait <event>
        timeout <duration>
    foreach <variable>
        -> <step>
    while <condition>
        -> <step>
    emit <event>
    Return <value>

onError
    <ErrorType>
        -> <step>
    Default
        -> <step>

compensate
    <step> <compensation_step>

output
    <name>
```

---

## Flow Declaration

Every file starts with a version and flow name:

```
version 1

flow order_processing
```

The name must be a valid identifier (letters, digits, underscores).

---

## Description and Tags

```
description
    Process incoming orders through validation, payment, and fulfillment.
    tags
        production
        orders
        critical
```

Tags are used for filtering and categorization in the admin API.

---

## Imports and Includes

```
import "shared/utils.flow"
import "lib/auth.flow" as auth

include "config.yaml"
```

- `import` - imports another `.flow` file (can be aliased with `as`)
- `include` - includes a non-flow configuration file

---

## Variables

Variables declare typed state available throughout the workflow:

```
variables
    order_id string
    total float
    retry_count int
    is_premium bool
```

Supported types: `string`, `int`, `float`, `bool`, `object`, `array`.

---

## Constants

Constants define immutable values:

```
constants
    MAX_RETRIES = 3
    TIMEOUT_MS = 5000
    DEFAULT_REGION = "us-east-1"
```

---

## Services

Services declare external system connections.

### gRPC Service

```
service payment
    type grpc
    address payment-svc:50051
    tls true
```

### HTTP Service

```
service notification
    type http
    url https://api.notify.example.com
    timeout 5s
```

### Kafka Service

```
service events
    type kafka
    brokers
        kafka1:9092
        kafka2:9092
        kafka3:9092
    topic orders
```

### Redis Service

```
service cache
    type redis
    address redis-cluster:6379
    connection
        pool_size 10
        timeout 2s
```

### PostgreSQL Service

```
service datastore
    type postgres
    connection
        host db.example.com
        port 5432
        database orders
        sslmode require
```

### TCP Service

```
service legacy
    type tcp
    address legacy-svc:9000
    idempotent true
```

**Service options summary:**

| Option | Types | Description |
|--------|-------|-------------|
| `type` | all | Connection protocol |
| `address` | grpc, redis, tcp | Host:port |
| `url` | http | Base URL |
| `connection` | postgres, redis | Key-value config map |
| `brokers` | kafka | List of broker addresses |
| `topic` | kafka | Default topic |
| `tls` | grpc | Enable TLS |
| `timeout` | http, grpc | Connection timeout |
| `idempotent` | tcp | Mark as idempotent |
| `enabled` | all | Enable/disable service |

---

## Events

Events declare named messages the flow can emit:

```
event OrderCreated
    payload OrderPayload

event PaymentProcessed
    payload PaymentResult

event OrderFailed
```

Events must be declared before referencing them in `emit` steps.

---

## Workflow Steps

The `workflow` block contains the execution logic. Steps are prefixed with `->`:

```
workflow
    -> Start
    -> validate_order
    -> process_payment
    -> ship_order
    -> End
```

### Service Calls

Reference services with `-> <service_name>` or `-> <service_name>.<method>`:

```
workflow
    -> payment.authorize
    -> inventory.reserve
    -> notification.send_confirmation
```

### Conditional Branching (if/else)

```
workflow
    -> validate_order
    if total > 1000
        -> premium_shipping
    else
        -> standard_shipping
    -> send_notification
```

### Multi-Way Branching (switch/case)

```
workflow
    -> classify_order
    switch order_type
        case "express"
            -> express_fulfillment
        case "standard"
            -> standard_fulfillment
        case "bulk"
            -> bulk_fulfillment
        default
            -> default_fulfillment
    -> update_status
```

### Parallel Execution

Run steps concurrently and join:

```
workflow
    -> validate_order
    parallel
        -> check_inventory
        -> calculate_shipping
        -> apply_promotions
    join
    -> finalize_order
```

### Wait for Event

Pause execution until an external event arrives:

```
workflow
    -> submit_order
    wait OrderConfirmed
        timeout 30s
    -> process_payment
```

### Foreach Loop

Iterate over a collection:

```
workflow
    -> get_items
    foreach items
        -> process_item
    -> finalize_batch
```

### While Loop

Loop while a condition holds:

```
workflow
    -> initialize
    while retry_count < MAX_RETRIES
        -> attempt_operation
    -> finalize
```

### Emit Events

Fire-and-forget event emission:

```
workflow
    -> process_order
    emit OrderCreated
    -> update_inventory
    emit InventoryUpdated
```

### Return Values

Return a value from the workflow:

```
workflow
    -> process_order
    Return order_result
```

Or return void:

```
workflow
    -> cleanup
    Return
```

---

## Retry Policy

Configures automatic retry for the entire flow or specific steps:

```
retry
    attempts 3
    backoff exponential
    delay 1s
    maxDelay 30s
```

**Backoff strategies:**
- `fixed` - constant delay between retries
- `linear` - delay increases linearly (delay x attempt)
- `exponential` - delay doubles each retry (delay x 2^attempt)

---

## Circuit Breaker

Protects against cascading failures:

```
breaker
    failureRate 50
    window 60
    cooldown 30s
```

- `failureRate` - percentage of failures to trip the breaker (0-100)
- `window` - time window in seconds to measure failures
- `cooldown` - how long to wait before half-opening

---

## Timeout

Sets a global timeout for the entire flow execution:

```
timeout 60s
```

Duration format: `<number><unit>` where unit is `ms` (milliseconds), `s` (seconds), `m` (minutes), `h` (hours).

---

## Error Handling

The `onError` block defines typed error handlers:

```
onError
    PaymentDeclined
        -> handle_payment_decline
        -> notify_customer
    InsufficientInventory
        -> handle_out_of_stock
        -> notify_warehouse
    Default
        -> handle_generic_error
        -> log_error
        -> notify_admin
```

Error types are matched by name. The `Default` case catches all unmatched errors.

---

## Compensation (Saga Pattern)

Maps workflow steps to their compensation actions. If any step fails, compensations run in reverse order:

```
workflow
    -> reserve_inventory
    -> charge_payment
    -> schedule_shipping

compensate
    reserve_inventory release_inventory
    charge_payment refund_payment
    schedule_shipping cancel_shipping
```

If `schedule_shipping` fails:
1. `refund_payment` runs (compensate `charge_payment`)
2. `release_inventory` runs (compensate `reserve_inventory`)

---

## Outputs

Declares named outputs the flow produces:

```
output
    order_id
    total_amount
    tracking_number
```

---

## Complete Examples

### Order Processing Flow

```
version 1

flow order_processing

description
    End-to-end order processing with payment and shipping.
    tags
        production
        orders

variables
    order_id string
    total float
    status string

constants
    TAX_RATE = 0.08
    FREE_SHIPPING_THRESHOLD = 50.00

service payment
    type grpc
    address payment-svc:50051
    tls true

service inventory
    type grpc
    address inventory-svc:50052

service shipping
    type http
    url https://api.shipping.example.com
    timeout 10s

service notification
    type kafka
    brokers
        kafka1:9092
        kafka2:9092
    topic notifications

event OrderCreated
    payload OrderPayload

event OrderShipped

retry
    attempts 3
    backoff exponential
    delay 1s
    maxDelay 10s

breaker
    failureRate 50
    window 60
    cooldown 30s

timeout 120s

workflow
    -> Start
    -> validate_order

    if total > FREE_SHIPPING_THRESHOLD
        -> free_shipping
    else
        -> calculate_shipping

    -> charge_payment
    -> schedule_delivery
    -> send_confirmation
    emit OrderCreated
    -> End

onError
    PaymentDeclined
        -> handle_payment_decline
        -> notify_customer_failure
    InsufficientInventory
        -> release_reserved_stock
        -> notify_customer_oos
    Default
        -> handle_generic_error
        -> notify_admin

compensate
    charge_payment refund_payment
    schedule_delivery cancel_delivery

output
    order_id
    total
    status
```

### User Onboarding Flow

```
version 1

flow user_onboarding

description
    Register new user, create profile, send welcome email.
    tags
        users
        onboarding

variables
    user_id string
    email string
    plan string

constants
    DEFAULT_PLAN = "free"

service auth
    type grpc
    address auth-svc:50051

service profile
    type grpc
    address profile-svc:50052

service email
    type http
    url https://api.email.example.com
    timeout 5s

service analytics
    type kafka
    brokers
        kafka1:9092
    topic user-events

event UserRegistered

workflow
    -> Start
    -> create_user_account
    -> create_profile

    if plan == "premium"
        -> setup_premium_features
    else
        -> setup_free_features

    -> send_welcome_email
    emit UserRegistered
    -> track_onboarding
    -> End

onError
    DuplicateEmail
        -> notify_existing_user
    Default
        -> cleanup_partial_data
        -> notify_admin

output
    user_id
    email
```

### Data Pipeline Flow

```
version 1

flow data_pipeline

description
    Ingest, transform, validate, and store data records.
    tags
        data
        batch

variables
    batch_id string
    record_count int
    error_count int

service ingest
    type kafka
    brokers
        kafka1:9092
        kafka2:9092
    topic raw-data

service transform
    type grpc
    address transform-svc:50051

service validator
    type grpc
    address validator-svc:50052

service warehouse
    type postgres
    connection
        host db.warehouse.example.com
        port 5432
        database analytics
        sslmode require

service dead_letter
    type kafka
    brokers
        kafka1:9092
    topic dead-letter

service metrics
    type http
    url https://metrics.example.com

event BatchProcessed

timeout 300s

workflow
    -> Start
    -> fetch_batch
    -> transform_records
    -> validate_records

    if error_count > 0
        -> route_errors
    else
        -> store_records

    -> update_metrics
    emit BatchProcessed
    -> End

onError
    ValidationError
        -> route_errors
        -> store_valid_records
    DatabaseError
        -> retry_with_backoff
        -> alert_oncall
    Default
        -> log_and_alert

output
    batch_id
    record_count
    error_count
```

### Saga: Travel Booking Flow

```
version 1

flow travel_booking

description
    Book flight, hotel, and car with full saga compensation.
    tags
        travel
        saga
        critical

variables
    booking_id string
    flight_ref string
    hotel_ref string
    car_ref string
    total_cost float

service flights
    type grpc
    address flights-svc:50051
    tls true

service hotels
    type grpc
    address hotels-svc:50052
    tls true

service cars
    type http
    url https://api.cars.example.com
    timeout 10s

service payment
    type grpc
    address payment-svc:50053
    tls true

service notifications
    type kafka
    brokers
        kafka1:9092
    topic travel-events

event BookingConfirmed
event BookingFailed

retry
    attempts 2
    backoff exponential
    delay 2s
    maxDelay 30s

timeout 180s

workflow
    -> Start
    -> search_flights
    -> reserve_flight
    -> search_hotels
    -> reserve_hotel
    -> search_cars
    -> reserve_car
    -> charge_total_payment
    -> confirm_all_bookings
    emit BookingConfirmed
    -> send_itinerary
    -> End

onError
    FlightUnavailable
        -> cancel_flight
        -> cancel_hotel
        -> cancel_car
        -> refund_payment
        -> notify_booking_failed
    PaymentDeclined
        -> cancel_flight
        -> cancel_hotel
        -> cancel_car
        -> notify_payment_failed
    Default
        -> compensate_all
        -> notify_booking_failed

compensate
    reserve_flight cancel_flight
    reserve_hotel cancel_hotel
    reserve_car cancel_car
    charge_total_payment refund_payment

output
    booking_id
    flight_ref
    hotel_ref
    car_ref
    total_cost
```

### Scheduled Cleanup Flow

```
version 1

flow scheduled_cleanup

description
    Periodic cleanup of expired sessions and temp data.
    tags
        maintenance
        scheduled

variables
    deleted_sessions int
    deleted_files int

service session_store
    type redis
    address redis-cluster:6379
    connection
        pool_size 5
        timeout 1s

service file_store
    type http
    url https://storage.internal/cleanup
    timeout 30s

service audit_log
    type kafka
    brokers
        kafka1:9092
    topic audit

constants
    SESSION_TTL = "24h"
    FILE_TTL = "7d"

timeout 600s

workflow
    -> Start
    -> expire_old_sessions
    -> delete_temp_files
    -> compact_storage
    -> log_cleanup_summary
    -> End

onError
    Default
        -> log_error
        -> continue_anyway

output
    deleted_sessions
    deleted_files
```

### Event-Driven Notification Flow

```
version 1

flow notification_router

description
    Route notifications to email, SMS, or push based on user preferences.
    tags
        notifications
        event-driven

variables
    user_id string
    channel string
    priority string

service user_prefs
    type grpc
    address prefs-svc:50051

service email_service
    type http
    url https://api.email.example.com
    timeout 5s

service sms_service
    type http
    url https://api.sms.example.com
    timeout 5s

service push_service
    type http
    url https://api.push.example.com
    timeout 5s

service analytics
    type kafka
    brokers
        kafka1:9092
    topic notification-events

event NotificationSent

workflow
    -> Start
    -> load_user_preferences

    switch channel
        case "email"
            -> send_email
        case "sms"
            -> send_sms
        case "push"
            -> send_push
        default
            -> send_email

    emit NotificationSent
    -> track_delivery
    -> End

onError
    RateLimited
        -> queue_for_retry
    UserNotFound
        -> log_missing_user
    Default
        -> fallback_to_email
        -> log_error

output
    user_id
    channel
    priority
```

### Multi-Stage Approval Flow

```
version 1

flow approval_workflow

description
    Route requests through manager and director approval stages.
    tags
        approval
        workflow

variables
    request_id string
    amount float
    manager_approved bool
    director_approved bool

service hr_system
    type grpc
    address hr-svc:50051

service notification
    type kafka
    brokers
        kafka1:9092
    topic approvals

event ApprovalRequested
event ApprovalGranted
event ApprovalDenied

timeout 7d

workflow
    -> Start
    -> submit_request
    -> notify_manager
    wait ManagerDecision
        timeout 48h

    if manager_approved == true
        if amount > 10000
            -> notify_director
            wait DirectorDecision
                timeout 72h
            if director_approved == true
                -> grant_approval
            else
                -> deny_approval
        else
            -> grant_approval
    else
        -> deny_approval

    emit ApprovalGranted
    -> update_system
    -> End

onError
    TimeoutExpired
        -> auto_deny
        -> notify_timeout
    Default
        -> log_error
        -> notify_admin

output
    request_id
    amount
    manager_approved
    director_approved
```

### IoT Sensor Data Flow

```
version 1

flow sensor_ingestion

description
    Ingest IoT sensor data, validate, aggregate, and alert on anomalies.
    tags
        iot
        real-time

variables
    device_id string
    temperature float
    humidity float
    alert_level string

service mqtt_bridge
    type tcp
    address mqtt-broker:1883
    idempotent true

service data_store
    type postgres
    connection
        host timescale.internal
        port 5432
        database sensors

service rule_engine
    type grpc
    address rules-svc:50051

service alert_service
    type http
    url https://alerts.internal/notify
    timeout 3s

service dashboard
    type kafka
    brokers
        kafka1:9092
    topic sensor-updates

event SensorReading
event AlertTriggered

timeout 10s

workflow
    -> Start
    -> receive_reading
    -> validate_reading
    -> store_reading
    -> aggregate_metrics

    if temperature > 85.0
        -> trigger_high_temp_alert
    else
        if humidity > 90.0
            -> trigger_humidity_alert

    emit SensorReading
    -> update_dashboard
    -> End

onError
    InvalidReading
        -> log_and_discard
    DeviceOffline
        -> mark_device_stale
    Default
        -> log_error

output
    device_id
    temperature
    humidity
    alert_level
```
