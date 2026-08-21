# E-Commerce Complete

Full e-commerce platform flows — from product browse to delivery confirmation.

## 1. Product Search & Recommendation

### Bytecode DSL

```
schema:{!query:string,category:string,price_range:object} | n:search-service | m:{results: @.hits, total: @.total, facets: @.aggregations} | p:recommendations,popular-items | c | m:{search: @.results[0], recommended: @.results[1], popular: @.results[2]} | n:cache-store | e:analytics-search
```

### Flow DSL

```
version 1

flow ProductSearch

service search
    type grpc
    address search:50051

service recommendations
    type grpc
    address recommendations:50051

service popular
    type grpc
    address popular:50051

service cache
    type redis
    connection redis:6379

service analytics
    type kafka
    brokers kafka:9092
    topic search-events

timeout 2s

workflow

Start

-> search.Query

parallel
    -> recommendations.GetSimilar
    -> popular.GetTrending
join

-> cache.StoreResults

-> analytics.TrackSearch

-> End

onError
    TimeoutError
        -> cache.GetCached
    Default
        -> Return []
```

## 2. Cart Management

### Flow DSL

```
version 1

flow CartOperation

service cart
    type grpc
    address cart:50051

service inventory
    type grpc
    address inventory:50051

service pricing
    type grpc
    address pricing:50051

service recommendations
    type grpc
    address recommendations:50051

constants
    MAX_CART_ITEMS int = 100
    CART_TTL_HOURS int = 24

variables
    cart_total float = 0.0
    item_count int = 0

workflow

Start

-> cart.Get

-> if operation == "add"
    then
        -> inventory.CheckStock
        -> if inventory.in_stock == true
            then
                -> cart.AddItem
                -> pricing.CalculateTotal
                -> recommendations.GetCrossSell
            else
                -> Return {error: "out_of_stock", alternatives: inventory.alternatives}
    else
        -> if operation == "remove"
            then
                -> cart.RemoveItem
                -> pricing.CalculateTotal
            else
                -> if operation == "update"
                    then
                        -> inventory.CheckStock
                        -> cart.UpdateQuantity
                        -> pricing.CalculateTotal

-> cart.Save

-> Return cart

onError
    Default
        -> cart.Rollback
```

## 3. Checkout Pipeline (Full Saga)

### Flow DSL

```
version 1

flow Checkout

service auth
    type grpc
    address auth:50051

service cart
    type grpc
    address cart:50051

service inventory
    type grpc
    address inventory:50051

service pricing
    type grpc
    address pricing:50051

service payment
    type http
    url https://payment.internal/charge
    method POST

service tax
    type http
    url https://tax.internal/calculate

service shipping
    type http
    url https://shipping.internal/rates

service fraud
    type grpc
    address fraud:50051

service fulfillment
    type grpc
    address fulfillment:50051

service email
    type http
    url https://email.internal/send

service sms
    type http
    url https://sms.internal/send

service push
    type http
    url https://push.internal/send

service analytics
    type kafka
    brokers kafka:9092
    topic checkout-events

service dlq
    type kafka
    brokers kafka:9092
    topic dead-letter-queue

retry
    attempts 2
    backoff exponential
    delay 500ms

breaker
    failureRate 30
    window 60s
    cooldown 30s

timeout 30s

variables
    order_id string = ""
    cart_total float = 0.0
    tax_amount float = 0.0
    shipping_cost float = 0.0
    final_total float = 0.0

event CheckoutStarted
event OrderPlaced
event PaymentFailed
event OrderConfirmed

workflow

Start

-> auth.ValidateSession

-> cart.GetContents

-> if cart.item_count == 0
    then
        -> Return {error: "empty_cart"}

-> inventory.ReserveAll

-> pricing.Calculate

-> tax.CalculateTax

-> shipping.GetRates

-> m:{final_total: @.cart_total + @.tax_amount + @.shipping_cost}

-> fraud.CheckOrder

-> if fraud.risk_score > 0.8
    then
        -> inventory.ReleaseAll
        -> email.SendFraudAlert
        -> Return {error: "fraud_detected"}
    else
        -> if fraud.risk_score > 0.5
            then
                -> payment.ChargeWith3DS
            else
                -> payment.Charge

-> if payment.status == "success"
    then
        -> fulfillment.CreateOrder
        -> inventory.ConfirmReservation
        -> emit OrderConfirmed
        -> parallel
            -> email.SendOrderConfirmation
            -> sms.SendConfirmation
            -> push.SendNotification
        join
        -> analytics.TrackConversion
        -> Return {order_id: @.order_id, status: "confirmed"}
    else
        -> inventory.ReleaseAll
        -> email.SendPaymentFailed
        -> emit PaymentFailed
        -> Return {error: "payment_failed", reason: payment.error}

onError
    TimeoutError
        -> inventory.ReleaseAll
        -> dlq.SendTimeout
    InsufficientInventory
        -> payment.Refund
        -> email.SendOutOfStock
    Default
        -> inventory.ReleaseAll
        -> dlq.SendGeneric

compensate
    payment.Charge -> payment.Refund
    inventory.ReserveAll -> inventory.ReleaseAll
    fulfillment.CreateOrder -> fulfillment.CancelOrder
```

## 4. Order Tracking

### Bytecode DSL

```
schema:{!order_id:string} | n:get-order | p:shipping-status,payment-status,inventory-status | c | m:{order: @.results[0], shipping: @.results[1], payment: @.results[2], inventory: @.results[3], status: @.results[1].status} | n:enrich-address | e:analytics-tracking
```

### Flow DSL

```
version 1

flow OrderTracking

service order
    type grpc
    address order:50051

service shipping
    type grpc
    address shipping:50051

service payment
    type grpc
    address payment:50051

service inventory
    type grpc
    address inventory:50051

service geocoding
    type http
    url https://geocoding.internal/enrich

service notifications
    type kafka
    brokers kafka:9092
    topic tracking-events

timeout 5s

workflow

Start

parallel
    -> order.GetDetails
    -> shipping.GetStatus
    -> payment.GetStatus
    -> inventory.GetStockStatus
join

-> if shipping.status == "in_transit"
    then
        -> geocoding.EnrichLocation
        -> notifications.SendLocationUpdate
    else
        -> if shipping.status == "delivered"
            then
                -> notifications.SendDeliveredAlert

-> Return {order, shipping, payment, inventory}
```

## 5. Refund Processing

### Flow DSL

```
version 1

flow RefundProcessing

service order
    type grpc
    address order:50051

service payment
    type http
    url https://payment.internal/refund
    method POST

service inventory
    type grpc
    address inventory:50051

service accounting
    type grpc
    address accounting:50051

service email
    type http
    url https://email.internal/send

service analytics
    type kafka
    brokers kafka:9092
    topic refund-events

constants
    AUTO_REFUND_THRESHOLD float = 50.0
    MANUAL_REVIEW_THRESHOLD float = 500.0

variables
    refund_amount float = 0.0
    refund_type string = ""

workflow

Start

-> order.GetDetails

-> if order.total <= AUTO_REFUND_THRESHOLD
    then
        -> refund_type = "auto"
        -> payment.Refund
        -> inventory.RestockItems
        -> accounting.RecordRefund
        -> email.SendRefundConfirmation
        -> analytics.TrackRefund
        -> Return {status: "refunded", type: "auto"}
    else
        -> if order.total <= MANUAL_REVIEW_THRESHOLD
            then
                -> refund_type = "semi-auto"
                -> payment.HoldRefund
                -> email.SendRefundPending
                -> Return {status: "pending_review", type: "semi-auto"}
            else
                -> refund_type = "manual"
                -> email.SendManualReviewRequest
                -> Return {status: "manual_review", type: "manual"}

onError
    Default
        -> email.SendRefundFailed
        -> analytics.TrackRefundError
```

## 6. Inventory Replenishment

### Bytecode DSL

```
b100 | m:{batch: @, timestamp: now(), count: length(@)} | g:count>50 n:bulk-check | g:count<=50 n:individual-check | p:supplier-order,warehouse-transfer | c | n:confirm-replenishment | e:inventory-updated,analytics-replenish
```

### Flow DSL

```
version 1

flow InventoryReplenishment

service inventory
    type grpc
    address inventory:50051

service supplier
    type http
    url https://supplier.internal/order
    method POST

service warehouse
    type grpc
    address warehouse:50051

service analytics
    type kafka
    brokers kafka:9092
    topic inventory-events

constants
    REORDER_POINT int = 100
    REORDER_QUANTITY int = 500

workflow

Start

-> inventory.CheckLevels

-> if inventory.needs_reorder == true
    then
        parallel
            -> supplier.PlaceOrder
            -> warehouse.CheckTransfer
        join
        -> inventory.UpdateStock
        -> analytics.TrackReplenishment
    else
        -> Return {status: "adequate"}

-> End
```

## 7. Flash Sale Handling

### Flow DSL

```
version 1

flow FlashSale

service auth
    type grpc
    address auth:50051

service inventory
    type grpc
    address inventory:50051

service queue
    type redis
    connection redis:6379

service payment
    type http
    url https://payment.internal/charge

service notifications
    type kafka
    brokers kafka:9092
    topic flash-sale-events

constants
    MAX_CONCURRENT int = 100
    SALE_DURATION_MINS int = 30

retry
    attempts 3
    backoff fixed
    delay 100ms

timeout 5s

workflow

Start

-> auth.ValidateSession

-> queue.Enqueue

-> queue.WaitForTurn

-> inventory.ReserveItem

-> payment.Charge

-> if payment.status == "success"
    then
        -> inventory.ConfirmPurchase
        -> notifications.SendPurchaseConfirmation
        -> Return {status: "purchased"}
    else
        -> inventory.ReleaseReservation
        -> notifications.SendSoldOut
        -> Return {status: "failed"}

onError
    TimeoutError
        -> inventory.ReleaseReservation
        -> queue.MoveToBack
    Default
        -> inventory.ReleaseReservation
```

## 8. Multi-Currency & International

### Flow DSL

```
version 1

flow InternationalOrder

service auth
    type grpc
    address auth:50051

service currency
    type http
    url https://currency.internal/convert

service tax
    type http
    url https://tax.internal/international

service customs
    type http
    url https://customs.internal/calculate

service shipping
    type http
    url https://shipping.internal/international

service payment
    type http
    url https://payment.internal/charge-international

constants
    SUPPORTED_CURRENCIES string = "USD,EUR,GBP,JPY,AUD,CAD"

workflow

Start

-> auth.ValidateSession

-> currency.Convert

-> tax.CalculateInternational

-> customs.CalculateDuties

-> shipping.GetInternationalRates

-> payment.ChargeInternational

-> Return {order_id, total_in_local_currency, exchange_rate, duties, shipping}

onError
    Default
        -> Return {error: "unsupported_region"}
```

## 9. Loyalty Points System

### Flow DSL

```
version 1

flow LoyaltyPoints

service loyalty
    type grpc
    address loyalty:50051

service order
    type grpc
    address order:50051

service notifications
    type kafka
    brokers kafka:9092
    topic loyalty-events

constants
    POINTS_PER_DOLLAR int = 10
    BONUS_MULTIPLIER float = 1.5
    TIER_THRESHOLDS string = "bronze:0,silver:1000,gold:5000,platinum:10000"

variables
    points_earned int = 0
    new_tier string = ""

workflow

Start

-> order.GetOrderTotal

-> m:{points_earned: @.total * POINTS_PER_DOLLAR}

-> if user.is_vip == true
    then
        -> m:{points_earned: @.points_earned * BONUS_MULTIPLIER}

-> loyalty.AddPoints

-> loyalty.CheckTierUpgrade

-> if loyalty.tier_changed == true
    then
        -> notifications.SendTierUpgrade

-> notifications.SendPointsUpdate

-> Return {points_earned, total_points, tier}
```

## 10. Customer Support Ticket

### Flow DSL

```
version 1

flow SupportTicket

service ticket
    type grpc
    address ticket:50051

service auth
    type grpc
    address auth:50051

service ai-triage
    type http
    url https://ai.internal/triage

service assignment
    type grpc
    address assignment:50051

service notifications
    type kafka
    brokers kafka:9092
    topic support-events

constants
    PRIORITY_HIGH string = "high"
    PRIORITY_MEDIUM string = "medium"
    PRIORITY_LOW string = "low"

workflow

Start

-> auth.ValidateSession

-> ticket.Create

-> ai-triage.Analyze

-> if ai-triage.suggested_priority == "high"
    then
        -> assignment.AssignToSenior
        -> notifications.SendUrgentAlert
    else
        -> if ai-triage.suggested_priority == "medium"
            then
                -> assignment.AssignToAgent
            else
                -> assignment.AssignToQueue

-> notifications.SendTicketCreated

-> Return {ticket_id, assigned_to, priority}
```

## E-Commerce Edge Cases

### 1. Checkout Race Condition
Two concurrent checkouts for the last item:
```
# Both pass inventory check simultaneously
# Both try to charge payment
# One succeeds, one must rollback

# Solution: inventory.Reserve uses optimistic locking
# If reservation count < requested, fail immediately
```

### 2. Payment Timeout During Checkout
```
payment.Charge t30000 r2:exp
# If timeout: refund may not be needed (charge never completed)
# If timeout AFTER charge but BEFORE confirmation: need idempotency check
# Solution: payment service uses idempotency keys
```

### 3. Partial Inventory
```
# User wants 5 items, only 3 in stock
inventory.ReserveAll → returns {available: 3, requested: 5}
# Option 1: fail and notify
# Option 2: partial fulfillment
# Option 3: backorder
```

### 4. Currency Conversion Drift
```
# Price calculated at T1, payment charged at T2
# Exchange rate changed between T1 and T2
# Solution: lock rate at T1, use locked rate for payment
```

### 5. Webhook Replay Protection
```
# Stripe webhook received twice (at-least-once delivery)
# Solution: idempotency_key in event payload
# Check if order already processed before executing
```

### 6. Flash Sale Concurrency
```
# 1000 concurrent requests for 100 items
# Use Redis INCR for atomic counter
# If counter > 100: reject immediately
# No need to check inventory DB
```

### 7. Saga Compensation Order
```
# If fulfillment fails AFTER payment and inventory:
# 1. Compensate fulfillment (cancel shipment) — but shipment hasn't happened yet
# 2. Compensate payment (refund)
# 3. Compensate inventory (release)
# Order matters: compensate in REVERSE order of execution
```
