# Logistics & Supply Chain

Order tracking, routing, warehouse management, delivery optimization.

## 1. Order Fulfillment

### Flow DSL

```
version 1

flow OrderFulfillment

service order
    type grpc
    address order:50051

service warehouse
    type grpc
    address warehouse:50051

service picking
    type grpc
    address picking:50051

service packing
    type grpc
    address packing:50051

service shipping
    type http
    url https://shipping.internal/ship
    method POST

service notifications
    type kafka
    brokers kafka:9092
    topic fulfillment-events

timeout 30s

workflow

Start

-> order.GetOrder

-> warehouse.FindInventory

-> picking.PickItems

-> packing.PackOrder

-> shipping.CreateShipment

-> notifications.SendShippingConfirmation

-> Return {tracking_number: shipping.tracking, carrier: shipping.carrier}
```

## 2. Route Optimization

### Flow DSL

```
version 1

flow RouteOptimization

service delivery-requests
    type grpc
    address requests:50051

service traffic
    type http
    url https://traffic.internal/current

service optimizer
    type http
    url https://optimizer.internal/calculate
    method POST

service fleet
    type grpc
    address fleet:50051

service notifications
    type kafka
    brokers kafka:9092
    topic routing-events

constants
    MAX_STOPS_PER_ROUTE int = 20

timeout 15s

workflow

Start

-> delivery-requests.GetPending

-> traffic.GetCurrentConditions

-> optimizer.OptimizeRoutes

-> foreach route in optimizer.routes
    -> fleet.AssignRoute
    -> notifications.SendRouteToDriver

-> Return {routes: optimizer.route_count, total_stops: optimizer.total_stops}
```

## 3. Warehouse Management

### Flow DSL

```
version 1

flow WarehouseManagement

service inventory
    type grpc
    address inventory:50051

service receiving
    type grpc
    address receiving:50051

service putaway
    type grpc
    address putaway:50051

service cycle-count
    type grpc
    address cyclecount:50051

service notifications
    type kafka
    brokers kafka:9092
    topic warehouse-events

timeout 10s

workflow

Start

-> receiving.ReceiveShipment

-> inventory.UpdateStock

-> putaway.AssignLocation

-> if inventory.needs_cycle_count == true
    then
        -> cycle-count.ScheduleCount

-> notifications.SendStockUpdate

-> Return {received: receiving.item_count, location: putaway.location}
```

## 4. Delivery Tracking

### Bytecode DSL

```
schema:{!tracking_number:string} | n:get-shipment | p:carrier-status,warehouse-status,customs-status | c | m:{shipment: @.results[0], carrier: @.results[1], warehouse: @.results[2], customs: @.results[3]} | n:calculate-eta | e:tracking-updated
```

### Flow DSL

```
version 1

flow DeliveryTracking

service shipment
    type grpc
    address shipment:50051

service carrier
    type http
    url https://carrier.internal/track

service warehouse
    type grpc
    address warehouse:50051

service customs
    type http
    url https://customs.internal/status

service eta
    type grpc
    address eta:50051

service notifications
    type kafka
    brokers kafka:9092
    topic tracking-events

timeout 10s

workflow

Start

parallel
    -> shipment.GetDetails
    -> carrier.GetStatus
    -> warehouse.GetStatus
    -> customs.GetStatus
join

-> eta.Calculate

-> if carrier.status == "out_for_delivery"
    then
        -> notifications.SendOutForDelivery

-> if carrier.status == "delivered"
    then
        -> notifications.SendDelivered

-> Return {tracking, status: carrier.status, eta: eta.estimated}
```

## 5. Returns Processing

### Flow DSL

```
version 1

flow ReturnsProcessing

service returns
    type grpc
    address returns:50051

service inventory
    type grpc
    address inventory:50051

service refund
    type http
    url https://payment.internal/refund

service quality
    type grpc
    address quality:50051

service notifications
    type kafka
    brokers kafka:9092
    topic returns-events

timeout 30s

workflow

Start

-> returns.ProcessReturn

-> inventory.ReceiveReturn

-> quality.Inspect

-> if quality.condition == "resellable"
    then
        -> inventory.Restock
        -> refund.ProcessFullRefund
    else
        -> if quality.condition == "refurbishable"
            then
                -> inventory.SendToRefurbish
                -> refund.ProcessPartialRefund
            else
                -> inventory.MarkForDisposal
                -> refund.ProcessFullRefund

-> notifications.SendRefundConfirmation

-> Return {return_id: returns.id, refund_amount: refund.amount, disposition: quality.condition}
```
