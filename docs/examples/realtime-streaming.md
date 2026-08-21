# Real-Time Streaming

Kafka pipelines, event sourcing, CQRS, real-time analytics.

## 1. Event Sourcing

### Flow DSL

```
version 1

flow EventSourcing

service event-store
    type grpc
    address eventstore:50051

service projection
    type grpc
    address projection:50051

service snapshot
    type grpc
    address snapshot:50051

service query
    type grpc
    address query:50051

service notifications
    type kafka
    brokers kafka:9092
    topic eventstore-events

constants
    SNAPSHOT_THRESHOLD int = 100

timeout 10s

workflow

Start

-> event-store.AppendEvent

-> projection.UpdateProjection

-> if event-store.event_count > SNAPSHOT_THRESHOLD
    then
        -> snapshot.CreateSnapshot

-> query.UpdateReadModel

-> notifications.BroadcastEvent

-> Return {event_id: event-store.last_event_id, version: event-store.version}
```

## 2. Stream Processing

### Bytecode DSL

```
b1000 | m:{batch: @, count: length(@)} | n:parse-events | p:enrich-events,filter-events | c | m:{enriched: @[0], filtered: @[1]} | n:aggregate | chunk:100:par n:store | e:stream-processed
```

### Flow DSL

```
version 1

flow StreamProcessing

service kafka-consumer
    type kafka
    brokers kafka1:9092,kafka2:9092
    topic raw-events

service parser
    type grpc
    address parser:50051

service enricher
    type grpc
    address enricher:50051

service filter
    type grpc
    address filter:50051

service aggregator
    type grpc
    address aggregator:50051

service store
    type postgres
    connection postgres://db:5432/analytics

service kafka-producer
    type kafka
    brokers kafka1:9092,kafka2:9092
    topic processed-events

timeout 30s

workflow

Start

-> kafka-consumer.ConsumeBatch

-> parser.ParseEvents

parallel
    -> enricher.Enrich
    -> filter.ApplyFilters
join

-> aggregator.AggregateWindow

-> store.StoreAggregates

-> kafka-producer.Publish

-> Return {processed: aggregator.count}
```

## 3. CQRS Command Side

### Flow DSL

```
version 1

flow CQRSCommand

service auth
    type grpc
    address auth:50051

service validator
    type grpc
    address validator:50051

service event-store
    type grpc
    address eventstore:50051

service mediator
    type grpc
    address mediator:50051

service notifications
    type kafka
    brokers kafka:9092
    topic command-events

timeout 5s

workflow

Start

-> auth.ValidateCommand

-> validator.ValidatePayload

-> event-store.AppendCommand

-> mediator.NotifyProjections

-> notifications.PublishEvent

-> Return {command_id: event-store.command_id, status: "accepted"}
```

## 4. Real-Time Analytics

### Flow DSL

```
version 1

flow RealTimeAnalytics

service stream
    type kafka
    brokers kafka:9092
    topic analytics-raw

service windowing
    type grpc
    address windowing:50051

service aggregation
    type grpc
    address aggregation:50051

service dashboard
    type http
    url https://dashboard.internal/stream

service alerting
    type grpc
    address alerting:50051

constants
    WINDOW_SIZE_SECONDS int = 60
    ALERT_THRESHOLD float = 1000.0

timeout 5s

workflow

Start

-> stream.Consume

-> windowing.AssignWindow

-> aggregation.AggregateWindow

-> if aggregation.value > ALERT_THRESHOLD
    then
        -> alerting.SendThresholdAlert

-> dashboard.PushUpdate

-> Return {window: windowing.window_id, value: aggregation.value}
```

## 5. Dead Letter Queue Replay

### Flow DSL

```
version 1

flow DLQReplay

service dlq
    type kafka
    brokers kafka:9092
    topic dead-letter-queue

service replay-engine
    type http
    url https://replay.internal/process
    method POST

service validation
    type grpc
    address validation:50051

service original-flow
    type grpc
    address original:50051

service notifications
    type kafka
    brokers kafka:9092
    topic replay-events

constants
    MAX_REPLAY_ATTEMPTS int = 3

timeout 60s

workflow

Start

-> dlq.FetchFailedEvents

-> foreach event in dlq.failed_events
    -> if event.attempt_count < MAX_REPLAY_ATTEMPTS
        then
            -> validation.ValidateEvent
            -> if validation.valid == true
                then
                    -> original-flow.ReplayEvent
                    -> if original-flow.success == true
                        then
                            -> dlq.MarkReplayed
                            -> notifications.SendReplaySuccess
                        else
                            -> dlq.IncrementAttempt
                    else
                        -> dlq.MarkInvalid
                        -> notifications.SendInvalidEvent

-> Return {replayed: dlq.replay_count, failed: dlq.fail_count}
```

## 6. Schema Registry

### Flow DSL

```
version 1

flow SchemaRegistry

service schema-store
    type grpc
    address schemas:50051

service compatibility
    type grpc
    address compatibility:50051

service notifications
    type kafka
    brokers kafka:9092
    topic schema-events

timeout 5s

workflow

Start

-> schema-store.RegisterSchema

-> compatibility.CheckCompatibility

-> if compatibility.compatible == true
    then
        -> schema-store.ActivateSchema
        -> notifications.SendSchemaRegistered
        -> Return {schema_id: schema-store.schema_id, status: "registered"}
    else
        -> notifications.SendIncompatibleSchema
        -> Return {error: "incompatible", breaking_changes: compatibility.changes}
```
