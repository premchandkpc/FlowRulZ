# Transport Factory

**Status:** Implemented. The `TransportFactory` provides a pluggable backend for message transport, replacing the previous hardcoded Kafka approach.

## Overview

The transport factory abstracts message production and consumption behind a single interface, allowing the system to switch between Kafka, cluster gRPC, and in-memory backends at startup.

```
┌─────────────────────────────────────────────┐
│            TransportFactory                 │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
│  │  Kafka   │  │ Cluster  │  │  Memory  │  │
│  │ (Sarama) │  │ (gRPC)   │  │ (in-proc)│  │
│  └──────────┘  └──────────┘  └──────────┘  │
│       │              │              │        │
│       └──────────────┴──────────────┘        │
│                      │                       │
│              MessageConsumer                 │
│              MessageProducer                 │
└─────────────────────────────────────────────┘
```

## Interfaces

```go
type MessageConsumer interface {
    Topic() string
    Start(ctx context.Context)
    Stop()
}

type MessageProducer interface {
    Send(ctx context.Context, key, value []byte) error
    Close()
}
```

Backend-agnostic. Every transport implements these two interfaces.

## Transport Kinds

| Kind | Constant | Description |
|---|---|---|
| Kafka | `KindKafka` | Sarama-backed Kafka producer/consumer |
| Cluster | `KindCluster` | gRPC peer-to-peer via Cluster Bus |
| Memory | `KindMemory` | In-process buffered channels |
| Noop | `KindNoop` | Discards all messages (fallback) |

## Selection Logic

At startup in `node/factory.go`:

```
1. Create factory with KindMemory as default
2. Always register memory backend
3. If FLOWRULZ_KAFKA_BROKERS is set:
   → Register Kafka backend
   → Set active = KindKafka
4. If no Kafka brokers:
   → Create ClusterNode
   → Register cluster backend via cluster.RegisterClusterTransport()
   → Set active = KindCluster
5. If neither available:
   → Stay at KindMemory
```

## Runtime Switching

The active kind can be changed at runtime via `SetKind(kind)`. Thread-safe via `sync.RWMutex`. If no factory is registered for the requested kind, falls back to noop (discards messages silently).

## Registration

### In-Memory (`transport/registry.go`)

```go
transport.RegisterMemory(factory)
```

Uses simple buffered channels. Useful for testing and single-node mode.

### Kafka (`transport/kafka/registry.go`)

```go
kafka.RegisterKafka(factory, kafka.RegistrationConfig{
    Brokers:     []string{"localhost:9092"},
    GroupID:     "flowrulz",
    AcksLevel:   "all",
    Idempotent:  true,
})
```

Returns early if brokers is empty — no error, no registration.

### Cluster (`cluster/transport_factory.go`)

```go
cluster.RegisterClusterTransport(factory, clusterNode)
```

Registers KindCluster factories that delegate to the ClusterNode's gRPC bus for pub/sub.

## Convenience Constructor

```go
factory := kafka.NewTransportFactoryFromConfig(cfg)
```

Always registers memory. Registers Kafka if brokers provided. Selects Kafka as active if available, otherwise memory.

## Usage in ProdNode

The `MessageRouter` receives the factory via its `TransportFactory` interface and creates all consumers through it:

```go
membersConsumer := r.factory.NewConsumer("_flowrulz_members", handleNodeDiscovery)
planConsumer     := r.factory.NewConsumer(plandist.DefaultPlanTopic, handlePlan)
ackConsumer      := r.factory.NewConsumer(plandist.DefaultAckTopic, handleAck)
partitionConsumer := r.factory.NewConsumer(partition.PartitionTopic, handlePartition)
inputConsumer    := r.factory.NewConsumer(r.topic, handler)
```

Producers are created for DLQ, plans, acks, and partitions through the same factory.

## Files

| File | Purpose |
|---|---|
| `transport/factory.go` | `TransportFactory` with kind-based switching |
| `transport/registry.go` | In-memory transport registration |
| `transport/kafka/registry.go` | Kafka transport registration |
| `cluster/transport_factory.go` | Cluster transport registration adapter |
| `node/cluster_adapter.go` | Cluster → TransportFactory bridge |
