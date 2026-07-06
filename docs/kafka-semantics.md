# Kafka Transport (Legacy)

**Status:** Legacy fallback. FlowRulZ no longer requires Kafka. The default production transport is the gRPC-based **Cluster Bus** (`server/internal/cluster/`). Kafka remains available as a transport option when `FLOWRULZ_KAFKA_BROKERS` is explicitly set.

## Transport Factory

All transport backends are managed through the `TransportFactory` (`server/internal/transport/factory.go`). The factory selects the active backend at startup:

- **Kafka** (`KindKafka`): active when `FLOWRULZ_KAFKA_BROKERS` is set
- **Cluster** (`KindCluster`): active when no Kafka brokers configured (default)
- **Memory** (`KindMemory`): in-process fallback for testing
- **Noop** (`KindNoop`): discards all messages (fallback if no backend registered)

The factory can be switched at runtime via `SetKind()`. See `docs/transport-factory.md` for full details.

## Files

| File | Role |
|------|------|
| `server/internal/transport/factory.go` | `TransportFactory` with kind-based switching |
| `server/internal/transport/registry.go` | In-memory transport registration |
| `server/internal/transport/kafka/` | Sarama-backed Kafka consumer/producer (3 files) |
| `server/internal/transport/kafka/registry.go` | Kafka transport registration into factory |
| `server/internal/cluster/transport_factory.go` | Cluster transport registration into factory |
| `server/internal/transport/` | `MessageConsumer`/`MessageProducer` interfaces |

> **Note:** The following topics are also managed over the Cluster Bus in non-legacy mode:
> `_flowrulz_gossip` (gossip protocol push/pull) and `_flowrulz_partitions` (partition assignment).

## Internal Topics (Legacy)

| Topic | Retention | Description |
|-------|-----------|-------------|
| `_flowrulz_members` | Compacted | Cluster membership + heartbeats |
| `_flowrulz_plans` | Compacted | Compiled plans + activation commands |
| `_flowrulz_acks` | 1 hour | ACK records from followers |
| `_flowrulz_replies` | 1 hour | Cross-node reply routing |
| `_flowrulz_dlq` | Compacted | Dead-letter entries |

These are now managed over the Cluster Bus instead of Kafka, unless running in legacy mode.
