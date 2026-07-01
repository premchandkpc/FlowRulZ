# Kafka Transport (Legacy)

**Status:** Legacy fallback. FlowRulZ no longer requires Kafka. The default production transport is the gRPC-based **Cluster Bus** (`go/internal/cluster/`). Kafka remains available as a transport option when `FLOWRULZ_KAFKA_BROKERS` is explicitly set.

## Files

| File | Role |
|------|------|
| `go/internal/transport/kafka.go` | Sarama-backed Kafka consumer/producer |
| `go/internal/transport/transport.go` | `MessageConsumer`/`MessageProducer` interfaces |

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
