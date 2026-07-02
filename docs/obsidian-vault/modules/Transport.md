---
title: Transport
tags:
  - module
  - messaging
  - go
---

# Transport

> [!info] Pluggable messaging layer
> Path: `server/internal/transport/`

Provides message consume/produce patterns with three backends: Kafka, gRPC bus, or cluster transport.

## Backends

| Backend | Type | Use Case |
|---------|------|----------|
| `kafka` | Consumer/Producer | Production deployments with Kafka infra |
| `inproc` | gRPC bus | Embedded cluster communication |
| `cluster` | Cluster transport | Peer-to-peer raft/gossip |

## Interfaces

```go
type Consumer interface {
    Consume(ctx context.Context) (*Message, error)
    Close() error
}

type Producer interface {
    Produce(ctx context.Context, msg *Message) error
    Close() error
}
```

## Key Functions

- `MakeProducerFromCluster(deps)` — creates a producer from cluster config (refactored from execnode)  
- `MakeConsumerFromCluster(deps)` — creates a consumer from cluster config  

## Dependencies

- [[Registry]] — discover endpoints via heartbeat
- [[Cluster]] — node coordination
