---
title: Server
tags:
  - architecture
  - go
aliases:
  - Go Server
---

# Server

The Go server (`server/`) is the control plane of FlowRulZ. It handles cluster coordination, scheduling, transport, and lifecycle management.

## Directory Structure

```
server/
├── cmd/
│   ├── flowrulz/            # Main node binary
│   └── flowrulz-compiler/   # Standalone compiler service
├── internal/
│   ├── admin/               # Admin HTTP API
│   ├── bootstrap/           # DI composition root (NodeBuilder)
│   ├── cluster/             # Raft, gossip, node management
│   ├── common/              # Lifecycle registry, middleware
│   ├── compiler/            # Remote DSL compiler client
│   ├── engine/              # Rule engine (deploy, promote, etc.)
│   ├── execstate/           # Execution state FileStore
│   ├── flowengine/          # Flow orchestration (optional)
│   ├── membership/          # Leader lease, heartbeat eviction
│   ├── node/                # ProdNode (refactored, canonical)
│   ├── observability/       # OTel tracing, metrics
│   ├── partition/           # Partition management, rebalancing
│   ├── plandist/            # Plan distribution & ack protocol
│   ├── plugins/             # WASM plugin loader
│   ├── registry/            # Service registry
│   ├── reliability/         # DLQ, Saga, Circuit Breaker, Dedup
│   ├── replyrouter/         # Pending reply tracking
│   ├── scheduler/           # Priority lanes + work stealing
│   └── transport/           # Kafka, gRPC bus, cluster transport
├── pkg/                     # Public interfaces (Node, Scheduler, etc.)
└── bridge/                  # CGo FFI to Rust runtime
```

## Key Modules

- [[Node]] — main struct with DI, lifecycle, interface compliance
- [[Bootstrap]] — NodeBuilder wiring everything together
- [[Cluster]] — Raft-based cluster coordination
- [[Scheduler]] — priority lanes with work stealing
- [[Transport]] — Kafka, gRPC bus, cluster messaging
- [[Reliability]] — DLQ, Saga, Circuit Breaker, Dedup, Rate Limiter
- [[Engine]] — rule compilation, deployment, versioning
- [[PlanDist]] — plan distribution protocol
- [[Partition]] — shard management across nodes
- [[Membership]] — cluster membership and leader lease
- [[Registry]] — service discovery via HTTP heartbeat
- [[ExecState]] — execution persistence (FileStore)
