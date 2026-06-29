# Kafka Semantics Specification

**Status:** Kafka is the durable event log for FlowRulZ. FlowRulZ is a consumer group with programmable execution. The Control Plane manages rules; Data Plane nodes consume from Kafka, execute rules, and produce results. Kafka topics also serve as the coordination backbone for cluster membership, plan distribution, and cross-node replies.

## Architecture

```
                    FlowRulZ Cluster

         ┌──────────────────────────────┐
         │       Control Plane          │
         │  Rule Registry + Compiler    │
         │  Leader Election             │
         └──────────┬───────────────────┘
                    │ distribute plans via _flowrulz_plans
                    ▼
         ┌──────────────────────────────┐
         │        Data Plane            │
         │  Partition Workers           │
         │  ExecutionRuntime            │
         │  Service Callers             │
         │  Service Registry            │
         │  Reply Router                │
         └──────────┬───────────────────┘
                    │ consume / produce
                    ▼
              Kafka Cluster
         ┌──────────────────────────────┐
         │  User Topics                 │
         │  _flowrulz_members (compact) │
         │  _flowrulz_plans (compact)   │
         │  _flowrulz_acks (1hr TTL)    │
         │  _flowrulz_replies (1hr TTL) │
         └──────────────────────────────┘
```

FlowRulZ does NOT implement its own storage, WAL, or replication. Kafka handles durability. FlowRulZ handles routing, execution, and reply handling.

## Internal Topics

| Topic | Partitions | Retention | Description |
|-------|-----------|-----------|-------------|
| `_flowrulz_members` | 1 | Compacted | Cluster membership + heartbeats. Key = node_id |
| `_flowrulz_plans` | 1 | Compacted | Compiled plans + activation commands. Key = rule_id |
| `_flowrulz_acks` | 1 | 1 hour | ACK records from followers. Key = "rule_id:version" |
| `_flowrulz_replies` | N | 1 hour | Cross-node reply routing. Key = correlation_id |
| `_flowrulz_dlq` | 1 | Compacted | Dead-letter entries. Key = entry_id |

These topics are not exposed to client applications.

## Consumer Groups

```
flowrulz
└── consumer-group (per lane: fast / normal / heavy)
    ├── partition-0 ── ExecutionNode worker-0 ── Runtime
    ├── partition-1 ── ExecutionNode worker-1 ── Runtime
    └── partition-2 ── ExecutionNode worker-2 ── Runtime
```

- One consumer group per deployment
- Each partition assigned to exactly one worker goroutine
- Workers are long-lived (no per-message goroutine churn)
- Rebalance listener pauses/resumes partition workers

### Lane Routing

Rules are assigned to lanes based on `complexity_score` at deploy time:

| Score | Lane | BatchSize | PollTimeout | Scheduler Concurrency | Queue Size |
|-------|------|-----------|-------------|----------------------|------------|
| < 10  | fast | 500       | 10ms        | 50                   | 5000       |
| ≤ 50  | normal | 100     | 50ms         | 20                   | 2000       |
| > 50  | heavy | 10       | 500ms        | 5                    | 500        |

Lane assignment is done in `engine.Deploy()` via `flowrulz_plan_complexity` FFI.
Runtime queue management is handled by `go/internal/scheduler/`.

## Offset Commit

### Commit Strategies

| Mode | Behavior |
|------|----------|
| `at-least-once` | Commit after VM execution succeeds and outputs are produced |
| `manual` | User controls commit via admin API |

### Commit Timing

```
Message Received → Schedule (scheduler) → Create ExecutionContext → Execute Rule → Produce Output → Commit Offset
```

The scheduler (`go/internal/scheduler/`) introduces queueing between message receipt and execution.

## Batch Poll

```
Consumer.PollBatch(N) → ScheduleBatch → ExecuteBatch (Rust FFI N times)
```

Batch size configurable per lane. Backpressure: scheduler rejects on full for heavy lane.

## Exactly-Once & Idempotency

### Producer Idempotency

- Enable `enable.idempotence=true` on Kafka producer
- On retry, same sequence number → broker dedup

## Dead Letter Queue (DLQ)

DLQ entries are durably written to `_flowrulz_dlq` when a `transport.MessageProducer` is configured via `WithDLQProducer()`. The in-memory slice is a read cache for the admin API. On restart, `LoadFromTopic()` rebuilds the cache from the compacted topic. See `go/internal/reliability/dlq.go`.

### Poison Message Handling

```
Scheduler → RateLimiter → Engine → VM → Error
    ↓
DLQ.Send(entry{body, error, rule_id})
    │
    └── Admin API: POST /dlq/replay/{id}
    └── Admin API: POST /dlq/replay        (replay all)
    └── Admin API: DELETE /dlq             (clear)
```

### DLQ Entry Format

```go
type DeadLetterEntry struct {
    ID          string    // unique failure ID
    RuleID      string    // rule that failed
    Topic       string    // source topic
    Partition   int32     // source partition
    Offset      int64     // source offset
    Body        []byte    // original payload
    Error       string    // error message
    FailedAt    time.Time // when the failure occurred
    RetryCount  int       // how many retry attempts
}
```

Max entries defaults to 10,000 (configurable). Oldest entry is evicted when full.

## Backpressure

| Level | Mechanism | Trigger |
|-------|-----------|---------|
| Scheduler queue | Block send / reject on full | Queue at capacity (default: 500 heavy) |
| Rate limiter | Token bucket deny | Rate exceeded (configurable per name) |
| Kafka consumer | `pause()` partition | Memory threshold exceeded |

## Request / Reply Routing

Synchronous `Request()` calls produce a correlation_id and return a channel. The reply is routed back through the `_flowrulz_replies` topic keyed by `hash(correlation_id)`. The `ReplyRouter` (`go/internal/replyrouter/`) tracks pending requests and delivers responses.

```
Client → Request("payment", payload, timeout)
    │
    ├── generate correlation_id
    ├── register PendingRequest in ReplyRouter
    ├── publish event to Kafka (Mode=Request, corr_id in header)
    │
    ▼
Worker receives event → executes VM → produces reply
    │
    ├── reply published to _flowrulz_replies (key = correlation_id)
    │
    ▼
ReplyRouter consumes reply → matches corr_id → delivers to waiting channel
    │
    ▼
Client receives response
```

## Ordering & Partition Affinity

- Messages from the same partition processed sequentially by the same worker
- No reordering within a partition
- Output produced with same key as input to preserve partition affinity
- Event id and correlation_id propagate through the system for tracing
- Reply topic partitions: `hash(correlation_id) % N` ensures reply lands on origin node's partition

## Plan Distribution

```
Leader compiles rule → publishes PlanMessage{type:"plan", rule_id, version, plan, dsl}
    to _flowrulz_plans

Follower consumes plan → stores locally (inactive) → publishes AckMessage to _flowrulz_acks

Leader waits for ACK quorum (default=majority ⌊N/2⌋+1) → publishes PlanMessage{type:"activate"}

Follower receives activation → marks version active → begins executing new version
```

## Files Involved

| File | Role |
|------|------|
| `go/internal/transport/` | Kafka consumer/producer interfaces, in-memory + Kafka stubs |
| `go/internal/execnode/execnode.go` | Wires consumers, scheduler, DLQ, rate limiter, admin |
| `go/internal/engine/engine.go` | VersionedPlan store, ExecuteAll |
| `go/internal/scheduler/` | Priority queue lanes, concurrency limits |
| `go/internal/plandist/` | Plan/ACK message types, WaitForAcks quorum |
| `go/internal/replyrouter/` | Pending request tracking, timeout cleanup |
| `go/internal/reliability/dlq.go` | Dead-letter queue with replay |
| `go/internal/reliability/ratelimit.go` | Token bucket rate limiter |

## Health Checks

```
GET /health → {"status":"ok"}
```

Service Registry health checks: `go/internal/registry/` maps service names to healthy endpoints with passive (heartbeat) and active (periodic probe) health checking.

## Admin API

See `docs/specs/flow-architecture.md` for Admin API details including DLQ and metrics endpoints.
