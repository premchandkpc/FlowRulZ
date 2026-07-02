---
title: Reliability
tags:
  - module
  - resilience
  - go
---

# Reliability

> [!info] Resilience patterns
> Path: `server/internal/reliability/`

Implements production-grade resilience: Dead Letter Queue, Saga compensation, Circuit Breaker, Dedup, and Rate Limiter.

## Sub-modules

### Dead Letter Queue (DLQ)

| Type | File | Purpose |
|------|------|---------|
| `DLQ` | `dlq.go` | Failed message store with retry limit |
| `Message` | `dlq.go` | Dead letter entry (original + error) |

> [!warning] DLQ overflow
> When a step exceeds `MaxRetries` (default 3), the message goes to DLQ instead of being dropped. DLQ entries can be replayed via admin API.

### Saga

| Type | File | Purpose |
|------|------|---------|
| `Saga` | `saga.go` | Compensation registry by step ID |
| `Step` | `saga.go` | Action + Compensator pair |

> [!info] Saga compensation
> Each step can register a compensator. If any step fails after earlier successes, compensators execute in reverse order.

### Circuit Breaker

| Type | File | Purpose |
|------|------|---------|
| `Breaker` | `circuit_breaker.go` | Per-service circuit state |
| `State` | `circuit_breaker.go` | Closed / Open / HalfOpen |

> [!info] Circuit transitions
> Closed → Open (after N failures) → HalfOpen (after timeout) → Closed (on success) or Open (on failure).

### Dedup Tracker

| Type | File | Purpose |
|------|------|---------|
| `Tracker` | `dedup.go` | In-memory (or file-backed) dedup window |
| `Config` | `dedup.go` | Window size, TTL |

> [!tip] Dedup window
> Default: 5-minute TTL, 10,000 entries max. Purged periodically.

### Rate Limiter

| Type | File | Purpose |
|------|------|---------|
| `Limiter` | `ratelimit.go` | Token-bucket rate limiter |

> [!tip] Rate limit
> Applied before rule matching. Exceeded requests are dropped with `429 Too Many Requests`.

## Dependencies

- [[ExecState]] — persisted execution records for DLQ replay
- [[Node]] — hooks into executeAll pipeline
