---
title: Simulator
tags:
  - architecture
  - go
aliases:
  - Virtual Enterprise Platform
  - Load Simulator
---

# Simulator — Virtual Enterprise Platform

The simulator (`simulator/`) generates load, tests scenarios, and demonstrates FlowRulZ runtime capabilities against a running cluster. It serves as both a **testing environment** and an **end-to-end playground** for exploring retries, timeouts, circuit breakers, DAG execution, parallel processing, and dynamic configuration.

> **Not just 4 services.** This is an entire company running on FlowRulZ — 40+ virtual business services, 8 simulator modes, 50+ built-in scenarios, and comprehensive execution plans that demonstrate every capability of the platform.

## Structure

```
simulator/
├── cmd/simulator/          # CLI entry point (--scenario, --mode, --interactive)
├── config/                 # SimConfig, ChaosConfig
├── dashboard/              # HTTP dashboard + admin API (live metrics, send/rules/services)
├── dispatcher/             # Hash-based message routing to nodes
├── execution/              # ExecutionContext, Plan, queues (ReadyQueue, WaitingQueue)
├── loadgen/                # Traffic generation by scenario (constant, burst, ramp-up)
├── metrics/                # Metrics collector (throughput, latency, error rates)
├── modes.go                # 8 simulator modes (simple, enterprise, chaos, etc.)
├── network/                # Simulated network (latency, drop, slow, duplicate)
├── scheduler/              # Per-node worker pool, PlanCache, executeContext/executeBridge
├── scenarios/              # 50+ built-in scenarios across 6 categories
│   ├── registry.go         # Scenario definitions (descriptive)
│   └── scenarios.go        # Executable scenarios (Apply/Setup functions)
├── services/               # 40+ mock services with configurable latency/failure
├── timeline/               # Event timeline store
├── simulator.go            # Simulator struct — orchestrates all components
├── client.go               # Client — Send, RegisterService, AddRule
├── handlers.go             # Admin HTTP handlers (registered on dashboard mux)
├── routes.go               # HTTP route definitions
└── client_test.go          # Client tests
```

## Usage

```bash
# List all scenarios
simulator --scenarios

# Run in enterprise mode with scenario
simulator --mode enterprise --scenario order-placement --dashboard

# Learning mode (every step animated)
simulator --mode learning --scenario retry-success

# Chaos mode
simulator --mode chaos --scenario crash-service

# Performance mode (10K TPS)
simulator --mode performance --scenario throughput-10k

# Multi-region mode
simulator --mode multi-region --scenario multi-region-failover

# Interactive mode (no auto-stop)
simulator --mode enterprise --scenario order-placement --interactive

# Custom load
simulator --scenario black-friday --rate 1000 --speed 3.0
```

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--mode` | `enterprise` | Simulator mode (simple, enterprise, chaos, performance, distributed, multi-region, interview, learning) |
| `--scenario` | `black-friday` | Scenario name |
| `--nodes` | 3 | Number of execution nodes |
| `--workers` | 4 | Workers per node |
| `--rate` | 0 | Requests per second (overrides scenario) |
| `--duration` | 0 | Test duration (overrides scenario) |
| `--speed` | 1.0 | Simulation speed multiplier |
| `--dashboard` | true | Enable web dashboard |
| `--dashboard-addr` | `:8081` | Dashboard listen address |
| `--drop` | false | Enable packet dropping |
| `--slow` | false | Enable slow network |
| `--interactive` | false | Interactive mode (no auto-stop) |

## Simulator Modes

### Mode 1: Simple
4 core services — order, payment, inventory, notification. Perfect for getting started.

### Mode 2: Enterprise
40+ services — full virtual company on FlowRulZ. Demonstrates the complete platform.

### Mode 3: Chaos
Everything failing — test FlowRulZ resilience. High failure rates across all services.

### Mode 4: Performance
10K concurrent users — push FlowRulZ limits. Optimized for high throughput.

### Mode 5: Distributed
3 clusters — cross-cluster plan distribution. Tests distributed consensus.

### Mode 6: Multi-Region
US + Europe + Asia — geo-distributed with latency. Tests failover and rebalancing.

### Mode 7: Interview
Shows FlowRulZ architecture — perfect for demoing. Step-by-step animations.

### Mode 8: Learning
Every step animated — learn how FlowRulZ works. Slow, deliberate execution.

## Scenarios (50+)

### Business (10)

| Scenario | Description | Mode |
|----------|-------------|------|
| order-placement | Complete order flow — order → payment → inventory → shipping → notification | enterprise |
| order-cancellation | Cancel order → release inventory → refund payment → notify customer | enterprise |
| refund-processing | Refund workflow with approval and audit trail | enterprise |
| bulk-order | High-volume bulk order processing with batch inventory reservation | enterprise |
| flash-sale | Flash sale with 10K concurrent requests hitting inventory | performance |
| subscription-renewal | Monthly subscription renewal — billing → payment → invoice → notification | enterprise |
| payment-failure | Payment fails after 3 retries — triggers compensation workflow | enterprise |
| inventory-shortage | Inventory unavailable — partial fulfillment with backorder | enterprise |
| customer-support-ticket | Customer creates support ticket — AI classifies, routes, resolves | enterprise |
| product-recommendation | Personalized recommendation flow — user profile → history → AI → catalog | enterprise |

### Reliability (8)

| Scenario | Description | Mode |
|----------|-------------|------|
| retry-success | Payment fails twice then succeeds on third attempt | enterprise |
| retry-exhausted | Payment fails all 3 retries — enters DLQ | enterprise |
| circuit-breaker-opens | Payment gateway fails repeatedly → circuit opens → fallback activated | enterprise |
| timeout-handling | Service exceeds configured timeout — context cancelled | enterprise |
| dlq-replay | Replay messages from Dead Letter Queue after fixing downstream | enterprise |
| compensation-workflow | Saga pattern — compensate on failure, rollback in reverse order | enterprise |
| cascading-failure | Database failure cascades to all dependent services | chaos |
| rate-limit-throttle | Rate limiter triggers at 1K TPS — requests queued | performance |

### Distributed Systems (8)

| Scenario | Description | Mode |
|----------|-------------|------|
| leader-election | Leader node dies → new leader elected → cluster continues | distributed |
| node-joins | New node joins cluster — plan redistribution | distributed |
| node-leaves | Node gracefully leaves cluster — work redistributed | distributed |
| network-partition | Network split — cluster partitions, heals, reconciles | distributed |
| split-brain | Split-brain simulation — two leaders, then resolution | distributed |
| partition-rebalancing | Key-space shard rebalancing across nodes | distributed |
| cross-cluster-plan-distribution | Deploy rule to one cluster — propagates to others | distributed |
| multi-region-failover | US region fails — traffic fails over to Europe | multi-region |

### Metadata (6)

| Scenario | Description | Mode |
|----------|-------------|------|
| live-policy-update | Update retry policy at runtime — no restart | enterprise |
| dto-schema-evolution | Add new field to PaymentRequest — backward compatible | enterprise |
| version-rollback | Deploy v2, then rollback to v1 | enterprise |
| canary-rollout | Canary deploy — 10% traffic to v2, then 100% | enterprise |
| blue-green-deployment | Blue-green deploy — instant cutover | enterprise |
| feature-flag-toggle | Toggle feature flag — enable/disable feature at runtime | enterprise |

### Performance (6)

| Scenario | Description | Mode |
|----------|-------------|------|
| throughput-1k | 1,000 TPS sustained load | performance |
| throughput-10k | 10,000 TPS sustained load | performance |
| throughput-100k | 100,000 TPS sustained load | performance |
| backpressure | Slow downstream triggers backpressure | performance |
| queue-saturation | Scheduler queue fills to capacity | performance |
| worker-starvation | All workers busy — new tasks starved | performance |

### Chaos (8)

| Scenario | Description | Mode |
|----------|-------------|------|
| kill-runtime | Kill FlowRulZ runtime process — observe recovery | chaos |
| crash-service | Crash a single service — observe retry and fallback | chaos |
| corrupt-metadata | Corrupt metadata — observe validation rejection | chaos |
| slow-downstream | Payment gateway becomes slow — timeout cascade | chaos |
| random-latency | Random latency spikes across all services | chaos |
| duplicate-messages | Duplicate messages — test idempotency | chaos |
| memory-leak | Simulate memory leak — observe GC and recovery | chaos |
| cpu-spike | CPU spike on runtime — observe scheduler behavior | chaos |

## Services (40+)

### Customer Domain (10)

| Service | Latency | Jitter | Failure Rate | Max Concurrent | Methods |
|---------|---------|--------|--------------|----------------|---------|
| customer | 5ms | 2ms | 0.5% | 100 | create, get, update, delete, search |
| address | 3ms | 1ms | 0.3% | 100 | create, update, delete, validate |
| profile | 4ms | 2ms | 0.4% | 80 | get, update, preferences |
| identity | 8ms | 3ms | 1% | 50 | verify, create, revoke |
| authentication | 10ms | 4ms | 1.5% | 60 | login, logout, refresh, mfa |
| authorization | 6ms | 2ms | 0.8% | 80 | check, grant, revoke, roles |
| wishlist | 4ms | 2ms | 0.5% | 60 | add, remove, list |
| review | 6ms | 3ms | 1% | 50 | create, update, delete, moderate |
| support | 8ms | 4ms | 2% | 30 | ticket, escalate, resolve |
| chat | 2ms | 1ms | 1% | 200 | send, history, typing |

### Catalog Domain (8)

| Service | Latency | Jitter | Failure Rate | Max Concurrent | Methods |
|---------|---------|--------|--------------|----------------|---------|
| catalog | 8ms | 3ms | 0.8% | 100 | search, get, browse, filter |
| search | 12ms | 5ms | 1.5% | 60 | query, suggest, autocomplete, index |
| recommendation | 15ms | 6ms | 2% | 40 | get, similar, trending, personalize |
| inventory | 8ms | 4ms | 1.5% | 100 | reserve, release, check, batch |
| pricing | 6ms | 3ms | 1% | 80 | calculate, discount, bulk, dynamic |
| promotion | 5ms | 2ms | 0.8% | 60 | apply, validate, stack |
| coupon | 4ms | 2ms | 0.5% | 80 | validate, redeem, create |
| tax | 10ms | 4ms | 1.2% | 50 | calculate, validate, jurisdiction |

### Order Domain (6)

| Service | Latency | Jitter | Failure Rate | Max Concurrent | Methods |
|---------|---------|--------|--------------|----------------|---------|
| order | 10ms | 4ms | 1.2% | 80 | create, cancel, status, history, update |
| payment | 40ms | 15ms | 3% | 30 | authorize, capture, refund, void, status |
| fraud | 20ms | 8ms | 2% | 40 | check, score, flag, review |
| invoice | 12ms | 4ms | 1% | 60 | generate, send, void, pdf |
| billing | 15ms | 5ms | 1.5% | 50 | charge, credit, statement, balance |
| refund | 25ms | 10ms | 2% | 30 | process, status, approve, reject |

### Shipping Domain (3)

| Service | Latency | Jitter | Failure Rate | Max Concurrent | Methods |
|---------|---------|--------|--------------|----------------|---------|
| shipping | 15ms | 6ms | 1.8% | 50 | schedule, track, cancel, rate |
| courier | 20ms | 8ms | 2.5% | 30 | dispatch, track, proof |
| warehouse | 12ms | 5ms | 1.5% | 40 | allocate, pick, pack, ship |

### Notification Domain (5)

| Service | Latency | Jitter | Failure Rate | Max Concurrent | Methods |
|---------|---------|--------|--------------|----------------|---------|
| notification | 3ms | 1ms | 0.5% | 500 | email, sms, push, webhook |
| email | 10ms | 4ms | 1% | 100 | send, template, track, bounce |
| sms | 8ms | 3ms | 1.2% | 200 | send, status, verify |
| push | 5ms | 2ms | 0.8% | 300 | send, badge, silent |
| webhook | 6ms | 3ms | 1% | 150 | deliver, retry, status |

### Analytics Domain (3)

| Service | Latency | Jitter | Failure Rate | Max Concurrent | Methods |
|---------|---------|--------|--------------|----------------|---------|
| analytics | 15ms | 6ms | 1.5% | 40 | track, query, aggregate, export |
| audit | 8ms | 3ms | 0.8% | 60 | log, query, export |
| reporting | 20ms | 8ms | 2% | 30 | generate, schedule, download |

### AI Domain (5)

| Service | Latency | Jitter | Failure Rate | Max Concurrent | Methods |
|---------|---------|--------|--------------|----------------|---------|
| ai | 50ms | 20ms | 3% | 20 | predict, classify, generate |
| llm | 100ms | 40ms | 5% | 10 | complete, chat, embed |
| ocr | 30ms | 12ms | 2% | 20 | extract, verify |
| image | 25ms | 10ms | 1.8% | 25 | resize, crop, analyze |
| translation | 20ms | 8ms | 1.5% | 30 | translate, detect, batch |

### Utility Domain (4)

| Service | Latency | Jitter | Failure Rate | Max Concurrent | Methods |
|---------|---------|--------|--------------|----------------|---------|
| currency | 5ms | 2ms | 0.8% | 80 | convert, rate, history |
| geo | 8ms | 3ms | 1% | 60 | geocode, reverse, distance, timezone |
| weather | 12ms | 5ms | 1.5% | 40 | current, forecast, alerts |
| document | 10ms | 4ms | 1.2% | 50 | create, render, sign, archive |

### Platform Services (5)

| Service | Latency | Jitter | Failure Rate | Max Concurrent | Methods |
|---------|---------|--------|--------------|----------------|---------|
| validate | 0ms | 0ms | 0% | 1000 | check |
| metadata-server | 2ms | 1ms | 0.1% | 20 | get, put, watch |
| metrics-server | 1ms | 500µs | 0.1% | 15 | record, query |
| trace-viewer | 3ms | 1ms | 0.1% | 10 | get, export |
| log-viewer | 2ms | 1ms | 0.1% | 25 | write, read |

### Infrastructure (7)

| Simulator | Latency | Jitter | Failure Rate | Max Concurrent | Methods |
|-----------|---------|--------|--------------|----------------|---------|
| database | 2ms | 1ms | 0.1% | 50 | query, execute, transaction |
| redis | 1ms | 500µs | 0.1% | 100 | get, set, del, ttl |
| kafka | 5ms | 2ms | 0.1% | 30 | publish, consume, offset |
| payment-gateway | 50ms | 20ms | 5% | 10 | authorize, capture, refund |
| email-provider | 10ms | 3ms | 1% | 100 | send, bounce, complaint |
| sms-gateway | 8ms | 2ms | 1% | 200 | send, status |
| warehouse-api | 12ms | 4ms | 2% | 30 | stock, allocate, ship |

## Execution Plans (25+)

### Core Flows

| Plan | DSL | Description |
|------|-----|-------------|
| order-flow-v1 | validate → inventory → fraud → payment → email → notification | Full order pipeline with fraud routing |
| payment-flow-v1 | validate → payment → loyalty | Payment with loyalty points |
| refund-flow-v1 | validate → payment → invoice | Refund processing |
| shipping-flow-v1 | validate → inventory → [stock<10 → payment] → notification | Conditional shipping |
| service-discovery | validate → inventory → payment | Dynamic service lookup |
| dead-letter-queue | validate → payment → [status=failed → email → notification] | Failure recovery with DLQ |

### Customer Domain

| Plan | Description |
|------|-------------|
| customer-registration | identity → authentication → profile → address → email |
| customer-login | authentication → authorization |
| support-ticket | ai → support → customer → email |

### Catalog Domain

| Plan | Description |
|------|-------------|
| product-search | search → catalog → inventory |
| recommendation | profile → ai → catalog → recommendation |
| price-calculation | pricing → promotion → coupon → tax |

### Order Domain

| Plan | Description |
|------|-------------|
| order-cancellation | order → [shipped → shipping] → inventory → payment → notification |
| refund-processing | fraud → refund → payment → inventory → notification → audit |
| subscription-renewal | billing → payment → invoice → notification → analytics |

### Shipping Domain

| Plan | Description |
|------|-------------|
| shipping-schedule | warehouse → courier → shipping → notification |
| warehouse-fulfillment | inventory → warehouse → shipping → notification |

### Notification Domain

| Plan | Description |
|------|-------------|
| notification-dispatch | [channel=email → email] / [channel=sms → sms] / [channel=push → push] / [channel=webhook → webhook] |

### Analytics Domain

| Plan | Description |
|------|-------------|
| analytics-aggregation | analytics → audit → reporting |

### AI Domain

| Plan | Description |
|------|-------------|
| fraud-detection | ai → fraud → [risk>0.8 → notification] |
| document-processing | ocr → ai → document |
| image-processing | image → ai → document |
| translation | translation → ai |

### Utility Domain

| Plan | Description |
|------|-------------|
| currency-conversion | currency → geo |
| geo-lookup | geo → weather |

### Complex Workflows

| Plan | Description |
|------|-------------|
| complete-order-workflow | customer → inventory → pricing → promotion → coupon → tax → fraud → payment → order → warehouse → shipping → courier → invoice → email → sms → notification → analytics |
| ecommerce-checkout | customer → cart → pricing → promotion → tax → shipping → payment → order → notification |
