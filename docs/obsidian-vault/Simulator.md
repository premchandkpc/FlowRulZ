---
title: Simulator
tags:
  - architecture
  - go
aliases:
  - Load Simulator
---

# Simulator

The simulator (`simulator/`) generates load, tests scenarios, and demonstrates FlowRulZ runtime capabilities against a running cluster. It serves as both a **testing environment** and an **end-to-end playground** for exploring retries, timeouts, circuit breakers, DAG execution, parallel processing, and dynamic configuration.

## Structure

```
simulator/
├── cmd/simulator/          # CLI entry point (--scenario, --interactive, --dashboard)
├── config/                 # SimConfig, ChaosConfig
├── dashboard/              # HTTP dashboard + admin API (live metrics, send/rules/services)
├── dispatcher/             # Hash-based message routing to nodes
├── execution/              # ExecutionContext, Plan, queues (ReadyQueue, WaitingQueue)
├── loadgen/                # Traffic generation by scenario (constant, burst, ramp-up)
├── metrics/                # Metrics collector (throughput, latency, error rates)
├── network/                # Simulated network (latency, drop, slow, duplicate)
├── scheduler/              # Per-node worker pool, PlanCache, executeContext/executeBridge
├── scenarios/              # 9 built-in scenarios
├── services/               # MockService with configurable latency/failure, ServiceRegistry
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

# Run with scenario
simulator --scenario order-processing --nodes 3 --workers 4 --dashboard

# Interactive mode (no auto-stop)
simulator --scenario order-processing --interactive

# Custom load
simulator --scenario black-friday --rate 1000 --speed 3.0
```

## Flags

| Flag | Default | Description |
|------|---------|-------------|
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

## Scenarios

### 1. Black Friday (`black-friday`)
High load spike, inventory slow, payment normal, email slow. Tests retry under pressure.

### 2. Payment Outage (`payment-outage`)
Payment service 100% failure. Demonstrates DLQ and compensation flows.

### 3. Spike Test (`spike-test`)
Burst traffic every 5 seconds. Tests surge handling and resource management.

### 4. Chaos Monkey (`chaos-monkey`)
Packet loss, slow network, high failure rates across all services. Tests resilience.

### 5. Ramp Up (`ramp-up`)
Gradually increase load from 0 to target over duration. Tests scaling behavior.

### 6. Order Routing (`order-routing`)
Gate-based conditional routing: small vs large orders. Demonstrates branching logic.

### 7. Order Processing (`order-processing`)
Full order processing with retries, timeouts, and parallel execution. Demonstrates the complete order lifecycle: validate → inventory → payment → shipping → notification.

### 8. Metadata Updates (`metadata-updates`)
Live metadata updates and rule deployment without restart. Demonstrates dynamic configuration.

### 9. Circuit Breaker (`circuit-breaker`)
Circuit breaker behavior with fallback execution. Payment service at 95% failure triggers circuit, falling back to notification.

## Services (16 total)

### Business Services (10)

| Service | Latency | Jitter | Failure Rate | Max Concurrent | Methods |
|---------|---------|--------|--------------|----------------|---------|
| validate | 0ms | 0ms | 0% | 1000 | check |
| order | 5ms | 2ms | 0.5% | 100 | create, cancel, status |
| payment | 40ms | 10ms | 3% | 20 | validate, authorize, capture, refund, failure, retry |
| inventory | 8ms | 4ms | 1% | 100 | reserve, release, lowstock, warehouse |
| shipping | 15ms | 5ms | 2% | 50 | schedule, track, cancel |
| notification | 3ms | 1ms | 0.5% | 500 | email, sms, push, webhook |
| fraud | 15ms | 5ms | 2% | 50 | check |
| loyalty | 10ms | 3ms | 1% | 100 | award, redeem |
| invoice | 12ms | 3ms | 1% | 80 | generate, send, pay |

### Infrastructure Simulators (7)

| Simulator | Latency | Jitter | Failure Rate | Max Concurrent | Methods |
|-----------|---------|--------|--------------|----------------|---------|
| database | 2ms | 1ms | 0.1% | 50 | query, execute, transaction |
| redis | 1ms | 500µs | 0.1% | 100 | get, set, del |
| kafka | 5ms | 2ms | 0.1% | 30 | publish, consume |
| payment-gateway | 50ms | 20ms | 5% | 10 | authorize, capture, refund |
| email-provider | 10ms | 3ms | 1% | 100 | send, bounce, complaint |
| sms-gateway | 8ms | 2ms | 1% | 200 | send, status |
| warehouse-api | 12ms | 4ms | 2% | 30 | stock, allocate, ship |

### Platform Services (4)

| Service | Latency | Jitter | Failure Rate | Max Concurrent | Methods |
|---------|---------|--------|--------------|----------------|---------|
| metadata-server | 2ms | 1ms | 0.1% | 20 | get, put, watch |
| metrics-server | 1ms | 500µs | 0.1% | 15 | record, query |
| trace-viewer | 3ms | 1ms | 0.1% | 10 | get, export |
| log-viewer | 2ms | 1ms | 0.1% | 25 | write, read |

## Execution Plans

| Plan | DSL | Description |
|------|-----|-------------|
| order-flow-v1 | validate → inventory → fraud → payment → email → notification | Full order pipeline with fraud routing |
| payment-flow-v1 | validate → payment → loyalty | Payment with loyalty points |
| refund-flow-v1 | validate → payment → invoice | Refund processing |
| shipping-flow-v1 | validate → inventory → [stock<10 → payment] → notification | Conditional shipping |
| service-discovery | validate → inventory → payment | Dynamic service lookup |
| dead-letter-queue | validate → payment → [status=failed → email → notification] | Failure recovery with DLQ |
