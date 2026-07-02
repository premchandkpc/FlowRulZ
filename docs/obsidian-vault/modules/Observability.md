---
title: Observability
tags:
  - module
  - otel
  - go
---

# Observability

> [!info] OpenTelemetry tracing & metrics
> Path: `server/internal/observability/`

## Key Types

| Type | File | Purpose |
|------|------|---------|
| `Tracer` | `tracer.go` | OTel span creation and export |
| `Metrics` | `metrics.go` | Counter, histogram, gauge primitives |

## Spans

Each `ExecuteAll` creates a root span. Each step execution (MAP, GATE, SERVICE_CALL) creates a child span. Metrics track latency, throughput, and error rates per rule.

## Export

- Traces: OTLP exporter to any OTel collector
- Metrics: Prometheus endpoint at `/metrics`

## Dependencies

- [[Node]] — instrumentation hooks in ExecuteAll and scheduler
