# FlowRulZ Documentation

Event-driven DAG execution engine. DSL → bytecode → Raft cluster → Rust VM.

## Getting Started

| Doc | Description |
|-----|-------------|
| [DSL Syntax](dsl-syntax.md) | Bytecode DSL — compact pipeline syntax for the Rust VM |
| [Flow DSL](flow-dsl.md) | Block-structured workflow language with full control flow |

## Examples — By Feature

| Doc | Description |
|-----|-------------|
| [Basic Pipelines](examples/basic-pipelines.md) | Sequential service calls, request/reply, fire-and-forget |
| [Error Handling](examples/error-handling.md) | Fallbacks, DLQ, onError blocks, circuit breakers |
| [Parallel Processing](examples/parallel-processing.md) | Fan-out/fan-in, concurrent service calls, DAGs |
| [Conditional Logic](examples/conditional-logic.md) | Gates, if/else, switch/case routing |
| [Data Transformation](examples/data-transformation.md) | Map expressions, JMESPath, built-in functions |
| [DAG Execution](examples/dag-execution.md) | Directed acyclic graphs, dependency layers |
| [Schema Validation](examples/schema-validation.md) | Type guards, required fields, enums |
| [Retry & Resilience](examples/retry-resilience.md) | Exponential/linear/fixed retry, timeouts, circuit breakers |
| [Chunking & Buffering](examples/chunking-buffering.md) | Message batching, chunk splitting, buffer accumulation |
| [WASM Plugins](examples/wasm-plugins.md) | Custom logic via WebAssembly |
| [Scheduled Flows](examples/scheduled-flows.md) | Cron triggers, delayed execution, timers |
| [Advanced Patterns](examples/advanced-patterns.md) | Saga, choreography, CQRS, event sourcing, outbox |

## Examples — By Industry

| Doc | Description |
|-----|-------------|
| [E-Commerce](examples/ecommerce-complete.md) | Search, cart, checkout, tracking, refunds, flash sales |
| [Fintech & Payments](examples/fintech-payments.md) | Payments, fraud detection, wire transfers, KYC, trading |
| [IoT & Telemetry](examples/iot-telemetry.md) | Sensor ingestion, device mgmt, predictive maintenance |
| [ML Pipeline](examples/ml-pipeline.md) | Training, inference, A/B testing, feature engineering |
| [DevOps & CI/CD](examples/devops-cicd.md) | CI/CD, incident response, infra provisioning, chaos |
| [Real-Time Streaming](examples/realtime-streaming.md) | Event sourcing, CQRS, stream processing, DLQ replay |
| [Multi-Tenant SaaS](examples/multi-tenant-saas.md) | Tenant isolation, billing, provisioning, feature flags |
| [Gaming](examples/gaming-leaderboard.md) | Matchmaking, leaderboards, anti-cheat, in-game purchases |
| [Healthcare](examples/healthcare-compliance.md) | HIPAA, patient data, lab results, prescriptions |
| [Logistics](examples/logistics-supply-chain.md) | Order fulfillment, route optimization, delivery tracking |
| [Social Media](examples/social-media-feed.md) | Feed generation, content moderation, notifications |

## Examples — By Concern

| Doc | Description |
|-----|-------------|
| [Complex Workflows](examples/complex-workflows.md) | End-to-end production patterns combining all features |
| [Performance Tuning](examples/performance-tuning.md) | Benchmarks, optimization, profiling, capacity planning |
| [Testing Strategies](examples/testing-strategies.md) | Unit, integration, chaos, load, contract, fuzz testing |

## Architecture

| Doc | Description |
|-----|-------------|
| [Architecture Overview](architecture.md) | System components, data flow, deployment model |
| [VM Architecture](vm-architecture.md) | Rust VM internals — executor, memory, tracing |
| [Bytecode Format](bytecode-format.md) | Instruction encoding, opcodes, execution plan structure |
| [FFI API](ffi-api.md) | Go ↔ Rust bridge via CGo |
| [SDK Reference](sdk.md) | Go, Rust, Python, Java, JavaScript clients |
| [Cluster Model](cluster.md) | Raft consensus, plan distribution, membership |
| [Development Guide](development.md) | Building, testing, contributing |
