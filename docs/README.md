# FlowRulZ Documentation

Event-driven DAG execution engine. DSL → bytecode → Raft cluster → Rust VM.

## DSL Reference

| Doc | Description |
|-----|-------------|
| [Pipeline DSL](dsl-syntax.md) | Compact single-line DSL — 20+ operations, expressions, schemas, DAGs |
| [Flow DSL](flow-dsl.md) | Block-based `.flow` files — services, workflows, if/switch/parallel/wait/loops, error handling, compensation |

## Use Cases

| Doc | Description |
|-----|-------------|
| [Flow Examples](flows.md) | 30 complete flow patterns — from simple service calls to production saga workflows |

## Architecture

| Doc | Description |
|-----|-------------|
| [Architecture Overview](architecture-overview.md) | System components, data flow, deployment |
| [VM Architecture](vm-architecture.md) | Rust VM internals — executor, memory, tracing |
| [Bytecode Format](bytecode-format.md) | Instruction encoding, opcodes, plan structure |
| [FFI API](ffi-api.md) | Go ↔ Rust bridge via CGo |
| [Cluster Model](cluster-model.md) | Raft consensus, plan distribution, membership |

## Operations

| Doc | Description |
|-----|-------------|
| [Admin HTTP API](admin-http.md) | Rule CRUD, validation, DLQ, health |
| [Ingress Pipeline](ingress-pipeline.md) | Message ingestion and routing |
| [Transport Factory](transport-factory.md) | Kafka, gRPC, in-memory transport |
| [Cache System](cache-system.md) | In-memory and Redis caching |
| [Memory Management](memory-management.md) | Arena allocator, string interning |
| [Replication Design](replication-design.md) | Plan replication across cluster |
| [Development Guide](development.md) | Building, testing, contributing |

## SDKs

| Language | Location |
|----------|----------|
| Go | `sdk/flow/` |
| Rust | `sdk/rust/` |
| Python | `sdk/python/` |
| Java | `sdk/java/` |
| JavaScript/TypeScript | `sdk/javascript/` |
