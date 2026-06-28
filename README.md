# FlowRulZ

**Distributed programmable event runtime and message bus.**

One platform for publish/subscribe, request/reply, rule execution, and workflow orchestration. Owns the entire message lifecycle.

- **Four communication models**: Publish (async), Request (sync), Execute (rule), Stream (subscription)
- **Any payload**: JSON, Protobuf, Avro, MessagePack, binary — runtime is format-agnostic
- **One VM**: bytecode execution runtime drives all models — publish, request, and rule execution are the same engine with different bytecode
- **Services enrich context** instead of replacing it — build stateful workflows, not JSON pipelines
- **Kafka-backed** for durable storage, but FlowRulZ owns routing, execution, and reply handling

## Quick Start

```bash
make          # build Rust cdylib + Go binary
make test     # run all tests (Rust 111 + Go vet)
./flowrulz    # start node on :8080
```

## Client SDK

```go
client := flow.New(flow.Config{Address: "localhost:8080"})

// Fire-and-forget
client.Publish(ctx, "orders", orderPayload)

// Synchronous RPC
resp, err := client.Request(ctx, "payment", paymentReq, 5*time.Second)

// Rule execution (DSL pipeline)
result, err := client.Execute(ctx, "order-flow", order)

// Stream subscription
stream, err := client.Stream(ctx, "events", handler)
```

## Project

```
FlowRulZ/
├── rust/          # Core: DSL toolchain + bytecode VM + Event types + type system
├── go/            # SDK + Engine + Bridge + Admin + Transport
├── go/flow/       # Client SDK (Publish, Request, Execute, Stream)
├── docs/          # Specs: architecture, DSL syntax, bytecode format, VM
└── Makefile
```

Go module: `github.com/premchandkpc/FlowRulZ`
Rust crate: `flowrulz-core` (cdylib + rlib)
Binary: `flowrulz`

### Rust (Core)

| Module | Description |
|--------|-------------|
| `bytecode/event.rs` | `Event`, `Mode`, `EventMetadata` — universal message type |
| `bytecode/execution.rs` | `ExecutionContext` — event + body + variables + service outputs |
| `bytecode/plan.rs` | `ExecutionPlan` — compiled bytecode with schema, services, DAG tables |
| `bytecode/resolved_type.rs` | Type system with `Enum`, schema validation |
| `dsl/` | Lexer → Parser → Optimizer → Compiler with type checking |
| `executor/` | VM dispatching 23 opcodes, ExecutionRuntime for Chunk/Buffer |
| `ffi.rs` | C FFI: `flowrulz_compile`, `flowrulz_execute`, `flowrulz_get_spans` |

### Go (SDK + Data Plane)

| Module | Description |
|--------|-------------|
| `flow/` | Client SDK: `New()`, `Publish()`, `Request()`, `Execute()`, `Stream()` |
| `internal/engine/` | `Engine`: versioned plans, lane routing, persistence, `ExecuteAll()` |
| `internal/bridge/` | CGo bindings: `Compile()`, `Execute()`, `GetSpans()` |
| `internal/execnode/` | `ExecutionNode`: process wrapping engine + transport + admin lifecycle |
| `internal/admin/` | HTTP API: rule CRUD, validate, promote, lanes |
| `internal/transport/` | Kafka consumer/producer |

## Architecture

```
                    FlowRulZ Cluster

                  ┌──────────────────────┐
                  │    Control Plane      │
                  │  Registry · Compiler  │
                  │  Scheduler · Election  │
                  └───────┬──────────────┘
                          │ bytecode plans
                          ▼
                  ┌──────────────────────┐
                  │     Data Plane        │
                  │  Workers · Runtime    │
                  │  Router · Service Reg │
                  └───────┬──────────────┘
                          │
        ┌─────────────────┼──────────────────┐
        ▼                 ▼                  ▼
    Publish           Request            Execute
    (async)           (sync)           (rule/workflow)
```

## Key Features

- **Four communication models**: Publish, Request, Execute, Stream — single SDK
- **Payload-agnostic**: JSON, Protobuf, Avro, MessagePack, binary — schema-validated at runtime
- **ExecutionContext**: services enrich context (body + outputs + variables) instead of replacing a single JSON blob
- **23 opcodes**: Next, Parallel, Collect, Fallback, Gate, Split, Map, Emit, Drop, Buffer, Key, Retry, Pipe, Timeout, Async, Chunk, DAG, Jmp, Label, SvcArg, RetryData, JumpOffset, TypeGuard
- **Type system**: schema attachment with compile-time Gate/Map type checking and runtime `TypeGuard` validation
- **Complexity scoring**: automatic lane routing based on compile-time cost estimates
- **Versioned rules**: promote/rollback with active execution draining
- **Admin API**: rule CRUD, validate, promote, rollback, lanes — API key auth
- **Span tracing**: per-opcode lock-free ring buffer, drained via `flowrulz_get_spans`
- **Zero-alloc message path**: slab pool + bump arena + string interning
- **22 expression builtins**: uuid, now, epoch, lower, upper, trim, length, concat, base64, json, substring, replace, to_string, parse_int, parse_float, coalesce, default, contains, keys, merge, hash

## Communication Models

### Publish (Fire-and-forget)

```go
client.Publish(ctx, "orders", order)
```

```text
Producer → FlowRulZ → Persist → Ack → Workers execute later
```

### Request (Synchronous RPC)

```go
resp, err := client.Request(ctx, "payment", payment, 5*time.Second)
```

```text
Client → FlowRulZ → Route to Payment Worker → Wait → Response
```

### Execute (Rule + Workflow)

```go
result, err := client.Execute(ctx, "order-flow", order)
```

```text
Order → Compile Rule → Rust VM → Gate → Parallel → DAG → Emit → Result
```

### Hybrid Pipeline

One DSL rule can mix everything:

```dsl
schema:{!order_id:string,!amount:float}

t500 n:payment

a:analytics

p:inventory,fraud c

dag:{A:[B],C:[A]}

e:audit,email

reply
```

Execution:
```text
Request → Payment (wait) → Analytics (async) → Inventory + Fraud (parallel)
→ DAG → Publish Audit → Send Email → Return Response
```

## Event Model

```rust
Event {
    payload: T,    // opaque — any serialization
    headers,
    metadata {
        mode,      // Publish | Request | Reply | Stream | Workflow
        reply_to,
        correlation_id,
        content_type,
        schema_name,
        ...
    },
}
```

## Makefile

```bash
make all       # rust release + go binary
make test      # all rust (111) + go tests
make bench     # criterion benchmarks
make vet       # go vet
make clean     # cargo clean + remove binary
```
