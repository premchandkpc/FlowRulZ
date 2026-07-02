# FlowRulZ

**Distributed execution runtime.** Pub/Sub, RPC, workflows, and rules are all execution plans running on the same VM.

- **One VM**: bytecode execution runtime drives all models — publish, request, rule execution, and workflows are the same engine with different bytecode
- **Any payload**: JSON, Protobuf, Avro, MessagePack, binary — runtime is format-agnostic
- **Services enrich context** instead of replacing it — build stateful workflows, not JSON pipelines
- **Kafka-backed** for durable storage, but FlowRulZ owns routing, execution, and reply handling

## Quick Start

```bash
make          # build Rust cdylib + Go binary
make test     # run all tests (Rust 401 + Go vet)
./flowrulz    # start node on :8080
```

## Client SDKs

All SDKs expose the same four operations: **Publish** (fire-and-forget), **Request** (sync RPC), **Execute** (rule), **Stream** (subscription).

| Language | Package | Source |
|----------|---------|--------|
| Go | `github.com/premchandkpc/FlowRulZ/sdk/flow` | `sdk/flow/` |
| Java | `com.flowrulz:flowrulz-sdk` | `sdk/java/` |
| Python | `flowrulz` | `sdk/python/` |
| JavaScript/TypeScript | `flowrulz` | `sdk/javascript/` |
| Rust | `flowrulz-sdk` | `sdk/rust/` |

```go
client := flow.New(flow.Config{Address: "localhost:8080"})

client.Publish(ctx, "orders", orderPayload)
resp, err := client.Request(ctx, "payment", paymentReq, 5*time.Second)
result, err := client.Execute(ctx, "order-flow", order)
stream, err := client.Stream(ctx, "events", handler)
```

## Project

```
FlowRulZ/
├── runtime/        # Rust bytecode VM + DSL compiler
├── server/         # Go control plane + data plane
├── sdk/            # Polyglot SDKs (Go, Java, Python, JS/TS, Rust)
├── docs/           # Specs + architecture + Obsidian vault
├── simulator/      # Load gen + scenario testing
└── Makefile
```

Go module: `github.com/premchandkpc/FlowRulZ/server`
Rust crate: `flowrulz-core` (cdylib + rlib)
Binary: `flowrulz`

### Rust — `runtime/`

| Module | Description |
|--------|-------------|
| `bytecode/event.rs` | `Event`, `Mode`, `EventMetadata` — universal message type |
| `bytecode/execution.rs` | `ExecutionContext` — event + body + variables + service outputs |
| `bytecode/plan.rs` | `ExecutionPlan` — compiled bytecode with schema, services, DAG tables |
| `bytecode/resolved_type.rs` | Type system with `Enum`, schema validation |
| `dsl/` | Lexer → Parser → Optimizer → Compiler with type checking |
| `executor/` | VM dispatching 23 opcodes, ExecutionRuntime for Chunk/Buffer |
| `ffi.rs` | C FFI: `flowrulz_compile`, `flowrulz_execute`, `flowrulz_get_spans` |

### Go — `server/`

| Module | Description |
|--------|-------------|
| `bridge/` | CGo bindings: `Compile()`, `Execute()`, `GetSpans()` |
| `cmd/flowrulz/` | Entry point using `bootstrap.NodeBuilder.WithDefaults()` |
| `internal/admin/` | HTTP API: rule CRUD, validate, promote, lanes |
| `internal/engine/` | `Engine`: versioned plans, lane routing, persistence |
| `internal/scheduler/` | Priority lanes (Fast/Normal/Heavy) + work stealing |
| `internal/cluster/` | gRPC p2p Cluster Bus + gossip membership |
| `internal/node/` | `ProdNode` — central struct wiring all modules |
| `internal/bootstrap/` | `NodeBuilder` — DI composition root |
| `internal/plandist/` | Plan distribution + ack protocol |
| `internal/partition/` | Key-space shard mgmt + rebalancing |
| `internal/membership/` | Gossip, leader lease, heartbeat eviction |
| `internal/execstate/` | FileStore — JSON execution records |
| `internal/reliability/` | DLQ, Saga, Circuit Breaker, Dedup, Rate Limiter |
| `internal/registry/` | Service registry via HTTP heartbeat |
| `internal/observability/` | OTel tracing, Prometheus metrics |
| `pkg/` | Public interfaces (for DI/testability) |

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
- **Documentation**: Obsidian vault at `docs/obsidian-vault/` — 26 notes, architecture map, cross-linked
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
make test      # all rust (401) + go tests (28 packages)
make bench     # criterion benchmarks
make vet       # go vet
make clean     # cargo clean + remove binary
```
