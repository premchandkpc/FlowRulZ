# Flow Architecture

## Distributed Event Runtime

FlowRulZ is a **distributed programmable event runtime and message bus**. It owns the entire message lifecycle — send, receive, route, execute, reply — within a single platform.

```
                    FlowRulZ Cluster

                  ┌─────────────────────────────┐
                  │         Control Plane        │
                  │  Rule Registry | Compiler    │
                  │  Scheduler | Leader Election  │
                  └──────────┬──────────────────┘
                             │ bytecode plans
                             ▼
                  ┌─────────────────────────────┐
                  │          Data Plane          │
                  │  Partition Workers | Runtime │
                  │  Service Registry | Router   │
                  └──────────┬──────────────────┘
                             │
        ┌────────────────────┼────────────────────┐
        ▼                    ▼                    ▼
    Publish             Request              Execute
   (async)              (sync)              (rule/workflow)
```

### Core Idea

Every operation — publish, request, execute, workflow — becomes a **bytecode execution plan**. The same VM that runs `n:payment > g:amount>1000` also runs `publish` and `request`. The difference is only which bytecode gets emitted.

---

## Event Model

The universal data type is `Event<T>`:

```rust
Event<T> {
    payload: T,                    // opaque — any serialization format
    headers: HashMap<String, Value>,
    metadata: EventMetadata,
}
```

Payload is opaque to the runtime. The VM never cares whether it came from JSON, Protobuf, Avro, MessagePack, or binary. Schema validation and field access happen against a **Schema Registry**, not against a serialization format.

### ExecutionContext

Every in-flight execution carries a context that services enrich:

```rust
ExecutionContext {
    event: Event,                  // original event
    body: Vec<u8>,                 // current working payload
    variables: Map<String, Value>, // intermediate state
    outputs: Map<String, Value>,   // service call results
    headers: Map<String, String>,  // mutable headers
    failed: bool,
    hop_count: u16,
    retry_count: u32,
}
```

When a service runs:
```text
Order Event
    │
    ▼
Fraud Service
    │
    ├── context.outputs["fraud"] = FraudResponse
    ├── context.body = updated body
    │
    ▼
Inventory Service
    │
    ├── context.outputs["inventory"] = InventoryResponse
    └── context.body = merged context
```

Services enrich the context rather than overwrite it.

---

## Communication Models

### 1. Fire-and-forget (Publish)

```go
client.Publish("orders", order)
```

```text
Producer → FlowRulZ → Persist → Ack immediately → Workers execute later
```

No response expected. Mode: `Mode::Publish`

### 2. Request / Reply (Sync)

```go
resp, err := client.Request("payment", paymentReq, 5*time.Second)
```

```text
Client → FlowRulZ → Route to Payment Worker → Wait → Response → Client
```

Mode: `Mode::Request`. FlowRulZ tracks reply_to + correlation_id.

### 3. Rule Execution

```go
result, err := client.Execute("order-flow", order)
```

```text
Order → Compile Rule → Rust VM → Gate / Parallel / DAG / Emit → Result
```

Mode: `Mode::Workflow`. Full DSL pipeline execution.

### 4. Streaming

```go
stream, err := client.Stream("events", handler)
```

```text
Client → FlowRulZ → Persistent subscription → Event stream
```

Mode: `Mode::Stream`.

---

## Message Flow

```
Client
    │
    │  Event { payload, headers, metadata { mode, reply_to, ... } }
    ▼
┌─────────────────────────┐
│  Connection Handler     │  Accepts connections, auth
│  (Go)                   │
└──────────┬──────────────┘
           │
           ▼
┌─────────────────────────┐
│  Router / Partitioner   │  Determine topic, partition, owning node
│  (Go)                   │
└──────────┬──────────────┘
           │
           ▼
┌─────────────────────────┐
│  Message Store (Kafka)  │  Durable log, replication
│  (optional per mode)    │
└──────────┬──────────────┘
           │
           ▼
┌─────────────────────────┐
│  ExecutionRuntime       │  Event → ExecutionPlan → VM → Output
│  (Rust)                 │
│                         │
│  ┌───────────────────┐  │
│  │  Bytecode VM      │  │  Dispatches Next/Parallel/DAG/Gate/Emit
│  │  (ExecutionContext)│  │  Services enrich context, not replace it
│  └───────────────────┘  │
└──────────┬──────────────┘
           │
           ▼
┌─────────────────────────┐
│  Reply Router           │  Mode::Request → route response to caller
│  Mode::Publish → done   │
│  Mode::Workflow → result│
└─────────────────────────┘
```

### Execution Path Detail

```
Producer
    │
    ▼
flow.Publish("orders", order)
    │
    ├── Serialize order (Protobuf, JSON, Avro, ...)
    ├── Create Event { payload, headers, mode: Publish, topic: "orders" }
    │
    ▼
FlowRulZ Node
    │
    ├── Persist to Kafka topic "orders"
    ├── Ack producer (fire-and-forget)
    │
    ▼
Partition Worker
    │
    ├── Dequeue event from Kafka
    ├── Load matching ExecutionPlans for topic "orders"
    │
    ▼
ExecutionRuntime.execute(plan, event)
    │
    ├── Create ExecutionContext { event, body, variables, ... }
    │
    ▼
VM.run()
    │
    ├── dispatch(Gate)    → field lookup against schema
    ├── dispatch(Next)    → call service, store in context.outputs
    ├── dispatch(Parallel) → fan-out to multiple services
    ├── dispatch(DAG)     → topological execution with parent merging
    ├── dispatch(Emit)    → publish result to output topic
    │
    ▼
Result emitted to configured output (Kafka topic, reply channel, etc.)
```

---

## 1. Rule Deployment Flow

**Scenario:** Admin deploys a new rule via HTTP POST to the Control Plane.

```
Client                  Control Plane                Data Plane
  │                           │                          │
  │  POST /rules              │                          │
  │  {"id":"order-flow",      │                          │
  │   "dsl":"n:validate"}     │                          │
  │──────────────────────────►│                          │
  │                           │  compile DSL             │
  │                           │  validate against schema │
  │                           │  persist (atomic write)  │
  │  ◄── 201 Created ────────┤                          │
  │                           │                          │
  │                           │  distribute plan to      │
  │                           │  active execution nodes  │
  │                           │─────────────────────────►│
  │                           │                          │
  │                           │                          │  load into ExecutionRuntime
```

### Files Involved

| Step | File | What It Does |
|------|------|-------------|
| HTTP handler | `go/internal/admin/server.go` | Parses JSON, calls `engine.Deploy(id, dsl)` |
| Engine | `go/internal/engine/engine.go` | `Deploy()`: compile, assign lane, persist |
| Bridge | `go/internal/bridge/bridge.go` | `Compile()`: C FFI call to `flowrulz_compile()` |
| Rust FFI | `rust/src/ffi.rs` | `flowrulz_compile()`: lex → parse → optimize → compile |
| DSL Compiler | `rust/src/dsl/compiler.rs` | Compiles AST → `ExecutionPlan`, type checking |
| Persistence | `go/internal/engine/engine.go` | `saveRules()`: atomic `.tmp` → `os.Rename` |

---

## 2. Message Execution Flow

**Scenario:** An Event arrives at a Data Plane node and runs through deployed rules.

```
Kafka      Partition Worker         Engine              ExecutionRuntime        VM
  │               │                    │                      │                │
  │  Event        │                    │                      │                │
  │──────────────►│                    │                      │                │
  │               │  ExecuteAll(event) │                      │                │
  │               │───────────────────►│                      │                │
  │               │                    │  for each plan:      │                │
  │               │                    │    runtime.execute() │                │
  │               │                    │─────────────────────►│                │
  │               │                    │                      │  run(context)  │
  │               │                    │                      │───────────────►│
  │               │                    │                      │  dispatch...   │
  │               │                    │◄── callback ────────┤                │
  │               │                    │                      │                │
  │               │                    │  ◄── result ────────┤                │
  │               │  ◄── result ──────┤                      │                │
```

### Files Involved

| File | What It Does |
|------|-------------|
| `go/internal/transport/` | Kafka consumer, invokes handler with event bytes |
| `go/internal/execnode/` | ExecutionNode: engine + transport + admin lifecycle |
| `go/internal/engine/` | `ExecuteAll()`: collect active plans, bridge execute |
| `rust/src/ffi.rs` | `flowrulz_execute()`: deserialize plan, create VM with ExecutionContext |
| `rust/src/executor/mod.rs` | VM dispatch loop, opcode handlers |
| `rust/src/executor/runtime.rs` | ExecutionRuntime: Chunk/Buffer orchestration |
| `rust/src/executor/context.rs` | ExecutionContext: event + body + variables + outputs |

---

## 3. Service Call Flow (VM → Go)

**Scenario:** VM hits a Next instruction, calls a Go service, stores result in context.

```
VM.dispatch(OpCode::Next)
    │
    ▼
exec_next(ctx.body, instr, plan, caller)
    │
    ├── Get service name from plan.services[instr.a]
    │
    ▼
caller(svc_id, body, timeout)  → FFI → Go callback
    │
    ▼
ServiceCaller returns ([]byte, error)
    │
    ├── On success:
    │   ├── ctx.body = response
    │   ├── ctx.outputs["svc_name"] = response
    │   └── ctx.hop_count += 1
    │
    └── On failure:
        ├── ctx.failed = true
        └── ctx.errors.push(e)
```

### Files Involved

| File | Layer | Role |
|------|-------|------|
| `rust/src/executor/next.rs` | Rust | `exec_next()`: service call with retry logic |
| `rust/src/ffi.rs` | Rust | Closure calling `caller_cb` function pointer |
| `go/internal/bridge/caller_bridge.c` | C | Static C function forwarding to `goServiceCaller` |
| `go/internal/bridge/bridge.go` | Go | `//export goServiceCaller`: dispatches to `ServiceCaller` |

---

## 4. DAG Execution Flow

**Scenario:** DAG rule with parent merging and failure policies.

```
VM.dispatch(OpCode::Dag)
    │
    ▼
exec_dag(ctx.body, instr, plan, caller, arena)
    │
    ├── Layer 0: [A] → call A, store in context.outputs["A"]
    │
    ├── Layer 1: [B, C] (parallel)
    │   ├── B receives deep-merge(ctx.body, context.outputs["A"])
    │   └── C receives deep-merge(ctx.body, context.outputs["A"])
    │
    ├── Layer 2: [D] (depends on B, C)
    │   └── D receives deep-merge(ctx.body, outputs["A"], outputs["B"], outputs["C"])
    │
    ├── Merge terminal results via MergeStrategy
    └── ctx.body = merged result
```

### Files Involved

| File | What It Does |
|------|-------------|
| `rust/src/executor/dag.rs` | `exec_dag()`: topological execution, parent merging, failure policies |
| `rust/src/bytecode/dag_table.rs` | `DAGNode` with `parent_ids`, `DAGFailurePolicy`, `MergeStrategy` |

---

## 5. Schema & Type System

**Scenario:** Schema-validated event enters the runtime.

```
Event (opaque payload)
    │
    ├── Schema Registry lookup by schema_name + schema_version
    │
    ├── Deserialize payload into structured Value
    │   (format determined by content_type: JSON, Protobuf, etc.)
    │
    ├── TypeGuard opcode validates fields against schema
    │
    └── Gate/Map operations use schema-typed field access
        ├── payload.amount > 10000  (compiler knows amount is float)
        └── type_check_gate() validates operators at compile time
```

### DSL Schema

```
schema:{!order_id:string,!amount:float,role:enum[admin|user|guest]} n:validate
```

Compile-time validation:
- `g:amount>10000` → valid (float supports ordering)
- `g:role>admin` → compile error (enum does not support ordering)
- `g:role==admin` → valid (equals is allowed on any type)

### Files Involved

| File | What It Does |
|------|-------------|
| `rust/src/bytecode/resolved_type.rs` | `ResolvedType::Enum(Vec<String>)`, `Schema::is_valid()` |
| `rust/src/dsl/compiler.rs` | `compile_schema()`, `type_check_gate()`, `type_check_map()` |
| `rust/src/executor/mod.rs` | `op_type_guard()` validates body against schema |

---

## 6. Span Tracing

**Scenario:** VM emits trace spans during execution.

```
VM.dispatch(Next)
    │
    ├── Execute opcode
    ├── Record duration
    │
    └── emit_span(Span { opcode, service_id, layer, duration_ns, status })
        │
        └── thread_local! SPAN_BUFFER (lock-free ring buffer)
            │
            └── Drained via flowrulz_get_spans() from Go
```

### Span Format

```rust
#[repr(C)]
pub struct Span {
    opcode:      u8,     // 0-22
    service_id:  u16,
    layer:       u8,     // DAG layer
    duration_ns: u64,
    status:      u8,     // 0=ok, 1=error, 2=timeout
}
```

### Files Involved

| File | What It Does |
|------|-------------|
| `rust/src/tracing/mod.rs` | `Span` struct, `emit_span()` |
| `rust/src/tracing/ring_buffer.rs` | Lock-free ring buffer |
| `rust/src/ffi.rs` | `flowrulz_get_spans()` drains buffer |
| `go/internal/bridge/bridge.go` | `GetSpans()` calls `C.flowrulz_get_spans` |

---

## 7. Admin API

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| POST | `/rules` | Yes | Deploy rule |
| DELETE | `/rules/{id}` | Yes | Remove rule (drains active execs) |
| GET | `/rules` | Yes | List rules with versions |
| GET | `/rules/{id}` | Yes | Get rule detail with lane info |
| GET | `/rules/{id}/versions` | Yes | List versions |
| POST | `/rules/{id}/validate` | Yes | Compile-only, returns validity + complexity |
| POST | `/rules/{id}/promote?version=N` | Yes | Promote version |
| POST | `/rules/{id}/rollback` | Yes | Same as promote |
| GET | `/lanes` | Yes | Lane configs |
| GET | `/health` | No | Health check |

All endpoints (except `/health`) require `Authorization: Bearer <FLOWRULZ_API_KEY>` when set.

### Files Involved

| File | What It Does |
|------|-------------|
| `go/internal/admin/server.go` | Route handlers, `auth()` middleware |
| `go/cmd/flowrulz/main.go` | Mounts admin handler under `/admin/` |

---

## 8. Persistence

### Save (Atomic Write)

```
engine.saveRules()
    ├── RLock rules
    ├── json.Marshal → []byte
    ├── os.WriteFile(path + ".tmp", data, 0644)
    └── os.Rename(path + ".tmp", path)   ← atomic
```

### Load

```
engine.New(persistPath)
    └── os.ReadFile → json.Unmarshal → bridge.Compile() → create VersionedPlans
```

### Files Involved

| File | What It Does |
|------|-------------|
| `go/internal/engine/engine.go` | `New()`, `loadRules()`, `saveRules()` |

---

## Core Types

### Event

```rust
pub struct Event {
    pub id: String,
    pub topic: String,
    pub payload: Vec<u8>,
    pub headers: HashMap<String, String>,
    pub metadata: EventMetadata,
}

pub struct EventMetadata {
    pub mode: Mode,
    pub reply_to: String,
    pub correlation_id: String,
    pub trace_id: String,
    pub content_type: String,
    pub schema_name: String,
    pub schema_version: u32,
    pub partition: u32,
    pub offset: i64,
}

pub enum Mode {
    Publish = 0,
    Request = 1,
    Reply = 2,
    Stream = 3,
    Workflow = 4,
    Internal = 5,
}
```

### ExecutionContext

```rust
pub struct ExecutionContext {
    pub event: Event,
    pub body: Vec<u8>,                          // current working payload
    pub variables: HashMap<String, Vec<u8>>,    // intermediate state
    pub outputs: HashMap<String, Vec<u8>>,      // service call results
    pub headers: HashMap<String, String>,
    pub failed: bool,
    pub errors: Vec<String>,
    pub hop_count: u16,
    pub retry_count: u32,
    pub deadline_ms: u64,
}
```

---

## File → Scenario Matrix

| File | Scenarios |
|------|-----------|
| `rust/src/bytecode/event.rs` | Event, Mode core types |
| `rust/src/bytecode/execution.rs` | ExecutionContext |
| `rust/src/ffi.rs` | 1, 2, 3 |
| `rust/src/executor/mod.rs` | 2, 3, 4, 5 |
| `rust/src/executor/runtime.rs` | 2 |
| `rust/src/executor/next.rs` | 2, 3 |
| `rust/src/executor/dag.rs` | 4 |
| `rust/src/executor/gate.rs` | 5 |
| `rust/src/executor/map.rs` | 5 |
| `rust/src/executor/expr.rs` | 5 |
| `rust/src/dsl/compiler.rs` | 1, 5 |
| `rust/src/bytecode/resolved_type.rs` | 5 |
| `rust/src/tracing/` | 6 |
| `go/internal/admin/server.go` | 1, 7 |
| `go/internal/engine/engine.go` | 1, 2, 8 |
| `go/internal/bridge/bridge.go` | 1, 2, 3 |
| `go/internal/execnode/` | 2 |
| `go/cmd/flowrulz/main.go` | 2, 8 |
