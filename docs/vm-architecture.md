# VM Architecture Specification

## Overview

The FlowRulZ VM is a **register-less, stackless bytecode interpreter** that walks a linear `Vec<Instruction>` with an instruction pointer (IP). It processes a single event through a compiled `ExecutionPlan`, operating on an `ExecutionContext` that holds the event, working body, variables, and service outputs.

## Execution Model

```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐
│  Event In    │ ──→ │  VM.run()    │ ──→ │  Output      │
│  (opaque)    │     │  dispatch()  │     │  (bytes)     │
└─────────────┘     └──────────────┘     └──────────────┘
                          │
                    ┌─────┴──────┐
                    │ Instruction│
                    │   Pointer  │
                    └─────┬──────┘
                          │
                    ┌─────▼──────────────────┐
                    │  switch(opcode) {        │
                    │    Next → exec_next()    │
                    │    Async → exec_next(async) │
                    │    Gate → exec_gate()    │
                    │    Map  → exec_map()     │
                    │    TypeGuard → validate  │
                    │    ...                   │
                    │  }                       │
                    └────────────────────────┘
```

### VM State

```rust
pub struct VM<'a> {
    pub ip: usize,
    pub plan: &'a ExecutionPlan,
    pub arena: Arena,
    pub caller: Arc<dyn Fn(u16, &[u8], u64) -> Result<Vec<u8>, String> + Send + Sync>,
    pub ctx: ExecutionContext,
}

pub struct ExecutionContext {
    pub event: Event,                    // original event (id, topic, payload, headers, metadata)
    pub body: Vec<u8>,                   // current working payload
    pub variables: HashMap<String, Vec<u8>>,  // intermediate state
    pub outputs: HashMap<String, Vec<u8>>,    // service call results
    pub headers: HashMap<String, String>,
    pub failed: bool,
    pub errors: Vec<String>,
    pub hop_count: u16,
    pub retry_count: u32,
    pub deadline_ms: u64,
}
```

The VM does NOT have `last_response`, `failed`, or `hop_count` as separate fields — all state lives in `ExecutionContext`.

### Main Loop — `VM::run()`

```
while ip < plan.instructions.len():
    inst = plan.instructions[ip]
    ip += 1
    result = dispatch(inst)
    if Drop: ip = HALT (break)
    if Jmp: ip = inst.a
```

### Cooperative Step — `VM::step()`

For asynchronous execution, `step()` processes at most one instruction and returns control to the caller:

```
if response is Some(resp):
    ctx.body = resp
    ctx.hop_count += 1
    ip += 1

if ip >= instructions.len():
    return Done

inst = instructions[ip]
match inst.op:
    Next | Async | Emit | SvcCall → return Pending { svc_id, body }
    _ → ip += 1; dispatch(inst)
         if ip >= instructions.len() → Done else → Continue
```

Key differences from `run()`:
- **Never blocks**: service opcodes yield `Pending` instead of calling the callback
- **Response injection**: caller provides service response on the next `step()` call
- **Context serialization**: the caller stores the serialized `ExecutionContext` between steps
- **Used by**: `bridge.ExecuteStep()` in Go execnode and simulator for cooperative execution loops

## Event Model

Every message is an Event with opaque payload:

```rust
pub struct Event {
    pub id: String,
    pub topic: String,
    pub payload: Vec<u8>,          // opaque — any serialization
    pub headers: HashMap<String, String>,
    pub metadata: EventMetadata,
}

pub enum Mode {
    Publish = 0,    // fire-and-forget
    Request = 1,    // synchronous request/reply
    Reply = 2,      // response to a request
    Stream = 3,     // persistent subscription
    Workflow = 4,   // rule execution
    Internal = 5,   // system-internal
}
```

The VM never parses the payload format — it works with bytes. Schema validation happens via `TypeGuard` against the schema attached to the `ExecutionPlan`, not against a serialization format.

### Generic Core, Optional Schema

FlowRulZ is payload-agnostic by design. The VM treats all payloads as `Vec<u8>` — it never inspects or cares about serialization format (JSON, Protobuf, Avro, MessagePack, or raw binary). Schema is **opt-in**, not mandatory:

- **Without `schema:{...}` in DSL:** the TypeGuard opcode never fires. Arbitrary bytes pass through untouched. Gate/Map operators parse the body as JSON on-demand but impose no shape contract.
- **With `schema:{...}`:** the compiler emits a TypeGuard opcode that validates fields at runtime, and runs a pre-pass to type-check Gate/Map operators at compile time.

**Use schema only at boundary layers** (ingress rules, first-hop validation). Internal routing rules that `g:`, `e:`, or `n:` without type-sensitive operators should skip schema entirely.

## ExecutionContext Semantics

Services **enrich** the context rather than replacing a single body:

```
Event In
    │
    ▼
Fraud Service → ctx.outputs["fraud"] = FraudResponse
                ctx.body = updated body
    │
    ▼
Inventory Service → ctx.outputs["inventory"] = InventoryResponse
                     ctx.body = merged body
    │
    ▼
Gate on ctx.outputs["fraud"].score > 70
```

## Opcode Handlers

### Next (`n:service`)
1. Extract service name from `ServiceTable[inst.a]`
2. Call service via C FFI callback (with timeout from inst.b/c)
3. On success: store response in `ctx.body` AND `ctx.outputs["svc_name"]`, advance IP, increment `ctx.hop_count`
4. On failure: if retry configured, retry; else set `ctx.failed` flag

### Gate (`g:field op value`)
1. Extract field from body using dotted path navigation
2. Returns `FieldNotFound` error (not silent null) for missing intermediate fields
3. Compare value using operator (`==`, `!=`, `>`, `<`, `>=`, `<=`, `contains`)
4. False → set ip to jump_offset (skip to Fallback or next op)

### Map (`m:expr`)
1. Parse and evaluate expression from const pool
2. Insert/modify fields in `ctx.body` JSON
3. Uses `serde_json::Value` in-place mutation

### Parallel (`p:a,b,c`)
1. Fan-out: call each service with current `ctx.body`
2. Collect responses into `Vec<Value>` array
3. Deep-merge parallel results into existing body under `"_parallel"` key, preserving all other body fields
4. Non-object bodies (raw strings, arrays) are wrapped in a new object with `"_parallel"`

### Collect (`c`)
1. Read `"_parallel"` key from `ctx.body`
2. Extract the array, set `ctx.body` to its value
3. Remove `"_parallel"` key from body (error if missing — ensures `p:` always precedes `c`)

### Emit (`e:a,b,c`)
1. Fire-and-forget: call each service but discard response
2. Non-blocking, no retry

### Fallback (`f:service`)
1. On preceding op failure (`ctx.failed == true`): clear failed flag
2. Call fallback service with `ctx.body`
3. Continue execution

### Split (`s`)
1. Extract field value from body
2. Process each element of the array through remaining pipeline
3. Collect results into new array

### Drop (`d`)
1. Set ip to end of instructions → breaks execution
2. Message is discarded

### Buffer (`bN`)
1. Accumulate messages in the ExecutionRuntime (not VM)
2. When buffer count reaches N, flush accumulated messages as JSON array
3. Execute full pipeline on the buffered array

### Chunk (`chunk:N:mode`)
1. Split body by chunk size (handled at ExecutionRuntime level)
2. **Seq mode:** sequential loop
3. **Par mode:** parallel tasks (via rayon)
4. Reassemble results into JSON array

### DAG (`dag:{...}`)
1. Load DAGTable from plan
2. For each layer, execute all nodes in parallel
3. Parent results are deep-merged into downstream node input via `parent_ids`
4. Apply failure policy (AbortAll / ContinueOthers / SkipDependents)
5. Merge terminal node results via MergeStrategy (LastWins / ArrayConcat / DeepMerge / ExplicitMap)
6. Emit merged result

### TypeGuard
1. Read schema from `ExecutionPlan.schema`
2. Parse `ctx.body` as JSON
3. Validate each field against its expected type (including Enum validation)
4. Fields typed `any` accept any value (null, string, number, object, array — all allowed)
5. Required fields (`!name:type`) error if missing from body
6. On failure: return error with field name and expected vs actual type

### Jmp/Label
1. `Label` is a no-op (marker)
2. `Jmp` sets `ip = inst.a`

### Retry
1. Attached to preceding `Next` or `Fallback` via flags
2. On failure: check retry count; calculate delay from strategy
3. Sleep for delay, then retry call

## Error Propagation

```
Service call fails?
  ├── Retry configured? ──yes──→ Retry loop
  └── No retry?
        ├── Next instruction is Fallback? ──yes──→ Call fallback, continue
        └── No fallback? ──→ Set ctx.failed flag, continue
```

## Span Tracing

The VM emits a span at every `dispatch()` call via a thread-local lock-free ring buffer:

```rust
#[repr(C)]
pub struct Span {
    opcode:      u8,
    service_id:  u16,
    layer:       u8,        // DAG layer (0 for non-DAG)
    duration_ns: u64,
    status:      u8,        // 0=ok, 1=error, 2=timeout
}
```

- Thread-local `RefCell<RingBuffer>` (1024 spans per thread)
- Atomic head/tail for lock-free push and drain
- Drained via `flowrulz_get_spans(out_ptr, out_cap) -> size_t` from Go

## Memory Model

Messages are `&[u8]` / `Vec<u8>` at the VM boundary, internally parsed to `serde_json::Value` when needed for field access. The VM:
- Does **not** clone the message on every operation
- Clones only when necessary (Parallel fan-out, Fallback save)
- Uses bump arena (`bumpalo`) for temporary allocations

## Concurrency

- Parallel execution uses `rayon::scope` for bounded parallelism
- Service calls are synchronous from Rust's perspective (C FFI blocks)
- DAG node execution is fully parallel within each layer
- Chunk processing (par mode) uses rayon work-stealing

## Type System

Runtime type validation via `TypeGuard` opcode:

1. Schema attached to `ExecutionPlan` at compile time
2. `TypeGuard` opcode reads plan schema and validates body
3. Types: String, Integer, Float, Boolean, Object, Array, Null, Any, Enum(Vec\<String\>)
4. Required fields (`!name:type`) error if missing from body
5. Enum fields validate value is in allowed set
6. Fields typed `any` pass all compile-time Gate/Map checks silently (ordering, contains, equality) and accept any value at runtime

The `any` type serves as an escape hatch — it lets you declare a field in the schema for documentation or routing purposes while deferring all type enforcement to the producer. Pair `any` with specific types on fields you actually Gate on:

```
schema:{!order_id:string,!amount:int,routing_data:any}
  g:amount>10000 n:manual-review
```

Here `amount` gets compile-time type safety on the `>` operator; `routing_data` is documented in the schema but unconstrained.
