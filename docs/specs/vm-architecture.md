# VM Architecture Specification

## Overview

The FlowRulZ VM is a **register-less, stackless bytecode interpreter** that walks a linear `Vec<Instruction>` with an instruction pointer (IP). It processes a single JSON message through a compiled `ExecutionPlan`.

## Execution Model

```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐
│  Message In  │ ──→ │  VM.run()    │ ──→ │  Message Out │
│  (JSON str)  │     │  dispatch()  │     │  (JSON str)  │
└─────────────┘     └──────────────┘     └──────────────┘
                          │
                    ┌─────┴──────┐
                    │ Instruction │
                    │   Pointer   │
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
    pub last_response: Vec<u8>,
    pub arena: Arena,
    pub caller: Arc<dyn Fn(u16, &[u8], u64) -> Result<Vec<u8>, String> + Send + Sync>,
    pub failed: bool,
    pub errors: Vec<String>,
    pub hop_count: u16,
    pub ctx: ExecutionContext,
}
```

### Main Loop

```
while ip < plan.instructions.len():
    inst = plan.instructions[ip]
    ip += 1
    result = dispatch(inst)
    if Drop: ip = HALT (break)
    if Jmp: ip = inst.a
```

## Opcode Handlers

### Next (`n:service`)
1. Extract service name from `ServiceTable[inst.a]`
2. Call service via C FFI callback (with timeout from inst.b/c)
3. On success: replace `last_response` with response, advance IP
4. On failure: if retry configured, retry; else set failed flag

### Gate (`g:field op value`)
1. Extract field from body using dotted path navigation
2. Returns `FieldNotFound` error (not silent null) for missing intermediate fields
3. Compare value using operator (`==`, `!=`, `>`, `<`, `>=`, `<=`, `contains`)
4. False → set ip to jump_offset (skip to Fallback or next op)

### Map (`m:expr`)
1. Parse and evaluate expression from const pool
2. Insert/modify fields in `last_response` JSON
3. Uses `serde_json::Value` in-place mutation

### Parallel (`p:a,b,c`)
1. Clone current body for each fan-out branch
2. Spawn rayon parallel tasks for each service call
3. Collect results into `Vec<Value>`

### Collect (`c`)
1. Walk `_parallel` array from parallel results
2. Merge unique keys into body
3. Remove `_parallel` after merge

### Emit (`e:a,b,c`)
1. Fire-and-forget: call each service but discard response
2. Non-blocking, no retry

### Fallback (`f:service`)
1. On preceding op failure (self.failed == true): clear failed flag
2. Call fallback service with saved body
3. Continue execution

### Split (`s`)
1. Extract field value from body
2. Process each element of the array through remaining pipeline
3. Collect results into new array

### Drop (`d`)
1. Set ip to end of instructions → breaks execution
2. Message is discarded

### Chunk (`chunk:N:mode`)
1. Split body by chunk size
2. **Seq mode:** sequential loop
3. **Par mode:** rayon parallel tasks
4. Reassemble results

### DAG (`dag:{...}`)
1. Build execution frontier starting with layer 0
2. For each layer, execute all nodes in parallel (rayon)
3. When a node completes, check if its dependents' dependencies are satisfied
4. Advance frontier with newly unblocked nodes
5. Merge terminal node results

### TypeGuard
1. Read schema from `ExecutionPlan.schema`
2. Parse `last_response` as JSON
3. Validate each field against its expected type
4. On failure: return error with `FieldNotFound` details

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
        └── No fallback? ──→ Set failed flag, continue
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

Messages are `&[u8]` / `Vec<u8>` at the VM boundary, internally parsed to `serde_json::Value`. The VM:
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
3. Errors include field name and expected vs actual type
4. Required fields (`!name:type`) error if missing from body
