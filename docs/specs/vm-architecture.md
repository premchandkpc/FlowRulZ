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
                    │  switch(opcode) {       │
                    │    Next → exec_next()   │
                    │    Gate → exec_gate()   │
                    │    Map  → exec_map()    │
                    │    ...                  │
                    │  }                      │
                    └────────────────────────┘
```

### VM State

```rust
pub struct VM<'a> {
    plan: &'a ExecutionPlan,
    ip: usize,                    // Instruction pointer
    body: serde_json::Value,       // Current message body (mutated in place)
    fallback_body: Option<serde_json::Value>, // Saved body for fallback
}
```

### Main Loop

```
while ip < plan.instructions.len():
    inst = plan.instructions[ip]
    ip += 1
    result = dispatch(inst)
    if result is Break:
        break
    if result is Halt:
        ip = HALT (break)
    if inst is Jmp:
        ip = inst.offset
```

## Opcode Handlers

### Next (`n:service`)
1. Extract service name from `ServiceTable[inst.a]`
2. Call service via C FFI callback
3. If timeout > 0, set deadline
4. On success: replace `body` with response, advance IP
5. On failure: if retry configured, retry; else jump to Fallback (next instruction)
6. Supports chunking: split body, call service for each chunk, reassemble

### Gate (`g:field op value`)
1. Extract field from `body` using dotted path navigation
2. Compare value using operator
3. True → advance IP to `then_offset`
4. False → set IP to `else_offset`

### Map (`m:dest=expr` or `m:dest:src`)
1. **Copy mode:** Extract value at `source_field`, insert at `dest_field`
2. **Expression mode:** Parse and evaluate expression, insert result at `dest_field`
3. Uses `serde_json::Value` in-place mutation (no full clone)

### Parallel (`p:a,b,c`)
1. Clone current `body` for each fan-out branch
2. Spawn rayon parallel tasks for each service call
3. Collect results into `Vec<Value>`
4. Merge into `body["_parallel"]` array

### Collect (`c`)
1. If `body` has no `_parallel` key, this is a no-op (passthrough)
2. Otherwise: walk the `_parallel` array, merge unique keys from each result object into `body`
3. Remove `_parallel` from `body` after merge

### Emit (`e:a,b,c`)
1. Fire-and-forget: call each service but discard response
2. Non-blocking, no retry
3. Continue execution immediately

### Fallback (`f:service`)
1. On preceding op failure: replace `body` with saved `fallback_body`
2. Call fallback service
3. Continue execution

### Split (`s`)
1. Validate `body` is a JSON array
2. For each element, execute remaining pipeline
3. Collect results into new array
4. Replace `body` with array of results

### Drop (`d`)
1. Return `Halt` to VM main loop → breaks execution
2. Message is discarded

### Chunk (`chunk:N:mode`)
1. Determine chunk boundaries from `body` (array or by size)
2. **Seq mode:** process chunks in sequential loop
3. **Par mode:** process chunks via rayon parallel tasks
4. Reassemble chunked results

### DAG (`dag:{...}`)
1. Build execution frontier starting with layer 0 nodes
2. For each layer, execute all nodes in parallel (rayon)
3. When a node completes, check if its dependents' dependencies are all satisfied
4. Advance frontier with newly unblocked nodes
5. After all layers complete, merge terminal node results
6. Emit merged result

### Jmp/Label
1. `Label` is a no-op (just a marker)
2. `Jmp` sets `ip = inst.offset` (resolved at compile time)

### Retry
1. Attached to preceding `Next` or `Fallback` instruction
2. On failure: check retry count; if exhausted, propagate error
3. Calculate delay from strategy (exp/fixed/linear)
4. Sleep for delay, then retry call

### Key (`k:expr`)
1. Evaluate expression and set routing key on message

## Error Propagation

```
Service call fails?
  ├── Retry configured? ──yes──→ Retry loop
  └── No retry?
        ├── Next instruction is Fallback? ──yes──→ Call fallback, continue
        └── No fallback? ──→ Return error from VM
```

## Memory Model

Messages are `serde_json::Value` (owned, heap-allocated JSON trees). The VM:
- Does **not** clone the message on every operation (passes `&mut`)
- Clones only when necessary (Parallel fan-out, Split, Fallback save)
- Returns owned `Value` at end of execution

## Concurrency

- Parallel execution uses `rayon::scope` for bounded parallelism
- Service calls are synchronous from Rust's perspective (C FFI blocks)
- DAG node execution is fully parallel within each layer
- Chunk processing (par mode) uses rayon work-stealing

---

## Gaps & Planned Features

### VM Trace Spans (Planned)

The VM currently emits no observability data. A per-opcode span ring buffer is planned:

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

- Thread-local lock-free ring buffer (1024 spans per thread)
- Span emitted at each `dispatch()` call
- Drained via `flowrulz_get_spans(out_ptr, out_cap) -> size_t` (currently stubbed)

### TypeGuard Opcode (Planned)

New opcode 23: runtime type check for fields without schema coverage.

```rust
OpCode::TypeGuard => {
    // a = field_const_id, b = expected_type_tag, c = else_offset
    let field_value = get_field(instr.a);
    if !type_check(field_value, instr.b) {
        self.ip = instr.c as usize;  // jump to fallback
    }
}
```

### DAG Distributed Execution (Planned)

Current DAG is intra-node (rayon within one VM). Future: each DAG node maps to a Kafka topic:

```
flowrulz-dag-{rule_id}-A   ← A produces here
flowrulz-dag-{rule_id}-B   ← B consumes A
flowrulz-dag-coord-{rule}  ← coordinator tracks per-message completion
```

Requires a coordinator state machine with correlation ID tracking per node.

### MergeStrategy for DAG Output (Planned)

Current DAG merges terminal node results naively. Need explicit strategy:

```rust
enum MergeStrategy {
    LastWins,          // last terminal node's value wins
    ArrayConcat,       // concatenate arrays
    DeepMerge,         // deep merge JSON objects
    ExplicitMap,       // explicit output field mapping
}
```
