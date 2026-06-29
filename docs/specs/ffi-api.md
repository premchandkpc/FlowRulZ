# C FFI API Specification

## Overview

The Rust core exposes a C-compatible FFI interface for the Go data plane. All functions use `extern "C"` with C-compatible types. The VM operates on `ExecutionContext` (Event + body + variables + outputs) rather than a raw JSON body.

## Memory Ownership Convention

- **Input buffers** (`*const u8` + `size_t`): Caller owns; callee reads during call
- **Output buffers** (`*mut u8` + `size_t` capacity + `*mut size_t` written): Caller allocates; callee writes
- **Error buffers** (`*mut u8` + `size_t` capacity + `*mut size_t` written): Same as output
- All functions return `i32` status codes (0 = success, negative = error)

## Functions

### Compilation

```c
int flowrulz_compile(
    const unsigned char* dsl_ptr, size_t dsl_len,
    const unsigned char* rule_id_ptr, size_t rule_id_len,
    unsigned char* out_ptr, size_t out_cap, size_t* out_len,
    unsigned char* err_ptr, size_t err_cap, size_t* err_len
);
```

Returns a bincode-serialized `ExecutionPlan`. The compiler pipeline: lex → parse → optimize → compile (with type checking) → bincode serialize.

### Execution

```c
int flowrulz_execute(
    uint64_t ctx_id,
    const unsigned char* plan_ptr, size_t plan_len,
    const unsigned char* body_ptr, size_t body_len,
    int (*caller_cb)(uint64_t, uint16_t, const unsigned char*, size_t, unsigned char*, size_t*),
    unsigned char* out_ptr, size_t out_cap, size_t* out_len,
    unsigned char* err_ptr, size_t err_cap, size_t* err_len,
    const unsigned char* msg_id_ptr, size_t msg_id_len,
    const unsigned char* corr_id_ptr, size_t corr_id_len,
    const unsigned char* trace_id_ptr, size_t trace_id_len,
    uint32_t partition, int64_t offset
);
```

Creates an `ExecutionContext` from the body bytes and event metadata (msg_id → event.id, corr_id → event.metadata.correlation_id, trace_id → event.metadata.trace_id, partition/offset → event metadata). Runs the VM on the context, returns `ctx.body` in the output buffer.

Optional context params (`msg_id`, `corr_id`, `trace_id`) default to empty when null.

### Step Execution

```c
int flowrulz_execute_step(
    uint64_t ctx_id,
    const unsigned char* plan_ptr, size_t plan_len,
    const unsigned char* ctx_bytes_ptr, size_t ctx_bytes_len,
    const unsigned char* resp_ptr, size_t resp_len,
    int (*caller_cb)(uint64_t, uint16_t, const unsigned char*, size_t, unsigned char*, size_t*),
    unsigned char* out_ptr, size_t out_cap, size_t* out_len,
    unsigned char* err_ptr, size_t err_cap, size_t* err_len,
    uint16_t* pending_svc_id,
    unsigned char* pending_body_ptr, size_t pending_body_cap, size_t* pending_body_len,
    unsigned char* ctx_out_ptr, size_t ctx_out_cap, size_t* ctx_out_len
);
```

Cooperative single-step execution. Takes a serialized `ExecutionContext` (from previous step) and an optional service response. Executes one instruction:

| Return | Meaning |
|--------|---------|
| `0` (Done) | Plan finished; final body in `out_ptr`, final context in `ctx_out_ptr` |
| `1` (Pending) | Service call needed; `pending_svc_id` + `pending_body` populated, context in `ctx_out_ptr` for next `execute_step` call |
| `2` (Continue) | Non-service instruction processed; call `execute_step` again with `resp_ptr=NULL` |

On first call, pass `ctx_bytes_ptr=NULL, ctx_bytes_len=0` to create a fresh context from the response (if any) in `resp_ptr`.

### Callback Signature

```c
int caller_callback(
    uint64_t ctx_id,
    uint16_t svc_id,
    const unsigned char* body, size_t body_len,
    unsigned char* resp, size_t* resp_len
);
```

The `ctx_id` matches the one passed to `flowrulz_execute`/`flowrulz_execute_step`. Service names are interned; the host can look up the name via `flowrulz_intern_lookup`. The response replaces `ctx.body` and is also stored in `ctx.outputs["service_name"]`.

### Message Memory

```c
unsigned char* flowrulz_msg_alloc(size_t size);
void flowrulz_msg_release(unsigned char* ptr);
```

Slab pool backed — returns pre-allocated buffers. Falls back to fresh allocation if pool empty.

### String Interning

```c
uint16_t flowrulz_intern(const unsigned char* s_ptr, size_t s_len);
void flowrulz_intern_lookup(uint16_t id, unsigned char* out_ptr, size_t* out_len);
```

### Observability

```c
size_t flowrulz_get_spans(unsigned char* out_ptr, size_t out_cap);
```

Drains accumulated trace spans from the current thread's ring buffer. Span format: `{opcode: u8, service_id: u16, layer: u8, duration_ns: u64, status: u8}` repeated.

### Complexity

```c
uint32_t flowrulz_plan_complexity(const unsigned char* plan_ptr, size_t plan_len);
```

Deserializes the plan and returns `complexity_score`. Used by Go engine for lane routing.

## Error Codes

| Constant | Value | Meaning |
|----------|-------|---------|
| `OK` | 0 | Success |
| `FFI_ERROR_NULL_POINTER` | -1 | Null pointer in input |
| `FFI_ERROR_BUFFER_TOO_SMALL` | -2 | Output buffer insufficient |
| `FFI_ERROR_LEX` | -3 | DSL lexer error |
| `FFI_ERROR_PARSE` | -4 | DSL parser error |
| `FFI_ERROR_COMPILE` | -5 | DSL compiler error |
| `FFI_ERROR_SERIALIZE` | -6 | Plan serialization error |
| `FFI_ERROR_DESERIALIZE` | -7 | Plan deserialization error |
| `FFI_ERROR_EXEC` | -8 | VM execution error |

## Integration with Go

The Go layer uses cgo to call these functions.

### Callback Pattern (Synchronous — `flowrulz_execute`)

Three-layer dispatching:

```
C (flowrulz_execute) → C (callerBridge) → Go (//export goServiceCaller) → sync.Map lookup
```

1. `flowrulz_execute` receives `ctx_id` and passes it to each callback invocation
2. `callerBridge` (C helper) forwards to the `//export`'d Go function
3. `goServiceCaller` (Go) looks up `ServiceCaller` by `ctx_id` in a `sync.Map`

Each `Execute()` call generates a unique `ctx_id` via `atomic.Uint64` and stores its `ServiceCaller` in the map, enabling concurrent service dispatch without mutex contention.

### Execution Pattern (Cooperative — `flowrulz_execute_step`)

The step API inverts control: instead of the VM calling back into Go, Go drives the loop:

```
Go loop:
  out = flowrulz_execute_step(plan, ctx_bytes, resp_bytes, nil)
  switch out.result:
    Done     → return out.output
    Pending  → resp_bytes = callService(out.svc_id, out.body); continue
    Continue → resp_bytes = nil; continue
```

The Go side resolves service calls between steps, enabling async multiplexing, circuit breakers, rate limiting, and observability hooks between each instruction. The `caller_cb` parameter is always passed but never called for service opcodes — it may be used for Parallel/DAG inner calls.

### Thread Safety

- All FFI functions are safe to call concurrently
- `flowrulz_msg_alloc` / `flowrulz_msg_release` use lock-free slab pools
- The Go `ServiceCaller` callback uses `sync.Map` keyed by execution `ctx_id`
- Multiple callers can be active concurrently for `Parallel` and `DAG` opcodes
