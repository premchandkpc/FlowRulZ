# C FFI API Specification

## Overview

The Rust core exposes a C-compatible FFI interface for the Go I/O shell to call. All functions use `extern "C"` with C-compatible types. The Rust crate compiles as both `cdylib` (for cgo linking) and `rlib` (for Rust integration tests).

## Memory Ownership Convention

- **Input buffers** (`*const u8` + `size_t`): Caller owns; callee reads during call
- **Output buffers** (`*mut u8` + `size_t` capacity + `*mut size_t` written): Caller allocates; callee writes; caller owns
- **Error buffers** (`*mut u8` + `size_t` capacity + `*mut size_t` written): Same as output
- All functions return `i32` status codes (0 = success, negative = error)
- No heap-allocated return values; all outputs go to caller-provided buffers

## Functions

### Compilation

```c
// Compile a DSL string into a binary ExecutionPlan (bincode-serialized)
// dsl_ptr   — UTF-8 DSL source
// dsl_len   — length in bytes
// rule_id_ptr — rule identifier (for error messages)
// rule_id_len — length in bytes
// out_ptr   — output buffer for serialized plan
// out_cap   — output buffer capacity
// out_len   — written bytes
// err_ptr   — error message buffer
// err_cap   — error buffer capacity
// err_len   — written error bytes
// Returns 0 on success, negative on error
int flowrulz_compile(
    const unsigned char* dsl_ptr, size_t dsl_len,
    const unsigned char* rule_id_ptr, size_t rule_id_len,
    unsigned char* out_ptr, size_t out_cap, size_t* out_len,
    unsigned char* err_ptr, size_t err_cap, size_t* err_len
);
```

### Execution

```c
// Execute a compiled plan against a message body
// ctx_id   — unique execution context ID (for concurrent caller dispatch)
// caller_cb — callback invoked for each service call
// plan_ptr  — bincode-serialized ExecutionPlan
// plan_len  — plan length
// body_ptr  — input JSON body
// body_len  — body length
// out_ptr   — output buffer for result JSON
// out_cap   — output buffer capacity
// out_len   — written bytes
// err_ptr   — error message buffer
// err_cap   — error buffer capacity
// err_len   — written error bytes
// msg_id_ptr   — message ID (optional, null if absent)
// msg_id_len   — message ID length
// corr_id_ptr  — correlation ID (optional)
// corr_id_len  — correlation ID length
// trace_id_ptr — trace/distributed tracing ID (optional)
// trace_id_len — trace ID length
// partition    — Kafka partition (0 if unknown)
// offset       — Kafka offset (0 if unknown)
// All optional context params default to empty/zero when null pointers are passed.
// Returns 0 on success, negative on error
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

### Callback Signature

The `caller_cb` callback dispatches service calls from the VM:

```c
// ctx_id   — execution context ID (matches the ctx_id passed to flowrulz_execute)
// svc_id   — interned service name ID
// body     — request body for the service
// body_len — body length
// resp     — output buffer for service response
// resp_len — in: capacity, out: written bytes
// Returns 0 on success, negative on error
int caller_callback(
    uint64_t ctx_id,
    uint16_t svc_id,
    const unsigned char* body, size_t body_len,
    unsigned char* resp, size_t* resp_len
);
```

Service names are interned via `flowrulz_intern`. The VM passes the integer ID, and the host can look up the name via `flowrulz_intern_lookup` if needed.

### Message Memory Management

```c
// Allocate a message buffer
// Returns pointer to buffer, or implementation-defined on failure
unsigned char* flowrulz_msg_alloc(size_t size);

// Release a message buffer
void flowrulz_msg_release(unsigned char* ptr);
```

### String Interning

```c
// Intern a string, returning its 16-bit ID
// Returns 0 for empty strings or on failure
uint16_t flowrulz_intern(const unsigned char* s_ptr, size_t s_len);

// Look up an interned string by ID
// Writes string bytes into caller-provided buffer
void flowrulz_intern_lookup(uint16_t id, unsigned char* out_ptr, size_t* out_len);
```

### Observability (Planned)

```c
// Drain accumulated VM trace spans into caller buffer.
// Returns number of bytes written (0 = no spans available).
// Span format: (opcode u8, service_id u16, duration_ns u64) repeated.
// This is a no-op stub until the ring buffer is implemented.
size_t flowrulz_get_spans(unsigned char* out_ptr, size_t out_cap);
```

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

The Go layer `go/internal/bridge/` uses cgo to call these functions.

### Callback Pattern

The Go bridge uses a three-layer callback dispatching:

```
C (flowrulz_execute) → C (callerBridge) → Go (//export goServiceCaller) → Go (ServiceCaller lookup via sync.Map)
```

1. `flowrulz_execute` (Rust) receives `ctx_id` and passes it to each callback invocation
2. `callerBridge` (C helper) forwards `ctx_id` + args to the `//export`'d Go function
3. `goServiceCaller` (Go, `//export`) looks up the registered `ServiceCaller` by `ctx_id` in a `sync.Map`

Each `Execute()` call generates a unique `ctx_id` (atomic counter) and stores its `ServiceCaller` in the map, enabling concurrent service dispatch without mutex contention.

### Thread Safety

- `flowrulz_execute` is safe to call concurrently on different plans
- `flowrulz_compile` is safe to call concurrently
- `flowrulz_msg_alloc` / `flowrulz_msg_release` use lock-free slab pools
- `flowrulz_intern` / `flowrulz_intern_lookup` use concurrent hash maps
- The Go `ServiceCaller` callback uses `sync.Map` keyed by execution `ctx_id` — multiple callers can be active concurrently, enabling true parallelism for `Parallel` and `DAG` opcodes
