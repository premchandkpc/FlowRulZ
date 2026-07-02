---
title: Bridge
tags:
  - concept
  - ffi
  - go
  - rust
---

# Bridge

> [!info] Go ↔ Rust FFI boundary
> Path: `server/bridge/`

The bridge provides CGo functions that the Go server calls to execute plans in the Rust VM.

## Key Functions

| Function | Signature | Purpose |
|----------|-----------|---------|
| `ExecuteStep` | `(ctx, *C.char, C.int, *C.char) → *C.char` | Executes a single plan step |
| `FreeCString` | `(*C.char)` | Frees C string allocated by Rust |

## Data Flow

```
Go: bridge.ExecuteStep(state, planPtr) → C function call → Rust: executor::dispatch()
                                                                    │
                                                                    ▼
                                                          exec_map / exec_gate / exec_service_call
                                                                    │
                                                                    ▼
                                                          JSON result as C string
                                                                    │
                                                                    ▼
Go: parse C string → *ExecuteResult → return to scheduler
```

## Memory Ownership

Rust allocates result strings via `CString::new()`. Go is responsible for calling `FreeCString` after consuming the result. The arena allocator in the [[Runtime]] is reset between plan executions.

## Dependencies

- [[Runtime]] — the Rust library loaded at runtime via `dlopen` or linked statically
- [[Node]] — calls bridge functions during `executePlan()`
