# Memory Management Specification

## Overview

Three-layer memory architecture designed for high-throughput message processing with minimal allocation overhead.

```
┌──────────────────┐
│   Slab Pool       │  ← Three-tier pre-allocated buffer pool
│  (2KB / 8KB / 64KB)│
└────────┬─────────┘
         │ acquire/release
┌────────▼─────────┐
│   Arena (Bump)    │  ← Bump allocator for per-message temporaries
│  (bumpalo::Bump)  │
└────────┬─────────┘
         │ allocate
┌────────▼─────────┐
│ String Interner   │  ← Lock-free deduplicated string storage
│  (boxcar + HashMap)│
└──────────────────┘
```

## Slab Pool

Pre-allocated, lock-free message buffer pool.

### Size Classes

| Class | Size | Pre-alloc Count |
|-------|------|-----------------|
| Small | 2,048 bytes | 1024 |
| Medium | 8,192 bytes | 512 |
| Large | 65,536 bytes | 64 |

### Implementation

```rust
static SLAB_POOL: Lazy<Mutex<SlabPool>> = Lazy::new(|| {
    Mutex::new(SlabPool::new(1024, 512, 64))
});
```

Uses `crossbeam::SegQueue` (lock-free MPSC queue) for contention-free acquire/release.

## Arena

Bump allocator for per-message temporary allocations.

```rust
pub struct Arena {
    inner: bumpalo::Bump,
}
```

- O(1) allocation (bump pointer)
- Entire arena reset between messages (no individual frees)
- Used for temporary JSON manipulation during VM execution

## String Interner

Lock-free concurrent string interning.

```rust
static INTERN_TABLE: Lazy<InternTable> = Lazy::new(|| {
    let table = InternTable::new();
    table.prefill(&["content-type", "content-length", ...]);
    table
});
```

- O(1) amortized interning
- Lock-free reads via `boxcar::Vec`
- Pre-filled with common HTTP headers at startup

## Message Lifecycle

```
1. flowrulz_msg_alloc(size)  → Slab pool returns buffer
2. Write message JSON into slab buffer
3. flowrulz_execute(plan, body)
   ├─ Arena reset
   ├─ VM dispatch loop (TypeGuard validates schema)
   ├─ Span emitted per opcode
   └─ Result in body
4. Read result from body
5. flowrulz_msg_release(body) → Buffer back to slab pool
```

## Span Ring Buffer

Per-thread lock-free ring buffer for VM tracing spans (1024 entries):

```rust
thread_local! {
    pub static SPAN_BUFFER: RefCell<RingBuffer> = ...;
}
```

- Atomic head/tail with `Acquire`/`Release` ordering
- Span emitted at each `dispatch()` call
- Drained via `flowrulz_get_spans` after execution
- Compatible with OpenTelemetry span output format
