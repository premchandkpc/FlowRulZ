# Memory Management Specification

## Overview

Two-layer memory architecture: Arena bump allocator for per-message temporaries + shared intern table. Slab pool was removed as dead code — `flowrulz_msg_alloc`/`release` use `std::alloc` directly.

```
┌──────────────────┐
│ Arena (Bump)      │  ← Bump allocator for per-message temporaries
│ (bumpalo::Bump)   │
└────────┬─────────┘
         │ allocate
┌────────▼─────────┐
│ String Interner   │  ← Lock-free deduplicated string storage
│  (boxcar + HashMap)│
└──────────────────┘
```

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

## Message Memory — `flowrulz_msg_alloc` / `flowrulz_msg_release`

Simple `std::alloc` allocator (was slab pool). No pooling — each call allocates/frees directly. The slab pool (`memory::slab`) was removed as dead code since every allocation went through the pool and was immediately discarded (the pool never actually reused arenas).

## Message Lifecycle

## Span Ring Buffer

Per-thread lock-free ring buffer for VM tracing spans (1024 entries):

```rust
thread_local! {
    pub static SPAN_BUFFER: RefCell<SpanRingBuffer> = ...;
}
```

- Atomic head/tail with `Acquire`/`Release` ordering
- Span emitted at each `dispatch()` call
- Drained via `flowrulz_get_spans` after execution
- Compatible with OpenTelemetry span output format
