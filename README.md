# FlowRulZ

Low-latency rule evaluation engine for event-driven systems. Rust core (bytecode VM + DSL compiler) + Go I/O shell.

## Quick Start

```bash
make          # build Rust cdylib + Go binary
make test     # run all tests (Rust 86 + Go)
make bench    # criterion benchmarks
./flowrulz    # start HTTP server on :8080
```

## Architecture

```
┌─────────────────────────────────────────────┐
│              Go I/O Shell                    │
│  HTTP Admin (:8080) → Engine → Transport     │
│                              │ cgo           │
├──────────────────────────────┼───────────────┤
│              Rust Core (cdylib)              │
│  DSL → Lexer → Parser → Opt → Compiler      │
│  → Bytecode VM → dispatch loop → FFI cb     │
└──────────────────────────────────────────────┘
```

DSL compiled once to bytecode, executed per message. VM calls back into Go for service dispatch.

## Project

```
FlowRulZ/
├── rust/          # Core: DSL toolchain + bytecode VM + memory mgmt + type system
├── go/            # Shell: HTTP admin, engine, transport, cgo bridge
├── docs/          # Specs: DSL syntax, bytecode format, VM, FFI, memory, Kafka
└── Makefile
```

Go module: `github.com/premchandkpc/FlowRulZ`
Rust crate: `flowrulz-core` (cdylib + rlib)
Binary: `flowrulz`

## Key Features

- **DSL → bytecode compiler**: compile once, execute millions of times
- **23 opcodes**: Next, Parallel, Collect, Fallback, Gate, Split, Map, Emit, Drop, Buffer, Key, Retry, Pipe, Timeout, Async, Chunk, DAG, Jmp, Label, SvcArg, RetryData, JumpOffset, TypeGuard
- **Type system**: schema attachment with runtime type validation via `TypeGuard`
- **Complexity scoring**: automatic lane routing based on compile-time cost estimates
- **Versioned rules**: promote/rollback with active execution draining
- **Rule persistence**: file-based save/load via `FLOWRULZ_PERSIST_PATH`
- **Admin API**: rule CRUD, validate, promote, rollback, lanes — API key auth
- **Span tracing**: per-opcode lock-free ring buffer, drained via `flowrulz_get_spans`
- **Zero-alloc message path**: slab pool + bump arena + string interning
- **21 expression builtins**: uuid, now, epoch, lower, upper, trim, length, concat, base64, json, substring, replace, to_string, parse_int, parse_float, coalesce, default, contains, keys, merge, hash
