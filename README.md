# FlowRulZ

High-performance Kafka message routing engine. Rust core (bytecode VM + DSL compiler) + Go I/O shell.

## Quick Start

```bash
make          # build Rust cdylib + Go binary
make test     # run all tests (Rust + Go)
./flowrulz    # start HTTP server on :8080
```

## Architecture

```
┌─────────────────────────────────────────────┐
│              Go I/O Shell                     │
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
├── rust/          # Core: DSL toolchain + bytecode VM + memory mgmt
├── go/            # Shell: HTTP admin, engine, transport, cgo bridge
├── docs/          # Specs: DSL syntax, bytecode format, VM, FFI, memory, Kafka
├── config/        # (placeholder)
└── test/          # (placeholder)
```

Go module: `github.com/premchandkpc/FlowRulZ`
Rust crate: `flowrulz-core` (cdylib + rlib)
Binary: `flowrulz`
