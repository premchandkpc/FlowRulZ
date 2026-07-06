# Flow DSL

**Status:** Implemented. The Flow DSL is a high-level, block-structured workflow definition language written in pure Go. It compiles to an IR graph and can generate source code in Go, Rust, Java, and Python. It is separate from the Rust bytecode DSL (`runtime/src/dsl/`).

## Overview

The Flow DSL provides a human-readable, indentation-based syntax for defining multi-step workflows with services, control flow, error handling, and compensating transactions.

### Example

```flow
version 1

flow UserSignup

description
    Complete signup workflow

service auth
    type grpc
    address auth:50051

service email
    type http
    url https://email.internal

workflow

Start

-> auth.CreateUser

-> email.SendWelcome

-> End
```

## Comparison with Rust DSL

| Dimension | Rust DSL | Flow DSL |
|---|---|---|
| Language | Rust (compiles to bytecode) | Go (compiles to IR/graph) |
| Syntax | Compact, single-line, infix (`n:svc r3:exp t500`) | Indentation-based, block-structured |
| Execution target | `ExecutionPlan` bytecode (VM) | IR graph (not yet wired to execution) |
| Control flow | Gate, Jump, DAG | `if`/`else`, `switch`/`case`, `parallel`/`join`, `foreach`, `while` |
| Service model | Implicit (via Go bridge) | Explicit declarations (`service auth type grpc address ...`) |
| Type system | Opt-in `schema:{...}`, `TypeGuard` at VM level | Variables + constants blocks |
| Resilience | Retry (`r3:exp`) per operation | Flow-wide `retry`, `breaker`, `timeout`, `onError`, `compensate` |
| Error handling | Fallback op (`f:svc`) + DLQ | `onError` block with typed error cases |
| Code generation | Produces Rust bytecode | Generates Go, Rust, Java, Python source |

## Pipeline

```
.flow source
     |
 [Lexer]       lexer.go       → []Token
     |
 [Parser]      parser.go      → *Flow AST (ast.go)
     |
 [Analyzer]    semantic.go    → []SemanticError
     |
 [Compiler]    ir.go          → *IR (graph of nodes + edges)
     |
 [CodeGen]     codegen.go     → Go / Rust / Java / Python source
```

### Stage 1: Lexer (`lexer.go`)

Hand-written character-by-character tokenizer producing 40+ token types:

- **Literals:** `TokenIdent`, `TokenString`, `TokenNumber`, `TokenDuration`, `TokenVersion`
- **Keywords (29):** `flow`, `service`, `event`, `workflow`, `if`, `else`, `switch`, `case`, `default`, `parallel`, `join`, `retry`, `breaker`, `timeout`, `wait`, `foreach`, `while`, `output`, `import`, `include`, `compensate`, `onError`, `emit`, `Start`, `End`, `Return`, `success`, `failure`
- **Service types:** `grpc`, `http`, `kafka`, `redis`, `postgres`, `tcp`
- **Operators:** `->`, `=`, `!=`, `<`, `>`, `<=`, `>=`, `&&`, `||`, `!`
- **Comments:** `#`, `//` (line), `/* ... */` (block)
- **Duration suffixes:** `ms`, `s`, `m`, `h`, `d`, `w`, `M`, `y`

### Stage 2: Parser (`parser.go`)

Recursive descent parser consuming the token stream into the AST defined in `ast.go`.

**Top-level structure:** optional `version`, `flow <name>`, then sections in any order:
- `description` — flow description text
- `variables` / `constants` — typed declarations
- `output` — output type declarations
- `import` / `include` — external dependencies
- `service` blocks — service declarations with options
- `event` blocks — event declarations
- `retry` / `breaker` / `timeout` — resilience config
- `onError` — error handling with typed cases
- `compensate` — saga compensation mappings
- `workflow` — step definitions

**Service options** support 6 shapes: boolean (`tls`, `idempotent`), list (`brokers`, `headers`), map (`connection`), typed keywords (`type grpc`), and key-value with colon-containing values.

**Workflow steps** dispatch to 10 step types: `StepRef`, `IfBlock`, `SwitchBlock`, `ParallelBlock`, `WaitBlock`, `ForeachLoop`, `WhileLoop`, `EmitEvent`, `ReturnStep`, and sentinel `Start`/`End`.

### Stage 3: Semantic Analysis (`semantic.go`)

Validates all references in the workflow tree:
- Service references (`auth.CreateUser`) — checks left side matches a declared service
- Event references — checks event name is declared
- `onError` cases and fallback steps
- `compensate` entries reference actual workflow step names

Returns `[]SemanticError` with line numbers.

### Stage 4: IR Compilation (`ir.go`)

Converts AST into a directed graph (`IR` struct with `[]IRNode` + `[]IREdge`).

**Node types:** `start`, `end`, `step`, `if`, `merge`, `parallel`, `join`, `wait`, `emit`, `return`

**Edge model:** `from` → `to` with optional `condition` label (e.g., `"success"`, `"failure"` for if-blocks).

IR is JSON-serializable (`MarshalIR` / `UnmarshalIR`) for caching and transport.

### Stage 5: Code Generation (`codegen.go`)

Generates source code in 4 target languages:

| Target | Output |
|---|---|
| Go | Package with struct, service interfaces (`Call(ctx, method, req)`), `Execute` function |
| Rust | Struct with `#[derive(Debug, Clone)]`, service traits with async `call`, `async fn execute` |
| Java | Class with fields, inner service interfaces with `CompletableFuture` |
| Python | Class with `__init__`, inner `Protocol` classes, `async def execute` |

Type mapping: `string`/`int`/`float`/`bool` → language equivalents; unknown → `interface{}` / `serde_json::Value` / `Object` / `Any`.

## Service Types

| Type | Typical Options |
|---|---|
| gRPC | `address`, `tls`, `connection` map |
| HTTP | `url`, `headers` list, `method` |
| Kafka | `brokers` list, `topic`, `connection` map |
| Redis | `address`, `connection` map |
| Postgres | `address`, `connection` map, `idempotent` |
| TCP | `address` |

## Graph Visualization

`graph.go` produces two output formats:

- **Graphviz DOT:** Services as yellow ellipses, nodes as blue boxes, if=yellow diamonds, parallel/join=pink hexagons, emit=cyan notes, start/end=green ellipses
- **Mermaid:** `flowchart TD` with appropriate node shapes

## CLI

```
flow fmt *.flow          # Format .flow files in canonical style
flow validate signup.flow # Parse + semantic analysis
flow graph -format dot signup.flow  # Generate graph (dot or mermaid)
flow codegen -target go signup.flow # Generate source code
flow info signup.flow    # Print summary (name, services, nodes, edges)
flow help                # Usage
```

## LSP

`lsp.go` implements a subset of the Language Server Protocol:

| Method | Description |
|---|---|
| `textDocument/didOpen` | Parse + semantic analysis, store diagnostics |
| `textDocument/didChange` | Re-parse on change |
| `textDocument/formatting` | Return formatted document |
| `textDocument/completion` | Keyword + service name completions (trigger: `.`, `:`) |
| `textDocument/hover` | Service name hover info |

Currently a library implementation — needs stdin/stdout JSON-RPC transport for standalone use.

## Hot-Reload

`watcher.go` provides filesystem-based hot-reloading:

- **Polling model** (default 5s interval) — checks `ModTime` of `.flow` files
- **`DebouncedWatcher`** — wraps `FileWatcher` with debounce delay for editor-save scenarios
- Uses `Registry.LoadFile()` to reload changed flows

## Registry

`registry.go` is the runtime store for flow definitions:

- Thread-safe via `sync.RWMutex`
- IR cached under `flow:<name>:ir` with 5-minute TTL
- Topic-to-flow routing cached under `flow:route:<topic>`
- `GetByTopic()` enables event-driven flow triggering

## Files

| File | Purpose |
|---|---|
| `ast.go` | AST node types (Flow, Service, WorkflowStep, etc.) |
| `lexer.go` | Hand-written tokenizer |
| `parser.go` | Recursive descent parser |
| `semantic.go` | Semantic analysis |
| `ir.go` | AST → IR compilation |
| `codegen.go` | IR → Go/Rust/Java/Python source |
| `graph.go` | IR → Graphviz/Mermaid |
| `formatter.go` | Canonical .flow formatting |
| `cli.go` | CLI commands |
| `lsp.go` | Language Server Protocol |
| `watcher.go` | File watcher with debounce |
| `registry.go` | Runtime flow store |
| `flow_test.go` | Tests (lexer, parser, formatter, semantic, IR) |
| `codegen_test.go` | Tests (graph, codegen, LSP) |
| `registry_test.go` | Tests (registry, cache) |
