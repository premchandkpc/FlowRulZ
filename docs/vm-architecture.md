# VM Architecture

Rust bytecode VM — executor, memory management, tracing, plugin system.

## Overview

The VM is a stackless bytecode interpreter with a work-stealing scheduler. It processes `ExecutionPlan` instructions and makes service calls via FFI back to Go.

## Components

```
┌─────────────────────────────────────────┐
│                Rust VM                   │
├─────────────────────────────────────────┤
│  ┌─────────┐  ┌──────────┐  ┌────────┐ │
│  │ Executor│  │  Memory   │  │Tracing │ │
│  │         │  │  Arena    │  │ Spans  │ │
│  │  VM     │  │  Intern   │  │        │ │
│  │  DAG    │  │           │  │        │ │
│  │  Plugin │  │           │  │        │ │
│  └────┬────┘  └───────────┘  └────────┘ │
│       │                                  │
│  ┌────▼────┐                             │
│  │   FFI   │                             │
│  └─────────┘                             │
└─────────────────────────────────────────┘
```

## Executor

### VM (Sequential)

The core interpreter loop:

```rust
pub struct VM {
    plan: ExecutionPlan,
    ctx: ExecutionContext,
}
```

Execution:
1. Read instruction at `ctx.ip`
2. Decode opcode + operands
3. Execute operation
4. Update `ctx.ip` (or jump)
5. Return step result (Done/Pending/Continue/Failed)

### DAG Executor

For `OpCode::Dag` instructions:

1. Load `DAGTable` from plan
2. Topological sort into layers
3. Execute layer by layer
4. Within each layer, execute nodes in parallel
5. Collect results between layers
6. Apply merge strategy

### Parallel Executor

For `OpCode::Parallel` instructions:

1. Emit `StepPending` for each service
2. Go bridge calls services concurrently
3. Results collected as they arrive
4. `OpCode::Collect` merges into array

### Chunk Executor

For `OpCode::Chunk` instructions:

1. Split input array into chunks
2. Sequential mode: process one chunk at a time
3. Parallel mode: process all chunks concurrently
4. Each chunk goes through the pipeline

### Gate Executor

For `OpCode::Gate` instructions:

1. Extract field from payload using JMESPath
2. Apply gate operator (==, !=, >, <, >=, <=, contains)
3. Compare against expected value
4. If true: continue to next instruction
5. If false: skip next instruction (ip += 2)

### Map Executor

For `OpCode::Map` instructions:

1. Look up expression in constant pool
2. Evaluate JMESPath expression against payload
3. Replace payload with result

## Memory Management

### Arena Allocator

Pre-allocated memory blocks for fast allocation:

```rust
pub struct Arena {
    blocks: Vec<Block>,
    current: usize,
}
```

- Blocks are 64KB each
- Allocation is bump-pointer (O(1))
- Deallocation is bulk (drop entire arena)
- Used for temporary VM allocations

### String Interning

Deduplicate strings to reduce memory:

```rust
pub struct Interner {
    strings: HashMap<String, u16>,
    pool: Vec<String>,
}
```

- Strings are interned by index (u16)
- Constant pool uses interning
- Service table uses interning
- Reduces memory usage for repeated strings

## Execution Plan

```rust
pub struct ExecutionPlan {
    pub rule_id: String,
    pub version: u64,
    pub instr_count: u32,
    pub complexity_score: u32,
    pub instructions: Vec<Instruction>,
    pub const_pool: ConstantPool,
    pub services: ServiceTable,
    pub dag_tables: Vec<DAGTable>,
    pub retry_configs: Vec<RetryConfig>,
    pub chunk_configs: Vec<ChunkConfig>,
    pub schema: Option<Schema>,
}
```

### Constant Pool

Deduplicated string store:

```rust
pub struct ConstantPool {
    strings: Vec<String>,
}

impl ConstantPool {
    pub fn add(&mut self, s: &str) -> u16;
    pub fn get(&self, idx: u16) -> &str;
}
```

### Service Table

Service name registry:

```rust
pub struct ServiceTable {
    names: Vec<String>,
}

impl ServiceTable {
    pub fn add(&mut self, name: &str) -> u16;
    pub fn get(&self, idx: u16) -> &str;
    pub fn find(&self, name: &str) -> Option<u16>;
}
```

## Execution Context

```rust
pub struct ExecutionContext {
    pub event: Event,
    pub body: Vec<u8>,
    pub variables: HashMap<String, Vec<u8>>,
    pub outputs: HashMap<String, Vec<u8>>,
    pub headers: HashMap<String, String>,
    pub failed: bool,
    pub errors: Vec<String>,
    pub hop_count: u16,
    pub retry_count: u32,
    pub deadline_ms: u64,
    pub ip: usize,
}
```

### Event

```rust
pub struct Event {
    pub id: String,
    pub topic: String,
    pub payload: Vec<u8>,
    pub headers: HashMap<String, String>,
    pub metadata: EventMetadata,
}

pub struct EventMetadata {
    pub mode: Mode,
    pub reply_to: Option<String>,
    pub correlation_id: Option<String>,
    pub trace_id: Option<String>,
    pub timestamp: u64,
}
```

### Modes

| Mode | Value | Description |
|------|-------|-------------|
| Publish | 0 | Fire-and-forget |
| Request | 1 | Request/reply |
| Reply | 2 | Reply to request |
| Stream | 3 | Streaming |
| Workflow | 4 | Workflow execution |
| Internal | 5 | Internal cluster |

## Plugin System

### WASM Plugins

```rust
pub trait Plugin {
    fn call(&self, function: &str, input: &[u8]) -> Result<Vec<u8>, PluginError>;
}
```

- Plugins loaded from `.wasm` files
- Each plugin runs in isolated WASM instance
- Memory bounded by WASM runtime
- No network access (sandboxed)

### Plugin Loading

```rust
pub struct PluginLoader {
    plugins_dir: PathBuf,
    loaded: HashMap<String, Plugin>,
}
```

Plugins are loaded on first `w:` reference and cached.

## Tracing

### Span Recording

```rust
pub struct SpanRingBuffer {
    spans: Vec<Span>,
    capacity: usize,
}
```

- Ring buffer for trace spans
- `drain_global_buffer()` before emitting in tests
- Propagated via HTTP `X-Trace-ID` and gRPC metadata

### Trace Context

```rust
pub fn context_with_trace_id(ctx: Context, trace_id: String) -> Context;
pub fn trace_id_from_context(ctx: Context) -> Option<String>;
```

## Error Handling

### VM Errors

| Error | Description |
|-------|-------------|
| `InvalidOpcode` | Unknown instruction |
| `OutOfBounds` | Instruction pointer out of range |
| `SchemaMismatch` | Type guard failed |
| `ServiceNotFound` | Unknown service ID |
| `Timeout` | Execution deadline exceeded |
| `Serialization` | JSON serialization failed |

### Panic Safety

The VM has `defer recover()` — panics write error to `ResultCh`, callers never hang.

## Performance

### Complexity Scoring

| Operation | Score |
|-----------|-------|
| Next, Async | 10 |
| Parallel, DAG | 20 |
| Chunk | 25 |
| Gate | 5 |
| Map | 3 |
| Emit | 8 |
| Buffer | 15 |

### Lane Assignment

| Score | Lane | Concurrency |
|-------|------|-------------|
| < 10 | Fast | 50 |
| 10-50 | Normal | 20 |
| > 50 | Heavy | 5 |

### Work Stealing

When a lane is idle, workers steal from other lanes' queues. This prevents starvation and improves CPU utilization across cores.
