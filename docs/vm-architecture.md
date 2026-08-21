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

## Complete Execution Walkthrough

### Step-by-Step: `n:validate t500 | g:status==ok n:process f:dlq`

```
1. VM reads instruction 0: Next(service_id=0, timeout=500)
2. VM looks up service_id=0 in ServiceTable → "validate"
3. VM emits StepPending { svc_id: 0, body: <original_payload> }
4. Go bridge receives pending
5. Go bridge looks up "validate" in service registry → HTTP endpoint
6. Go bridge calls HTTP POST with body, 500ms timeout
7. Go bridge receives response
8. Go bridge feeds response back into VM
9. VM stores response as new body
10. VM advances to instruction 1

11. VM reads instruction 1: Gate(field="status", op=Eq, value="ok")
12. VM extracts "status" from body using JMESPath
13. VM compares status == "ok"
14. If TRUE: VM advances to instruction 2 (ip += 1)
15. If FALSE: VM skips instruction 2 (ip += 2)

16. VM reads instruction 2: Next(service_id=1, timeout=0)
17. VM calls "process" service
18. VM continues to instruction 3

19. VM reads instruction 3: Drop (or whatever follows)
20. Execution complete
```

### Step-by-Step: `p:fraud,inventory | c | n:fulfill`

```
1. VM reads Parallel(count=2, first_svc=0)
2. VM emits StepPending for svc_id=0 ("fraud")
3. VM emits StepPending for svc_id=1 ("inventory")
4. Go bridge calls both concurrently
5. Results arrive (any order) → stored in ExecutionContext.outputs

6. VM reads Collect
7. VM merges outputs into array: [fraud_result, inventory_result]
8. Array becomes new body

9. VM reads Next(service_id=2, timeout=0)
10. VM calls "fulfill" with merged array as input
```

### Step-by-Step: `dag:{a:[],b:[a],c:[a,b]}`

```
1. VM reads Dag(dag_table_id=0)
2. VM loads DAGTable from plan
3. VM topological sorts: layers = [[0], [1], [2]]
4. VM executes Layer 0: node "a" (no parents)
   → emit StepPending for "a"
   → wait for response
5. VM executes Layer 1: node "b" (parent: a)
   → emit StepPending for "b" with "a"'s result as input
   → wait for response
6. VM executes Layer 2: node "c" (parents: a, b)
   → emit StepPending for "c" with merged input
   → wait for response
7. VM applies merge strategy to combine results
```

## Error Propagation

### VM Error Types

| Error | Bytecode | Description |
|-------|----------|-------------|
| `InvalidOpcode` | any | Unknown instruction byte |
| `OutOfBounds` | any | IP exceeds instruction count |
| `SchemaMismatch` | TypeGuard | Payload doesn't match schema |
| `ServiceNotFound` | Next/Async/Emit | Unknown service ID |
| `Timeout` | any | Deadline exceeded |
| `Serialization` | Map | JSON serialization failed |
| `GateEvalError` | Gate | Field not found or type mismatch |

### Error Flow

```
VM error
  → StepFailed { error: "...", ip: N }
  → Go bridge receives failure
  → Go bridge checks for f: fallback
  → If fallback exists: route to fallback service
  → If no fallback: check onError handler
  → If no handler: write error to ResultCh
  → Caller receives error
```

### Panic Safety

The VM has `defer recover()` at the execution boundary:
```go
func execTask(ctx *ExecutionContext) (Result, error) {
    defer func() {
        if r := recover(); r != nil {
            // Write panic to result channel
            resultCh <- Result{Error: fmt.Sprintf("panic: %v", r)}
        }
    }()
    // ... VM execution
}
```

This means:
- Panics in Rust VM code write error to `ResultCh`
- Callers never hang waiting for a response
- The Go process never crashes from a VM panic

## Memory Layout

### Execution Plan (in memory)

```
ExecutionPlan {
    rule_id: String (heap)
    instructions: Vec<Instruction> (heap, 8 bytes each)
    const_pool: ConstantPool {
        strings: Vec<String> (heap)
    }
    services: ServiceTable {
        names: Vec<String> (heap)
    }
    dag_tables: Vec<DAGTable> (heap)
    retry_configs: Vec<RetryConfig> (heap)
    chunk_configs: Vec<ChunkConfig> (heap)
    schema: Option<Schema> (heap)
}
```

### ExecutionContext (per-execution)

```
ExecutionContext {
    event: Event {
        id: String (heap)
        topic: String (heap)
        payload: Vec<u8> (heap)
        headers: HashMap (heap)
        metadata: EventMetadata (stack)
    }
    body: Vec<u8> (heap) ← working payload
    variables: HashMap<String, Vec<u8>> (heap)
    outputs: HashMap<String, Vec<u8>> (heap) ← service call results
    headers: HashMap<String, String> (heap)
    ip: usize (stack)
    failed: bool (stack)
    errors: Vec<String> (heap)
    hop_count: u16 (stack)
    retry_count: u32 (stack)
    deadline_ms: u64 (stack)
}
```

### Arena Allocator Layout

```
Arena {
    blocks: Vec<Block> (heap)
        Block 0: [0..65535] bytes (heap)
        Block 1: [0..65535] bytes (heap)
        ...
    current: usize (stack) ← bump pointer
}
```

Allocation:
```rust
fn alloc(&mut self, size: usize) -> *mut u8 {
    if self.current + size > BLOCK_SIZE {
        self.new_block();
    }
    let ptr = self.blocks.last().unwrap().as_ptr().add(self.current);
    self.current += size;
    ptr
}
```

Deallocation: drop entire arena at once (O(1)).

## Tracing Integration

### Span Lifecycle

```
VM starts execution
  → SpanRingBuffer.push(Span::start("exec", trace_id))
  → Each instruction
    → SpanRingBuffer.push(Span::event("step", ip))
  → VM completes
    → SpanRingBuffer.push(Span::end("exec"))
  → Go bridge drains buffer
    → Sends to OpenTelemetry collector
```

### Trace Context Propagation

```
HTTP request
  → X-Trace-ID header
  → Go bridge extracts trace_id
  → Passes to VM via Event.metadata.trace_id
  → VM includes trace_id in all spans
  → Go bridge propagates to downstream services
```

## Plugin System Deep Dive

### WASM Plugin Loading

```rust
pub struct PluginLoader {
    plugins_dir: PathBuf,
    loaded: HashMap<String, Plugin>,
}

impl PluginLoader {
    pub fn load(&mut self, name: &str) -> Result<&Plugin, PluginError> {
        if let Some(p) = self.loaded.get(name) {
            return Ok(p);
        }
        let path = self.plugins_dir.join(format!("{}.wasm", name));
        let bytes = std::fs::read(&path)?;
        let module = wasmtime::Module::new(&engine, &bytes)?;
        let plugin = Plugin::new(module)?;
        self.loaded.insert(name.to_string(), plugin);
        Ok(self.loaded.get(name).unwrap())
    }
}
```

### Plugin Execution

```rust
impl Plugin {
    pub fn call(&self, function: &str, input: &[u8]) -> Result<Vec<u8>, PluginError> {
        let mut store = Store::new(&self.engine, ());
        let func = self.instance.get_func(&mut store, function)
            .ok_or(PluginError::FunctionNotFound)?;
        
        // Allocate memory in WASM for input
        let input_ptr = self.alloc_memory(&mut store, input)?;
        
        // Call the function
        func.call(&mut store, &[Val::I32(input_ptr), Val::I32(input.len() as i32)], 
                  &mut results)?;
        
        // Read output from WASM memory
        let output = self.read_memory(&mut store, results[0].i32()?)?;
        Ok(output)
    }
}
```
