# Bytecode Format

Instruction encoding, opcodes, execution plan structure.

## Instruction Format

Each instruction is 8 bytes (64 bits), packed:

```
Bit:  63..48  47..32  31..16  15..8  7..0
      +-------+-------+-------+------+------+
      |   c   |   b   |   a   |flag  |opcode|
      +-------+-------+-------+------+------+
       u16     u16     u16     u8     u8
```

| Field | Width | Description |
|-------|-------|-------------|
| `opcode` | u8 | Operation code (0-255) |
| `flags` | u8 | Modifier flags |
| `a` | u16 | First operand |
| `b` | u16 | Second operand |
| `c` | u16 | Third operand |

## Complete Opcode Table

| # | Opcode | a | b | c | flags | Description |
|---|--------|---|---|---|-------|-------------|
| 0 | `Next` | service_id | timeout_hi | timeout_lo | bit0=has_retry | Synchronous service call |
| 1 | `Parallel` | count | first_svc | — | — | Fan-out to multiple services |
| 2 | `Collect` | — | — | — | — | Merge parallel results |
| 3 | `Fallback` | service_id | — | — | — | Route on failure |
| 4 | `Gate` | field_const_id | value_const_id | — | gate_op (0-6) | Conditional branch |
| 6 | `Map` | expr_const_id | — | — | — | Transform payload |
| 7 | `Emit` | count | first_svc | — | — | Fire-and-forget publish |
| 8 | `Drop` | — | — | — | — | Halt execution |
| 9 | `Buffer` | n | — | — | — | Accumulate N messages |
| 10 | `Key` | field_const_id | — | — | — | Set routing key |
| 14 | `Async` | service_id | timeout_hi | timeout_lo | bit0=has_retry | Fire-and-forget call |
| 15 | `Chunk` | count | mode | — | — | Split into chunks |
| 16 | `Dag` | dag_table_id | — | — | — | DAG execution |
| 17 | `Jmp` | ip_offset | — | — | — | Unconditional jump |
| 18 | `Label` | — | — | — | — | Jump target (no-op) |
| 19 | `SvcArg` | svc_id | — | — | — | Service argument |
| 21 | `JumpOffset` | offset | — | — | — | Relative jump |
| 22 | `TypeGuard` | strict | — | — | — | Schema validation |
| 23 | `SvcCall` | service_id | — | — | — | Service call (alt) |
| 24 | `Delay` | — | delay_hi | delay_lo | — | Delayed execution |

## Gate Operators

| Value | Operator | Description |
|-------|----------|-------------|
| 0 | `Eq` | Equal |
| 1 | `Ne` | Not equal |
| 2 | `Gt` | Greater than |
| 3 | `Lt` | Less than |
| 4 | `Gte` | Greater or equal |
| 5 | `Lte` | Less or equal |
| 6 | `Contains` | Substring/membership |

## Retry Strategies

| Value | Strategy | Behavior |
|-------|----------|----------|
| 0 | `Exponential` | 100ms → 200ms → 400ms (doubles) |
| 1 | `Linear` | 100ms → 200ms → 300ms (adds 100ms) |
| 2 | `Fixed` | Configurable fixed delay |

## Chunk Modes

| Value | Mode | Behavior |
|-------|------|----------|
| 0 | `Sequential` | Process chunks one at a time |
| 1 | `Parallel` | Process chunks concurrently |

## DAG Failure Policies

| Value | Policy | Behavior |
|-------|--------|----------|
| 0 | `AbortAll` | Cancel all on first failure |
| 1 | `ContinueOthers` | Let non-dependent finish |
| 2 | `SkipDependents` | Skip downstream of failed |

## DAG Merge Strategies

| Value | Strategy | Behavior |
|-------|----------|----------|
| 0 | `LastWins` | Last result overwrites |
| 1 | `ArrayConcat` | Concatenate results |
| 2 | `DeepMerge` | Recursive merge |
| 3 | `ExplicitMap` | Map by node name |

## Execution Plan Structure

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

### Instruction

```rust
pub struct Instruction {
    pub op: u8,
    pub flags: u8,
    pub a: u16,
    pub b: u16,
    pub c: u16,
}
```

### Constant Pool

```rust
pub struct ConstantPool {
    strings: Vec<String>,
}
```

- Indexed by u16
- Deduplicated on insert
- Used for field names, expressions, values

### Service Table

```rust
pub struct ServiceTable {
    names: Vec<String>,
}
```

- Indexed by u16
- Maps service_id to service name
- Used by Go bridge to resolve actual service endpoints

### DAG Table

```rust
pub struct DAGTable {
    pub nodes: Vec<DAGNode>,
    pub layers: Vec<Vec<u16>>,
    pub terminal_nodes: Vec<u16>,
    pub failure_policy: DAGFailurePolicy,
    pub node_timeouts: Vec<u32>,
    pub merge_strategy: MergeStrategy,
    pub distributed: bool,
}

pub struct DAGNode {
    pub service_id: u16,
    pub layer: u16,
    pub parent_ids: Vec<u16>,
}
```

### Schema

```rust
pub struct Schema {
    pub fields: Vec<FieldSchema>,
}

pub struct FieldSchema {
    pub name: String,
    pub r#type: ResolvedType,
    pub required: bool,
}
```

### ResolvedType

| Variant | Description |
|---------|-------------|
| `String` | Text |
| `Integer` | Integer |
| `Float` | Floating point |
| `Boolean` | Boolean |
| `Object` | JSON object |
| `Array` | JSON array |
| `Null` | Null value |
| `Any` | Any type |
| `Enum(Vec<String>)` | Restricted values |

## Serialization

Plans are serialized using bincode for compact binary encoding:

```
DSL string → Lexer → Tokens → Parser → Pipeline → Optimizer → Compiler → ExecutionPlan → bincode → bytes
```

### Deserialization

```
bytes → bincode → ExecutionPlan → VM execution
```

## Instruction Examples

### Next (service call)

```
n:validate t500

opcode: 0 (Next)
a: service_id for "validate"
b: 500 >> 8 = 1 (timeout high byte)
c: 500 & 0xFF = 244 (timeout low byte)
flags: 0 (no retry)
```

### Gate

```
g:amount>10000

opcode: 4 (Gate)
a: const_id for "amount"
b: const_id for "10000"
c: 0
flags: 2 (Gt)
```

### Parallel

```
p:fraud,inventory

opcode: 1 (Parallel)
a: 2 (count)
b: service_id for "fraud"
c: 0
flags: 0
```

### Map

```
m:{status: "processed"}

opcode: 6 (Map)
a: const_id for the JMESPath expression
b: 0
c: 0
flags: 0
```

### Emit

```
e:notify,analytics

opcode: 7 (Emit)
a: 2 (count)
b: service_id for "notify"
c: 0
flags: 0
```

## Complexity Scoring

Each instruction contributes to the plan's complexity score:

| Opcode | Score |
|--------|-------|
| Next | 10 |
| Async | 10 |
| Parallel | 20 |
| DAG | 20 |
| Chunk | 25 |
| Collect | 1 |
| Fallback | 1 |
| Gate | 5 |
| Map | 3 |
| Emit | 8 |
| Buffer | 15 |
| Drop | 1 |
| Key | 1 |
| Label | 1 |
| Jmp | 1 |
| TypeGuard | 1 |
| Delay | 1 |

The total score determines lane assignment:

| Score | Lane | Concurrency |
|-------|------|-------------|
| < 10 | Fast | 50 |
| 10-50 | Normal | 20 |
| > 50 | Heavy | 5 |

## Opcode Deep Dive

### Next (0) — Synchronous Service Call

```
Fields:
  a = service_id (index into ServiceTable)
  b = timeout_hi (timeout_ms >> 8)
  c = timeout_lo (timeout_ms & 0xFF)
  flags.bit0 = has_retry (1 = retry configured)

Behavior:
  1. Extract service name from ServiceTable[service_id]
  2. Extract timeout from (b << 8) | c
  3. Emit StepPending { svc_id, body }
  4. Wait for response
  5. If response received: body = response, ip += 1
  6. If timeout: StepFailed { error: "timeout" }
  7. If has_retry: apply retry policy on failure

Encoding example:
  n:validate t500
  opcode=0, a=service_id_for("validate"), b=500>>8=1, c=500&0xFF=244, flags=0
```

### Parallel (1) — Fan-Out

```
Fields:
  a = count (number of services)
  b = first_svc_id (first service in range)
  c = unused

Behavior:
  1. For i in 0..count:
     - Emit StepPending { svc_id: first_svc + i, body }
  2. Wait for all responses
  3. Store responses in ExecutionContext.outputs
  4. ip += 1

Encoding example:
  p:fraud,inventory
  opcode=1, a=2, b=service_id_for("fraud"), c=0, flags=0
```

### Collect (2) — Merge Parallel Results

```
Fields:
  a, b, c = unused

Behavior:
  1. Read ExecutionContext.outputs
  2. Merge into JSON array [result_0, result_1, ...]
  3. body = merged array
  4. ip += 1

Note: Must follow a Parallel instruction. Results are in call order, not response order.
```

### Fallback (3) — Error Routing

```
Fields:
  a = fallback_service_id

Behavior:
  1. If preceding instruction failed:
     - Call fallback_service with original body
     - body = fallback response
  2. If preceding instruction succeeded:
     - Do nothing (pass through)
  3. ip += 1

Note: Fallback modifies the PRECEDING instruction's behavior. It is not standalone.
```

### Gate (4) — Conditional Branch

```
Fields:
  a = field_const_id (field name in ConstantPool)
  b = value_const_id (comparison value in ConstantPool)
  c = unused
  flags = gate_op (0-6)

Behavior:
  1. Extract field from body using JMESPath
  2. Parse comparison value from ConstantPool
  3. Apply gate operator
  4. If TRUE: ip += 1 (continue to next instruction)
  5. If FALSE: ip += 2 (skip next instruction)

Gate operators:
  0 = Eq, 1 = Ne, 2 = Gt, 3 = Lt, 4 = Gte, 5 = Lte, 6 = Contains

Encoding example:
  g:amount>10000
  opcode=4, a=const_id_for("amount"), b=const_id_for("10000"), c=0, flags=2(Gt)
```

### Map (6) — Transform Payload

```
Fields:
  a = expr_const_id (JMESPath expression in ConstantPool)

Behavior:
  1. Read expression from ConstantPool[expr_const_id]
  2. Evaluate JMESPath against body
  3. body = result
  4. ip += 1

Encoding example:
  m:{status: "processed"}
  opcode=6, a=const_id_for("{status: \"processed\"}"), b=0, c=0, flags=0
```

### Emit (7) — Fire-and-Forget Publish

```
Fields:
  a = count (number of services)
  b = first_svc_id

Behavior:
  1. For i in 0..count:
     - Publish body to service (first_svc + i)
     - Don't wait for response
  2. ip += 1

Note: Emit is non-blocking. If a service is unreachable, the publish is silently dropped.
```

### Drop (8) — Halt Execution

```
Fields: none

Behavior:
  1. Set failed = false (drop is not an error)
  2. Return StepDone
  3. Execution terminates
```

### Buffer (9) — Accumulate Messages

```
Fields:
  a = N (buffer threshold)

Behavior:
  1. Add current message to buffer
  2. If buffer.count >= N:
     - body = buffer.collect()
     - ip += 1
  3. Else:
     - Return StepPending (wait for more messages)

Note: Buffer spans multiple executions. The VM state is persisted between messages.
```

### Key (10) — Routing Key

```
Fields:
  a = field_const_id

Behavior:
  1. Extract field value from body
  2. Set routing key for downstream partitioning
  3. body unchanged
  4. ip += 1

Note: Used with Kafka to ensure messages with the same key go to the same partition.
```

### Async (14) — Fire-and-Forget Call

```
Fields:
  a = service_id
  b = timeout_hi (unused for async, but encoded)
  c = timeout_lo
  flags.bit0 = has_retry

Behavior:
  1. Emit StepPending { svc_id, body }
  2. Don't wait for response
  3. ip += 1

Note: Async is non-blocking. The service call happens in the background.
```

### Chunk (15) — Split Array

```
Fields:
  a = chunk_size
  b = mode (0=sequential, 1=parallel)

Behavior:
  1. body must be a JSON array
  2. Split into chunks of chunk_size
  3. If sequential: process chunks one by one
  4. If parallel: process all chunks concurrently
  5. Merge results
  6. ip += 1
```

### DAG (16) — Directed Acyclic Graph

```
Fields:
  a = dag_table_id (index into plan.dag_tables)

Behavior:
  1. Load DAGTable from plan
  2. Topological sort into layers
  3. Execute layer by layer
  4. Within layer: execute nodes in parallel
  5. Between layers: wait for all nodes to complete
  6. Apply merge strategy
  7. ip += 1
```

### Jmp (17) — Unconditional Jump

```
Fields:
  a = ip_offset (relative offset)

Behavior:
  1. ip = ip + ip_offset

Note: Used with labels to implement loops and if/else. Labels compile to Jmp instructions.
```

### Label (18) — Jump Target

```
Fields: none

Behavior:
  1. No-op (ip += 1)
  2. Marks a position for Jmp instructions

Note: Labels are resolved at compile time. The Label instruction itself does nothing.
```

### TypeGuard (22) — Schema Validation

```
Fields:
  a = strict (0=warn, 1=error)

Behavior:
  1. Load schema from plan
  2. Validate body against schema
  3. If strict=1 and validation fails: StepFailed
  4. If strict=0 and validation fails: log warning, continue
  5. ip += 1
```

### Delay (24) — Deferred Execution

```
Fields:
  b = delay_hi (delay_ms >> 8)
  c = delay_lo (delay_ms & 0xFF)

Behavior:
  1. Extract delay from (b << 8) | c
  2. Schedule delayed execution
  3. Return StepPending
  4. After delay: resume execution from ip + 1

Note: Delay persists the execution context and resumes after the timer fires.
```
