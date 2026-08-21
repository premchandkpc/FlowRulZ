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
