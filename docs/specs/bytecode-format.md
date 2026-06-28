# Bytecode Format Specification

## Overview

The `ExecutionPlan` is the compiled output of the DSL compiler. Bincode-serialized for FFI transfer.

## Instruction Encoding

Each instruction is **8 bytes** packed:

```
Bit:  63..48  47..32  31..16  15..8  7..0
      +-------+-------+-------+------+------+
      |   c   |   b   |   a   |flag  |opcode|
      +-------+-------+-------+------+------+
       u16     u16     u16     u8     u8
```

### Fields

| Field | Bits | Type | Description |
|-------|------|------|-------------|
| `opcode` | 7:0 | u8 | Operation code (0–22) |
| `flags` | 15:8 | u8 | Per-opcode modifier flags |
| `a` | 31:16 | u16 | Primary operand |
| `b` | 47:32 | u16 | Secondary operand |
| `c` | 63:48 | u16 | Tertiary operand |

### Rust Representation

```rust
#[repr(C)]
pub struct Instruction {
    pub op: OpCode,
    pub flags: u8,
    pub a: u16,
    pub b: u16,
    pub c: u16,
}
```

## Opcode Table

| # | Opcode | a | b | c |
|---|--------|---|---|---|
| 0 | Next | service_id | timeout_hi | timeout_lo |
| 1 | Parallel | count | first_svc | — |
| 2 | Collect | — | — | — |
| 3 | Fallback | service_id | — | — |
| 4 | Gate | field_const_id | value_const_id | — |
| 5 | Split | field_const_id | — | — |
| 6 | Map | expr_const_id | — | — |
| 7 | Emit | count | first_svc | — |
| 8 | Drop | — | — | — |
| 9 | Buffer | n | — | — |
| 10 | Key | field_const_id | — | — |
| 11 | Retry | flags | — | — |
| 12 | Pipe | — | — | — |
| 13 | Timeout | — | ms_hi | ms_lo |
| 14 | Async | service_id | timeout_hi | timeout_lo |
| 15 | Chunk | count | mode | — |
| 16 | Dag | dag_table_id | — | — |
| 17 | Jmp | ip_offset | — | — |
| 18 | Label | — | — | — |
| 19 | SvcArg | svc_id | — | — |
| 20 | RetryData | flags(max_attempts,strategy) | fixed_ms_hi | fixed_ms_lo |
| 21 | JumpOffset | offset | — | — |
| **22** | **TypeGuard** | **strict(0/1)** | — | — |

### TypeGuard

Opcode 22 validates the message body against the schema stored in `ExecutionPlan.schema`. When `strict=1`, a missing schema produces an error. Reads the plan's schema directly (no field/value operands).

The schema is also used by the compiler's pre-pass for **compile-time type inference** — validating Gate operators and Map expressions against declared field types before any bytecode is emitted (`type_check_gate()` and `type_check_map()` in `rust/src/dsl/compiler.rs`).

## ExecutionPlan

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
    pub map_exprs: Vec<MapExpr>,
    pub retry_configs: Vec<RetryConfig>,
    pub chunk_configs: Vec<ChunkConfig>,
    pub schema: Option<Schema>,
}
```

### ConstantPool

```rust
pub struct ConstantPool {
    pub strings: Vec<String>,
    pub lookup: HashMap<String, u16>,
}
```

### ServiceTable

```rust
pub struct ServiceTable {
    pub services: Vec<ServiceEntry>,
    pub lookup: HashMap<String, u16>,
}

pub struct ServiceEntry {
    pub id: u16,
    pub name: String,
}
```

### DAGTable

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

pub enum DAGFailurePolicy {
    AbortAll,
    ContinueOthers,
    SkipDependents,
}

pub enum MergeStrategy {
    LastWins,
    ArrayConcat,
    DeepMerge,
    ExplicitMap,
}
```

### Schema & ResolvedType

```rust
pub struct Schema {
    pub fields: Vec<FieldSchema>,
}

pub struct FieldSchema {
    pub name: String,
    pub r#type: ResolvedType,
    pub required: bool,
}

pub enum ResolvedType {
    String,
    Integer,
    Float,
    Boolean,
    Object,
    Array,
    Null,
    Any,
}

**Compile-time use:** `Schema` is read by the compiler's pre-pass to type-check Gate operators (`type_check_gate()`) and Map expressions (`type_check_map()`) before final bytecode emission. See `rust/src/dsl/compiler.rs`.

### RetryConfig / ChunkConfig

```rust
pub struct RetryConfig {
    pub max_attempts: u8,
    pub strategy: RetryStrategy,  // Exp | Linear | Fixed
    pub fixed_ms: u32,
}

pub struct ChunkConfig {
    pub count: u8,
    pub mode: ChunkMode,  // Seq | Par
}
```

## Serialization

Bincode across FFI boundary:

```rust
let bytes = bincode::serialize(&plan).unwrap();
let plan: ExecutionPlan = bincode::deserialize(&bytes).unwrap();
```

## Complexity Scoring

Computed at compile time by `calc_complexity()`:

| Opcode | Score |
|--------|-------|
| Next, Async | 10 |
| Parallel, DAG | 20 |
| Chunk | 25 |
| Gate | 5 |
| Map | 3 |
| Emit | 8 |
| Buffer | 15 |
| All others | 1 |

Used by the Go engine for lane routing (`flowrulz_plan_complexity` FFI).
