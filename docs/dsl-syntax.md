# Bytecode DSL Reference

The compact pipeline DSL compiles to Rust VM bytecode. Single-line, infix syntax.

## Overview

```
<op>:<args> <op>:<args> ...
```

Operations execute left-to-right. Output of one feeds the next. Use `|` as a visual separator (removed by optimizer).

```
t500 n:validate | p:fraud,inventory | c | f:dlq | n:fulfill | e:notify,analytics
```

## Operations

### Service Calls

| Op | Syntax | Description | Bytecode |
|----|--------|-------------|----------|
| Next | `n:<service>` | Synchronous call — blocks until response | `OpCode::Next` |
| Async | `a:<service>` | Fire-and-forget — returns immediately | `OpCode::Async` |
| Emit | `e:<svc1>,<svc2>` | Publish to one or more services | `OpCode::Emit` |

```
n:validate          # call "validate", wait for result
a:audit-log         # call "audit-log", don't wait
e:notify,analytics  # publish to both "notify" and "analytics"
```

### Parallelism

| Op | Syntax | Description | Bytecode |
|----|--------|-------------|----------|
| Parallel | `p:<svc1>,<svc2>` | Fan-out to multiple services concurrently | `OpCode::Parallel` |
| Collect | `c` | Merge parallel results into array | `OpCode::Collect` |

```
p:fraud,inventory   # call both simultaneously
c                   # merge their results
```

### Control Flow

| Op | Syntax | Description | Bytecode |
|----|--------|-------------|----------|
| Gate | `g:<field><op><value>` | Conditional branch | `OpCode::Gate` |
| Fallback | `f:<service>` | Route to service on failure | `OpCode::Fallback` |
| Drop | `d` | Halt execution immediately | `OpCode::Drop` |
| Label | `<name>:` | Define a jump target | `OpCode::Label` |
| Jump | `j:<label>` | Unconditional jump to label | `OpCode::Jmp` |

```
g:amount>10000 n:manual-review    # only if amount > 10000
f:dlq                             # if anything fails, send to DLQ
start: n:auth j:end               # labels for structured jumps
```

### Data Operations

| Op | Syntax | Description | Bytecode |
|----|--------|-------------|----------|
| Map | `m:<expr>` | Transform payload via JMESPath | `OpCode::Map` |
| Key | `k:<field>` | Set routing key from field | `OpCode::Key` |
| Split | `s:<field>` | Split array by field | `OpCode::Key` |
| Buffer | `b<N>` | Accumulate N messages before processing | `OpCode::Buffer` |
| Chunk | `chunk:<N>:<mode>` | Split into chunks of N | `OpCode::Chunk` |

```
m:{status: "processed"}           # transform to new shape
k:order_id                        # route by order_id field
b10                               # buffer 10 messages, then process
chunk:100:seq                     # split into chunks of 100, sequentially
```

### DAG

| Op | Syntax | Description | Bytecode |
|----|--------|-------------|----------|
| DAG | `dag:{<edges>}` | Directed acyclic graph execution | `OpCode::Dag` |

```
dag:{enrich:[],validate:[enrich],store:[validate],notify:[store]}
```

Format: `dag:{node:[parent1,parent2],...}` — nodes without parents execute first.

### Schema

| Op | Syntax | Description | Bytecode |
|----|--------|-------------|----------|
| Schema | `schema:{<fields>}` | Type-guard the incoming payload | `OpCode::TypeGuard` |

```
schema:{!order_id:string,!amount:float,metadata:any}
```

Fields prefixed with `!` are required.

### Modifiers

| Op | Syntax | Description |
|----|--------|-------------|
| Retry | `r<N>:<strategy>` | Retry policy for preceding call |
| Timeout | `t<ms>` | Timeout in milliseconds for preceding call |
| Delay | `delay:<ms>` | Delay execution by N milliseconds |

```
n:validate t500           # 500ms timeout on validate
r3:exp                    # 3 retries, exponential backoff
r5:lin                    # 5 retries, linear backoff
r3:fixed:200              # 3 retries, 200ms fixed delay
delay:5000 n:svc          # wait 5s before calling svc
```

## Gate Operators

| Op | Meaning |
|----|---------|
| `==` | Equal |
| `!=` | Not equal |
| `>` | Greater than |
| `<` | Less than |
| `>=` | Greater or equal |
| `<=` | Less or equal |
| `contains` | Substring or list membership |

```
g:status==active n:activate
g:role!=admin f:access-denied
g:amount>10000 n:manual-review
g:tags.contains vip n:priority-queue
```

## Retry Strategies

| Strategy | Syntax | Behavior |
|----------|--------|----------|
| Exponential | `r3:exp` | 100ms → 200ms → 400ms (doubles each attempt) |
| Linear | `r5:lin` | 100ms → 200ms → 300ms → 400ms → 500ms |
| Fixed | `r3:fixed:200` | 200ms → 200ms → 200ms |

## Chunk Modes

| Mode | Syntax | Behavior |
|------|--------|----------|
| Sequential | `chunk:10:seq` | Process chunks one at a time |
| Parallel | `chunk:4:par` | Process chunks concurrently |

## Schema Types

| Type | Supports |
|------|----------|
| `string` | Ordering, contains |
| `int` | Ordering, numeric comparisons |
| `float` | Ordering, numeric comparisons |
| `bool` | Equality only |
| `object` | Equality only |
| `array` | Contains only |
| `null` | Equality only |
| `any` | All operators pass |
| `enum[v1\|v2]` | Equality against allowed values |

## Built-in Map Functions

### String

`lower(s)`, `upper(s)`, `trim(s)`, `length(s)`, `substring(s,start,end)`, `replace(s,from,to)`, `split(s,delim)`, `concat(a,b)`, `contains(list,val)`

### Numeric

`abs(n)`, `round(n)`, `ceil(n)`, `floor(n)`, `min(a,b)`, `max(a,b)`

### Type Conversion

`to_string(v)`, `parse_int(s)`, `parse_float(s)`, `parse_bool(s)`, `typeof(v)`

### Encoding

`base64(s)`, `base64_decode(s)`, `hash(s)`, `json(s)`

### Object/Array

`keys(obj)`, `merge(a,b)`, `coalesce(a,b)`, `default(field,val)`

### Utility

`uuid()`, `now()`, `epoch()`

## Complete Examples

### Order Processing Pipeline

```
t500 n:validate | p:fraud,inventory | c | g:fraud_score>0.8 f:dlq | n:fulfill | e:notify,analytics
```

### High-Value Order Review

```
g:amount>10000 n:manual-review r3:exp f:auto-reject
```

### DAG with Dependencies

```
dag:{enrich:[],validate:[enrich],store:[validate],notify:[store]} e:audit-log
```

### Schema-Validated Pipeline

```
schema:{!order_id:string,!amount:float,!user:string} t500 n:validate e:notify
```

### Labeled Jump Flow

```
start: n:auth g:role==admin n:admin-panel j:end n:user-panel end: e:done
```

### Batch Processing

```
chunk:10:seq n:storage
```

### Delayed Execution

```
delay:5000 n:svc
```

### Buffered Aggregation

```
b100 m:{batch: @, count: length(@)} n:analytics
```

## Bytecode Representation

Each operation compiles to an 8-byte instruction:

```
Bit:  63..48  47..32  31..16  15..8  7..0
      +-------+-------+-------+------+------+
      |   c   |   b   |   a   |flag  |opcode|
      +-------+-------+-------+------+------+
       u16     u16     u16     u8     u8
```

See [bytecode-format.md](bytecode-format.md) for full instruction set details.
