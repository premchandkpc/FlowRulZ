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

| Op | Syntax | Description | Bytecode | Timeout |
|----|--------|-------------|----------|---------|
| Next | `n:<service>` | Synchronous call — blocks until response | `OpCode::Next` | configurable |
| Async | `a:<service>` | Fire-and-forget — returns immediately | `OpCode::Async` | N/A |
| Emit | `e:<svc1>,<svc2>` | Publish to one or more services | `OpCode::Emit` | N/A |

```
n:validate          # call "validate", wait for result
a:audit-log         # call "audit-log", don't wait
e:notify,analytics  # publish to both "notify" and "analytics"
```

**How service calls work:**
1. VM reads the instruction, looks up service_id in the ServiceTable
2. VM emits `StepPending { svc_id, body }` to the Go bridge
3. Go bridge resolves the actual endpoint (HTTP/gRPC/Kafka) from the service registry
4. Go bridge makes the call and returns the response
5. VM feeds the response back into the execution context as the new body
6. VM advances to the next instruction

**Edge cases:**
- If a service is not registered, the VM returns `StepFailed` with a `ServiceNotFound` error
- If the service call panics (in Go), the bridge's `defer recover()` catches it and returns an error
- `e:` with multiple targets publishes to all simultaneously; if one fails, others still succeed
- `a:` does not wait — the response is discarded, execution continues immediately

### Parallelism

| Op | Syntax | Description | Bytecode |
|----|--------|-------------|----------|
| Parallel | `p:<svc1>,<svc2>` | Fan-out to multiple services concurrently | `OpCode::Parallel` |
| Collect | `c` | Merge parallel results into array | `OpCode::Collect` |

```
p:fraud,inventory   # call both simultaneously
c                   # merge their results
```

**How parallel works:**
1. `p:svc1,svc2,svc3` emits one `StepPending` per service
2. Go bridge calls all services concurrently (goroutines)
3. Results arrive as services respond (any order)
4. `c` (collect) merges results into a JSON array in call order
5. The array becomes the new body for the next operation

**Edge cases:**
- If one parallel call fails, `c` still collects whatever succeeded; the failed slot is `null`
- Use `f:dlq` after `c` to handle partial failures
- `p:svc1,svc2` without `c` means results are discarded (just parallel side effects)
- Maximum parallelism is bounded by the Heavy lane (5 concurrent VMs)

**Complete parallel patterns:**

```
# Basic fan-out/fan-out
p:svc1,svc2,svc3 | c

# Parallel with individual fallbacks
p:svc1 f:dlq1,svc2 f:dlq2 | c

# Parallel emit (fire-and-forget all)
e:svc1,svc2,svc3

# Parallel then gate on collected results
p:check-a,check-b | c g:results[0]==pass n:proceed f:reject
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

**Gate behavior (the skip mechanism):**
- Gate evaluates the condition against the current body
- If TRUE: execution continues to the next instruction (ip += 1)
- If FALSE: execution skips the next instruction (ip += 2)
- This is how if/else works — gate skips the "then" branch

**Fallback behavior:**
- `f:svc` is NOT a standalone operation — it modifies the *preceding* instruction
- If the preceding `n:svc` fails, execution routes to `f:svc` instead of halting
- `f:dlq` after `n:process` means: try process, on failure go to DLQ
- Multiple fallbacks chain: `n:a f:b f:c` — try a, then b, then c

**Labeled jumps (if/else in bytecode):**

```
# Pattern: if condition then A else B
g:condition n:A j:skip n:B skip:

# Pattern: nested if/else
g:x>0 n:positive g:x>100 n:very-positive j:end n:mildly-positive end:

# Pattern: while loop
loop: g:count<10 n:process increment_count j:loop done:
```

**Edge cases:**
- Label names must be unique within a pipeline
- Jump targets are resolved at compile time — invalid labels cause compile error
- `d` (drop) halts execution immediately — no fallback, no error, just stops
- Gate with string comparison: `g:name=="john"` (quotes around string values)

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

**Map expression syntax:**
```
# Replace entire body
m:{key: "value", count: @.items.length}

# Extract fields
m:{user_id: @.user.id, email: @.user.email}

# Compute values
m:{total: @.items[*].price | sum(@), avg: @.items[*].price | avg(@)}

# Conditional in map
m:{status: @.score > 80 ? "pass" : "fail"}

# Add metadata
m:{data: @, processed_at: now(), version: "1.0"}
```

**Buffer vs Chunk:**
- `b100` buffers 100 individual messages, then processes them as a batch
- `chunk:10:par` takes an array and splits it into chunks of 10 for parallel processing
- Buffer is for message-level aggregation; chunk is for array-level splitting

**Key operation:**
- `k:field` sets the routing key for downstream message routing
- Used with Kafka to partition messages by field value
- Example: `k:customer_id` routes all events from the same customer to the same partition

### DAG

| Op | Syntax | Description | Bytecode |
|----|--------|-------------|----------|
| DAG | `dag:{<edges>}` | Directed acyclic graph execution | `OpCode::Dag` |

```
dag:{enrich:[],validate:[enrich],store:[validate],notify:[store]}
```

Format: `dag:{node:[parent1,parent2],...}` — nodes without parents execute first.

**DAG execution layers:**
```
dag:{a:[],b:[],c:[a],d:[a,b],e:[c,d],f:[e]}

Layer 0: a, b (no dependencies — run in parallel)
Layer 1: c (depends on a), d (depends on a,b — wait for both)
Layer 2: e (depends on c,d — wait for both)
Layer 3: f (depends on e)
```

**DAG failure policies:**
- `AbortAll` (default): cancel all running nodes on first failure
- `ContinueOthers`: let non-dependent nodes finish
- `SkipDependents`: skip downstream of failed node

**DAG merge strategies:**
- `LastWins`: last node's result overwrites
- `ArrayConcat`: concatenate all results into array
- `DeepMerge`: recursively merge result objects
- `ExplicitMap`: map results by node name

**Edge cases:**
- Circular dependencies cause compile error
- Empty DAG `dag:{}` is a no-op
- Single node DAG `dag:{a:[]}` is equivalent to `n:a`
- DAG nodes cannot use gate/fallback — they are pure service calls

### Schema

| Op | Syntax | Description | Bytecode |
|----|--------|-------------|----------|
| Schema | `schema:{<fields>}` | Type-guard the incoming payload | `OpCode::TypeGuard` |

```
schema:{!order_id:string,!amount:float,metadata:any}
```

Fields prefixed with `!` are required.

**Schema type checking at compile time:**
```
# This compiles — amount is int, > is valid
schema:{amount:int} g:amount>100 n:process

# This fails at compile — string does not support >
schema:{name:string} g:name>100
```

**Schema type matrix:**

| Type | `==` | `!=` | `>` | `<` | `>=` | `<=` | `contains` |
|------|------|------|-----|-----|------|------|------------|
| string | y | y | y | y | y | y | y |
| int | y | y | y | y | y | y | - |
| float | y | y | y | y | y | y | - |
| bool | y | y | - | - | - | - | - |
| object | y | y | - | - | - | - | - |
| array | - | - | - | - | - | - | y |
| null | y | y | - | - | - | - | - |
| any | y | y | y | y | y | y | y |
| enum | y | y | - | - | - | - | - |

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

**Retry timing:**
```
r3:exp      → attempt 1: 100ms, attempt 2: 200ms, attempt 3: 400ms
r5:lin      → attempt 1: 100ms, 2: 200ms, 3: 300ms, 4: 400ms, 5: 500ms
r3:fixed:200 → attempt 1: 200ms, 2: 200ms, 3: 200ms
```

**Modifier stacking:**
```
n:flaky-service t3000 r3:exp    # timeout + retry
n:slow-api t10000 r2:lin f:cache # timeout + retry + fallback
```

**Edge cases:**
- Retry only applies to the *preceding* `n:` or `a:` instruction
- Timeout is per-attempt, not total — `n:svc t1000 r3:exp` means 1s per attempt, up to 3 attempts
- Delay is absolute — `delay:5000 n:svc` waits 5s before the call
- Modifiers cannot be applied to `e:` (emit) or `c` (collect)

## Gate Operators

| Op | Meaning | Example |
|----|---------|---------|
| `==` | Equal | `g:status==active` |
| `!=` | Not equal | `g:role!=admin` |
| `>` | Greater than | `g:amount>1000` |
| `<` | Less than | `g:score<50` |
| `>=` | Greater or equal | `g:age>=18` |
| `<=` | Less or equal | `g:temp<=0` |
| `contains` | Substring or list membership | `g:tags.contains vip` |

**String comparison:**
```
g:name=="john"          # exact match
g:name!="jane"          # not equal
g:email.contains @      # contains substring
g:status==active        # no quotes needed for simple words
```

**Numeric comparison:**
```
g:amount>1000           # greater than
g:count>=10             # greater or equal
g:score<0.5             # less than
g:balance<=0            # less or equal
```

**Array comparison:**
```
g:tags.contains vip     # array contains "vip"
g:items.contains widget # array contains "widget"
```

## Retry Strategies

| Strategy | Syntax | Behavior | Best For |
|----------|--------|----------|----------|
| Exponential | `r3:exp` | 100ms → 200ms → 400ms (doubles each attempt) | Transient failures |
| Linear | `r5:lin` | 100ms → 200ms → 300ms → 400ms → 500ms | Rate limiting |
| Fixed | `r3:fixed:200` | 200ms → 200ms → 200ms | Predictable retry |

**Backoff formulas:**
```
Exponential: delay_ms * 2^(attempt-1)
Linear:      delay_ms * attempt
Fixed:       delay_ms (constant)
```

## Chunk Modes

| Mode | Syntax | Behavior | Use Case |
|------|--------|----------|----------|
| Sequential | `chunk:10:seq` | Process chunks one at a time | Rate-limited APIs |
| Parallel | `chunk:4:par` | Process chunks concurrently | High-throughput ETL |

## Schema Types

| Type | Supports | Example |
|------|----------|---------|
| `string` | Ordering, contains | `g:name=="john"` |
| `int` | Ordering, numeric | `g:count>10` |
| `float` | Ordering, numeric | `g:score>=0.5` |
| `bool` | Equality only | `g:active==true` |
| `object` | Equality only | `g:meta=={}` |
| `array` | Contains only | `g:tags.contains vip` |
| `null` | Equality only | `g:data==null` |
| `any` | All operators pass | `g:x>0` (always valid) |
| `enum[v1\|v2]` | Equality against allowed values | `g:status==active` |

## Built-in Map Functions (31 total)

### String (9)

| Function | Syntax | Description |
|----------|--------|-------------|
| `lower` | `lower(s)` | Lowercase |
| `upper` | `upper(s)` | Uppercase |
| `trim` | `trim(s)` | Trim whitespace |
| `length` | `length(s)` | String length |
| `substring` | `substring(s,start,end)` | Extract substring |
| `replace` | `replace(s,from,to)` | String replacement |
| `split` | `split(s,delim)` | Split into array |
| `concat` | `concat(a,b)` | Concatenate strings |
| `contains` | `contains(list,val)` | Check membership |

### Numeric (6)

| Function | Syntax | Description |
|----------|--------|-------------|
| `abs` | `abs(n)` | Absolute value |
| `round` | `round(n)` | Round to nearest int |
| `ceil` | `ceil(n)` | Round up |
| `floor` | `floor(n)` | Round down |
| `min` | `min(a,b)` | Minimum of two values |
| `max` | `max(a,b)` | Maximum of two values |

### Type Conversion (5)

| Function | Syntax | Description |
|----------|--------|-------------|
| `to_string` | `to_string(v)` | Any to string |
| `parse_int` | `parse_int(s)` | String to int |
| `parse_float` | `parse_float(s)` | String to float |
| `parse_bool` | `parse_bool(s)` | String to bool |
| `typeof` | `typeof(v)` | Get type name |

### Encoding (4)

| Function | Syntax | Description |
|----------|--------|-------------|
| `base64` | `base64(s)` | Base64 encode |
| `base64_decode` | `base64_decode(s)` | Base64 decode |
| `hash` | `hash(s)` | Hash string |
| `json` | `json(s)` | Serialize to JSON |

### Object/Array (4)

| Function | Syntax | Description |
|----------|--------|-------------|
| `keys` | `keys(obj)` | Get object keys |
| `merge` | `merge(a,b)` | Deep merge objects |
| `coalesce` | `coalesce(a,b)` | First non-null |
| `default` | `default(field,val)` | Default if null |

### Utility (3)

| Function | Syntax | Description |
|----------|--------|-------------|
| `uuid` | `uuid()` | Generate UUID |
| `now` | `now()` | Current ISO timestamp |
| `epoch` | `epoch()` | Current epoch seconds |

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

### Labeled Jump Flow (if/else)
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

### Complete Resilience Chain
```
schema:{!order_id:string} t500 n:validate r3:exp | p:fraud,inventory | c g:score>0.8 f:dlq | n:fulfill r2:lin t2000 f:cached | e:notify,audit
```

### Labeled Loop (while pattern)
```
init: m:{count: 0} loop: g:count<10 n:process m:{count: @.count + 1} j:loop done: e:complete
```

## Anti-Patterns

### Don't: Unbounded parallel
```
# BAD — could overwhelm downstream services
p:svc1,svc2,svc3,svc4,svc5,svc6,svc7,svc8,svc9,svc10 | c

# BETTER — chunk the parallelism
chunk:3:par n:batch-process
```

### Don't: Gate without fallback
```
# BAD — silently drops if condition is false
g:status==active n:process

# BETTER — explicit fallback
g:status==active n:process f:handle-inactive
```

### Don't: Retry on non-transient errors
```
# BAD — retrying a 400 (bad request) won't help
n:api r5:exp

# BETTER — only retry on timeout/5xx
n:api t3000 r3:exp f:handle-error
```

### Don't: Deeply nested gates
```
# BAD — hard to read and maintain
g:a==1 g:b==2 g:c==3 g:d==4 n:deep-process

# BETTER — use DAG or labeled jumps
dag:{validate:[enrich],process:[validate],store:[process]}
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
