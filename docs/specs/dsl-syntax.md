# DSL Syntax Specification

## Overview

A compact, single-line DSL for defining message routing pipelines. The compiler compiles DSL → AST → optimized AST → bytecode `ExecutionPlan`.

## Pipeline Structure

A pipeline is a sequence of **operations** separated by spaces:

```
[schema:{...}] [t<timeout>] <operations...>
```

## Operations

| Op | Syntax | Description |
|----|--------|-------------|
| **Next** | `n:<service>` | Forward message to service |
| **Async** | `a:<service>` | Fire-and-forget with no wait |
| **Parallel** | `p:<svc1>,<svc2>,...` | Fan-out to multiple services |
| **Collect** | `c` | Collect parallel results into JSON array |
| **Fallback** | `f:<service>` | Route on failure |
| **Gate** | `g:<field><op><value>` | Conditional jump |
| **Split** | `s:<field>` | Split array message by field |
| **Map** | `m:<expr>` | Transform message fields |
| **Emit** | `e:<svc1>,<svc2>` | Fire-and-forget publish |
| **Drop** | `d` | Halt processing (dead end) |
| **Key** | `k:<field>` | Routing key field |
| **Pipe** | `\|` | Pass-thru (nop, removed by optimizer) |
| **Timeout** | `t<ms>` | Set timeout for subsequent calls |
| **Retry** | `r<N>:<strategy>` | Attach retry policy to preceding call |
| **Chunk** | `chunk:<N>:<mode>` | Split payload into chunks |
| **DAG** | `dag:{<edges>}` | Directed acyclic graph routing |
| **Schema** | `schema:{<field specs>}` | Attach type schema (TypeGuard) |
| **Label** | `<name>:` | Target for Jmp |
| **Jmp** | `j:<label>` | Unconditional jump |

## Detailed Reference

### Next (Service Call)

```
n:<service_name>
```

Synchronous call to a named service. Waits for response.

### Async

```
a:<service_name>
```

Non-blocking call; execution continues immediately.

### Timeout

```
t<milliseconds>
```

Sets the timeout for the next service call. The optimizer hoists timeouts to precede their associated `n:` operation.

**Examples:**
```
t500 n:validate
t1000 n:ship
```

### Retry

```
r<N>:<strategy>[:<param>]
```

Attaches a retry policy to the preceding service call. Must directly follow a Next or Fallback.

**Strategies:**
| Strategy | Syntax | Behavior |
|----------|--------|----------|
| Exponential | `r3:exp` | 2^x backoff (1s, 2s, 4s) |
| Fixed | `r3:fixed:200` | Fixed 200ms interval |
| Linear | `r3:lin:500` | Linear 500ms, 1000ms, 1500ms |

**Examples:**
```
n:validate r3:exp
n:payment r3:fixed:100
```

### Gate (Conditional Branch)

```
g:<field><operator><value> <on-true> [f:<on-false>]
```

Evaluates a JSON field against a value. On match, executes the next operation; on failure, jumps to Fallback. Field path navigation emits a structured `FieldNotFound` error (not silent null) for missing intermediate fields.

**Operators:**
| Op | Meaning |
|----|---------|
| `==` | Equal |
| `!=` | Not equal |
| `>` | Greater than |
| `<` | Less than |
| `>=` | Greater or equal |
| `<=` | Less or equal |
| `contains` | Substring/array membership |

**Field paths** support dotted navigation:
```
g:user.role==admin n:admin-panel f:user-panel
g:amount>10000 n:manual-review f:auto-approve
g:tags.containsurgent n:priority-queue
```

**Compile-time type checking:** When a `schema:{...}` is present, the compiler validates Gate operators against field types at compile time:
- `>`, `<`, `>=`, `<=` require the field type to be `int`, `float`, or `string` (rejects `bool`, `object`, `array`)
- `contains` requires the field type to be `string` or `array`
- `==`, `!=` are always allowed (any scalar)
- Fields not in the schema are allowed (assumed dynamic)
- Errors are reported as `TypeMismatch` during compilation and surfaced via the admin validate endpoint

### Pipe

```
<operation1> | <operation2>
```

A no-op separator. The optimizer removes pipe nodes.

### Parallel / Collect

```
p:<svc_a>,<svc_b>,<svc_c> c
```

Fan-out to multiple services in parallel. `c` (collect) merges all responses into a JSON array under the `_parallel` field.

**Validation:** `c` must immediately follow a Parallel.

### Fallback

```
f:<service_name>
```

Executed when the preceding operation fails.

### Split

```
s:<field>
```

Split a message by the specified field. Each element of the array field is processed independently.

### Map (Field Transformation)

```
m:<expr>
```

Evaluates an expression and transforms the message. Expressions use dots for field paths and support function calls.

**Built-in functions (21 total):**

| Function | Description |
|----------|-------------|
| `uuid()` | Generate UUID v4 |
| `now()` | Current ISO timestamp |
| `epoch()` | Unix timestamp (seconds since epoch) |
| `lower(s)` | Lowercase string |
| `upper(s)` | Uppercase string |
| `trim(s)` | Trim whitespace |
| `length(s)` | String length |
| `concat(a, b)` | Concatenate strings |
| `base64(s)` | Base64 encode |
| `json(s)` | Parse JSON string |
| `substring(s, start, end)` | Substring |
| `replace(s, from, to)` | String replace |
| `to_string(v)` | Coerce any value to string |
| `parse_int(s)` | Parse string → integer |
| `parse_float(s)` | Parse string → float |
| `coalesce(a, b)` | First non-null value |
| `default(field, val)` | Field or default value if null/missing |
| `contains(list, val)` | Array membership check |
| `keys(obj)` | Object key extraction |
| `merge(a, b)` | Deep merge two objects |
| `hash(alg, s)` | Hash (md5/sha1/sha256) |

`call_builtin` takes `&[serde_json::Value]` (not `&[&str]`).

**Examples:**
```
m:.processed_at=now()
m:.user_id=.id
m:.display=upper(.name)
m:.greeting='hello ' + .name
m:.payload=json(.raw_json)
m:.hash=hash(md5, .email)
```

### Emit

```
e:<service_a>,<service_b>,...
```

Fire-and-forget publish to one or more services.

### Drop

```
d
```

Terminates pipeline execution immediately.

### Key (Routing Key)

```
k:<field>
```

Sets the routing key used for partitioning.

### Chunk

```
chunk:<N>:<mode> <operation>
```

Splits the message into chunks of size N before the subsequent operation.

**Modes:**
| Mode | Description |
|------|-------------|
| `seq` | Process chunks sequentially |
| `par` | Process chunks in parallel |

### DAG (Directed Acyclic Graph)

```
dag:{<node>: [<dependencies>], ...} e:<output>
```

Declarative DAG routing. Each node is a service; dependencies are listed as a comma-separated list.

**Syntax:**
```
dag:{A:[],B:[A],C:[A],D:[B,C]} e:output
```

Creates:
```
Layer 0: A
Layer 1: B, C (parallel, depend on A)
Layer 2: D (depends on B and C)
```

After all layers complete, results are merged. `e:<service>` emits the merged result.

**Validation at compile time:**
- Cycle detection (error on cycles)
- Unknown service references
- Compile-time DAGTable fields: `failure_policy` (AbortAll/ContinueOthers/SkipDependents), `node_timeouts`, `merge_strategy` (LastWins/ArrayConcat/DeepMerge/ExplicitMap), `distributed`

### Schema (Type Guard)

```
schema:{field:type,!required_field:type}
```

Attaches a type schema to the pipeline. The compiler emits a `TypeGuard` opcode that validates the incoming message body against the schema at runtime.

**Type tags:**
| Tag | Rust Type |
|-----|-----------|
| `string` | `ResolvedType::String` |
| `int` | `ResolvedType::Integer` |
| `float` | `ResolvedType::Float` |
| `bool` | `ResolvedType::Boolean` |
| `object` | `ResolvedType::Object` |
| `array` | `ResolvedType::Array` |
| `null` | `ResolvedType::Null` |
| `any` | `ResolvedType::Any` |

Fields prefixed with `!` are required (error if missing). Non-required fields default to `Null` when absent.

**Examples:**
```
schema:{name:string,!age:int,!amount:float} n:validate
schema:{id:string} n:process
```

**Compile-time type inference:** When a schema is present, the compiler runs a pre-pass that type-checks all Gate and Map operations before emitting bytecode. See "Compile-time Type Checking" above for Gate rules. For Map expressions, `concat()` and `+` operators require all field arguments to be `string` type. Fields not declared in the schema are assumed dynamic (no compile-time check).

### Labels and Jumps

```
<label_name>: <operation> j:<label_name>
```

Labels mark a position in the pipeline. Jumps transfer control unconditionally.

**Example:**
```
start: n:auth g:role==admin n:admin-panel j:end n:user-panel end: e:done
```

## Full Pipeline Examples

**Simple validation and routing:**
```
t500 n:validate t1000 p:fraud,inventory c f:dlq n:fulfill e:notify,analytics
```

**Gate with retry:**
```
g:amount>10000 n:manual-review r3:exp f:auto-reject
```

**DAG with downstream emit:**
```
dag:{enrich:[],validate:[enrich],store:[validate]} e:audit-log
```

**Schema-typed pipeline:**
```
schema:{!order_id:string,!amount:float,!user:string} t500 n:validate e:notify
```

## Complexity Scoring

`complexity_score` is computed at compile time for lane routing:

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

**Lane assignment:**
| Score | Lane |
|-------|------|
| < 10 | fast |
| ≤ 50 | normal |
| > 50 | heavy |

## Error Handling

| Error | Cause |
|-------|-------|
| Empty pipeline | No operations provided |
| Invalid token | Unrecognized syntax |
| FieldNotFound | Missing field in path navigation |
| Collect without parallel | `c` not preceded by `p:` |
| Retry without service | `r:` not following a call |
| Unknown service in DAG | Dependency references missing node |
| Cycle detected | DAG has directed cycles |
| Duplicate label | Label name used twice |
| Undefined jump target | `j:` references non-existent label |
| SchemaParseError | Invalid schema field spec |
| TypeGuard | Runtime type validation failure |
| TypeMismatch | Compile-time type check failure (operator/field type incompatibility) |
