# Parallel Processing

Fan-out/fan-in, concurrent service calls, DAG execution.

## Bytecode DSL

### Basic Parallel + Collect

```
p:fraud-check,inventory-check | c
```

Call both services simultaneously. `c` (collect) merges results into an array.

**Execution flow:**
1. VM emits `StepPending` for `fraud-check` and `inventory-check`
2. Go control plane calls both concurrently
3. When both respond, `collect` merges `[fraud_result, inventory_result]`
4. Next operation receives the merged array

### Parallel with Fallback

```
p:primary,secondary | c f:emergency
```

Try both services in parallel. If either fails, fall back to `emergency`.

### Parallel Emit

```
e:notify,email,sms
```

Publish to three services simultaneously. No collection — fire-and-forget to all.

### Parallel with Gate

```
p:check-a,check-b | c g:result[0]==pass n:proceed f:reject
```

Run two checks in parallel, collect results, then gate on the first result.

## Flow DSL

### Basic Parallel

```
version 1

flow ParallelChecks

service fraud
    type grpc
    address fraud:50051

service inventory
    type grpc
    address inventory:50051

workflow

Start

parallel
    -> fraud.Check
    -> inventory.Check
join

-> fulfillment.Process

-> End
```

### Parallel with Different Service Types

```
version 1

flow MultiProtocol

service grpc-svc
    type grpc
    address grpc:50051

service http-svc
    type http
    url https://http.internal/api

service kafka-svc
    type kafka
    brokers kafka:9092
    topic results

workflow

Start

parallel
    -> grpc-svc.Query
    -> http-svc.Fetch
    -> kafka-svc.Publish
join

-> aggregator.Merge

-> End
```

### Parallel in Conditional Branch

```
version 1

flow ConditionalParallel

service fast-path
    type grpc
    address fast:50051

service slow-path
    type grpc
    address slow:50051

service db
    type postgres
    connection postgres://db:5432/app

workflow

Start

-> if order.priority == "high"
    then
        parallel
            -> fast-path.Process
            -> fast-path.Validate
        join
    else
        parallel
            -> slow-path.Process
            -> slow-path.Validate
        join

-> db.Store

-> End
```

### Nested Parallelism

```
version 1

flow NestedParallel

service auth
    type grpc
    address auth:50051

service check-a
    type grpc
    address check-a:50051

service check-b
    type grpc
    address check-b:50051

service db
    type postgres
    connection postgres://db:5432/app

workflow

Start

-> auth.Validate

parallel
    parallel
        -> check-a.Run
        -> check-b.Run
    join

    -> db.Write
join

-> End
```

## DAG Execution

For complex dependency graphs, use the DAG operator.

### Basic DAG

```
dag:{enrich:[],validate:[enrich],store:[validate],notify:[store]}
```

**Execution layers:**
1. Layer 0: `enrich` (no dependencies)
2. Layer 1: `validate` (depends on `enrich`)
3. Layer 2: `store` (depends on `validate`)
3. Layer 3: `notify` (depends on `store`)

### Diamond DAG

```
dag:{fetch:[],transform:[fetch],validate:[fetch],store:[transform,validate]}
```

`fetch` runs first. `transform` and `validate` run in parallel (both depend on `fetch`). `store` runs after both complete.

### DAG with Failure Policy

The VM supports three failure policies for DAGs:

| Policy | Behavior |
|--------|----------|
| `AbortAll` | Cancel all running nodes on first failure |
| `ContinueOthers` | Let non-dependent nodes finish |
| `SkipDependents` | Skip nodes that depend on the failed node |

### DAG Merge Strategies

| Strategy | Behavior |
|----------|----------|
| `LastWins` | Last node's result overwrites |
| `ArrayConcat` | Concatenate all results into array |
| `DeepMerge` | Recursively merge result objects |
| `ExplicitMap` | Map results by node name |

## Concurrency Model

The VM uses a work-stealing scheduler:

| Lane | Concurrency | Use Case |
|------|-------------|----------|
| Fast (score < 10) | 50 concurrent | Simple gates, maps |
| Normal (score ≤ 50) | 20 concurrent | Sequential chains, emits |
| Heavy (score > 50) | 5 concurrent | Parallel, DAG, chunk |

Complexity scores per operation:

| Operation | Score |
|-----------|-------|
| Next, Async | 10 |
| Parallel, DAG | 20 |
| Chunk | 25 |
| Gate | 5 |
| Map | 3 |
| Emit | 8 |
| Buffer | 15 |

## Use Cases

| Pattern | Example |
|---------|---------|
| Fraud + inventory check | Validate order from multiple angles |
| Multi-source aggregation | Fetch from API + DB + cache simultaneously |
| Fan-out notifications | Send email + SMS + push at once |
| DAG pipeline | ETL with dependency stages |
| Load balancing | Parallel calls to redundant services |

## Edge Cases

### 1. Partial Failure in Parallel
When one parallel call fails, `c` still collects results. Failed slots are `null`:
```
p:svc1,svc2,svc3 | c
# If svc2 fails: result = [result1, null, result3]
```

### 2. Empty Parallel
```
p: | c
# No services — collect returns empty array []
```

### 3. Single Service Parallel
```
p:only-one | c
# Equivalent to n:only-one, result is [result]
```

### 4. Parallel Without Collect
```
p:svc1,svc2
# Results are discarded — just parallel side effects
```

### 5. Collect Without Parallel
```
n:svc1 | c
# c is a no-op — svc1 result is the body
```

### 6. DAG Layer Execution
```
dag:{a:[],b:[a],c:[a,b]}
# Layer 0: a (runs first)
# Layer 1: b (waits for a)
# Layer 2: c (waits for a AND b)
# c receives both a and b results merged
```

### 7. DAG Failure Policy Impact
```
# AbortAll — if a fails, b and c never run
dag:{a:[],b:[a],c:[b]}  policy=AbortAll

# ContinueOthers — b fails, c still runs if c depends on a
dag:{a:[],b:[a],c:[a]}  policy=ContinueOthers

# SkipDependents — a fails, b is skipped (depends on a), c runs (depends on a but policy skips)
dag:{a:[],b:[a],c:[a,b]}  policy=SkipDependents
```

### 8. Work Stealing Under Load
When the Heavy lane (parallel/DAG) is busy and the Fast lane is idle:
- Fast lane workers steal from Heavy lane queue
- This prevents starvation
- But stolen tasks may run on the wrong lane (performance impact)
