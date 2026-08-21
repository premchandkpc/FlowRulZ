# DAG Execution

Directed acyclic graphs — complex dependency-based execution.

## Bytecode DSL

### Basic DAG

```
dag:{enrich:[],validate:[enrich],store:[validate]}
```

**Execution order:**
1. `enrich` — no dependencies, runs first
2. `validate` — depends on `enrich`, runs after
3. `store` — depends on `validate`, runs last

### Diamond DAG

```
dag:{fetch:[],transform:[fetch],validate:[fetch],store:[transform,validate]}
```

```
       fetch
      /     \
transform  validate
      \     /
       store
```

`transform` and `validate` run in parallel after `fetch`. `store` waits for both.

### Complex DAG

```
dag:{a:[],b:[],c:[a],d:[a,b],e:[c,d],f:[e]}
```

```
a   b
|   |
c   d
 \ /
  e
  |
  f
```

### DAG with Emit

```
dag:{enrich:[],validate:[enrich],store:[validate],notify:[store]} e:audit-log
```

Run the DAG, then emit to `audit-log` after completion.

## Flow DSL

### Explicit Parallel with Dependencies

```
version 1

flow DAGPipeline

service step-a
    type grpc
    address a:50051

service step-b
    type grpc
    address b:50051

service step-c
    type grpc
    address c:50051

service merge
    type grpc
    address merge:50051

workflow

Start

-> step-a.Run

parallel
    -> step-b.Run
    -> step-c.Run
join

-> merge.Combine

-> End
```

### Multi-Stage Pipeline

```
version 1

flow MultiStageETL

service extract
    type grpc
    address extract:50051

service transform-a
    type grpc
    address transform-a:50051

service transform-b
    type grpc
    address transform-b:50051

service validate
    type grpc
    address validate:50051

service load
    type postgres
    connection postgres://db:5432/warehouse

workflow

Start

-> extract.Pull

parallel
    -> transform-a.Normalize
    -> transform-b.Enrich
join

-> validate.Check

-> load.Store

-> End
```

## DAG Configuration

### Failure Policies

| Policy | Behavior | Use When |
|--------|----------|----------|
| `AbortAll` | Cancel all nodes on first failure | Strict consistency needed |
| `ContinueOthers` | Let non-dependent nodes finish | Partial results acceptable |
| `SkipDependents` | Skip downstream of failed node | Fail-soft with partial output |

### Merge Strategies

| Strategy | Behavior | Example |
|----------|----------|---------|
| `LastWins` | Last node's result overwrites | Single source of truth |
| `ArrayConcat` | Concatenate all results | Collecting list results |
| `DeepMerge` | Recursively merge objects | Combining partial updates |
| `ExplicitMap` | Map results by node name | Named result access |

### Node Timeouts

Each DAG node can have an individual timeout:

```
dag:{fast:[],slow:[fast]} timeout:fast=1000,slow=5000
```

### Distributed DAG

For large DAGs across cluster nodes:

```
dag:{a:[],b:[a],c:[a],d:[b,c]} distributed
```

The `distributed` flag enables the scheduler to assign different DAG nodes to different cluster nodes based on load.

## Bytecode Encoding

DAGs compile to a `DAGTable` in the execution plan:

```
DAGTable {
    nodes: [
        DAGNode { service_id, layer: 0, parent_ids: [] },
        DAGNode { service_id, layer: 1, parent_ids: [0] },
        DAGNode { service_id, layer: 2, parent_ids: [1] },
    ],
    layers: [[0], [1], [2]],
    terminal_nodes: [2],
    failure_policy: AbortAll,
    merge_strategy: LastWins,
    distributed: false,
}
```

## Use Cases

| Pattern | Example |
|---------|---------|
| ETL pipeline | Extract, Transform, Validate, Load |
| Microservice orchestration | Auth, Enrich, Process, Store, Notify |
| Data fusion | Merge results from multiple sources |
| CI/CD pipeline | Build, Test, Deploy, Verify |
| ML pipeline | Preprocess, Train, Evaluate, Publish |
