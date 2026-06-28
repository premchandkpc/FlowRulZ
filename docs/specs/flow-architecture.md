# Flow Architecture

## Overview

Every message through FlowRulZ follows a path across 4 layers: **Admin API → Engine → Bridge/FFI → Rust VM**.
This doc traces every flow scenario end-to-end with the files involved at each step.

```
Client ─→ Admin API (Go) ─→ Engine (Go) ─→ Bridge (CGo) ─→ FFI (Rust) ─→ VM (Rust)
                               │                                                │
                               └── Persistence (file)                           │
                                                                                │
Kafka ─→ Transport (Go) ─→ Engine.ExecuteAll() ────────────────────────────────┘
```

---

## 1. Rule Deployment Flow

**Scenario:** Admin deploys a new rule via HTTP POST.

### Sequence

```
Client                     Admin API                   Engine              Bridge           Rust FFI
  │                           │                          │                   │                │
  │  POST /rules              │                          │                   │                │
  │  {"id":"my-rule",         │                          │                   │                │
  │   "dsl":"n:validate"}     │                          │                   │                │
  │──────────────────────────►│                          │                   │                │
  │                           │  engine.Deploy()         │                   │                │
  │                           │─────────────────────────►│                   │                │
  │                           │                          │  bridge.Compile() │                │
  │                           │                          │──────────────────►│                │
  │                           │                          │                   │                │
  │                           │                          │                   │ flowrulz_compile│
  │                           │                          │                   │────────────────►│
  │                           │                          │                   │                │
  │                           │                          │                   │ ◄── bincode plan│
  │                           │                          │  ◄── plan bytes ──┤                │
  │                           │                          │                   │                │
  │                           │                          │  bridge.PlanComplexity()            │
  │                           │                          │──────────────────►│                │
  │                           │                          │                   │ flowrulz_plan_ │
  │                           │                          │                   │   complexity() │
  │                           │                          │  ◄── score ───────┤                │
  │                           │                          │                   │                │
  │                           │                          │  create VersionedPlan              │
  │                           │                          │  assign Lane (fast/normal/heavy)  │
  │                           │                          │  saveRules() (JSON file)          │
  │                           │                          │  return success                   │
  │  ◄── 201 Created ────────┤                          │                   │                │
  │  {"id":"my-rule"}         │                          │                   │                │
```

### Files Involved

| Step | File | What It Does |
|------|------|-------------|
| HTTP handler | `go/internal/admin/server.go` | Parses JSON request, calls `engine.Deploy(id, dsl)` |
| API key check | `go/internal/admin/server.go` | `auth()` middleware checks `Authorization: Bearer` against `FLOWRULZ_API_KEY` |
| Rule engine | `go/internal/engine/engine.go` | `Deploy()` compiles DSL, computes complexity, creates `VersionedPlan`, assigns lane, calls `saveRules()` |
| Bridge call | `go/internal/bridge/bridge.go` | `Compile()` copies strings to C buffers, calls `C.flowrulz_compile()` |
| Bridge call | `go/internal/bridge/bridge.go` | `PlanComplexity()` calls `C.flowrulz_plan_complexity()` |
| Rust FFI | `rust/src/ffi.rs` | `flowrulz_compile()`: lex → parse → optimize → compile → bincode serialize |
| Rust FFI | `rust/src/ffi.rs` | `flowrulz_plan_complexity()`: deserialize plan → return `complexity_score` |
| DSL Lexer | `rust/src/dsl/lexer.rs` | Tokenizes DSL string into `Vec<Token>` |
| DSL Parser | `rust/src/dsl/parser.rs` | Builds `Pipeline` (AST) from tokens |
| DSL Optimizer | `rust/src/dsl/optimizer.rs` | Removes dead code, hoists timeouts, merges emits |
| DSL Compiler | `rust/src/dsl/compiler.rs` | Compiles AST → `ExecutionPlan`, calls `calc_complexity()`, stores schema |
| Persistence | `go/internal/engine/engine.go` | `saveRules()`: serializes all rules to JSON file at `FLOWRULZ_PERSIST_PATH` |

### Full Example

```bash
# Deploy
curl -X POST http://localhost:8080/rules \
  -H "Authorization: Bearer my-key" \
  -d '{"id":"order-flow","dsl":"schema:{!order_id:string,!amount:float} t500 n:validate e:notify,analytics"}'

# Response: 201
{"id":"order-flow"}
```

---

## 2. Message Execution Flow

**Scenario:** A Kafka message arrives and runs through all deployed rules.

### Sequence

```
Transport                  Engine                  Bridge                     Rust VM
   │                         │                       │                          │
   │  msg body (JSON)        │                       │                          │
   │────────────────────────►│                       │                          │
   │                         │                       │                          │
   │                         │  ExecuteAll(body)     │                          │
   │                         │  lock RLock           │                          │
   │                         │  collect active plans │                          │
   │                         │  for each rule:       │                          │
   │                         │    vp.ActiveExec.Add()│                          │
   │                         │  unlock RUnlock       │                          │
   │                         │                       │                          │
   │                         │  for each plan:       │                          │
   │                         │    bridge.Execute()   │                          │
   │                         │──────────────────────►│                          │
   │                         │                       │   flowrulz_execute()     │
   │                         │                       │─────────────────────────►│
   │                         │                       │                          │
   │                         │                       │   VM::new(plan,body)     │
   │                         │                       │   VM::run()              │
   │                         │                       │     dispatch(Next)       │
   │                         │                       │       │                  │
   │                         │                       │     caller_cb(ctx_id,   │
   │                         │                       │       svc_id, body)     │
   │                         │                       │◄─────────────────────────│
   │                         │                       │                          │
   │                         │  ◄── Go callback ─────┤                          │
   │                         │  sync.Map lookup      │                          │
   │                         │  by ctx_id            │                          │
   │                         │                       │                          │
   │                         │  dispatch(Next)       │                          │
   │                         │  ... loop until done  │                          │
   │                         │                       │                          │
   │                         │                       │  ◄── result ────────────│
   │                         │  ◄── result bytes ────┤                          │
   │                         │                       │                          │
   │                         │  vp.ActiveExec.Done() │                          │
   │                         │                       │                          │
   │  ◄── results ──────────┤                       │                          │
```

### Files Involved

| Step | File | What It Does |
|------|------|-------------|
| Transport consumer | `go/internal/transport/` | Polls Kafka, invokes handler with message bytes |
| Engine ExecuteAll | `go/internal/engine/engine.go` | RLock, collects active plans with `ActiveExec.Add(1)`, calls `bridge.Execute()` for each |
| Bridge Execute | `go/internal/bridge/bridge.go` | Stores `ServiceCaller` in `sync.Map` under `ctx_id`, calls `C.flowrulz_execute()` |
| FFI Execute | `rust/src/ffi.rs` | Deserializes plan, creates `VM`, sets context fields, runs VM |
| VM dispatch | `rust/src/executor/mod.rs` | Main loop: reads instruction, calls `dispatch()`, emits span |
| Op handlers | `rust/src/executor/next.rs` | `exec_next()`: calls service via FFI callback, handles retry |
| Service callback | `go/internal/bridge/bridge.go` | `//export goServiceCaller`: looks up `ServiceCaller` by `ctx_id` in `sync.Map` |
| Span emit | `rust/src/tracing/` | `emit_span()`: pushes to thread-local ring buffer |

### Full Example

```go
// Engine.ExecuteAll() called by transport handler
results, err := eng.ExecuteAll([]byte(`{"order_id":"ORD-123","amount":99.99}`), svcCaller)
```

---

## 3. Service Call Flow (VM → Go)

**Scenario:** VM hits a Next instruction and needs to call a Go service.

### Sequence

```
VM.dispatch(OpCode::Next)
    │
    ▼
exec_next(body, instr, plan, caller)
    │
    ├── Get service name from plan.services[instr.a]
    ├── Get timeout from instr.b/c
    │
    ▼
caller(svc_id, body, timeout)
    │
    ▼
caller_cb(ctx_id, svc_id, body_ptr, body_len, resp_ptr, &resp_len)
    │
    ├── C FFI boundary
    │
    ▼
callerBridge (C helper)
    │
    ▼
goServiceCaller (Go //export)
    │
    ├── ctx_id lookup in sync.Map → get ServiceCaller func
    ├── Call ServiceCaller(svc_id, body)
    │
    ▼
ServiceCaller returns ([]byte, error)
    │
    ├── Write response to resp buffer
    ├── Set resp_len
    └── Return 0 (ok) or -1 (error)
```

### Files Involved

| File | Layer | Role |
|------|-------|------|
| `rust/src/executor/next.rs` | Rust | `exec_next()` wraps service call with timeout + retry logic |
| `rust/src/ffi.rs` | Rust | Creates closure calling `caller_cb` function pointer |
| `go/internal/bridge/caller_bridge.c` | C | Static C function `callerBridge` that forwards to `goServiceCaller` |
| `go/internal/bridge/bridge.go` | Go | `//export goServiceCaller`: dispatches to registered `ServiceCaller` by `ctx_id` |
| `go/internal/engine/engine.go` | Go | Creates `svcCaller` closure passed to `bridge.Execute()` |

---

## 4. Version Promote/Rollback Flow

**Scenario:** Admin promotes an older version of a rule to active.

### Sequence

```
Client                     Admin API                  Engine
  │                           │                         │
  │  POST /rules/{id}/promote │                         │
  │  ?version=1              │                         │
  │──────────────────────────►│                         │
  │                           │  engine.Promote(id, 1)  │
  │                           │────────────────────────►│
  │                           │                         │
  │                           │                         │  lock mutex
  │                           │                         │  find rule by id
  │                           │                         │  find version by version num
  │                           │                         │  set ActiveVersion = index
  │                           │                         │  unlock mutex
  │                           │                         │  saveRules() (persist new active)
  │                           │                         │
  │  ◄── 200 OK ─────────────┤                         │
  │  {"id":"my-rule",        │                         │
  │   "active_version":1}   │                         │
```

### Drain Flow

```
POST /rules/{id}/drain?version=1
    │
    ▼
engine.Drain(id, 1)
    │
    ├── lock mutex, find version
    ├── unlock mutex
    ├── vp.ActiveExec.Wait()  ← blocks until all in-flight executions finish
    ├── lock mutex
    ├── remove version from array
    ├── adjust ActiveVersion index
    ├── if no versions left: delete rule
    └── unlock + saveRules()
```

### Files Involved

| File | What It Does |
|------|-------------|
| `go/internal/admin/server.go` | `promoteVersion()` handler: parses `version` query param, calls `engine.Promote()` |
| `go/internal/engine/engine.go` | `Promote()`: finds version, sets `ActiveVersion` |
| `go/internal/engine/engine.go` | `Drain()`: waits for `ActiveExec.WaitGroup`, removes version |
| `go/internal/engine/engine.go` | `VersionedPlan.ActiveExec`: `sync.WaitGroup` incremented before `bridge.Execute()`, decremented after |

---

## 5. DAG Execution Flow

**Scenario:** A DAG rule executes with 4 nodes across 3 layers.

### DSL

```
dag:{A:[],B:[A],C:[A],D:[B,C]} e:output
```

### DAG Structure

```
Layer 0:    A
             │
      ┌──────┴──────┐
      ▼             ▼
Layer 1:    B             C
             │             │
      ┌──────┴─────────────┴──────┐
      ▼                           ▼
Layer 2:              D
                      │
                      ▼
               emit: output
```

### Execution Sequence

```
VM.dispatch(OpCode::Dag)
    │
    ▼
exec_dag(body, instr, plan, caller, arena)
    │
    ├── Load DAGTable from plan.dag_tables[instr.a]
    │
    ├── Layer 0: [A]
    │   └── spawn parallel tasks for each node in layer
    │       └── A: call service A, store result
    │
    ├── Layer 1: [B, C]  (parallel—depend on A)
    │   └── rayon scope:
    │       ├── B: call service B with A's result as input
    │       └── C: call service C with A's result as input
    │
    ├── Layer 2: [D]  (depends on B and C)
    │   └── D: call service D with merged (B+C) input
    │
    ├── Merge terminal nodes (D) via merge_strategy
    └── Return merged result
```

### Files Involved

| File | What It Does |
|------|-------------|
| `rust/src/executor/dag.rs` | `exec_dag()`: topological execution, parallel per layer, result merging |
| `rust/src/dsl/compiler.rs` | `compile_dag()`: validates DAG, detects cycles, topo-sort, stores in `plan.dag_tables` |
| `rust/src/bytecode/dag_table.rs` | `DAGTable`, `DAGNode`, `DAGFailurePolicy`, `MergeStrategy` |

---

## 6. Parallel Execution Flow

**Scenario:** Parallel fan-out to 3 services.

### DSL

```
p:fraud,inventory,shipping c
```

### Execution

```
VM.dispatch(OpCode::Parallel)
    │
    ▼
exec_parallel(body, instr, plan, caller, arena)
    │
    ├── Get service IDs from plan (svc_arg instructions after parallel)
    ├── Clone body for each branch
    │
    ├── rayon scope:
    │   ├── fraud     → call service, store result
    │   ├── inventory → call service, store result
    │   └── shipping  → call service, store result
    │
    ├── Collect results into Vec<Value>
    └── Store as body["_parallel"] = [fraud, inventory, shipping]
```

### Collect

```
VM.dispatch(OpCode::Collect)
    │
    ▼
exec_collect()
    │
    ├── Check body["_parallel"] exists
    ├── For each result object, merge unique keys into body
    └── Remove "_parallel" from body
```

### Files Involved

| File | What It Does |
|------|-------------|
| `rust/src/executor/parallel.rs` | `exec_parallel()`: rayon parallel fan-out |
| `rust/src/executor/mod.rs` | `op_collect()`: merge parallel results |

---

## 7. Gate/Conditional Flow

**Scenario:** Route based on a field value.

### DSL

```
g:amount>10000 n:manual-review f:auto-approve
```

### Execution

```
VM.dispatch(OpCode::Gate)
    │
    ▼
exec_jmp_if_false(body, instr, plan, arena, &mut skip)
    │
    ├── Extract field "amount" from body via resolve_field("amount")
    │   │
    │   ├── Field found → compare with 10000 using GateOp::Gt
    │   │   ├── True (amount > 10000) → skip = 0 (execute n:manual-review)
    │   │   └── False → skip = jump_offset (skip to f:auto-approve)
    │   │
    │   └── Field not found → returns FieldNotFound error
    │
    └── VM sets ip += skip
```

### Field Resolution

```
resolve_field(body, "user.address.city")
    │
    ├── Parse path: ["user", "address", "city"]
    ├── Navigate: body["user"]["address"]["city"]
    │
    ├── All path segments found → return Ok(value)
    │
    └── Missing intermediate field (e.g., address is null)
        └── Return Err("FieldNotFound: path segment 'address'...")
```

### Files Involved

| File | What It Does |
|------|-------------|
| `rust/src/executor/gate.rs` | `exec_jmp_if_false()`: gate evaluation with jump offset |
| `rust/src/executor/expr.rs` | `resolve_field()`: dotted path navigation with FieldNotFound errors |

---

## 8. Error/Retry/Fallback Flow

**Scenario:** Service call fails, retry configured, then fallback.

### DSL

```
n:payment r3:fixed:100 f:dlq
```

### Execution

```
VM.dispatch(OpCode::Next) → exec_next()
    │
    ├── call service "payment"
    │   └── ERROR (timeout or service error)
    │
    ├── Check retry config (instr.flags & 0x01)
    │   └── Has retry → plan.retry_configs[instr.c]
    │
    ├── Retry loop:
    │   ├── Attempt 1: call payment → ERROR
    │   │   └── Wait 100ms (fixed strategy)
    │   ├── Attempt 2: call payment → ERROR
    │   │   └── Wait 100ms
    │   └── Attempt 3: call payment → ERROR
    │       └── Exhausted → set self.failed = true
    │
    ├── VM continues to next instruction
    │
    ▼
VM.dispatch(OpCode::Fallback)
    │
    ▼
exec_fallback(body, instr, plan, caller)
    │
    ├── self.failed == true
    ├── Clear failed flag
    └── Call service "dlq" with original body
```

### Files Involved

| File | What It Does |
|------|-------------|
| `rust/src/executor/next.rs` | `exec_next()`: service call with retry logic, sets `self.failed` on exhaustion |
| `rust/src/executor/mod.rs` | `op_fallback()`: checks `self.failed`, calls fallback service |
| `rust/src/dsl/compiler.rs` | Retry config attached to preceding Next/Async instruction flag |

---

## 9. Schema Validation Flow (TypeGuard)

**Scenario:** Schema-typed rule validates incoming message.

### DSL

```
schema:{!order_id:string,!amount:float} n:validate
```

### Compilation

```
Lexer: Token::Schema("{!order_id:string,!amount:float}")
    │
Parser: ASTNode::Schema("{!order_id:string,!amount:float}")
    │
Compiler::compile_schema()
    ├── Strip braces: "!order_id:string,!amount:float"
    ├── Split by comma:
    │   ├── "!order_id:string" → required=true, name="order_id", type=String
    │   └── "!amount:float" → required=true, name="amount", type=Float
    ├── Build Schema { fields: [FieldSchema, FieldSchema] }
    └── plan.schema = Some(schema)
    └── Emit Instruction::type_guard(1)
```

### Execution

```
VM.dispatch(OpCode::TypeGuard)
    │
    ▼
op_type_guard(instr)
    │
    ├── Read plan.schema
    │   └── None + strict=1 → error "schema required"
    │
    ├── Parse last_response as JSON
    │
    ├── For each field in schema.fields:
    │   ├── "order_id" (required, String):
    │   │   ├── body["order_id"] exists? → Yes
    │   │   └── body["order_id"] is string? → Yes ✓
    │   ├── "amount" (required, Float):
    │   │   ├── body["amount"] exists? → Yes
    │   │   └── body["amount"] is number? → Yes ✓
    │   └── All fields valid → Ok(())
    │
    └── On error: return Err("TypeGuard: field 'amount' expected Float...")
```

### Files Involved

| File | What It Does |
|------|-------------|
| `rust/src/dsl/lexer.rs` | Lexes `schema:{...}` into `Token::Schema` |
| `rust/src/dsl/parser.rs` | Parses into `ASTNode::Schema` |
| `rust/src/dsl/compiler.rs` | `compile_schema()` parses field specs, stores in `plan.schema` |
| `rust/src/bytecode/instruction.rs` | `Instruction::type_guard(strict)` builder |
| `rust/src/bytecode/opcode.rs` | `OpCode::TypeGuard = 22` |
| `rust/src/bytecode/resolved_type.rs` | `ResolvedType`, `FieldSchema`, `Schema`, `Schema::is_valid()` |
| `rust/src/executor/mod.rs` | `op_type_guard()` validates body against schema |

### Validation Examples

```json
// Valid — passes TypeGuard
{"order_id": "ORD-123", "amount": 99.99}

// Invalid — missing required field
{"order_id": "ORD-123"}
// Error: "TypeGuard: missing required field 'amount'"

// Invalid — wrong type
{"order_id": "ORD-123", "amount": "expensive"}
// Error: "TypeGuard: field 'amount' expected Float, got String"
```

---

## 10. Span Tracing Flow

**Scenario:** VM emits trace spans during execution, Go drains them.

### Sequence

```
VM.dispatch(Next)
    │
    ├── Execute opcode
    ├── Record start time
    │
    ├── On complete:
    │   ├── duration = start.elapsed()
    │   └── emit_span(Span { opcode, service_id, layer, duration_ns, status })
    │       │
    │       └── thread_local! SPAN_BUFFER.borrow_mut().push(span)
    │           │
    │           └── Lock-free ring buffer: head = (head+1) % 1024
    │
    ├── ... more opcodes ...
    │
    ▼
Go calls bridge.GetSpans()
    │
    ├── C.flowrulz_get_spans(out_ptr, out_cap)
    │
    ├── Rust: SPAN_BUFFER.with(|buf| buf.borrow_mut().drain(out_slice))
    │   │
    │   └── Copy spans from ring buffer to out_slice (atomic tail advance)
    │
    └── Return bytes written
```

### Span Format

```rust
#[repr(C)]
pub struct Span {
    opcode:      u8,     // 0-22
    service_id:  u16,    // from instruction
    layer:       u8,     // DAG layer (0 for non-DAG)
    duration_ns: u64,    // nanoseconds
    status:      u8,     // 0=ok, 1=error, 2=timeout
}
// Total: 14 bytes per span
```

### Files Involved

| File | What It Does |
|------|-------------|
| `rust/src/tracing/mod.rs` | `Span` struct definition, `thread_local! SPAN_BUFFER`, `emit_span()` |
| `rust/src/tracing/ring_buffer.rs` | Lock-free ring buffer with atomic head/tail |
| `rust/src/executor/mod.rs` | `dispatch()` emits span after each opcode handler |
| `rust/src/ffi.rs` | `flowrulz_get_spans()` drains ring buffer to C caller |
| `go/internal/bridge/bridge.go` | `GetSpans()` calls `C.flowrulz_get_spans`, returns `[]byte` |

---

## 11. Lane Routing Flow

**Scenario:** Engine assigns deployed rule to a consumer lane based on complexity.

### Sequence

```
engine.Deploy("order-flow", dsl)
    │
    ├── bridge.Compile() → plan bytes
    │
    ├── bridge.PlanComplexity(plan) → score: u32
    │   │
    │   └── Rust: deserialize plan, return plan.complexity_score
    │
    ├── laneForScore(score):
    │   ├── score < 10  → LaneFast   (batch=500, timeout=10ms)
    │   ├── score ≤ 50  → LaneNormal (batch=100, timeout=50ms)
    │   └── score > 50  → LaneHeavy  (batch=10,  timeout=500ms)
    │
    └── VersionedPlan.Lane = lane
```

### Complexity Scoring Table

| Instruction | Weight | Example Count | Total |
|-------------|--------|--------------|-------|
| Next | 10 | 2 | 20 |
| Parallel | 20 | 1 | 20 |
| Collect | 1 | 1 | 1 |
| Gate | 5 | 1 | 5 |
| Emit | 8 | 1 | 8 |
| **Total** | | | **54 → LaneHeavy** |

### Lane Config

```go
var DefaultLanes = []LaneConfig{
    {Name: LaneFast,   BatchSize: 500, PollTimeout: 10},
    {Name: LaneNormal, BatchSize: 100, PollTimeout: 50},
    {Name: LaneHeavy,  BatchSize: 10,  PollTimeout: 500},
}
```

### Files Involved

| File | What It Does |
|------|-------------|
| `go/internal/engine/engine.go` | `laneForScore()` maps score to lane |
| `go/internal/engine/engine.go` | `Deploy()` assigns lane to `VersionedPlan` |
| `rust/src/dsl/compiler.rs` | `calc_complexity()` computes score at compile time |
| `rust/src/ffi.rs` | `flowrulz_plan_complexity()` returns score across FFI |

---

## 12. Persistence Flow

**Scenario:** Engine saves rules to disk on deploy, loads on startup.

### Save Sequence

```
engine.Deploy()
    │
    ├── ... compile, create VersionedPlan ...
    │
    └── engine.saveRules()
        │
        ├── RLock all rules
        ├── Serialize rules to []rulePersistence (ID + DSL + Version + Lane)
        ├── RUnlock
        ├── json.Marshal(rules)
        └── os.WriteFile(persistPath, data, 0644)
```

### Load Sequence

```
engine.New(persistPath)
    │
    ├── persistPath == "" → skip
    │
    └── engine.loadRules()
        │
        ├── os.ReadFile(persistPath)
        │   └── File not found → return (first run)
        │
        ├── json.Unmarshal → []rulePersistence
        │
        ├── For each rule:
        │   ├── bridge.Compile(DSL, id) → rehydrate plan bytes
        │   ├── Create VersionedPlan
        │   ├── Restore Version and Lane
        │   └── Track max version number
        │
        └── nextVersion.Store(maxVersion)
```

### Files Involved

| File | What It Does |
|------|-------------|
| `go/internal/engine/engine.go` | `New(persistPath)`, `loadRules()`, `saveRules()`, `rulePersistence`, `versionPersistence` |
| `go/cmd/flowrulz/main.go` | Reads `FLOWRULZ_PERSIST_PATH` env var, passes to `engine.New()` |

### Example File

```json
[
  {
    "id": "order-flow",
    "versions": [
      {
        "dsl": "schema:{!order_id:string,!amount:float} t500 n:validate e:notify",
        "version": 1,
        "lane": "normal"
      },
      {
        "dsl": "schema:{!order_id:string,!amount:float,!currency:string} t500 n:validate e:notify",
        "version": 2,
        "lane": "normal"
      }
    ]
  }
]
```

---

## 13. Admin API Request Flow

**Scenario:** All admin endpoints with their request/response patterns.

### Endpoint Table

| Method | Path | Auth | Handler | File |
|--------|------|------|---------|------|
| POST | `/rules` | Yes | `deployRule()` | `server.go:37` |
| DELETE | `/rules/{id}` | Yes | `removeRule()` | `server.go:55` |
| GET | `/rules` | Yes | `listRules()` | `server.go:61` |
| GET | `/rules/{id}` | Yes | `getRule()` | `server.go:91` |
| GET | `/rules/{id}/versions` | Yes | `listVersions()` | `server.go:114` |
| POST | `/rules/{id}/validate` | Yes | `validateRule()` | `server.go:139` |
| POST | `/rules/{id}/promote` | Yes | `promoteVersion()` | `server.go:172` |
| POST | `/rules/{id}/rollback` | Yes | `rollbackVersion()` | `server.go:194` |
| GET | `/lanes` | Yes | `listLanes()` | `server.go:197` |
| GET | `/health` | No | `health()` | `server.go:210` |

### Auth Middleware

```
Request → auth() → check Authorization header
    │
    ├── FLOWRULZ_API_KEY not set → pass through (no auth)
    │
    ├── Header matches "Bearer <key>" → pass through
    │
    └── Mismatch → 401 Unauthorized
```

### Files Involved

| File | What It Does |
|------|-------------|
| `go/internal/admin/server.go` | All route handlers, `auth()` middleware, `New()` registers routes |
| `go/cmd/flowrulz/main.go` | Mounts admin handler under `/admin/` prefix |

### API Examples

```bash
# Validate DSL (compile-only, no deploy)
curl -X POST http://localhost:8080/rules/my-rule/validate \
  -H "Authorization: Bearer key" \
  -d '{"dsl":"schema:{!id:string} n:validate"}'

# Response
{"valid": true, "complexity_score": 13, "plan_bytes": 256}

# List lanes
curl -H "Authorization: Bearer key" http://localhost:8080/lanes

# Response
[
  {"name": "fast", "batch_size": 500, "poll_timeout_ms": 10},
  {"name": "normal", "batch_size": 100, "poll_timeout_ms": 50},
  {"name": "heavy", "batch_size": 10, "poll_timeout_ms": 500}
]
```

---

## 14. Map Expression Flow

**Scenario:** Expression transforms message fields at runtime.

### DSL

```
m:.full_name=upper(.first_name) + ' ' + upper(.last_name)
```

### Expression Evaluation

```
VM.dispatch(OpCode::Map)
    │
    ▼
exec_map(body, instr, plan, arena)
    │
    ├── Read expression from plan.const_pool[instr.a]
    │   = ".full_name=upper(.first_name) + ' ' + upper(.last_name)"
    │
    ├── Parse LHS ".full_name" → destination field path
    ├── Parse RHS "upper(.first_name) + ' ' + upper(.last_name)"
    │
    ├── Evaluate RHS:
    │   ├── upper(.first_name)
    │   │   ├── resolve_field(body, "first_name") → "alice"
    │   │   └── call_builtin("upper", &[Value::String("alice")]) → "ALICE"
    │   ├── ' ' → " "
    │   └── upper(.last_name)
    │       ├── resolve_field(body, "last_name") → "smith"
    │       └── call_builtin("upper", &[Value::String("smith")]) → "SMITH"
    │
    ├── Concat: "ALICE" + " " + "SMITH" → "ALICE SMITH"
    │
    └── set_field(body, "full_name", Value::String("ALICE SMITH"))
```

### Files Involved

| File | What It Does |
|------|-------------|
| `rust/src/executor/map.rs` | `exec_map()`: parses dest=expr, evaluates, assigns |
| `rust/src/executor/expr.rs` | `eval_expr()`: recursive descent expression evaluator, `call_builtin()` dispatches 21 functions |

---

## 15. Expression Builtin Dispatcher

**Scenario:** Expression engine dispatches to the correct builtin function.

### Dispatcher

```rust
fn call_builtin(name: &str, args: &[serde_json::Value]) -> Result<Value, String> {
    match name {
        "uuid"      => Ok(Value::String(uuid::Uuid::new_v4().to_string())),
        "now"       => Ok(Value::String(chrono_now())),
        "epoch"     => Ok(Value::Number(epoch_seconds())),
        "lower"     => string_op(args, |s| s.to_lowercase()),
        "upper"     => string_op(args, |s| s.to_uppercase()),
        "trim"      => string_op(args, |s| s.trim().to_string()),
        "length"    => Ok(Value::Number(args[0].as_str().map_or(0, |s| s.len() as f64).into())),
        "concat"    => Ok(Value::String(concat_args(args))),
        "base64"    => string_op(args, |s| general_purpose::STANDARD.encode(s)),
        "json"      => serde_json::from_str(args[0].as_str().unwrap_or("null")).map_err(|e| e.to_string()),
        "substring" => substr(args),
        "replace"   => replace(args),
        "to_string" => Ok(Value::String(args[0].to_string())),
        "parse_int" => parse_int(args),
        "parse_float" => parse_float(args),
        "coalesce"  => Ok(coalesce(args)),
        "default"   => Ok(default(args)),
        "contains"  => Ok(Value::Bool(contains(args))),
        "keys"      => Ok(Value::Array(keys(args))),
        "merge"     => Ok(merge(args)),
        "hash"      => hash_fn(args),
    }
}
```

All functions take `&[serde_json::Value]` (not `&[&str]`) so JSON-aware functions like `contains`, `keys`, `merge` work directly on array/object values.

### Files Involved

| File | What It Does |
|------|-------------|
| `rust/src/executor/expr.rs` | `call_builtin()` dispatcher, all 21 function implementations |
| `rust/src/executor/expr.rs` | `eval_expr()`: expression parser + evaluator |

---

## Summary: File → Scenario Matrix

| File | Scenarios |
|------|-----------|
| `rust/src/ffi.rs` | 1, 2, 3, 9, 10, 11 |
| `rust/src/executor/mod.rs` | 2, 5, 6, 7, 8, 9, 10, 14 |
| `rust/src/executor/next.rs` | 2, 3, 8 |
| `rust/src/executor/dag.rs` | 5 |
| `rust/src/executor/parallel.rs` | 6 |
| `rust/src/executor/gate.rs` | 7 |
| `rust/src/executor/map.rs` | 14 |
| `rust/src/executor/expr.rs` | 7, 14, 15 |
| `rust/src/dsl/lexer.rs` | 1, 9 |
| `rust/src/dsl/parser.rs` | 1, 9 |
| `rust/src/dsl/compiler.rs` | 1, 9, 11 |
| `rust/src/bytecode/resolved_type.rs` | 9 |
| `rust/src/bytecode/dag_table.rs` | 5 |
| `rust/src/bytecode/instruction.rs` | 9 |
| `rust/src/tracing/mod.rs` | 10 |
| `rust/src/tracing/ring_buffer.rs` | 10 |
| `go/internal/admin/server.go` | 1, 4, 13 |
| `go/internal/engine/engine.go` | 1, 2, 4, 11, 12 |
| `go/internal/bridge/bridge.go` | 1, 2, 3, 10, 11 |
| `go/internal/bridge/caller_bridge.c` | 3 |
| `go/cmd/flowrulz/main.go` | 2, 12 |
