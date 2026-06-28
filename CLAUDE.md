# FlowRulZ

Low-latency rule evaluation engine for event-driven systems.

> **AI rules** — On each conversation start, read `docs/` dir. After any code change, update relevant `.md` files in `docs/` to stay in sync. Never let docs go stale.

## Build & Test

```bash
make all       # rust release + go binary
make test      # all rust (86) + go tests
make bench     # criterion benchmarks
make vet       # go vet
make clean     # cargo clean + remove binary
```

## Architecture

- **Rust** (`rust/`): DSL compiler, bytecode VM, executor, FFI layer (`cdylib`)
- **Go** (`go/`): Engine, bridge (CGo), admin HTTP server, transport, observability
- C FFI prefix: `flowrulz_` — all exported functions use `#[no_mangle] pub extern "C"`
- Bridge: `sync.Map callerMap` + `atomic.Uint64 nextExecID` — no mutex in hot path
- Span tracing: `thread_local!` ring buffer, lock-free atomic head/tail, drained via `flowrulz_get_spans`

## Key Layers

| Layer | Dir | Description |
|---|---|---|
| DSL | `rust/src/dsl/` | Lexer → Parser → Optimizer → Compiler |
| Bytecode | `rust/src/bytecode/` | OpCode (0-22), Instruction (8 bytes), ExecutionPlan |
| VM | `rust/src/executor/` | `VM::run()` dispatches opcodes, tracks `hop_count`, `errors` |
| FFI | `rust/src/ffi.rs` | `flowrulz_compile`, `flowrulz_execute`, `flowrulz_get_spans`, etc. |
| Bridge | `go/internal/bridge/` | CGo bindings + C caller bridge |
| Engine | `go/internal/engine/` | `VersionedPlan`, lane routing, persistence, `ExecuteAll` |
| Admin | `go/internal/admin/` | HTTP API with API key auth, rule CRUD, validate, lanes |

## Conventions

- `caller_cb_t` signature: `int(uint64_t ctx_id, uint16_t svc_id, const u8* body, size_t body_len, u8* resp, size_t* resp_len)`
- `Instruction` is 8 bytes: `{op: u8, flags: u8, a: u16, b: u16, c: u16}`
- `ExecutionPlan` serialized via bincode across FFI boundary
- `ExecutionPlan` fields: `rule_id`, `version`, `instr_count`, `complexity_score`, `instructions`, `const_pool`, `services`, `dag_tables`, `map_exprs`, `retry_configs`, `chunk_configs`, `schema`
- Complexity scoring: Next/Async=10, Parallel/DAG=20, Chunk=25, Gate=5, Map=3, Emit=8, Buffer=15
- Lane routing: score <10 → fast, ≤50 → normal, >50 → heavy
- Versioned plans with `ActiveExec sync.WaitGroup` — Add before bridge.Execute, Done after
- Schema DSL: `schema:{field:type,!required_field:type}` — emits `TypeGuard` opcode (22)
- Compile-time type inference: when `schema:{...}` is present, the compiler pre-pass validates Gate operators (`type_check_gate()`) and Map expressions (`type_check_map()`) against declared field types, emitting `TypeMismatch` errors for incompatible operations
- DAGTable fields: `failure_policy` (AbortAll/ContinueOthers/SkipDependents), `node_timeouts`, `merge_strategy` (LastWins/ArrayConcat/DeepMerge/ExplicitMap), `distributed`
- DAGNode has `parent_ids: Vec<u16>` — populated during compile from deps, used at runtime to merge parent results into downstream node input
- DAG exec_dag.rs implements all three failure policies: AbortAll (immediate error), ContinueOthers (record failure, continue), SkipDependents (skip nodes with failed parents)
- DAG merge_dag_results implements MergeStrategy: LastWins (keyed JSON object), ArrayConcat (JSON array), DeepMerge (recursive), ExplicitMap (same as LastWins, no explicit map config yet)
- Schema DSL: `enum[val1|val2|...]` syntax for `ResolvedType::Enum(Vec<String>)`
- Persistence: atomic write via write-to-temp-then-rename pattern (`saveRules()` uses `os.WriteFile` to `.tmp` then `os.Rename`)

## Expression Builtins

`to_string`, `parse_int`, `parse_float`, `coalesce`, `default`, `contains`, `keys`, `merge`, `epoch`, `hash`, `uuid`, `now`, `lower`, `upper`, `trim`, `length`, `concat`, `base64`, `json`, `substring`, `replace`

`call_builtin` takes `&[serde_json::Value]` (not `&[&str]`).

## Persistence

Engine accepts `persistPath` on creation. Saves/loads rules as JSON. Set via `FLOWRULZ_PERSIST_PATH` env var.

## Admin API

All endpoints (except `/health`) require `Authorization: Bearer <FLOWRULZ_API_KEY>` header when `FLOWRULZ_API_KEY` is set.

- `POST /rules` — deploy rule
- `DELETE /rules/{id}` — remove rule (drains active execs)
- `GET /rules` — list rules with versions
- `GET /rules/{id}` — get rule detail with lane info
- `GET /rules/{id}/versions` — list versions
- `POST /rules/{id}/validate` — compile-only, returns validity + complexity
- `POST /rules/{id}/promote?version=N` — promote version
- `POST /rules/{id}/rollback` — same as promote
- `GET /lanes` — lane configs
- `GET /health` — health check

## Opcodes (0-22)

0=Next, 1=Parallel, 2=Collect, 3=Fallback, 4=Gate, 5=Split, 6=Map, 7=Emit, 8=Drop, 9=Buffer, 10=Key, 11=Retry, 12=Pipe, 13=Timeout, 14=Async, 15=Chunk, 16=Dag, 17=Jmp, 18=Label, 19=SvcArg, 20=RetryData, 21=JumpOffset, 22=TypeGuard
