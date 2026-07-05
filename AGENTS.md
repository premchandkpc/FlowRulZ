# AGENTS.md — FlowRulZ

Instructions for AI coding agents working in this repo. Read fully before writing code.
This file is grounded in the actual docs in `docs/` (architecture-review-complete.md,
bytecode-format.md, cluster-model.md, development.md, dsl-syntax.md, ffi-api.md,
kafka-semantics.md, memory-management.md, policy-engine-implementation.md,
replication-design.md, vm-architecture.md, README.md). Where those docs disagree with
each other, it's called out explicitly (§9) instead of silently picked.

---

## 0. What this is

Distributed execution runtime where **Pub/Sub, RPC, workflows, and rules are all the
same thing**: compiled bytecode `ExecutionPlan`s run on one VM. Only the bytecode differs.

- **Rust (`runtime/`)**: DSL compiler + register-less, stackless bytecode VM
- **Go (`server/`)**: control plane, cluster membership, scheduling, transport, reliability
- **Bridge (`server/bridge/`)**: CGo FFI seam — highest blast-radius code in the repo
- **`sdk/`**: 5-language clients, all exposing the same 4 ops (Publish/Request/Execute/Stream)
- **`simulator/`**: test harness — 40+ mock services, 50+ scenarios, dashboard

---

## 1. Build & test — use exactly these, not guessed equivalents

```bash
make                # full build: Rust cdylib (release) + Go binary
make test           # Rust 401 tests + Go tests, all packages
make bench          # Criterion benchmarks
make vet            # go vet
make e2e            # 3-node docker-compose cluster test
make clean

# Granular:
cd runtime && cargo build --release && cargo test
cd server && CGO_ENABLED=1 go test -count=1 ./server/... ./simulator/...
CGO_ENABLED=1 go vet ./server/... ./simulator/...
```

Rust builds as **both `cdylib` and `rlib`** — the `cdylib` (`libflowrulz_core.dylib`/`.so`)
is what Go links via cgo. If you change a public Rust type used across FFI, both targets
must build clean, and `bytecode-format.md`'s bincode layout must still round-trip.

Prereqs: Rust 1.70+ (edition 2021), Go 1.26+. No other system deps.

---

## 2. Repo map (as it actually exists, not idealized)

```
runtime/src/
  lib.rs           C FFI exports, module declarations
  bytecode/        event.rs, execution.rs, opcode.rs, instruction.rs, consts.rs,
                    services.rs, dag_table.rs, resolved_type.rs, plan.rs
  dsl/             lexer.rs, parser.rs, optimizer.rs, compiler.rs (type checking lives here)
  executor/        mod.rs (dispatch + TypeGuard), runtime.rs, next.rs, parallel.rs,
                    gate.rs, emit.rs, map.rs, plugin.rs (WASM/wasmtime), dag.rs, chunk.rs,
                    helpers.rs, expr.rs (31 builtins)
  ffi.rs           extern "C" exports
  tracing/         mod.rs — span struct + ring buffer (inline, no separate file)
  memory/          mod.rs, arena.rs (bumpalo), intern.rs

server/
  bridge/          bridge.go, caller_bridge.c, bridge_test.go — cgo + sync.Map dispatch
  cmd/flowrulz/     entry point via bootstrap.NodeBuilder
  pkg/             13 public interface packages (DI/testability boundary)
  internal/
    node/          ProdNode — central struct
    bootstrap/     NodeBuilder — DI composition root
    engine/        rule lifecycle, versioning, lane routing, persistence
    scheduler/     priority lanes + work stealing
    cluster/       gRPC p2p Cluster Bus + Gossip (NOT Raft — see §3)
    transport/     Kafka (Sarama, legacy) + gRPC transport adapters
    admin/         HTTP API — rules CRUD, validate, promote, lanes
    plandist/      plan distribution + ack protocol
    partition/     key-space shard mgmt + rebalancing
    membership/    gossip, leader lease, heartbeat eviction
    execstate/     FileStore — JSON execution records, local disk only
    reliability/   DLQ, saga, circuit breaker, dedup, rate limiter
    registry/      service registry via HTTP heartbeat
    replyrouter/   correlation ID → pending request channel
    observability/ OTel tracing, Prometheus metrics
    compiler/      DSL compiler abstraction (local/remote)
    plugins/       WASM plugin loader
    flowengine/    flow orchestration state machine
    policy/        9-level policy resolver — ALREADY IMPLEMENTED, see §7
    adapters/      pkg/ interface adapters over internal/ types
    ports/         secondary port interfaces (mostly unused, don't extend without asking)

sdk/{flow,java,python,javascript,rust}/
simulator/         cmd/, config/, dashboard/, dispatcher/, execution/, loadgen/,
                   metrics/, modes.go (8 modes), network/, scheduler/, scenarios/
                   (50+, in registry.go + scenarios.go), services/ (40+ mocks), timeline/
docs/              architecture + Obsidian vault (26 notes, 1 canvas)
```

---

## 3. Cluster model — single-leader, NOT Raft consensus for state

**Correction from an earlier version of this file:** this is a single-leader cluster with
**no Raft, no Paxos** for cluster state distribution. Do not assume `hashicorp/raft` governs
plan/partition state — it doesn't, per `cluster-model.md`. (There is a partial Raft mention
elsewhere for leader election only — see the discrepancy flagged in §9. Treat
`cluster-model.md`'s account as authoritative for day-to-day work since it's the more
detailed, current spec.)

**Transport:** gRPC-based **Cluster Bus** (`server/internal/cluster/`) — peer-to-peer,
no Kafka/ZK required. Kafka (`server/internal/transport/kafka/`) is a **legacy fallback**,
only active when `FLOWRULZ_KAFKA_BROKERS` is explicitly set.

**Leader election — simple ordering, not a consensus protocol:**
1. Every node consumes `_flowrulz_members` topic (heartbeats every 3s)
2. Alive nodes sorted by `(ID, ascending)` — lowest-ID node is leader
3. Leader crash detected after 3× heartbeat interval (LeaderLease, default 8s)
4. Next-lowest-ID node promotes itself, increments `term`
5. **Epoch-based fencing**: every leader embeds its `term` in every `PlanMessage`;
   followers reject activation from a term lower than their known current term
6. Term + leader ID persisted to `cluster-term.json` (`TermStore`) — survives restart
7. **Step-down on higher term seen**: if a non-leader heartbeat carries a higher term,
   the current leader steps down immediately

**If you touch leader election or plan activation code:** never bypass the term check.
A node applying a plan/partition assignment without validating
`NodeID == leaderID && Term >= currentTerm` re-opens the exact split-brain risk this
fencing exists to prevent (see `partition/manager.go: HandleAssignmentMessage`).

**Gossip (epidemic protocol, `cluster.Gossiper`)** runs alongside heartbeats for faster
convergence: push every 2s to 2 random peers, pull (anti-entropy) every 10s. Conflict
resolution: higher epoch wins, then higher term. Don't add a second membership
propagation mechanism — extend gossip if you need faster convergence.

**Partitioning**: fixed N partitions (default 64, `FLOWRULZ_NUM_PARTITIONS`), FNV-32a
key hashing, round-robin assignment, rebalance triggered on join/leave/election.

---

## 4. Bytecode / VM contract

The VM (`runtime/src/executor/`) is **register-less and stackless** — it walks
`Vec<Instruction>` with an instruction pointer. It never sees raw bodies directly; it
operates on `ExecutionContext`:

```rust
pub struct ExecutionContext {
    pub event: Event,
    pub body: Vec<u8>,
    pub variables: HashMap<String, Vec<u8>>,
    pub outputs: HashMap<String, Vec<u8>>,   // per-service results — enrichment, not replacement
    pub headers: HashMap<String, String>,
    pub failed: bool,
    pub errors: Vec<String>,
    pub hop_count: u16,
    pub retry_count: u32,
    pub deadline_ms: u64,
}
```

**Key invariant: services enrich context, they don't replace it.** A new opcode or
executor change that overwrites `ctx.body` wholesale instead of merging into
`ctx.outputs["svc_name"]` breaks the enrichment model every downstream Gate/DAG
depends on.

**Two execution modes, don't conflate them:**
- `VM::run()` — blocking loop, calls back into Go synchronously per service call
  (used by `flowrulz_execute`)
- `VM::step()` — cooperative, yields `Pending{svc_id, body}` instead of blocking, caller
  (Go bridge or simulator) resolves the service call and re-enters with the response
  (used by `flowrulz_execute_step`). **Never mix**: code that assumes `run()`
  semantics (blocking callback) will not behave correctly if driven through the step API.

**25 opcodes (0–24)** — full table in `bytecode-format.md`. Two worth remembering because
they're easy to misuse:
- **`SvcCall` (23)** is dispatchable by the VM but **the compiler never emits it** — it's
  reserved for manual plan construction. Don't wire the DSL to emit it without discussing
  the design first.
- **`Delay` (24)** is a no-op inside `run()` — it only does something under the `step()`
  API (`StepResult::Delay`). If you add delay-based DSL behavior, verify it against
  whichever execution mode the caller actually uses.

**Type system is opt-in.** No `schema:{...}` in the DSL → `TypeGuard` never fires, VM
treats payload as opaque `Vec<u8>`. `ResolvedType::Any` is an intentional escape hatch:
passes all compile-time Gate/Map checks and all runtime validation except
existence-if-required. Use schema at boundary/ingress rules; skip it for pure routing or
third-party/non-JSON payloads. Don't make schema mandatory anywhere in the pipeline —
that breaks the payload-agnostic design goal stated in the top-level README.

**Adding a new opcode** — the doc-mandated sequence (`development.md`), do not skip steps
or reorder: `opcode.rs` variant → `instruction.rs` builder → `lexer.rs` token → `parser.rs`
AST node → `optimizer.rs` (if applicable) → `compiler.rs` emission → `executor/mod.rs`
dispatch arm → tests. Compile-time type checking additions belong in `compiler.rs`'s
`type_check_gate()` / `type_check_map()` pre-pass, not in the executor.

---

## 5. FFI boundary — memory & concurrency rules

Convention (`ffi-api.md`), apply to any new `extern "C"` function:
- Input buffers: caller owns, callee reads during the call only
- Output/error buffers: caller allocates, callee writes, capacity + written-length pair
- All functions return `i32` status (`0` = success, negative = specific error code)
- Never return a pointer into pooled/reused memory — `flowrulz_msg_alloc`/`_release`
  use plain `std::alloc` directly (the slab pool was removed as dead code because
  every allocation was immediately discarded anyway — don't reintroduce pooling here
  without a measured reason)

**Concurrency dispatch pattern for `flowrulz_execute`** (three layers):
```
C (flowrulz_execute) → C (callerBridge) → Go (//export goServiceCaller) → sync.Map lookup by ctx_id
```
`ctx_id` is generated via `atomic.Uint64` per `Execute()` call and keys a `sync.Map` of
`ServiceCaller`s — this is what allows concurrent service dispatch **without mutex
contention**. If you add a new FFI entry point that needs a Go-side callback, follow this
exact ctx_id + sync.Map pattern, not a new locking scheme.

**Step API inverts control** — Go drives the loop, not Rust:
```go
for {
    out := flowrulz_execute_step(plan, ctxBytes, respBytes, nil)
    switch out.Result {
    case Done:     return out.Output
    case Pending:  respBytes = callService(out.SvcID, out.Body); continue
    case Continue: respBytes = nil; continue
    }
}
```
This is what lets Go interleave circuit breakers, rate limiting, and observability
between individual VM instructions. Don't collapse this back into a single blocking
call for convenience — that's the whole point of the step API existing.

**Known fixed bug, don't reintroduce:** `Compile`/`Execute`/`InitContext` used to alias
`sync.Pool` buffers across calls (`TestExecuteStepMultiCall` caught this). Fix was
`make+copy` instead of returning pooled buffers. Any FFI function returning a buffer
must copy out of any pool before returning — never hand back a pooled slice directly.

---

## 6. Replication & consistency model (`replication-design.md`)

Three data classes, each with **deliberately different** durability — do not unify them
without understanding why they're split:

| Class | Mechanism | Consistency | On node death |
|---|---|---|---|
| Control-plane (partitions, plans, membership) | Cluster Bus / legacy Kafka pub-sub | Eventual, <200ms window | Re-rebalance after election; fencing token prevents stale writes |
| Per-execution state | In-memory only (`execstate.MemoryStore`) | None — ephemeral | In-flight executions are lost, by design |
| Message ack | Manual offset/ack commit after execution completes | At-least-once + dedup (FNV-128a, 5min TTL) | Redelivery + dedup drop, no double-processing |

**Stateless by design**: DLQ, SagaTracker, and execstate are all in-memory only. This is
intentional — these components are either ephemeral by nature (execution state) or backed
by external durable stores (Kafka for DLQ). The only persisted component is the engine's
rule store (`engine.Engine`), which writes to disk for rule durability.

**The fencing token pattern is mandatory for any new leadership-gated operation:**
```go
token := node.CaptureLeadershipToken()
if !token.Valid() { return }              // not leader, bail
assignments := doWork(token.Term)         // do the work
if !node.ValidateLeadershipToken(token) { return }  // re-check before side effect
publish(assignments)                      // only now is it safe
```
Skipping the re-validation before the publish step is exactly the split-brain bug this
pattern exists to prevent — capture-then-immediately-trust is not enough because
leadership can change between steps 1 and the actual publish.

**Ordering constraint for message processing**: execute → save execution state → commit
offset. Never commit the offset before the execution state is durably saved — that
ordering is what makes "redeliver + dedup" a correct recovery strategy instead of a
silent data-loss window.

---

## 7. Policy Resolution Engine — already built, not a gap

Earlier assessments of this repo describe policy resolution as a partial/missing
capability. **That's stale.** Per `policy-engine-implementation.md`, this is fully
implemented at `server/internal/policy/`:

- 9-level hierarchy (Platform → Environment → Tenant → Application → Service →
  Endpoint → Method → Workflow → Runtime), deep-merge semantics (non-nil overrides,
  nil inherits, maps merge rather than replace)
- `Resolver` (cached, O(1) on hit), `Validator` (built-in + custom rules), `Store`
  (Memory + File, atomic writes), all RWMutex-protected, 40+ tests passing with `-race`

**If asked to add dynamic config / feature-flag watching**, this is the integration
point — wire a watch/notify layer on top of this resolver's cache invalidation, don't
build a second config system. Per the doc's own next-steps: Phase 1 (wiring
`PolicyResolver` into `node.Dependencies` and `handleIncomingMessage`) may still be
pending — verify current wiring state in `node/factory.go` before assuming it's live
end-to-end; "implemented" and "wired into the execution path" are recorded as two
different milestones in the source doc.

---

## 8. DSL quick-reference for anyone generating or validating rules

- Pipeline = space-separated ops; `t<ms>` sets timeout for the next call; `r<N>:<strategy>`
  attaches retry to the preceding `n:`/`f:` only — retry with no preceding call is a
  compile error, not a silent no-op
- `schema:{...}` is optional and should stay optional (§4) — only add it to rules that
  need Gate/Map type checking or enum validation
- `dag:{A:[],B:[A],...}` — cycle-checked, unknown-service-checked at compile time; layers
  execute in parallel, results deep-merge via the plan's `MergeStrategy`
- 31 expression builtins available in `m:` (see `dsl-syntax.md` for the full table) —
  don't hand-roll string/date logic in the executor when an existing builtin covers it;
  add a new builtin to `expr.rs` instead (§4's opcode-add steps don't apply to builtins,
  which only touch `expr.rs`)

---

## 9. ⚠ Documentation conflict — flagged, not resolved

`cluster-model.md` states plainly: *"single-leader cluster with no Raft... Simple
ordering — no Raft, no Paxos, no external dependency"* for both leader election and
partition/plan state.

`replication-design.md` states: *"Raft is used only for leader election (NoopFSM — no
state goes through the Raft log)"* and builds its fencing-token argument partly on
Raft-confirmed-leader terminology ("Gossip proposes, Raft-confirmed-leader disposes").

These cannot both be fully accurate as written. Two most likely explanations: (a)
`hashicorp/raft` is vendored/imported for its leader-election primitive only, with the
actual term/lowest-ID mechanism in `cluster-model.md` describing the real behavior on
top of or instead of it, or (b) `replication-design.md` is describing a target/future
state, not current behavior. **Do not silently pick one and build on top of it.** If your
task touches leader election, membership, or the fencing token pattern, confirm which
account is current against the actual code in `server/internal/cluster/` and
`server/internal/membership/` before writing anything that assumes one or the other.

---

## 10. Definition of done

1. `make && make test` clean — Rust 401 + full Go suite, **with `-race`** where the
   test command supports it
2. If you touched FFI (§5): re-run the specific bridge test by name (e.g.
   `TestExecuteStepMultiCall`), not just the suite
3. If you touched cluster/membership/partition code (§3, §9): confirmed against actual
   code which account (Raft-assisted or pure lowest-ID) is real before changing fencing
   logic
4. If you added an opcode (§4): followed the full 7-step sequence, not a shortcut
5. If you added a builtin (§8): added to `expr.rs` only, with a test, no new DSL token
6. No new interface under `internal/ports/` without confirming it's actually wired to a
   real adapter first (see prior maturity review — this pattern has partial history of
   going nowhere)
7. Relevant `docs/*.md` updated if you changed behavior it describes — and if you notice
   a doc conflict like §9 while doing so, add a note rather than quietly resolving it
   yourself