# FlowRulZ Documentation

Distributed execution runtime. Pub/Sub, RPC, workflows, and rules are all execution plans running on the same VM.

## Project Map

```
FlowRulZ/
├── runtime/        # Rust bytecode VM + DSL compiler
│   ├── src/
│   │   ├── bytecode/   # Event, ExecutionContext, Instruction set, plan format, type system
│   │   ├── dsl/        # Lexer, parser, optimizer, compiler (with type checking)
│   │   ├── executor/   # VM dispatch loop + op handlers + ExecutionRuntime + expr engine
│   │   ├── tracing/    # Lock-free span ring buffer
│   │   └── memory/     # Arena allocator, string interning
│   ├── benches/        # Criterion benchmarks
│   └── Cargo.toml
├── server/         # Go control plane + data plane
│   ├── bridge/          # cgo bindings to Rust FFI (sync.Map caller dispatch)
│   ├── cmd/flowrulz/    # Entry point (ProdNode via NodeBuilder)
│   ├── pkg/             # Public interfaces (for DI/testability) — 13 packages
│   │   ├── cluster/     # Raft consensus + membership interfaces
│   │   ├── transport/   # EventBus interface (canonical pub/sub abstraction)
│   │   ├── scheduler/   # Task scheduling + lane interfaces
│   │   ├── engine/      # Rule lifecycle interfaces
│   │   ├── node/        # Node interface
│   │   ├── plandist/    # Plan distribution interfaces
│   │   ├── partition/   # Partition management interfaces
│   │   ├── membership/  # Node membership interfaces
│   │   ├── store/       # Execution state persistence interfaces
│   │   ├── registry/    # Service registry interfaces
│   │   ├── reliability/ # Circuit breaker, DLQ, rate limit, dedup, saga interfaces
│   │   ├── replyrouter/ # Reply router interface
│   │   └── vm/          # Plan compilation + execution interfaces
│   └── internal/
│       ├── node/         # ProdNode — central struct wiring all modules
│       ├── bootstrap/    # NodeBuilder — DI composition root
│       ├── engine/       # Rule lifecycle, versioning, lane routing, persistence
│       ├── scheduler/    # Priority lanes + work stealing
│       ├── cluster/      # Raft + gRPC p2p Cluster Bus + Gossip
│       ├── transport/    # Kafka (Sarama) + gRPC transport adapters
│       ├── admin/        # HTTP API (rules CRUD, validate, promote, lanes)
│       ├── plandist/     # Plan distribution + ack protocol
│       ├── partition/    # Key-space shard mgmt + rebalancing
│       ├── membership/   # Gossip, leader lease, heartbeat eviction
│       ├── execstate/    # FileStore — JSON execution records
│       ├── reliability/  # DLQ, saga, circuit breaker, dedup, rate limiter
│       ├── registry/     # Service registry via HTTP heartbeat
│       ├── replyrouter/  # ReplyRouter — correlation ID → pending request channel
│       ├── observability/ # OTel tracing, Prometheus metrics
│       ├── compiler/     # DSL compiler abstraction (local/remote)
│       ├── plugins/      # WASM plugin loader
│       ├── flowengine/   # Flow orchestration state machine
│       ├── adapters/     # Adapters implementing pkg/ interfaces from internal/ types
│       └── ports/        # Port interfaces
├── sdk/             # Polyglot SDKs
│   ├── flow/            # Go client library
│   ├── java/            # Java SDK (Maven, com.flowrulz)
│   ├── python/          # Python SDK (pip, flowrulz)
│   ├── javascript/      # JS/TS SDK (npm, flowrulz)
│   └── rust/            # Rust SDK (cargo, flowrulz-sdk)
├── simulator/       # Simulator for testing rules, services, and cluster behavior
│   ├── cmd/simulator/   # CLI entry point (--scenario, --interactive, --dashboard)
│   ├── config/          # SimConfig, ChaosConfig
│   ├── dashboard/       # HTTP dashboard + admin API
│   ├── scenarios/       # 9 built-in scenarios (order-processing, circuit-breaker, etc.)
│   ├── services/        # 16 mock services (10 business + 6 infrastructure)
│   ├── client.go        # Programmatic Client (Send, AddRule, RegisterService)
│   ├── handlers.go      # Admin HTTP handlers
│   └── ...              # scheduler, dispatcher, loadgen, network, etc.
├── docs/
│   ├── flow-architecture.md  # Distributed Event Runtime — architecture, Event model, ExecutionContext, flows
│   ├── dsl-syntax.md         # DSL language specification
│   ├── bytecode-format.md    # ExecutionPlan, Instruction, opcodes, types
│   ├── vm-architecture.md    # VM dispatch, opcode handlers, ExecutionContext
│   ├── memory-management.md  # Arena, interning, message lifecycle
│   ├── ffi-api.md            # C FFI surface for Go bridge
│   ├── kafka-semantics.md    # Legacy Kafka transport reference
│   ├── cluster-model.md      # Single-leader cluster, membership, plan distribution, service registry
│   ├── flows.md              # Every data path: membership → deployment → execution → DLQ → metrics
│   ├── file-index.md         # Every source file: package, purpose, key exports
│   ├── software-review.md    # Multi-layer codebase review (architecture, bugs, security, ops)
│   ├── ultimate-review-prompt.md # Architecture review prompt for decoupling, SOLID, DRY, and OOP design
│   ├── development.md
│   ├── obsidian-vault/       # Obsidian vault (26 notes, arch map, canvas)
│   └── README.md
├── AGENTS.md
├── Makefile
├── go.mod
└── README.md
```

## Quick Start

```bash
# Full build + all tests
make && make test

# Benchmarks
make bench

# Run server (HTTP admin on :8080)
./flowrulz
```

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Rust hot path, Go I/O | Performance-critical execution in Rust; Go for admin, observability, transport |
| Event as universal type | Opaque payload — any serialization (JSON, Protobuf, Avro, MessagePack, binary) |
| 8-byte packed instructions | Cache-friendly, easy to snapshot/serialize |
| Direct std::alloc | `flowrulz_msg_alloc` / `flowrulz_msg_release` use `std::alloc` directly — slab pool removed |
| DSL → bytecode compiler | Compile once, execute many; no parse cost per message |
| ExecutionContext | Services enrich context (body + outputs + variables) instead of replacing a single JSON blob |
| DAG as embedded sub-language | Complex routing expressed declaratively; validated at compile time |
| Go service caller bridge | Rust VM calls back into Go via `sync.Map` + C helper; concurrent callers by ctxID |
| Complexity scoring | Compile-time cost estimate → lane assignment (fast/normal/heavy) |
| Schema-typed fields | Runtime type validation via `TypeGuard` opcode; compile-time Gate/Map operator checking |
| Enum types | Field validation against allowed value set; `enum[val1|val2|...]` syntax |
| File-based persistence | Rules saved/loaded as JSON; atomic write via `.tmp` + `os.Rename` |
| 4 communication models | Publish (async), Request (sync), Execute (rule), Stream (subscription) — single SDK |
| Single-leader cluster | Lowest-ID alive node is leader; no Raft/Paxos — Cluster Bus provides transport |
| Seed-based membership | Nodes discover via seed peers; heartbeat on `_flowrulz_members` via Cluster Bus |
| Service Registry | Services self-register via POST /register with methods/version/protocol/zone/weight; heartbeat expiry (30s TTL) marks unhealthy; LookupInstance(name, method) selects method-aware endpoints |
| Reply Router | Per-node pending request tracker by correlation_id; timeout/cleanup goroutine; routed via Cluster Bus |
| Scheduler | Lane-based priority queues + work stealing; Fast (50 concurrent, 5k), Normal (20, 2k), Heavy (5, 500); idle workers steal from Heavy→Normal→Fast |
| Plan Distribution | `PlanDistributor` publishes plans on `_flowrulz_plans`; followers ACK on `_flowrulz_acks`; quorum-based activation |
| Rate Limiter | Token bucket per name; configurable rate/burst for ingress control |
| Dead Letter Queue | Bounded queue with replay support; JSON export, per-entry retry count |
| WASM Plugin SDK | Sandboxed WebAssembly plugins via wasmtime; `w:plugin.func()` DSL syntax; module caching, fuel limits, memory I/O convention |
| Metrics | Counters, gauges, histograms; global shortcuts for exec/error tracking |
