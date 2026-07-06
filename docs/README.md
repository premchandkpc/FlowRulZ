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
│   │   ├── node/        # Node interface + Dependencies (ExecRegistry, NodeEngine, etc.)
│   │   ├── plandist/    # Plan distribution interfaces
│   │   ├── partition/   # Partition management interfaces
│   │   ├── membership/  # Node membership interfaces
│   │   ├── store/       # Execution state persistence interfaces
│   │   ├── registry/    # Service registry interfaces
│   │   ├── reliability/ # Circuit breaker, DLQ, rate limit, dedup, saga interfaces
│   │   ├── replyrouter/ # Reply router interface
│   │   └── vm/          # Plan compilation + execution interfaces
│   └── internal/
│       ├── node/         # ProdNode — composition root with sub-components
│       │   ├── prod.go           # ProdNode struct + NewNode() constructor
│       │   ├── interfaces.go     # 16 DI interfaces (LeadershipStrategy, TransportFactory, etc.)
│       │   ├── layers.go         # 6 dependency bags (Cluster, Transport, Execution, etc.)
│       │   ├── execution_engine.go # VM step-loop + circuit breakers + saga
│       │   ├── ingress_pipeline.go # Rate limit → dedup → execute → DLQ
│       │   ├── message_router.go   # 5-topic consumer demux
│       │   ├── admin_http.go       # HTTP API (health, metrics, executions, partitions)
│       │   ├── leadership.go       # Strategy pattern: Raft or SingleLeader
│       │   ├── recovery.go         # Resume in-flight executions from state store
│       │   ├── production_invoker.go # Protocol-aware service dispatch (HTTP/gRPC/TCP)
│       │   └── cluster_adapter.go   # Cluster → TransportFactory bridge
│       ├── bootstrap/    # NodeBuilder — DI composition root
│       ├── engine/       # Rule lifecycle, versioning, lane routing, persistence
│       ├── scheduler/    # Priority lanes + work stealing
│       ├── cluster/      # gRPC p2p Cluster Bus + Gossip + transport adapter
│       ├── transport/    # Pluggable transport factory (Kafka, cluster, memory)
│       │   ├── factory.go         # TransportFactory with backend switching
│       │   ├── registry.go        # In-memory transport registration
│       │   └── kafka/             # Sarama-backed Kafka producer/consumer
│       ├── cache/        # Pluggable cache (memory, Redis)
│       ├── flow/         # Flow DSL — high-level workflow language
│       │   ├── lexer.go           # Hand-written tokenizer (40+ tokens)
│       │   ├── parser.go          # Recursive descent parser → AST
│       │   ├── semantic.go        # Semantic analysis (service refs, event refs)
│       │   ├── ir.go              # AST → IR graph compilation
│       │   ├── codegen.go         # IR → Go/Rust/Java/Python source
│       │   ├── graph.go           # IR → Graphviz DOT / Mermaid
│       │   ├── formatter.go       # Canonical .flow formatting
│       │   ├── cli.go             # CLI (fmt, validate, graph, codegen, info)
│       │   ├── lsp.go             # LSP server (completion, hover, diagnostics)
│       │   ├── watcher.go         # File watcher for hot-reload
│       │   └── registry.go        # Runtime store with cache-backed IR
│       ├── admin/        # HTTP API (rules CRUD, validate, promote, lanes)
│       ├── plandist/     # Plan distribution + ack protocol
│       ├── partition/    # Key-space shard mgmt + rebalancing
│       ├── membership/   # Gossip, leader lease, heartbeat eviction
│       ├── execstate/    # In-memory + file execution state persistence
│       ├── reliability/  # DLQ, saga, circuit breaker, dedup (16-shard LRU), rate limiter
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
│   ├── flow-architecture.md      # Distributed Event Runtime — architecture, Event model, ExecutionContext, flows
│   ├── dsl-syntax.md             # Rust DSL language specification
│   ├── bytecode-format.md        # ExecutionPlan, Instruction, opcodes, types
│   ├── vm-architecture.md        # VM dispatch, opcode handlers, ExecutionContext
│   ├── memory-management.md      # Arena, interning, message lifecycle
│   ├── ffi-api.md                # C FFI surface for Go bridge
│   ├── kafka-semantics.md        # Legacy Kafka transport reference
│   ├── cluster-model.md          # Single-leader cluster, membership, plan distribution, service registry
│   ├── flows.md                  # Every data path: membership → deployment → execution → DLQ → metrics
│   ├── file-index.md             # Every source file: package, purpose, key exports
│   ├── software-review.md        # Multi-layer codebase review
│   ├── ultimate-review-prompt.md # Architecture review prompt
│   ├── development.md            # Build, test, project layout, WASM plugins, Flow DSL CLI
│   ├── flow-dsl.md               # Flow DSL — high-level workflow language (new)
│   ├── transport-factory.md      # Pluggable transport backends (new)
│   ├── cache-system.md           # Cache abstraction — memory, Redis (new)
│   ├── admin-http.md             # Admin HTTP API endpoints (new)
│   ├── ingress-pipeline.md       # Reliability pipeline — rate limit, dedup, execute, DLQ (new)
│   ├── obsidian-vault/           # Obsidian vault (26 notes, arch map, canvas)
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
| Pluggable transport | `TransportFactory` selects Kafka, cluster gRPC, or in-memory backend at startup; no hard dependency on any transport |
| Cache abstraction | `Cache` interface with memory and Redis backends; used for flow IR caching with TTL |
| Flow DSL | High-level, block-structured workflow language (indentation-based); compiles to IR graph; separate from Rust bytecode DSL |
| Ingress pipeline | Reliability pipeline: rate limit → dedup (16-shard LRU) → execute → DLQ; atomic `CheckAndMark` prevents TOCTOU races |
| Leadership strategy | `LeadershipStrategy` interface abstracts Raft vs single-leader; fencing token pattern enforced everywhere |
| Interface-driven DI | 16 internal interfaces decouple ProdNode sub-components; testable with mocks |
| Node sub-components | ProdNode composed of ExecutionEngine, IngressPipeline, MessageRouter, AdminHTTPServer — not a monolith |
