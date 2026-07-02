# FlowRulZ Architecture Review and Refactoring Guide

## 1. Purpose

This document provides a complete architecture review of the FlowRulZ repository with a strong focus on:

- project structure and folder organization
- file-level responsibilities
- loose coupling and high cohesion
- SOLID principles
- DRY and KISS principles
- ACID-style consistency expectations for stateful components
- statelessness where appropriate
- maintainability, scalability, and evolution

The goal is not just to describe the current codebase, but to define a future target architecture that is modular, explicit, and easier to evolve.

---

## 2. Review Summary

FlowRulZ is a distributed execution runtime that combines:

- a Go-based control plane and orchestration layer
- a Rust-based execution/runtime core
- a simulator for testing and experimentation
- cluster coordination, plan distribution, and service routing

The system has a strong domain model and clear architectural intent, but it is still evolving. Some areas are already well-structured, while others still mix responsibilities, centralize too much behavior, or rely on overly broad types and files.

The main improvement opportunity is to continue moving the codebase toward:

- clear module boundaries
- explicit interfaces and abstractions
- reduced cross-package coupling
- better separation of infrastructure concerns from business logic
- state management that is explicit and predictable
- stateless execution components where possible

---

## 3. Current Architectural Strengths

### 3.1 Clear domain boundaries
The repository already has meaningful domain packages such as:

- engine
- ProdNode (via NodeBuilder)
- scheduler
- registry
- replyrouter
- plandist
- cluster
- reliability
- observability
- admin
- transport
- compiler
- execstate

These are a good foundation for modular design.

### 3.2 Multi-language architecture is intentional
The split between Go and Rust is understandable:

- Go: orchestration, control plane, cluster coordination, server components
- Rust: execution engine/runtime and low-level performance-sensitive logic

That separation is beneficial when kept disciplined.

### 3.3 Execution model is conceptually strong
The system already has a strong notion of:

- rules
- plans
- execution nodes
- service calls
- routing
- retries
- compensation
- plan distribution

This makes the platform architecturally meaningful rather than merely procedural.

---

## 4. Main Architectural Concerns

### 4.1 Some modules still contain too many responsibilities
A few important files and packages still mix concerns such as:

- lifecycle management
- message handling
- transport wiring
- execution flow
- HTTP endpoints
- state persistence
- cluster coordination

This makes them harder to test and harder to evolve.

### 4.2 Some abstractions are still implicit
A good architecture should make dependencies explicit. In some cases, the code depends heavily on concrete implementations rather than interfaces or adapters.

This increases coupling and makes future changes more expensive.

### 4.3 Some stateful components are too centralized
The system contains components that are both:

- orchestration engines
- lifecycle owners
- state managers
- transport coordinators

These should be separated where possible.

### 4.4 Some areas would benefit from stronger stateless design
Where possible, execution and routing logic should be stateless or at least operate through clearly scoped state containers.

This improves:

- testability
- replayability
- fault tolerance
- concurrency safety

---

## 5. Recommended Target Architecture

FlowRulZ should evolve toward a layered architecture with clear boundaries:

### 5.1 Layer 1: API and Entry Points
Responsibilities:

- CLI entrypoints
- admin HTTP API
- external request ingestion
- transport adapters

Examples:

- go/cmd/flowrulz
- go/cmd/flowrulz-compiler
- simulator/cmd/simulator

### 5.2 Layer 2: Application Services
Responsibilities:

- orchestration of rules and plans
- workflow execution coordination
- lifecycle control
- service routing decisions

Examples:

- engine
- ProdNode (via NodeBuilder)
- scheduler
- registry
- replyrouter

### 5.3 Layer 3: Domain Logic
Responsibilities:

- plan execution semantics
- rule evaluation
- service selection
- routing logic
- compensation logic
- reliability policy handling

These should be as independent as possible from transport and persistence concerns.

### 5.4 Layer 4: Infrastructure Adapters
Responsibilities:

- HTTP transport
- gRPC transport
- persistence stores
- cluster messaging
- DLQ integration
- metrics exporters

Examples:

- transport
- cluster
- execstate
- reliability

### 5.5 Layer 5: Runtime Core
Responsibilities:

- execution engine
- bytecode handling
- plan execution
- Rust runtime behavior

Examples:

- rust/src

This layering should be explicit and enforced by package boundaries.

---

## 6. Folder-by-Folder Structure Review

### 6.1 Root-level structure
Current strengths:

- clear separation between Go, Rust, simulator, docs, and infrastructure files

Recommended direction:

- keep configuration and deployment files at the top level
- keep long-term docs and architecture guidance in docs/
- keep build/test conventions in Makefile and CI configuration

### 6.2 server/
This is the heart of the control plane and runtime orchestration.

Recommended structure:

- server/cmd/ : application entrypoints only
- server/internal/admin/ : admin API concerns only
- server/internal/engine/ : rule/plan orchestration logic
- server/internal/scheduler/ : scheduling policy and queueing
- server/internal/registry/ : service registry and lookup
- server/internal/replyrouter/ : correlation and reply routing
- server/internal/plandist/ : plan propagation and activation
- server/internal/cluster/ : cluster transport + Raft consensus + membership
- server/internal/transport/ : low-level transport adapters
- server/internal/reliability/ : circuit breaker, DLQ, rate limit, dedup, saga
- server/internal/observability/ : metrics and tracing
- server/internal/execstate/ : execution state persistence
- server/internal/partition/ : partitioning and rebalance
- server/internal/node/ : ProdNode assembly
- server/internal/bootstrap/ : DI composition root
- server/internal/compiler/ : DSL compiler abstraction
- server/pkg/ : public interfaces (13 packages)
- sdk/flow/ : Go client SDK
- server/bridge/ : FFI bridge and Go-to-Rust integration

### 6.3 runtime/
This is the execution core and should remain relatively isolated.

Recommended structure:

- runtime/src/bytecode/ : instruction and bytecode model
- runtime/src/dsl/ : parser, lexer, optimizer, compiler
- runtime/src/executor/ : runtime execution engine
- runtime/src/ffi.rs : FFI boundary
- runtime/src/tracing/ : tracing and spans
- runtime/src/memory/ : allocation and memory helpers
- runtime/src/error.rs : shared errors

The Rust layer should stay focused on execution semantics and avoid becoming a dumping ground for Go-side orchestration concerns.

### 6.4 simulator/
The simulator should remain a testing and experimentation surface.

Recommended structure:

- simulator/cmd/ : entrypoints
- simulator/eventbus/ : in-memory event bus
- simulator/execution/ : execution simulation helpers
- simulator/scheduler/ : simulation-specific scheduler
- simulator/services/ : mock services
- simulator/scenarios/ : scenario definitions
- simulator/dashboard/ : dashboard and visualization

The simulator should depend on the same domain abstractions as production, but not become a second implementation of the runtime.

### 6.5 docs/
This should hold architecture, API, runtime, and engineering notes.

Recommended direction:

- keep docs synchronized with actual code
- avoid duplicating implementation details that can drift

---

## 7. File-Level Recommendations

### 7.1 Keep files focused on one role
A file should usually map to one of these responsibilities:

- domain model
- orchestration
- infrastructure adapter
- state store
- configuration
- transport handling
- tests

### 7.2 Avoid god files
Files that contain:

- initialization logic
- execution logic
- HTTP handling
- lifecycle management
- messaging logic
- persistence logic

in one place are too large and should be split.

### 7.3 Prefer smaller types over giant structs
Large structs should be decomposed into smaller collaborating types where possible.

Examples:

- execution node can delegate to dedicated services for startup, runtime handling, lifecycle, and message routes
- scheduler can split into queue management, policy evaluation, and dispatch coordination
- registry can split into registration, lookup, health, and refresh logic

---

## 8. SOLID Principles Review

### 8.1 Single Responsibility Principle
Many components are already close to this ideal, but some large orchestration types still do too much.

Recommendation:

- split node lifecycle, message handling, and execution orchestration into clearly scoped components
- keep each file and each type focused on one responsibility

### 8.2 Open/Closed Principle
The system should allow new behavior without modifying core logic repeatedly.

Recommendation:

- introduce strategies for scheduling, routing, plan distribution, and failure handling
- use interfaces or function hooks for pluggable behavior

### 8.3 Liskov Substitution Principle
This is less visible in the current code but will matter as more abstractions are introduced.

Recommendation:

- ensure adapter implementations honor the same contracts consistently
- avoid special-case behavior that breaks interface semantics

### 8.4 Interface Segregation Principle
Large interfaces should be avoided. Smaller, focused interfaces are better.

Recommendation:

- define small interfaces for transport, persistence, plan distribution, routing, and observability

### 8.5 Dependency Inversion Principle
This is one of the most important opportunities.

Recommendation:

- components should depend on abstractions for:
  - transport
  - plan storage
  - registry lookup
  - persistence
  - metrics
  - state management

---

## 9. DRY and KISS Review

### 9.1 DRY
The codebase already shows some repetition in configuration, startup, and lifecycle handling.

Recommendation:

- centralize repeated initialization patterns
- standardize startup wiring for node/bootstrap/admin components
- create shared builders or constructors for common runtime setup

### 9.2 KISS
Keep the number of abstractions reasonable.

Recommendation:

- do not over-abstract too early
- introduce interfaces only where there is real variation or testability value

---

## 10. ACID and Consistency Expectations

FlowRulZ is not a classic database, but it still has stateful concerns that should behave consistently.

### 10.1 Atomicity
Operations such as:

- deploying a rule
- promoting a plan
- persisting execution state
- updating service registry state

should be atomic where practical.

Recommendation:

- use temp-file + rename semantics for persisted state
- separate plan activation from plan publication clearly
- ensure state transitions are explicit and recoverable

### 10.2 Consistency
The system should not leave execution state partially updated after failures.

Recommendation:

- use explicit transition states
- provide recovery paths for interrupted execution
- keep versioning clear for rules and plans

### 10.3 Isolation
Concurrent execution should not corrupt shared state unexpectedly.

Recommendation:

- protect shared state with clear synchronization boundaries
- avoid shared mutable state in hot paths where possible

### 10.4 Durability
Persistent state should survive process restarts.

Recommendation:

- persist critical execution data and plan state
- make persistence optional but consistent

---

## 11. Statelessness Recommendations

### 11.1 Prefer stateless execution logic
Execution and routing logic should be as stateless as possible.

This helps with:

- replaying events
- testing
- horizontal scalability
- fault recovery

### 11.2 Keep state in explicit containers
If state is required, encapsulate it in dedicated types rather than burying it in orchestration objects.

Examples:

- execution state store
- plan state snapshot
- pending request tracker
- saga context

### 11.3 Avoid hidden mutable global state
Shared mutable state should be minimized or wrapped by well-defined abstractions.

---

## 12. Recommended Refactoring Priorities

### Priority 1: Split large orchestrators
Focus on:

- ProdNode (via NodeBuilder)
- engine
- scheduler
- admin server

Why:

- these are likely to grow further
- they mix orchestration, lifecycle, transport, and state concerns

### Priority 2: Introduce explicit abstractions for infrastructure
Introduce interfaces for:

- transport
- persistence
- plan distribution
- registry lookup
- metrics

Why:

- reduces coupling
- improves testability
- enables alternative backends

### Priority 3: Separate domain logic from infrastructure logic
Move business decisions into clear domain packages and keep infrastructure concerns in adapters.

### Priority 4: Improve startup and lifecycle composition
Replace large constructor/setup logic with composition helpers and builder patterns.

### Priority 5: Formalize state transitions and recovery
Make rule deploy, plan activation, execution recovery, and failure handling explicit and consistent.

---

## 13. Suggested Future Folder Structure

A more explicit future layout could look like this:

```text
server/
  cmd/
    flowrulz/
  internal/
    admin/
    app/
      engine/
      scheduler/
      registry/
      replyrouter/
      plandist/
    domain/
      rules/
      plans/
      execution/
      routing/
    infra/
      cluster/
      transport/
      persistence/
      observability/
      reliability/
    shared/
      config/
      errors/
      types/
```

And for Rust:

```text
runtime/src/
  core/
  bytecode/
  dsl/
  executor/
  ffi/
  tracing/
  memory/
```

This structure makes responsibilities more obvious and reduces the chance that infrastructure concerns leak into domain logic.

---

## 14. Service-by-Service Architecture Review

FlowRulZ should be reviewed not only as a monolithic runtime, but also as a set of collaborating services and subsystems. Each service should expose a clear contract and carry a minimal number of responsibilities.

### 14.1 Execution Node Service
Responsibilities:
- host runtime execution
- coordinate rule execution and lifecycle
- manage service calls and retries
- observe execution health

Architecture guidance:
- keep orchestration logic in the execution node layer
- delegate transport, recovery, and persistence to adapters
- avoid mixing inbound message handling, lifecycle management, and runtime execution in one file

### 14.2 Engine Service
Responsibilities:
- manage rule lifecycle
- compile and deploy plans
- track versions and promotions
- expose rule execution state

Architecture guidance:
- separate deployment orchestration from plan execution semantics
- keep rule versioning and promotion logic explicit
- use abstractions for persistence and runtime integration

### 14.3 Scheduler Service
Responsibilities:
- queue execution work
- enforce concurrency limits
- apply lane and priority policies
- manage backpressure

Architecture guidance:
- separate queueing policy from scheduling decisions
- make lane policies pluggable
- keep the scheduler focused on dispatch and capacity management

### 14.4 Registry Service
Responsibilities:
- register services
- track health
- perform lookup
- support load balancing and selection

Architecture guidance:
- split registration from health management from selection logic
- model service instances as explicit domain objects
- avoid embedding transport or HTTP-specific concerns in the core registry logic

### 14.5 Reply Router Service
Responsibilities:
- correlate request/response pairs
- manage pending requests
- handle timeout and duplicate detection

Architecture guidance:
- keep correlation logic independent of execution runtime details
- treat reply routing as a stable coordination layer
- separate cleanup and lifecycle policy from message matching logic

### 14.6 Plan Distribution Service
Responsibilities:
- publish plans
- collect acknowledgements
- activate versions after quorum or approval
- handle stale plan rejection

Architecture guidance:
- keep distribution policy separate from execution logic
- make transport and quorum semantics explicit and injectable
- avoid allowing this layer to directly own too much runtime behavior

### 14.7 Cluster and Membership Service
Responsibilities:
- discover peers
- maintain membership state
- elect leaders
- route messages across nodes

Architecture guidance:
- keep membership state explicit and versioned
- separate node health tracking from message transport
- make cluster transport backend swappable

### 14.8 Reliability Layer
Responsibilities:
- circuit breaking
- dead-letter queueing
- rate limiting
- deduplication
- saga/coordinator behavior

Architecture guidance:
- model each reliability service as an isolated policy component
- keep policies composable
- avoid embedding reliability concerns directly into execution paths unless necessary

### 14.9 Observability Layer
Responsibilities:
- collect metrics
- expose telemetry
- support tracing and health reporting

Architecture guidance:
- keep observability as a side-effecting adapter layer
- avoid making core business logic depend on concrete tracing implementation details

### 14.10 Admin API Service
Responsibilities:
- expose CRUD and lifecycle operations
- validate rules and plans
- present runtime status and diagnostics

Architecture guidance:
- keep HTTP transport concerns separate from domain operations
- move validation and orchestration logic into application services
- avoid letting the server package own too much business state

---

## 15. Rust Runtime Architecture Review

The Rust runtime should remain the execution core, but it should also be structured clearly and be loosely coupled from the Go-side orchestration layer.

### 15.1 Rust responsibilities
The Rust side should focus on:
- parsing DSL input
- compiling to bytecode
- executing plans
- managing execution context
- exposing FFI-safe interfaces

### 15.2 Recommended Rust module boundaries
- bytecode/: instruction definitions, plan layout, opcodes
- dsl/: lexer, parser, optimizer, compiler
- executor/: runtime execution, control flow, state transitions
- tracing/: span and event tracking
- memory/: allocation helpers and buffers
- ffi/: FFI boundary for Go integration
- error/: shared error modeling

### 15.3 Rust design guidance
- keep business rules inside the runtime core
- keep bridging and interop logic in FFI adapters
- make runtime execution stateless where appropriate
- avoid letting the Rust core depend on Go concepts directly

### 15.4 Coupling considerations
The Rust runtime should depend on:
- clear execution abstractions
- explicit input/output structures
- stable serialization formats

It should not depend on:
- concrete Go runtime types
- HTTP or cluster state directly
- admin API implementation details

---

## 16. Simulator and Testing Architecture Review

The simulator is valuable because it can exercise FlowRulZ concepts without the full runtime stack. It should remain a thin but expressive test surface.

### 16.1 Simulator responsibilities
- emulate services and event flows
- exercise scheduling and routing logic
- validate workflows and orchestration behavior
- provide dashboard and visualization capabilities

### 16.2 Recommended structure
- simulator/cmd/: entrypoints
- simulator/eventbus/: in-memory event propagation
- simulator/execution/: execution helpers
- simulator/scheduler/: simulation policy
- simulator/services/: fake or lightweight service implementations
- simulator/scenarios/: scenario definitions
- simulator/dashboard/: visualization UI/API

### 16.3 Architectural guidance
- keep simulator behavior consistent with production semantics
- avoid creating a second incompatible runtime model
- make shared interfaces and domain contracts reusable across simulator and production

---

## 17. Cross-Cutting Concerns Review

### 17.1 Configuration
Configuration should be centralized and explicit.

Recommendation:
- use one structured configuration layer for runtime, transport, cluster, and persistence
- avoid scattered environment parsing across packages

### 17.2 Error handling
Errors should be explicit and typed.

Recommendation:
- introduce domain-specific error types where useful
- return errors with context rather than generic messages
- keep error handling consistent across Go and Rust boundaries

### 17.3 Logging and observability
Logging should be side-effecting and structured.

Recommendation:
- keep logging concerns out of domain logic when possible
- use centralized logging and telemetry hooks

### 17.4 Security and authentication
If admin endpoints and service exposure grow, their boundaries should be explicit.

Recommendation:
- keep auth and authorization policy separate from routing and execution logic

---

## 18. Recommended Refactoring Priorities Across the Whole Project

### Priority 1: Split large orchestrators
- ProdNode (via NodeBuilder)
- engine
- admin server
- scheduler

### Priority 2: Introduce explicit interfaces for infrastructure boundaries
- transport
- persistence
- registry lookup
- plan distribution
- metrics

### Priority 3: Separate domain logic from infrastructure logic
- move execution policy and routing decisions into domain-oriented packages
- keep HTTP/gRPC/cluster concerns in adapters

### Priority 4: Improve state management and recovery
- explicit execution state models
- versioned plan state
- recoverable lifecycle transitions

### Priority 5: Standardize runtime wiring
- startup composition
- dependency injection or builder patterns
- consistent lifecycle management

### Priority 6: Make the Rust/Go boundary cleaner
- define stable FFI contracts
- ensure Rust remains a runtime core, not a transport or orchestration layer

---

## 19. Final Verdict

FlowRulZ already has a strong foundation and a meaningful architecture. The main opportunity is to continue making it more modular, explicit, and loosely coupled across every service and runtime boundary.

The next phase of improvement should focus on:

- clear responsibility boundaries for each service
- smaller and more focused files and types
- explicit abstractions for infrastructure and state management
- more stateless execution logic where practical
- consistent and recoverable lifecycle semantics
- better separation between orchestration, domain logic, and adapters

That approach will make FlowRulZ easier to evolve, test, operate, and scale as a distributed runtime platform.
