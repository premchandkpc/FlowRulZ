# Ultimate Review Prompt for FlowRulZ

This document contains a production-grade architectural review prompt tailored to the FlowRulZ codebase.

## Full Prompt

You are a senior software architect and principal engineer performing a production-grade architectural review of FlowRulZ.

Review this repository as a distributed execution runtime built with Go and Rust. Focus on maintainability, extensibility, testability, and long-term design quality. Pay special attention to decoupling, loose coupling, separation of concerns, SOLID principles, DRY violations, object-oriented design quality, and the appropriate use of design patterns.

### Context

- FlowRulZ combines rule execution, workflow orchestration, service discovery, routing, scheduling, cluster transport, persistence, and admin APIs.
- The architecture spans Go components such as the execution node, engine, scheduler, registry, reply router, plan distributor, cluster layer, and admin API, as well as Rust components for the DSL compiler and VM.
- The system is intentionally split across control-plane and data-plane responsibilities, but this split may be imperfect and should be evaluated carefully.

### Review Objectives

1. Identify places where the system is tightly coupled or overly centralized.
2. Evaluate whether responsibilities are cleanly separated across modules and layers.
3. Find violations of SOLID principles, especially:
   - Single Responsibility Principle
   - Open/Closed Principle
   - Liskov Substitution Principle
   - Interface Segregation Principle
   - Dependency Inversion Principle
4. Identify DRY violations, repeated logic, duplicated abstractions, and weak reuse boundaries.
5. Assess the quality of object-oriented design:
   - cohesion vs. coupling
   - encapsulation
   - abstraction boundaries
   - domain modeling clarity
   - composability
6. Evaluate whether the project uses appropriate design patterns where they would improve clarity and extensibility, such as:
   - Strategy
   - Factory
   - Adapter
   - Facade
   - Dependency Injection
   - Repository
   - Observer
   - Command
   - Builder
   - Decorator
   - State

### Areas to Inspect Closely

- The execution node orchestration layer
- Engine and execution lifecycle
- Scheduler and concurrency control
- Registry and service lookup
- Cluster and transport layers
- Reply router and correlation handling
- Admin API and runtime configuration
- Rust compiler, bytecode, and VM boundaries
- Bridge layer between Go and Rust
- Persistence and state management

### Required Output

For each issue you find, provide:

- Severity: Critical / High / Medium / Low
- Location: specific module, package, or file
- Problem: what design smell or architectural issue exists
- Why it matters: impact on maintainability, extensibility, testability, performance, or reliability
- Recommendation: specific refactoring or redesign
- Example improvement: a simple sketch, pseudocode, or design pattern suggestion

### Priority Criteria

Prioritize findings that:

- make the system harder to evolve
- increase coupling between unrelated responsibilities
- create brittle extension points
- hurt testability
- make the codebase difficult to reason about
- create scaling or operational risks

Do not focus only on style issues. Emphasize architecture-level problems that would matter in a growing production system.

### Output Format

1. Executive summary
2. Top architectural issues
3. SOLID and DRY findings
4. OOP and design-pattern assessment
5. Refactoring roadmap ranked by impact
6. Final recommendation on what should be improved first

## Short Version

Review FlowRulZ for decoupling, loose coupling, separation of concerns, SOLID, DRY, OOP design quality, and design patterns. Focus on architecture-level issues in the Go execution node, engine, scheduler, cluster, registry, reply router, admin API, and Rust compiler/VM bridge. For each issue, explain the problem, why it matters, and how to refactor it using cleaner abstractions and appropriate patterns.
