# Architecture Review

Use this skill when reviewing FlowRulZ for maintainability, decoupling, and long-term design quality.

## When to use
- Review the execution node, engine, scheduler, registry, cluster, and admin layers
- Evaluate decoupling, loose coupling, SOLID, DRY, OOP design, and design patterns
- Prepare architecture audits or refactoring recommendations

## Focus areas
- Separation of concerns between control plane and data plane
- Tight coupling between orchestration and subsystem implementation
- Repeated logic, weak abstractions, and unclear ownership boundaries
- Appropriate use of patterns such as Strategy, Factory, Adapter, Facade, Repository, and Dependency Injection

## Output expectations
- Summarize the highest-impact architectural issues first
- Explain the problem, why it matters, and a concrete refactoring path
- Suggest a pragmatic roadmap for incremental improvement
