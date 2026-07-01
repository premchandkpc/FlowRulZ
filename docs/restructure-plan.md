# FlowRulZ Restructuring Plan

Split large files (>200 lines) into focused single-responsibility files.

## Execution Order

1. ✅ Rust DSL: compiler.rs → compiler/
2. ✅ Rust DSL: lexer.rs → lexer/
3. ✅ Rust DSL: parser.rs → parser/
4. ✅ Rust DSL: optimizer.rs → optimizer/
5. ✅ Rust FFI: ffi.rs → ffi/
6. ✅ Rust executor: expr.rs → expr/
7. ✅ Rust executor: mod.rs → {mod,vm}.rs
8. ✅ Rust executor: plugin.rs → plugin/{mod,loader,runtime}.rs
9. ✅ Go bridge: bridge.go (478) → {bridge,compile,execute,plan,memory}.go
10. Go registry: registry.go → {registry,lookup,health,http}.go
11. Go execnode: execnode_execution.go → {call_service,execute_plan,recovery}.go
12. Go execnode: execnode_http.go → {server,handlers}.go
13. Go plandist: plandist.go → {plan,ack}.go
14. Go membership: membership.go → {membership,lease}.go
15. Go partition: manager.go → {manager,rebalance}.go
16. Go grpc: bus.go → {bus,client}.go
17. Simulator dashboard: dashboard.go → {dashboard,handlers,templates,websocket}.go
18. Simulator scheduler: scheduler.go → {scheduler,events,time,stats}.go
19. Simulator admin.go → {routes,handlers}.go
20. Test files
