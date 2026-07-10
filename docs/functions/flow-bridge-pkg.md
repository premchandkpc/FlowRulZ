# internal/flow, bridge, pkg — Function Reference

---

## internal/flow/ (DSL Compiler & CLI)

### Lexer (`lexer.go`)

#### `NewLexer(input string) *Lexer`
Creates lexer for `.flow` DSL input.

#### `(l *Lexer) Tokenize() ([]Token, error)`
**Flow:**
1. Skip whitespace/comments.
2. Read tokens: strings (`"..."`), numbers, identifiers, keywords, operators, version literals (`v1.2.3`).
3. Returns token stream or first syntax error.

**Edge Cases:**
- Unterminated strings → error.
- Block comments `/* ... */` → skipped.
- Line comments `// ...` → skipped.

#### `FilterNewlines(tokens []Token) []Token`
Removes newline tokens (for contexts where newlines are insignificant).

#### `TokenTypeName(typ TokenType) string`
Returns human-readable token type name.

---

### Parser (`parser.go`)

#### `NewParser() *Parser`

#### `(p *Parser) ParseFile(path string) (*Flow, error)`
Reads file → `Parse(data)`.

#### `(p *Parser) Parse(data []byte) (*Flow, error)` / `ParseString(src string) (*Flow, error)`

**Flow:**
1. Tokenize input.
2. Parse flow structure:
   - `description:` block → metadata
   - `variables:` block → typed variables
   - `constants:` block → constant values
   - `outputs:` block → output definitions
   - `import:` / `include:` → dependencies
   - `service:` blocks → service definitions with endpoints
   - `event:` blocks → event definitions
   - `workflow:` block → step definitions with control flow

**Edge Cases:**
- Missing `workflow:` block → error.
- Unknown keywords → error.
- Nested control flow (if/switch/parallel/foreach/while) → recursive parsing.

#### Control Flow Parsing
- `parseIfBlock()` — `if (condition) { ... } else { ... }`
- `parseSwitchBlock()` — `switch (var) { case ...: ... }`
- `parseParallelBlock()` — `parallel { ... }`
- `parseWaitBlock()` — `wait (condition) { ... }`
- `parseForeachLoop()` — `foreach (item in collection) { ... }`
- `parseWhileLoop()` — `while (condition) { ... }`

#### Resilience Parsing
- `parseRetry()` — `retry (max: N, backoff: duration) { ... }`
- `parseBreaker()` — `breaker (threshold: N, timeout: duration) { ... }`
- `parseOnError()` — `on_error { compensate ... }`

---

### Semantic Analyzer (`semantic.go`)

#### `NewAnalyzer() *Analyzer`

#### `(a *Analyzer) Analyze(flow *Flow) []SemanticError`
**Flow:**
1. Register all services, events, variables, constants.
2. Validate workflow:
   - All referenced services exist.
   - All referenced steps exist.
   - No circular dependencies.
   - Compensation targets are valid step names.
3. Validate error blocks reference valid services.

**Edge Cases:**
- Undefined service reference → semantic error.
- Undefined step reference → semantic error.
- Duplicate service names → semantic error.

---

### Compiler (`ir.go`)

#### `NewCompiler() *Compiler`

#### `(c *Compiler) Compile(flow *Flow) (*IR, error)`
**Flow:**
1. Walk AST, emit IR nodes:
   - Service calls → `IRNode` with service ID, method, inputs/outputs.
   - Control flow → conditional branches, loops, parallel forks/joins.
   - Error handling → compensate paths.
2. Build dependency graph.
3. Assign node IDs.

#### `MarshalIR(ir *IR) ([]byte, error)` / `UnmarshalIR(data []byte) (*IR, error)`
Serialization for IR transport.

---

### Code Generator (`codegen.go`)

#### `NewCodeGenerator(target CodeGenTarget) *CodeGenerator`

#### `(g *CodeGenerator) Generate(ir *IR) (string, error)`
Dispatches to target-specific generator.

#### Target Generators:
- `generateGo(ir)` — Go source code
- `generateRust(ir)` — Rust source code
- `generateJava(ir)` — Java source code
- `generatePython(ir)` — Python source code

---

### Graph Generator (`graph.go`)

#### `NewGraphGenerator() *GraphGenerator`

#### `(g *GraphGenerator) DOT(ir *IR) string`
Generates Graphviz DOT format.

#### `(g *GraphGenerator) Mermaid(ir *IR) string`
Generates Mermaid diagram format.

---

### Formatter (`formatter.go`)

#### `NewFormatter() *Formatter`

#### `(f *Formatter) Format(flow *Flow) string`
Pretty-prints a Flow AST back to `.flow` DSL syntax.

---

### Registry (`registry.go`)

#### `NewRegistry(c cache.Cache) *Registry`
Creates flow registry with cache.

#### `(r *Registry) LoadFile(ctx, path) error`
Loads single `.flow` file, parses, registers.

#### `(r *Registry) LoadDirectory(ctx, dir) error`
Walks directory, loads all `.flow` files.

#### `(r *Registry) Register(ctx, ast *Flow) error`
**Flow:**
1. Semantic analysis.
2. Compile DSL → bytecode via bridge.
3. Store compiled plan in cache.

#### `(r *Registry) Get(ctx, name) (*FlowState, error)` / `GetByTopic(ctx, topic)`
Retrieves compiled flow by name or topic.

#### `(r *Registry) List(ctx) []*FlowState` / `Delete(ctx, name)` / `Format(name)`

---

### LSP Server (`lsp.go`)

#### `NewLSPServer() *LSPServer`

#### `(s *LSPServer) HandleRequest(request []byte) ([]byte, error)`
JSON-RPC handler for LSP protocol.

#### Key Methods:
- `OpenDocument(uri, content)` → parses + returns diagnostics.
- `UpdateDocument(uri, content)` → re-parses + returns diagnostics.
- `CloseDocument(uri)` → removes from state.
- `FormatDocument(uri)` → returns formatted source.
- `Completion(uri, pos)` → returns completion items.
- `Hover(uri, pos)` → returns hover info.
- `Graph(uri)` → returns Mermaid diagram.

---

### CLI (`cli.go`)

#### `NewCLI() *CLI`

#### `(c *CLI) Run(args []string) error`
Dispatches to subcommands:

| Command | Description |
|---|---|
| `fmt` | Format `.flow` files |
| `validate` | Validate `.flow` files |
| `graph` | Generate graph (DOT/Mermaid) |
| `codegen` | Generate code (Go/Rust/Java/Python) |
| `info` | Show flow metadata |

#### `ParseArgs(args []string) (flags map[string]string, files []string)`
Parses CLI arguments into flags and file paths.

---

## bridge/ (CGo FFI Bridge)

### `Compile(dsl string, ruleID string) ([]byte, error)`
**Flow:**
1. Intern DSL string.
2. Call Rust `flowrulz_compile` via CGo.
3. Return compiled bytecode.

**Edge Cases:**
- CGo FFI error → Go error.
- String interning via global table.

### `InitContext(body []byte) ([]byte, error)`
Creates execution context from JSON body via Rust VM.

### `Execute(plan []byte, body []byte, caller ServiceCaller, ctx *ExecContext) ([]byte, error)`
Single-shot execution: runs all steps sequentially.

### `ExecuteStep(plan, ctxBytes, respBytes []byte, caller ServiceCaller) (*StepOutput, error)`
**Flow:**
1. Call Rust `flowrulz_execute_step` via CGo.
2. Returns `StepOutput` with:
   - `Result`: StepDone/StepPending/StepContinue
   - `Output`: final output bytes
   - `PendingSvc`: service ID waiting on
   - `PendingBody`: request body for service
   - `TimeoutMs`: service call timeout
   - `CtxBytes`: updated context

**Edge Cases:**
- FFI error → Go error.
- Context bytes truncated → unexpected result code.

### `PlanServices(plan []byte) ([]ServiceEntry, error)`
Extracts service references from plan bytecode.

### `PlanComplexity(plan []byte) uint32`
Returns complexity score for lane assignment.

### `Intern(s string) uint16` / `InternLookup(id uint16) string`
String interning for compact bytecode representation.

### `ParseServiceMethod(s string) (service, method string)`
Parses `"service/method"` format.

### `ParseCompensation(s string) (service, method, compensator, compMethod string)`
Parses `"service/method->compSvc/compMethod"` format.

### `NewBridgeVM() *BridgeVM`
VM adapter with compile caching.

#### `(b *BridgeVM) Compile(ctx, dsl, ruleID) (*vm.CompileResult, error)`
#### `(b *BridgeVM) CompileAndCache(ctx, dsl, ruleID) (*vm.CompileResult, error)`
#### `(b *BridgeVM) ExecuteStep(ctx, plan, ctxBytes, respBytes, opts) (*vm.StepResult, error)`

---

## pkg/ (Public APIs)

### `pkg/common.HashBody(body []byte) string`
SHA-256 hex encoding of body.

### `pkg/common.HashBodyPrefixed(prefix string, body []byte) string`
SHA-256 with prefix: `prefix + ":" + hex(sha256(body))`.

### `pkg/common.NewBearerAuth() *BearerAuth`
Creates auth from `FLOWRULZ_CLUSTER_TOKEN` env.

#### `(a *BearerAuth) Check(r *http.Request) bool`
Validates `Authorization: Bearer {token}` header.

#### `(a *BearerAuth) Require(next http.HandlerFunc) http.HandlerFunc`
Middleware: rejects if auth fails.

### `pkg/common.WriteJSON(path string, v any) error`
Atomic JSON write (tmp + rename).

### `pkg/common.ReadJSON(path string, v any) error`
Reads and unmarshals JSON.

### `pkg/common.LoadDir[T](dir, ext string, decode func([]byte) (T, error)) ([]T, error)`
Loads all files with given extension, decodes each.

### `pkg/cluster.LeadershipToken`
```go
type LeadershipToken struct {
    Leader bool
    Term   uint64
}
```

#### `(lt LeadershipToken) Valid() bool`
Returns `lt.Leader && lt.Term > 0`.

### `pkg/scheduler.DefaultLaneConfigs()`
Returns `Fast(50)/Normal(20)/Heavy(5)` lane configs for SDK usage.

---

## internal/flow/watcher.go — File Watchers

### `NewFileWatcher(registry, dirs...) *FileWatcher`
Watches `.flow` files for changes. Default interval: 5s.

### `(w *FileWatcher) SetInterval(d)` — Changes poll interval.
### `(w *FileWatcher) Start(ctx) error` — Loads all dirs initially, then polls for modified `.flow` files.
### `(w *FileWatcher) Stop()` — Stops watching.

### `NewDebouncedWatcher(watcher, delay) *DebouncedWatcher`
Debounces file change events. `Notify()` resets timer; reload fires after `delay`.

---

## internal/flow/ — Additional Types

### `Lexer` — `Position() (line, col)`, `Remaining() string`, `PeekToken(tokens, pos) Token`
### `Token` — `String() string`
### `SemanticError` — `Error() string`
### `Registry` — `Close() error`
### `LSPServer` — `Diagnostics(uri) []Diagnostic`

---

## bridge/ — Additional Functions

### `SpanSize() int` — Returns size of raw span struct in bytes (for OTel export).
### `GetSpans() []byte` — Drains global span ring buffer.
### `RegisterPlugin(name, wasmBytes) error` — Registers WASM plugin by name.
### `MsgAlloc(size) unsafe.Pointer` / `MsgRelease(ptr)` — FFI message buffer allocation.
### `(BridgeVM).InvalidateCache(ruleID)` — Removes compiled plan from cache.
### `(BridgeVM).InitContext(ctx, body) ([]byte, error)` — Initializes VM execution context.
### `(BridgeVM).ParseServiceMethod(raw) (svc, method)` — Splits "svc/method" string.
### `(StepOutput).DelayMs() uint64` — Returns step delay in milliseconds.

---

## bridge/ — Types

### `ServiceCaller` — `func(svcID uint16, body []byte) ([]byte, error)` — callback for service invocation.
### `ExecContext` — FFI context bridging Go ↔ Rust VM.
### `ServiceEntry` — Compiled service reference in plan.
### `StepResult` — Step execution result code (int).
### `StepOutput` — Step output with delay metadata.
### `BridgeVM` — Rust VM adapter via CGo FFI.

---

## pkg/ — Interface Summary

### `pkg/engine.Engine` — `AddRule`, `RemoveRule`, `GetRule`, `ListRules`, `Execute`, `CompileRule`, `InvalidateCompilation`, `Start`, `Stop`
### `pkg/scheduler.Scheduler` — `ID`, `Enqueue`, `Snapshot`, `Start`, `Stop`
### `pkg/store.Store` — `Create`, `Save`, `Load`, `List`, `Delete`, `Close`
### `pkg/transport.EventBus` — `Publish`, `Subscribe`, `Unsubscribe`, `Request`, `Reply`, `Broadcast`, `PublishToPartition`, `TopicStats`
### `pkg/cluster.ClusterMember` — `IsLeader`, `LeaderID`, `Start`, `Stop`, `Join`, `Leave`, `CaptureLeadershipToken`, `ValidateLeadershipToken`, `ClusterSize`, `SubscribeLeaderChanges`, `Raft`
### `pkg/membership.Membership` — `Add`, `Remove`, `Heartbeat`, `MarkDead`, `MarkAlive`, `AliveCount`, `AliveNodes`, `LeaderID`, `Snapshot`, `Lookup`, `Start`, `Stop`, `SetLeaderLease`, `OnLeaseExpiry`, `StartEviction`, `StartLeaderLeaseChecker`
### `pkg/registry.Registry` — `Register`, `Unregister`, `Lookup`, `Pick`, `PickWithStrategy`, `MarkHealthy`, `MarkUnhealthy`, `AllServiceInfo`, `WatchChanges`
### `pkg/partition.PartitionManager` — `Assign`, `GetAssignment`, `Rebalance`, `LeaderID`
### `pkg/plandist.PlanDistributor` — `PublishPlan`, `ActivatePlan`, `WaitForAcks`, `CurrentTerm`, `IncDeployTerm`, `RecordAck`, `Start`, `Stop`
### `pkg/reliability.CircuitBreaker` — `RecordSuccess`, `RecordFailure`, `IsOpen`
### `pkg/reliability.Deduplicator` — `IsDuplicate`, `MarkSeen`
### `pkg/reliability.RateLimiter` — `Allow`, `SetRate`
### `pkg/reliability.SagaOrchestrator` — `Compensate`, `StatusInfo`
### `pkg/reliability.DLQ` — `Send`, `ReplayOne`, `ReplayAll`, `Entries`, `SetReplayFn`
### `pkg/replyrouter.ReplyRouter` — `Register`, `Deliver`, `Cancel`, `StartCleanup`, `StopCleanup`
### `pkg/vm.PlanCompiler` — `Compile`
### `pkg/vm.VMRunner` — `ExecutePlan`, `ExecuteStep`
