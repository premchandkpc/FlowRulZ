# internal/engine, internal/scheduler, internal/reliability — Function Reference

---

## internal/engine/engine.go

### Types

```go
type Lane string

const (
    LaneFast   Lane = "fast"
    LaneNormal Lane = "normal"
    LaneHeavy  Lane = "heavy"
)

type LaneConfig struct {
    Name        Lane
    BatchSize   int
    PollTimeout int // ms
}

var DefaultLanes = []LaneConfig{
    {Name: LaneFast, BatchSize: 500, PollTimeout: 10},
    {Name: LaneNormal, BatchSize: 100, PollTimeout: 50},
    {Name: LaneHeavy, BatchSize: 10, PollTimeout: 500},
}

type VersionedPlan struct {
    Plan       []byte
    DSL        string
    Version    uint64
    Lane       Lane
    ActiveExec sync.WaitGroup
}

type Rule struct {
    ID            string
    Versions      []*VersionedPlan
    ActiveVersion int
}

type Engine struct {
    mu          sync.RWMutex
    rules       map[string]*Rule
    nextVersion atomic.Uint64
    persistPath string
    compiler    compiler.Compiler
    AfterDeploy  func(id, dsl string, plan []byte, version uint64)
    AfterPromote func(id string, version uint64)
}
```

---

### `LaneForScore(score uint32) Lane`

**Signature:** `func LaneForScore(score uint32) Lane`

**Flow:**
1. Branch on score:
   - `score < 10` → return `LaneFast`
   - `score <= 50` → return `LaneNormal`
   - `score > 50` → return `LaneHeavy`

**Edge Cases:**
- Score of exactly 10 → Normal (not Fast).
- Score of exactly 50 → Normal (not Heavy).
- Score of 0 → Fast.
- No bounds check on uint32 max — all values map cleanly.

---

### `New(persistPath string) *Engine`

**Signature:** `func New(persistPath string) *Engine`

**Flow:**
1. Delegates to `NewWithCompiler(persistPath, compiler.NewLocal())`.
2. `compiler.NewLocal()` creates a `LocalCompiler` that calls `bridge.Compile` (CGo FFI to Rust VM).

**Edge Cases:**
- Empty `persistPath` → no file I/O at construction time.
- Non-empty `persistPath` → calls `loadRules()` which reads JSON; if file missing, silently starts empty.

---

### `NewWithCompiler(persistPath string, comp compiler.Compiler) *Engine`

**Signature:** `func NewWithCompiler(persistPath string, comp compiler.Compiler) *Engine`

**Flow:**
1. Initialize `rules` map, store `persistPath` and `compiler`.
2. If `persistPath != ""` → call `e.loadRules()` to restore rules from JSON file.
3. Return engine.

**Edge Cases:**
- `comp` must not be nil — nil compiler will panic on first `Deploy`.
- `loadRules()` silently ignores corrupt JSON or missing file (logs error, starts empty).
- `nextVersion` starts at 0; first `Deploy` increments to 1.

---

### `(e *Engine) Deploy(id, dsl string) error`

**Signature:** `func (e *Engine) Deploy(id, dsl string) error`

**Flow:**
1. Compile DSL via `e.compiler.Compile(dsl, id)` → `result.Plan`, `result.Complexity`.
2. Create `VersionedPlan` with:
   - `Version`: `e.nextVersion.Add(1)` (atomic, globally unique).
   - `Lane`: `LaneForScore(result.Complexity)`.
3. **Lock** `e.mu` → find or create `Rule` for `id` → append `VersionedPlan` → set `ActiveVersion` to last index → save to disk → **unlock**.
4. If `AfterDeploy` hook is set → call it (outside lock).

**Edge Cases:**
- Compile error → returns error immediately, no state mutation.
- First deploy for an ID → creates new `Rule` with `ActiveVersion = 0`.
- Concurrent deploys to same ID → serialized by mutex; each gets unique version number.
- `AfterDeploy` hook is called outside the lock — safe to mutate engine or add hooks.
- `saveRules()` persists to JSON; if disk write fails, rule is still in memory but not durable.

---

### `(e *Engine) AddVersion(id, dsl string, plan []byte, version uint64) error`

**Signature:** `func (e *Engine) AddVersion(id, dsl string, plan []byte, version uint64) error`

**Flow:**
1. Create `VersionedPlan` from given plan/DSL/version. Lane computed from `bridge.PlanComplexity(plan)`.
2. **Lock** → if rule doesn't exist → create with `ActiveVersion = -1`.
3. Scan existing versions for matching `version` number:
   - Found → replace in-place (overwrite).
   - Not found → append to slice.
4. Save to disk → **unlock**.

**Edge Cases:**
- Version collision (same version number) → replaces existing entry silently. No error.
- Rule doesn't exist → created implicitly with `ActiveVersion = -1` (not active until `Promote`).
- `ActiveVersion = -1` means no version is active — `ActivePlan()` returns nil.
- Replacing an active version index doesn't change which index is active — only the plan bytes change.

---

### `(e *Engine) Promote(id string, version uint64) error`

**Signature:** `func (e *Engine) Promote(id string, version uint64) error`

**Flow:**
1. **Lock** → find rule by ID.
2. If rule not found → return `fmt.Errorf("rule not found: %s", id)`.
3. Scan versions for matching `version` number → set `ActiveVersion = index`.
4. If `AfterPromote` hook is set → call it (still under lock).
5. **Unlock**.

**Edge Cases:**
- Rule not found → error.
- Version not found → error (`"version %d not found for rule %s"`).
- Promoting to the already-active version → no functional change, but `AfterPromote` hook still fires.
- `AfterPromote` runs under lock — callers must not call back into the engine from the hook or risk deadlock.

---

### `(e *Engine) Drain(id string, version uint64) error`

**Signature:** `func (e *Engine) Drain(id string, version uint64) error`

**Flow:**
1. **Lock** → find rule → find version index → get `*VersionedPlan` reference → **unlock** (release lock before blocking).
2. `vp.ActiveExec.Wait()` — blocks until all in-flight executions for this specific version complete.
3. **Re-lock** → re-find rule (may have been deleted by concurrent `Remove`) → re-find version index.
4. Remove version from slice via `append(v[:i], v[i+1:]...)`.
5. Adjust `ActiveVersion` index:
   - If drained version was the active one → reset to `0` if versions remain, else `-1`.
   - If drained version index < active → decrement active index by 1.
6. If rule has no versions left → delete the rule entirely.
7. **Unlock**.

**Edge Cases:**
- Drains without holding lock → allows in-flight executions to call `Done()` without contention.
- Rule could be deleted by concurrent `Remove` between lock releases → re-check after re-lock, return nil.
- Version could be drained twice → second drain returns nil (rule/version already gone).
- If the drained version was the active version and others remain, index resets to 0 (first remaining), not to the next-best.
- No disk save after drain — persistence is stale until next mutation.

---

### `(e *Engine) Remove(id string)`

**Signature:** `func (e *Engine) Remove(id string)`

**Flow:**
1. **Lock** → find rule → copy versions slice → **unlock**.
2. For each version → `v.ActiveExec.Wait()` (blocks until all in-flight executions complete across ALL versions).
3. **Re-lock** → `delete(e.rules, id)` → save to disk → **unlock**.

**Edge Cases:**
- No-op if rule doesn't exist (early return after first lock).
- Waits for ALL versions' in-flight executions — this is more aggressive than `Drain` which targets one version.
- Concurrent calls to `Remove` for same ID → second call finds rule already deleted, returns immediately.
- No version adjustment needed — entire rule is deleted.
- Disk save after deletion ensures persistence.

---

### `(e *Engine) Rules() []Rule`

**Signature:** `func (e *Engine) Rules() []Rule`

**Flow:**
1. **RLock** → iterate all rules → for each rule, copy the struct and shallow-copy the versions slice → append to output → **RUnlock**.
2. Return slice of rule copies.

**Edge Cases:**
- Returns a deep copy — callers can safely mutate the returned slice without affecting engine state.
- `Versions` slice is shallow-copied (`append([]*VersionedPlan(nil), r.Versions...)`) — the `VersionedPlan` pointers themselves are shared, but the slice is independent.
- `ActiveExec` (sync.WaitGroup) is shared — callers should not Wait() on copied VersionedPlans during active execution.

---

### `(e *Engine) ActivePlanBytes() [][]byte`

**Signature:** `func (e *Engine) ActivePlanBytes() [][]byte`

**Flow:**
1. **RLock** → for each rule → call `r.ActivePlan()` → if non-nil and plan bytes non-empty → append to output → **RUnlock**.
2. Return slice of plan byte slices.

**Edge Cases:**
- Rules with `ActiveVersion = -1` → `ActivePlan()` returns nil → skipped.
- Rules with empty plan bytes → skipped.
- Order of output is non-deterministic (map iteration).
- Returns nil (not empty slice) if no rules have active plans.

---

### `(e *Engine) ExecuteAll(body []byte, caller bridge.ServiceCaller, ctx *bridge.ExecContext) ([][]byte, error)`

**Signature:** `func (e *Engine) ExecuteAll(body []byte, caller bridge.ServiceCaller, ctx *bridge.ExecContext) ([][]byte, error)`

**Flow:**
1. **RLock** → collect all rules with active plans → `ActiveExec.Add(1)` for each → **RUnlock**.
2. Sequentially call `bridge.Execute(vp.Plan, body, caller, ctx)` for each plan.
3. On success → append result to output, `ActiveExec.Done()`.
4. On error → `ActiveExec.Done()`, return partial results and error.

**Edge Cases:**
- First error stops execution — remaining plans are NOT executed but their `ActiveExec` is not incremented (only collected plans get incremented).
- This is a **single-shot** execution — the entire plan is executed in one bridge call, not step-by-step.
- **Testing only** — production uses `node.executeAll()` which runs plans concurrently via the scheduler with step-by-step execution (`bridge.ExecuteStep`).
- `ActiveExec` tracking ensures `Drain`/`Remove` blocks until all in-flight executions complete.
- No goroutine — runs synchronously on the caller's goroutine.
- `caller` is a function type `func(svcID uint16, body []byte) ([]byte, error)` — invoked by the Rust VM via CGo callback for `op_svc_call`.

---

## internal/scheduler/prod.go (Scheduler)

### Types

```go
var ErrQueueFull = errors.New("scheduler: queue full")

type Priority int

const (
    PriorityFast   Priority = 0
    PriorityNormal Priority = 1
    PriorityHeavy  Priority = 2
)

type Task struct {
    ID       string
    Priority Priority
    Body     []byte
    Deadline time.Time
    Execute  func(ctx context.Context, task *Task) ([]byte, error)
    ResultCh chan TaskResult
}

type TaskResult struct {
    Output []byte
    Error  error
}

type LaneConfig struct {
    Name          Priority
    MaxConcurrent int
    QueueSize     int
    RejectOnFull  bool
}

var DefaultLanes = []LaneConfig{
    {Name: PriorityFast, MaxConcurrent: 50, QueueSize: 5000, RejectOnFull: false},
    {Name: PriorityNormal, MaxConcurrent: 20, QueueSize: 2000, RejectOnFull: false},
    {Name: PriorityHeavy, MaxConcurrent: 5, QueueSize: 500, RejectOnFull: true},
}

type Scheduler struct {
    mu       sync.Mutex
    lanes    map[Priority]*lane
    started  bool
    stopCh   chan struct{}
    totalEnq atomic.Int64
    totalDeq atomic.Int64
    totalRej atomic.Int64
}
```

---

### `New(lanes []LaneConfig) *Scheduler`

**Signature:** `func New(lanes []LaneConfig) *Scheduler`

**Flow:**
1. If `lanes` is nil → use `DefaultLanes`.
2. Initialize `lanes` map and `stopCh` channel.
3. For each `LaneConfig` → create a `lane` with a buffered channel of size `QueueSize`.

**Edge Cases:**
- Duplicate priority in `lanes` slice → last one wins (map overwrite).
- `QueueSize = 0` → creates unbuffered channel → enqueue blocks until consumed (deadlock risk).
- `MaxConcurrent = 0` → `Start()` spawns zero workers → tasks queue forever.

---

### `(s *Scheduler) Start(ctx context.Context) error`

**Signature:** `func (s *Scheduler) Start(ctx context.Context) error`

**Flow:**
1. **Lock** → if already started → **unlock**, return nil (idempotent).
2. Set `started = true` → **unlock**.
3. For each lane → `wg.Add(l.cfg.MaxConcurrent)`.
4. For each lane → spawn `laneWorker` goroutine → each spawns `MaxConcurrent` `slotWorker` goroutines.
5. Log startup.

**Edge Cases:**
- Idempotent — calling `Start()` twice is safe, returns nil.
- Workers inherit the passed `ctx` — cancellation propagates to all slot workers.
- `wg.Add` is called before spawning workers — ensures `Stop()` waits correctly.
- Lane with `MaxConcurrent = 50` → 50 goroutines total for that lane.

---

### `(s *Scheduler) Stop() error`

**Signature:** `func (s *Scheduler) Stop() error`

**Flow:**
1. **Lock** → if not started → **unlock**, return nil.
2. Close `stopCh` → signals all workers via channel select.
3. For each lane → `wg.Wait()` → blocks until all `slotWorker` goroutines exit.
4. Set `started = false` → **unlock**.

**Edge Cases:**
- Releases mutex before `wg.Wait()` (per AGENTS.md) → prevents deadlock if tasks call `Snapshot()` during shutdown.
- Closing `stopCh` causes `dequeueOrSteal` to return nil → workers exit their loops.
- Idempotent — safe to call multiple times.
- Tasks with pending `ResultCh` reads will hang if not drained → `EnqueueAndWait` spawns a drain goroutine on context cancel.

---

### `(s *Scheduler) EnqueueTask(task *Task) error`

**Signature:** `func (s *Scheduler) EnqueueTask(task *Task) error`

**Flow:**
1. If `task == nil` → return error `"scheduler: nil task"`.
2. If `task.Priority` is out of range (`< PriorityFast` or `> PriorityHeavy`) → default to `PriorityNormal`.
3. Look up lane by priority → if not found → return error `"scheduler: unknown priority"`.
4. Call `lane.enqueue(task)` → if returns false → increment `totalRej`, return `ErrQueueFull`.
5. Increment `totalEnq`.

**Edge Cases:**
- Heavy lane has `RejectOnFull = true` → returns `ErrQueueFull` when queue is full (500 tasks).
- Fast/Normal lanes have `RejectOnFull = false` → enqueue blocks (via channel send) until a worker is available or `stopCh` closes.
- If scheduler is not started → tasks still enqueue (channels are buffered) but no workers consume them. This fills the buffer, then blocks.
- Invalid priority silently coerced to Normal — no error.

---

### `(s *Scheduler) EnqueueAndWait(ctx context.Context, task *Task) ([]byte, error)`

**Signature:** `func (s *Scheduler) EnqueueAndWait(ctx context.Context, task *Task) ([]byte, error)`

**Flow:**
1. Create buffered `ResultCh` (capacity 1) on the task.
2. Call `EnqueueTask(task)`.
3. `select` on:
   - `task.ResultCh` → return `Output` and `Error`.
   - `ctx.Done()` → return `ctx.Err()`.

**Edge Cases:**
- **Context cancellation leak prevention:** spawns a background goroutine to drain `ResultCh` after context cancel, preventing the slot worker from blocking on a send to a nobody-reads channel.
- If task panics → `execTask`'s `defer recover()` catches it, sends `TaskResult{Error: ...}` to `ResultCh`. Caller receives the error instead of hanging.
- If scheduler stops while task is pending → drain goroutine also listens on `s.stopCh`.
- Deadline on task → `execTask` creates a child context with that deadline; if deadline expires, `Execute` receives a cancelled context.
- Returns partial results if `EnqueueTask` fails (returns nil, error).

---

### `(s *Scheduler) dequeueOrSteal(ctx context.Context, myLane *lane) *Task`

**Signature:** `func (s *Scheduler) dequeueOrSteal(ctx context.Context, myLane *lane) *Task`

**Flow (work-stealing):**
1. Non-blocking read from own lane queue (`select` with `default`).
2. If empty → **steal**: iterate other lanes from Heavy to Fast (descending priority), non-blocking read from each.
3. If still empty → blocking read from own lane (with `stopCh` and `ctx.Done()` checks).

**Edge Cases:**
- Steals from **higher-priority** lanes first (Heavy → Fast) — this prioritizes work that would otherwise block high-priority workers.
- Falls back to blocking on own lane to prevent starvation when all lanes are empty.
- `select` on `stopCh`/`ctx.Done()` ensures clean shutdown.
- Non-blocking reads never block — if no work is available anywhere, falls through to blocking.
- A Fast worker can steal from Heavy lane — Heavy tasks get processed by whatever worker is free.

---

### `(s *Scheduler) execTask(ctx context.Context, task *Task)`

**Signature:** `func (s *Scheduler) execTask(ctx context.Context, task *Task)`

**Flow:**
1. If `task.Deadline` is set → create child context with that deadline via `context.WithDeadline`.
2. `defer cancel()` if deadline context was created.
3. **`defer recover()`** — catches panics, logs error, sends `TaskResult{Error}` to `ResultCh`.
4. Call `task.Execute(execCtx, task)`.
5. If `task.ResultCh != nil` → send `TaskResult{Output, Error}`.

**Edge Cases:**
- **Panic safety:** `defer recover()` ensures panicking tasks don't crash the worker goroutine. Error is sent to `ResultCh` instead.
- If `ResultCh` is nil (fire-and-forget task) → result is discarded, panic still recovered.
- Deadline context is properly cancelled via `defer cancel()` to release timer resources.
- Worker goroutine is never killed by task panic — continues to next task.

---

## internal/scheduler/worker.go (TimerWheel)

### Types

```go
type Timer struct {
    ID       uint64
    Callback func()
}

type TimerWheel struct {
    mu          sync.Mutex
    tick        time.Duration
    slotCount   int
    slots       []*list.List
    currentSlot int
    nextID      atomic.Uint64
    ticker      *time.Ticker
    done        chan struct{}
    stopOnce    sync.Once
    entries     map[uint64]*list.Element
    wg          sync.WaitGroup
}
```

---

### `NewTimerWheel(tick time.Duration, slotCount int) *TimerWheel`

**Signature:** `func NewTimerWheel(tick time.Duration, slotCount int) *TimerWheel`

**Flow:**
1. Create `slotCount` empty linked lists.
2. Initialize `entries` map, `done` channel.
3. Return `TimerWheel`.

**Edge Cases:**
- `slotCount = 0` → will panic on slot access (division by zero in modulo).
- `tick = 0` → `time.NewTicker(0)` panics.
- Not started — must call `Start()` before timers fire.

---

### `(tw *TimerWheel) Start()`

**Signature:** `func (tw *TimerWheel) Start()`

**Flow:**
1. Create `time.Ticker` with `tw.tick` interval.
2. Spawn background goroutine that calls `tickOnce()` on each tick or exits on `done` channel close.

**Edge Cases:**
- Calling `Start()` twice → creates second ticker/goroutine (no idempotency guard).
- Ticker runs until `Stop()` closes `done` channel.

---

### `(tw *TimerWheel) Stop()`

**Signature:** `func (tw *TimerWheel) Stop()`

**Flow:**
1. `stopOnce.Do` → stop ticker → close `done` channel.
2. `tw.wg.Wait()` — blocks until all in-flight callbacks complete.

**Edge Cases:**
- **Safe to call multiple times** — `stopOnce` ensures ticker is stopped and `done` is closed only once.
- `wg.Wait()` blocks until all callbacks that are currently executing complete. Callbacks launched after `done` close won't happen because the `run` goroutine exits.
- If a callback blocks forever → `Stop()` hangs forever.
- Callbacks fire in separate goroutines (via `go func` in `tickOnce`) — `wg.Wait` handles them.

---

### `(tw *TimerWheel) Add(d time.Duration, callback func()) *Timer`

**Signature:** `func (tw *TimerWheel) Add(d time.Duration, callback func()) *Timer`

**Flow:**
1. **Lock** → assign unique ID via `nextID.Add(1)`.
2. Calculate ticks: `int(d / tw.tick)` — minimum 1.
3. Compute target slot: `(currentSlot + ticks) % slotCount`.
4. Compute remaining wraps: `ticks / slotCount` (how many full wheel rotations needed).
5. Create `timerEntry` with `remaining`, `callback`, `slotIdx`.
6. Push to target slot's linked list → store in `entries` map → **unlock**.
7. Return `&Timer{ID, Callback}`.

**Edge Cases:**
- Duration of 0 → `ticks = 1` (minimum) → fires on next tick cycle.
- Very large duration → `remaining` wraps multiple times; timer is rescheduled each tick until remaining reaches 0.
- Timer IDs are globally unique and monotonically increasing (atomic uint64).
- No `wg.Add(1)` here — the WaitGroup is managed per-callback in `tickOnce`.

---

### `(tw *TimerWheel) Cancel(id uint64) bool`

**Signature:** `func (tw *TimerWheel) Cancel(id uint64) bool`

**Flow:**
1. **Lock** → look up `id` in `entries` map.
2. If not found → **unlock**, return `false`.
3. Remove from slot's linked list → delete from `entries` map → **unlock**.
4. Return `true`.

**Edge Cases:**
- Returns `false` if timer already fired (removed from map by `tickOnce`).
- Returns `false` if ID never existed.
- No `wg.Done()` on cancel — the WaitGroup was never incremented for this timer (increment happens in `tickOnce` when callback is about to fire).

---

### `(tw *TimerWheel) Len() int`

**Signature:** `func (tw *TimerWheel) Len() int`

**Flow:**
1. **Lock** → return `len(tw.entries)` → **unlock**.

**Edge Cases:**
- Returns count of pending (not-yet-fired) timers.
- Timers currently being fired are removed from map before callback execution.

---

### `(tw *TimerWheel) tickOnce()`

**Signature:** `func (tw *TimerWheel) tickOnce()`

**Flow:**
1. **Lock** → get front element of `slots[currentSlot]`.
2. Iterate the linked list:
   - If `entry.remaining > 0` → decrement, reschedule to `(currentSlot + 1) % slotCount`.
   - If `entry.remaining == 0` → collect callback for firing.
   - Remove processed element from slot list and `entries` map.
3. Advance `currentSlot = (currentSlot + 1) % slotCount` → **unlock**.
4. For each collected callback → `wg.Add(1)` → spawn goroutine → `defer wg.Done()` → call callback.

**Edge Cases:**
- Callbacks fire outside the lock — no deadlock risk from callbacks calling `Add`/`Cancel`.
- Each callback runs in its own goroutine — concurrent execution of callbacks.
- `wg.Add(1)` per callback ensures `Stop()` waits for all in-flight callbacks.
- Rescheduled timers move to the next slot with `remaining` tracking full wheel rotations.
- Timer in slot 0 with `remaining = 1` → moves to slot 1 with `remaining = 0` → fires next tick.

---

## internal/reliability/circuitbreaker.go

### Types

```go
type State int32

const (
    StateClosed   State = iota  // 0
    StateHalfOpen               // 1
    StateOpen                   // 2
)

type CircuitBreaker struct {
    mu              sync.Mutex
    state           State
    failureCount    int64
    lastFailureTime time.Time
    threshold       int
    recoveryTimeout time.Duration
    halfOpenMaxReqs int
    halfOpenReqs    int64
}
```

---

### `NewCircuitBreaker(threshold int, recoveryTimeout time.Duration) *CircuitBreaker`

**Signature:** `func NewCircuitBreaker(threshold int, recoveryTimeout time.Duration) *CircuitBreaker`

**Flow:**
1. Create breaker in `StateClosed` state.
2. Store `threshold` and `recoveryTimeout`.
3. Hardcode `halfOpenMaxReqs = 3`.

**Edge Cases:**
- `threshold = 0` → first failure immediately transitions to Open (failureCount 0 >= threshold 0).
- `recoveryTimeout = 0` → recovery is instant — next `Allow()` after Open immediately transitions to HalfOpen.
- `threshold < 0` → behaves like threshold 0.

---

### `(cb *CircuitBreaker) Allow() bool`

**Signature:** `func (cb *CircuitBreaker) Allow() bool`

**Flow (all under mutex):**
1. **StateClosed** → return `true` (all requests allowed).
2. **StateOpen** → check if `time.Since(lastFailureTime) > recoveryTimeout`:
   - Yes → transition to `StateHalfOpen`, reset `halfOpenReqs = 0`, return `true`.
   - No → return `false`.
3. **StateHalfOpen** → if `halfOpenReqs < halfOpenMaxReqs` → increment `halfOpenReqs`, return `true`. Else return `false`.

**Edge Cases:**
- **TOCTOU-safe:** state check and transition are atomic under the mutex. Two concurrent `Allow()` calls cannot both transition from Open to HalfOpen.
- First request after recovery timeout → transitions to HalfOpen and is allowed (the "probe" request).
- In HalfOpen, up to 3 probe requests are allowed — if all succeed → `Success()` closes the breaker.
- If a probe fails → `Failure()` reopens the breaker immediately.

---

### `(cb *CircuitBreaker) Success()`

**Signature:** `func (cb *CircuitBreaker) Success()`

**Flow:**
1. **Lock** → set `state = StateClosed` → set `failureCount = 0` → **unlock**.

**Edge Cases:**
- Called from HalfOpen state → closes breaker (recovery successful).
- Called from Closed state → no-op (already closed, just resets count).
- Clears all failure state — fresh start.

---

### `(cb *CircuitBreaker) Failure()`

**Signature:** `func (cb *CircuitBreaker) Failure()`

**Flow:**
1. **Lock** → increment `failureCount` → record `lastFailureTime = time.Now()`.
2. If `state == StateHalfOpen` → transition to `StateOpen` (any failure in half-open reopens).
3. If `failureCount >= threshold` → transition to `StateOpen`.
4. **Unlock**.

**Edge Cases:**
- **Standard circuit breaker behavior:** any failure in HalfOpen immediately reopens — no tolerance.
- In Closed state, failure only opens the breaker when threshold is reached (accumulated failures).
- After reopening, `lastFailureTime` is updated → recovery timeout restarts from this point.
- Concurrent `Failure()` calls are serialized by mutex — failureCount increments are safe.

---

### `(cb *CircuitBreaker) FailureCount() int64`

**Signature:** `func (cb *CircuitBreaker) FailureCount() int64`

**Flow:**
1. **Lock** → return `failureCount` → **unlock**.

**Edge Cases:**
- Count is not reset on state transitions (only on `Success()`).
- Count is monotonically increasing within a closed/half-open cycle.

---

## internal/reliability/ratelimit.go

### Types

```go
type TokenBucket struct {
    mu         sync.Mutex
    rate       float64
    burst      int
    tokens     float64
    lastRefill time.Time
}

type RateLimiter struct {
    mu      sync.RWMutex
    buckets map[string]*TokenBucket
}
```

---

### `NewRateLimiter() *RateLimiter`

**Signature:** `func NewRateLimiter() *RateLimiter`

**Flow:**
1. Create `RateLimiter` with empty bucket map.

**Edge Cases:**
- No buckets exist until first `Bucket()`/`Allow()`/`AllowN()` call for a given name.
- Default bucket on first access: `rate=100`, `burst=100`.

---

### `NewTokenBucket(rate float64, burst int) *TokenBucket`

**Signature:** `func NewTokenBucket(rate float64, burst int) *TokenBucket`

**Flow:**
1. Set `tokens = float64(burst)` (start full).
2. Set `lastRefill = time.Now()`.
3. Store `rate` and `burst`.

**Edge Cases:**
- `rate = 0` → tokens never refill after initial burst is consumed.
- `burst = 0` → `tokens = 0` → all `Allow()` calls return false (bucket permanently empty).
- Negative rate/burst → undefined behavior (tokens can go negative).

---

### `(tb *TokenBucket) Allow() bool`

**Signature:** `func (tb *TokenBucket) Allow() bool`

**Flow:**
1. Delegates to `AllowN(1)`.

---

### `(tb *TokenBucket) AllowN(n int) bool`

**Signature:** `func (tb *TokenBucket) AllowN(n int) bool`

**Flow:**
1. **Lock** → call `refill()`.
2. `refill()`: calculate elapsed seconds since `lastRefill` → add `elapsed * rate` tokens → cap at `burst` → update `lastRefill = now`.
3. If `tokens >= float64(n)` → deduct `n`, return `true`.
4. Else → return `false`.

**Edge Cases:**
- Burst of 0 → `tokens` stays 0 after refill (capped at burst) → always denies.
- Rate of 0 → `tokens += 0` → only initial burst tokens available.
- Concurrent calls → serialized by mutex — token deduction is atomic.
- `n > burst` → always returns false (even full bucket can't satisfy).
- `n = 0` → returns true (vacuously, no tokens deducted).
- Time goes backward (clock skew) → `elapsed` could be negative → tokens could decrease slightly, but capped at burst.

---

### `(rl *RateLimiter) Bucket(name string) *TokenBucket`

**Signature:** `func (rl *RateLimiter) Bucket(name string) *TokenBucket`

**Flow (double-checked locking):**
1. **RLock** → check if bucket exists → if found → **RUnlock**, return it.
2. **Lock** → check again (another goroutine may have created it) → if found → **unlock**, return it.
3. Create new `TokenBucket(100, 100)` → store in map → **unlock** → return it.

**Edge Cases:**
- Double-checked locking prevents race conditions on bucket creation.
- Default bucket: `rate=100` tokens/sec, `burst=100`.
- Once created, bucket is never recreated (only replaced via `SetBucket`).
- Concurrent access to same name → only one bucket is created.

---

### `(rl *RateLimiter) SetBucket(name string, rate float64, burst int)`

**Signature:** `func (rl *RateLimiter) SetBucket(name string, rate float64, burst int)`

**Flow:**
1. **Lock** → create new `TokenBucket(rate, burst)` → replace in map → **unlock**.

**Edge Cases:**
- Creates a fresh bucket — previous token state is lost (tokens reset to `burst`).
- If bucket didn't exist → creates it (same as `Bucket()` but with custom params).
- Concurrent calls → last write wins.

---

### `(rl *RateLimiter) Allow(name string) bool`

**Signature:** `func (rl *RateLimiter) Allow(name string) bool`

**Flow:**
1. Call `rl.Bucket(name)` → call `Allow()` on the bucket.

---

### `(rl *RateLimiter) AllowN(name string, n int) bool`

**Signature:** `func (rl *RateLimiter) AllowN(name string, n int) bool`

**Flow:**
1. Call `rl.Bucket(name)` → call `AllowN(n)` on the bucket.

---

## internal/reliability/dedup.go

### Types

```go
type dedupEntry struct {
    key       string
    timestamp time.Time
    elem      *list.Element
}

type DedupTracker struct {
    mu      sync.RWMutex
    entries map[string]dedupEntry
    order   *list.List
    maxSize int
    ttl     time.Duration
}
```

---

### `NewDedupTracker(maxSize int, ttl time.Duration) *DedupTracker`

**Signature:** `func NewDedupTracker(maxSize int, ttl time.Duration) *DedupTracker`

**Flow:**
1. If `maxSize <= 0` → default to `10000`.
2. If `ttl <= 0` → default to `5 * time.Minute`.
3. Create `DedupTracker` with empty map and new LRU list.

**Edge Cases:**
- Very small `maxSize` (e.g., 1) → evicts on every new entry.
- Very short `ttl` → entries expire quickly, requires `StartCleanup` to reclaim memory.
- Zero/negative values use sensible defaults.

---

### `(dt *DedupTracker) Seen(key string) bool`

**Signature:** `func (dt *DedupTracker) Seen(key string) bool`

**Flow:**
1. **RLock** → check if key exists in map → **RUnlock**.

**Edge Cases:**
- Read-only — does not update timestamp or LRU position.
- **TOCTOU risk:** if `Seen` returns false and another goroutine calls `Mark` before this caller does, the key may already be marked. Use `CheckAndMark` for atomic operations.

---

### `(dt *DedupTracker) Mark(key string)`

**Signature:** `func (dt *DedupTracker) Mark(key string)`

**Flow:**
1. **Lock** → call `markLocked(key)` → **unlock**.

**`markLocked` internal flow:**
1. If key already exists → update timestamp, move to front of LRU list.
2. If key is new and tracker is full → evict LRU entry (back of list).
3. Push new key to front of list → store in map with current timestamp.

**Edge Cases:**
- Existing key → timestamp refreshed, moved to front (renewed).
- Full tracker → evicts least recently used entry (not oldest by time, but by access order).
- LRU eviction removes both from map and linked list.

---

### `(dt *DedupTracker) CheckAndMark(key string) bool`

**Signature:** `func (dt *DedupTracker) CheckAndMark(key string) bool`

**Flow (atomic under write-lock):**
1. **Lock** → check if key exists.
2. If exists → update timestamp, move to front → **unlock**, return `true` (duplicate).
3. If not exists → call `markLocked(key)` → **unlock**, return `false` (new).

**Edge Cases:**
- **Eliminates TOCTOU race** between separate `Seen()` + `Mark()` calls.
- Returns `true` if already seen (duplicate), `false` if this is the first occurrence.
- When full → evicts LRU before adding new entry.
- Single atomic operation — safe for concurrent use.

---

### `(dt *DedupTracker) StartCleanup(ctx context.Context, interval time.Duration)`

**Signature:** `func (dt *DedupTracker) StartCleanup(ctx context.Context, interval time.Duration)`

**Flow:**
1. Spawn background goroutine with `time.Ticker` at `interval`.
2. On each tick → **Lock** → iterate all entries → if `now - timestamp > ttl` → remove from map and list → **unlock**.
3. On `ctx.Done()` → exit goroutine.

**Edge Cases:**
- Multiple calls → multiple cleanup goroutines (no guard).
- Cleanup holds write lock for entire iteration — large maps may block `Mark`/`CheckAndMark`.
- Entries are removed by time, not by LRU order — TTL is enforced per-entry.
- No graceful shutdown — goroutine exits when context is cancelled, doesn't wait for in-progress cleanup.

---

### `(dt *DedupTracker) Len() int`

**Signature:** `func (dt *DedupTracker) Len() int`

**Flow:**
1. **RLock** → return `len(dt.entries)` → **RUnlock**.

---

### `(dt *DedupTracker) Clear()`

**Signature:** `func (dt *DedupTracker) Clear()`

**Flow:**
1. **Lock** → reinitialize `entries` map → reinitialize `order` list → **unlock**.

**Edge Cases:**
- Does not stop `StartCleanup` goroutine — cleanup continues on the new empty map (no-op).

---

## internal/reliability/saga.go

### Types

```go
type SagaStep struct {
    ServiceName string `json:"service_name"`
    Method      string `json:"method"`
    Body        []byte `json:"body"`
    CompSvc     string `json:"comp_svc"`
    CompMethod  string `json:"comp_method"`
}

type CompensatorFunc func(svcName, method string, body []byte) error

type SagaTracker struct {
    mu    sync.Mutex
    steps map[string][]SagaStep
    call  CompensatorFunc
    dir   string
}
```

---

### `NewSagaTracker(call CompensatorFunc) *SagaTracker`

**Signature:** `func NewSagaTracker(call CompensatorFunc) *SagaTracker`

**Flow:**
1. If `call` is nil → use no-op compensator (returns nil).
2. Create tracker with in-memory map, no disk persistence.

**Edge Cases:**
- Nil compensator → compensation is silently skipped (all steps "succeed").
- In-memory only → steps lost on process restart.

---

### `NewSagaTrackerWithDir(call CompensatorFunc, dir string) *SagaTracker`

**Signature:** `func NewSagaTrackerWithDir(call CompensatorFunc, dir string) *SagaTracker`

**Flow:**
1. If `call` is nil → use no-op compensator.
2. Create tracker with `dir` set.
3. Call `st.load()` → read all `*-saga.json` files from `dir` → unmarshal into `steps` map.

**Edge Cases:**
- `dir` doesn't exist → `os.ReadDir` fails silently, starts with empty map.
- Corrupt JSON in saga files → skipped silently (logged but not fatal).
- Filenames not matching `*-saga.json` pattern → skipped.
- Steps from previous process run are restored.

---

### `(st *SagaTracker) SetDir(dir string)`

**Signature:** `func (st *SagaTracker) SetDir(dir string)`

**Flow:**
1. **Lock** → set `st.dir = dir` → **unlock**.
2. Call `st.load()` → load existing steps from directory.

**Edge Cases:**
- Can be called at any time to enable persistence retroactively.
- Loading after construction merges on-disk state with in-memory state.
- Thread-safe for `dir` field, but `load()` acquires its own lock.

---

### `(st *SagaTracker) RegisterStep(execID string, step SagaStep)`

**Signature:** `func (st *SagaTracker) RegisterStep(execID string, step SagaStep)`

**Flow:**
1. **Lock** → append step to `steps[execID]` → call `persistLocked(execID)` → **unlock**.

**`persistLocked` internal flow:**
1. If `dir` is empty → skip persistence.
2. Marshal steps to JSON → write to `{dir}/{execID}-saga.json` via temp file + rename (atomic write).

**Edge Cases:**
- First step for an execID → creates new slice.
- Subsequent steps → appended to existing slice.
- Persistence uses atomic write (write to `.tmp`, then `os.Rename`) — prevents partial writes.
- Write failure → logged, not fatal (steps still in memory).

---

### `(st *SagaTracker) Compensate(execID string) error`

**Signature:** `func (st *SagaTracker) Compensate(execID string) error`

**Flow:**
1. **Lock** → get steps for `execID` → delete from map → **unlock**.
2. If no steps → return nil.
3. Iterate steps in **reverse order** (LIFO — last registered step compensated first).
4. For each step:
   - Skip if both `CompSvc` and `CompMethod` are empty.
   - Call `st.call(s.CompSvc, s.CompMethod, s.Body)`.
   - If error → append to error list.
5. If any errors → return aggregate error `fmt.Errorf("saga compensation errors: %v", errs)`.
6. If all succeed → return nil (steps already deleted from map).

**Edge Cases:**
- Steps are deleted from map BEFORE compensation → if compensate fails, steps are gone from memory (but may still be on disk).
- **Does NOT stop on first error** — continues compensating all steps, returns aggregate of all failures.
- Steps with empty `CompSvc` AND empty `CompMethod` are skipped (no compensation needed).
- Compensator receives the original step's `Body` (not the response).
- LIFO order ensures reverse execution semantics (e.g., if you create then update, update is compensated first).
- Disk file is NOT removed after compensation (only `Clear()` removes it).

---

### `(st *SagaTracker) StepsFor(execID string) []SagaStep`

**Signature:** `func (st *SagaTracker) StepsFor(execID string) []SagaStep`

**Flow:**
1. **Lock** → copy steps slice → **unlock** → return copy.

**Edge Cases:**
- Returns nil slice if no steps exist for the execID.
- Returns a copy — callers cannot mutate internal state.

---

### `(st *SagaTracker) Clear(execID string)`

**Signature:** `func (st *SagaTracker) Clear(execID string)`

**Flow:**
1. **Lock** → delete from `steps` map → compute `stepPath` → **unlock**.
2. If `stepPath` is non-empty → `os.Remove(path)` and `os.Remove(path + ".tmp")`.

**Edge Cases:**
- Removes both the main file and any leftover temp file.
- If `dir` is empty → no disk removal (in-memory only).
- Safe to call for non-existent execID (no-op).
- Does not compensate — just clears tracking state.

---

## internal/reliability/dlq.go

### Types

```go
const DefaultDLQTopic = "_flowrulz_dlq"

type DeadLetterEntry struct {
    ID         string    `json:"id"`
    RuleID     string    `json:"rule_id"`
    Topic      string    `json:"topic"`
    Partition  int32     `json:"partition"`
    Offset     int64     `json:"offset"`
    Body       []byte    `json:"body"`
    Error      string    `json:"error"`
    FailedAt   time.Time `json:"failed_at"`
    RetryCount int       `json:"retry_count"`
}

type DLQ struct {
    mu       sync.RWMutex
    entries  []*DeadLetterEntry
    maxSize  int
    replayFn func(ctx context.Context, entry *DeadLetterEntry) error
    producer transport.MessageProducer
    topic    string
    dir      string
}

type DLQOption func(*DLQ)

func WithDLQProducer(p transport.MessageProducer) DLQOption
func WithDLQDir(dir string) DLQOption
```

---

### `NewDLQ(maxSize int, opts ...DLQOption) *DLQ`

**Signature:** `func NewDLQ(maxSize int, opts ...DLQOption) *DLQ`

**Flow:**
1. If `maxSize <= 0` → default to `10000`.
2. Create DLQ with empty entries slice, default topic `"_flowrulz_dlq"`.
3. Apply all options.
4. If `dir` is set → call `loadFromDir()` to restore entries from JSON files.

**Edge Cases:**
- `loadFromDir` reads all `.json` files in dir, unmarshals `DeadLetterEntry` per file.
- Corrupt JSON files → skipped silently (logged but not fatal).
- No options → DLQ with no producer and no persistence.
- Options are applied in order — later options override earlier ones.

---

### `(d *DLQ) SetReplayFn(fn func(ctx context.Context, entry *DeadLetterEntry) error)`

**Signature:** `func (d *DLQ) SetReplayFn(fn func(ctx context.Context, entry *DeadLetterEntry) error)`

**Flow:**
1. **Lock** → set `d.replayFn = fn` → **unlock**.

**Edge Cases:**
- Can be called at any time to change the replay function.
- Nil `fn` → replay operations become no-ops.

---

### `(d *DLQ) Send(entry *DeadLetterEntry) error`

**Signature:** `func (d *DLQ) Send(entry *DeadLetterEntry) error`

**Flow:**
1. **Lock** → if at capacity → evict oldest entry (front of slice), remove from disk.
2. Set `entry.FailedAt = time.Now()`.
3. Append entry to slice → copy entry → **unlock**.
4. Persist copy to disk (if `dir` set) via atomic write (tmp + rename).
5. Log warning with rule ID, entry ID, error.
6. If producer set → marshal to JSON → send to Kafka topic.
7. If Kafka send fails → log error, return error.

**Edge Cases:**
- **Eviction:** FIFO eviction (oldest first) when at capacity — not LRU.
- **Kafka failure is NOT fatal for local DLQ** — entry is already in-memory and on-disk. The error IS returned to the caller though.
- Disk persistence failure → logged but not fatal (entry still in memory).
- `FailedAt` is always set to current time on send — caller's `FailedAt` is overwritten.
- Atomic disk write prevents corruption on crash.
- Evicted entry's disk file is cleaned up.

---

### `(d *DLQ) List() []*DeadLetterEntry`

**Signature:** `func (d *DLQ) List() []*DeadLetterEntry`

**Flow:**
1. **RLock** → copy entries slice → **RUnlock** → return copy.

**Edge Cases:**
- Returns a shallow copy — `DeadLetterEntry` pointers are shared but slice is independent.
- Order is insertion order (FIFO).

---

### `(d *DLQ) Len() int`

**Signature:** `func (d *DLQ) Len() int`

**Flow:**
1. **RLock** → return `len(d.entries)` → **RUnlock**.

---

### `(d *DLQ) Replay(ctx context.Context, id string) error`

**Signature:** `func (d *DLQ) Replay(ctx context.Context, id string) error`

**Flow:**
1. **Lock** → find entry by ID → remove from slice → **unlock**.
2. Remove from disk.
3. If `replayFn` is set → increment `RetryCount` → call `replayFn(ctx, entry)`.
4. If `replayFn` is nil → return nil (no-op).

**Edge Cases:**
- Entry removed from DLQ BEFORE replay → if replay fails, entry is lost (not re-queued).
- Entry not found → returns nil (no-op).
- `RetryCount` is incremented before replay — tracks total replay attempts.

---

### `(d *DLQ) ReplayAll(ctx context.Context) int`

**Signature:** `func (d *DLQ) ReplayAll(ctx context.Context) int`

**Flow:**
1. **Lock** → copy all entries → truncate internal slice to zero length → **unlock**.
2. Iterate copied entries:
   - If `replayFn` set → increment `RetryCount` → call with `defer recover()` wrapper.
   - On success → increment count.
   - On failure (including panic) → **re-enqueue** via `d.Send(entry)`.
3. Return count of successfully replayed entries.

**Edge Cases:**
- **Panic-safe:** each `replayFn` call is wrapped in `defer recover()` — panics are caught and the entry is re-queued.
- **Failed entries are re-queued** — they go back into the DLQ via `Send()`, which may evict other entries if at capacity.
- Returns count of successes, not failures.
- Truncates slice first, then processes — if replay panics mid-way, remaining entries are still in the copied slice and get re-queued on failure.
- Re-enqueued entries get new `FailedAt` timestamps.

---

### `(d *DLQ) Clear()`

**Signature:** `func (d *DLQ) Clear()`

**Flow:**
1. **Lock** → copy entries → truncate slice to zero → **unlock**.
2. If `dir` set → for each entry → remove from disk (both `.json` and `.tmp` files).

**Edge Cases:**
- Disk removal happens outside the lock — entries are already truncated from memory.
- If a new `Send()` occurs during `Clear()` disk removal, it may try to remove an already-gone file (harmless).
- Clears all entries regardless of age or retry count.

---

### `(d *DLQ) ToJSON() ([]byte, error)`

**Signature:** `func (d *DLQ) ToJSON() ([]byte, error)`

**Flow:**
1. **RLock** → marshal `d.entries` to JSON → **RUnlock**.

**Edge Cases:**
- Returns full JSON array of all entries.
- Empty DLQ → returns `[]` (empty JSON array), not null.

---

### `(d *DLQ) LoadFromTopic(ctx context.Context)`

**Signature:** `func (d *DLQ) LoadFromTopic(ctx context.Context)`

**Flow:**
1. Logs warning: "dlq: rebuild from topic not implemented".
2. No-op.

**Edge Cases:**
- Stub method — planned for Kafka topic rebuild.
