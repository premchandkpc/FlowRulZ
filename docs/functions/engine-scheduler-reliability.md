# internal/engine, internal/scheduler, internal/reliability — Function Reference

---

## internal/engine/engine.go

### `LaneForScore(score uint32) Lane`

**Flow:** Maps complexity score to execution lane:
- `< 10` → `LaneFast` (batch 500, poll 10ms)
- `≤ 50` → `LaneNormal` (batch 100, poll 50ms)
- `> 50` → `LaneHeavy` (batch 10, poll 500ms)

**Edge Cases:** Score of exactly 10 → Normal. Score of exactly 50 → Normal.

---

### `New(persistPath string) *Engine`

Creates engine with default local compiler. If `persistPath != ""`, loads rules from JSON file.

### `NewWithCompiler(persistPath string, comp compiler.Compiler) *Engine`

Same but with custom compiler implementation.

---

### `(e *Engine) Deploy(id, dsl string) error`

**Flow:**
1. Compile DSL → plan via `e.compiler.Compile(dsl, id)`.
2. Auto-assign lane from `LaneForScore(result.Complexity)`.
3. Auto-increment version via `e.nextVersion.Add(1)`.
4. Lock → append `VersionedPlan` to rule → set as active → save → unlock.
5. Fire `AfterDeploy` hook if set.

**Edge Cases:**
- Compile error → returns error, no state change.
- First deploy for an ID → creates new Rule.
- Concurrent deploys to same ID → serialized by mutex.

---

### `(e *Engine) AddVersion(id, dsl string, plan []byte, version uint64) error`

**Flow:**
1. Create `VersionedPlan` with given plan/DSL/version/lane.
2. Lock → if rule exists, check if version already exists (update in place) or append → if rule doesn't exist, create with `ActiveVersion=-1` → save → unlock.

**Edge Cases:**
- Version collision → replaces existing version in-place.
- Rule doesn't exist → created implicitly, not active until promoted.

---

### `(e *Engine) Promote(id string, version uint64) error`

**Flow:**
1. Lock → find rule → find version index → set `ActiveVersion = index`.
2. Fire `AfterPromote` hook if set.

**Edge Cases:**
- Rule not found → error.
- Version not found → error.
- Promoting to already-active version → no-op (hook still fires).

---

### `(e *Engine) Drain(id string, version uint64) error`

**Flow:**
1. Lock → find rule/version → get `*VersionedPlan` → **unlock** (don't hold lock during wait).
2. `vp.ActiveExec.Wait()` — blocks until all in-flight executions complete.
3. Re-lock → remove version from slice → adjust `ActiveVersion` → if rule empty, delete rule → unlock.

**Edge Cases:**
- Drains without holding lock (allows in-flight to finish).
- If drained version was active → `ActiveVersion` resets to 0 (or -1 if empty).
- If drained version index < active → active index decrements.

---

### `(e *Engine) Remove(id string)`

**Flow:**
1. Lock → copy versions slice → unlock.
2. Wait for `ActiveExec.Wait()` on ALL versions.
3. Re-lock → delete rule → save → unlock.

**Edge Cases:**
- No-op if rule doesn't exist.
- Waits for ALL in-flight executions across all versions.

---

### `(e *Engine) Rules() []Rule`
Returns deep copy of all rules (snapshot).

### `(e *Engine) ActivePlanBytes() [][]byte`
Returns slice of active plan bytes for all rules with non-empty plans.

### `(e *Engine) ExecuteAll(body, caller, ctx) ([][]byte, error)`
Sequential single-shot execution via `bridge.Execute`. Increments `ActiveExec` per plan. **Testing only** — production uses `node.executeAll` with step-by-step execution.

---

## internal/scheduler/prod.go (Scheduler)

### `New(lanes []LaneConfig) *Scheduler`

**Flow:** Creates scheduler with given lane configs (or `DefaultLanes` if nil).

**Default Lanes:**

| Lane | MaxConcurrent | QueueSize | RejectOnFull |
|---|---|---|---|
| Fast | 50 | 5000 | false |
| Normal | 20 | 2000 | false |
| Heavy | 5 | 500 | true |

---

### `(s *Scheduler) Start(ctx context.Context) error`

**Flow:**
1. Idempotent — returns nil if already started.
2. For each lane: `wg.Add(MaxConcurrent)`.
3. Spawn `laneWorker` goroutine per lane → each spawns `MaxConcurrent` `slotWorker` goroutines.

---

### `(s *Scheduler) Stop() error`

**Flow:**
1. Close `stopCh` → signals all workers to exit.
2. `wg.Wait()` per lane → blocks until all workers done.

---

### `(s *Scheduler) EnqueueTask(task *Task) error`

**Flow:**
1. Nil task → error.
2. Invalid priority → defaults to `PriorityNormal`.
3. `lane.enqueue(task)` → if lane full and `RejectOnFull` → `ErrQueueFull`.
4. Increment `totalEnq`.

**Edge Cases:**
- Heavy lane rejects when full (queue=500, `RejectOnFull=true`).
- Fast/Normal lanes never reject (grow unbounded, risk OOM under extreme load).

---

### `(s *Scheduler) EnqueueAndWait(ctx, task) ([]byte, error)`

**Flow:**
1. Create buffered `ResultCh` (capacity 1).
2. `EnqueueTask(task)`.
3. `select` on `task.ResultCh` or `ctx.Done()`.
4. On context cancel: spawns goroutine to drain `ResultCh` (prevents goroutine leak).

**Edge Cases:**
- Context cancellation → goroutine drains result to prevent leak.
- Task panics → `execTask`'s `defer recover()` catches panic, writes error to `ResultCh`. Caller receives error instead of hanging.

---

### `(s *Scheduler) dequeueOrSteal(ctx, myLane) *Task`

**Work-stealing algorithm:**
1. Non-blocking read from own lane queue.
2. If empty → try non-blocking read from other lanes (Heavy → Fast order).
3. If still empty → blocking read from own lane (with stop/ctx check).

**Edge Cases:**
- Steals from higher-priority lanes first (Heavy → Fast).
- Falls back to blocking on own lane to prevent starvation.

---

### `(s *Scheduler) execTask(ctx, task)`

**Flow:**
1. If `task.Deadline` set → create deadline context.
2. `defer recover()` — catches panics, writes error to `ResultCh` if set.
3. Call `task.Execute(execCtx, task)`.
4. If `task.ResultCh` set → send result.

---

## internal/scheduler/worker.go (TimerWheel)

### `NewTimerWheel(tick time.Duration, slotCount int) *TimerWheel`
Creates timer wheel. `tick` = resolution, `slotCount` = number of slots.

### `(tw *TimerWheel) Start()`
Starts background ticker goroutine calling `tickOnce` at `tick` interval.

### `(tw *TimerWheel) Stop()`
Stops ticker, closes `done` channel, waits for all callbacks via `sync.WaitGroup`. Safe to call multiple times (`stopOnce`).

### `(tw *TimerWheel) Add(d time.Duration, callback func()) *Timer`
**Flow:**
1. Calculate slots to advance: `int(d / tw.tick)`.
2. `slotIdx = (currentSlot + ticks) % slotCount`.
3. Create `timerEntry` with `remaining = ticks`.
4. Push to slot's linked list, store in `entries` map.
5. `wg.Add(1)` for callback tracking.

**Edge Cases:**
- Duration of 0 → lands in current slot, fires on next tick.
- Very large duration → wraps around wheel.

### `(tw *TimerWheel) Cancel(id uint64) bool`
Removes entry from map and slot list. Returns false if not found or already fired.

### `(tw *TimerWheel) tickOnce()`
**Flow:**
1. Advance `currentSlot`.
2. Iterate linked list at current slot.
3. Decrement `remaining`. If 0 → fire callback, `wg.Done()`. If > 0 → reschedule to `(currentSlot + remaining) % slotCount`.

---

## internal/reliability/circuitbreaker.go

### `NewCircuitBreaker(threshold int, recoveryTimeout time.Duration) *CircuitBreaker`
Creates with `StateClosed`, `halfOpenMaxReqs=3`.

### `(cb *CircuitBreaker) Allow() bool`

**Flow (all under mutex):**
- **Closed** → return `true`.
- **Open** → if `time.Since(lastFailureTime) > recoveryTimeout` → transition to HalfOpen, reset `halfOpenReqs`, return `true`. Else return `false`.
- **HalfOpen** → if `halfOpenReqs < halfOpenMaxReqs` → increment, return `true`. Else return `false`.

**Edge Cases:**
- TOCTOU-safe: state check and transition are atomic under mutex.
- First request after recovery → transitions to HalfOpen.

---

### `(cb *CircuitBreaker) Success()`
Resets to `StateClosed`, clears `failureCount`.

### `(cb *CircuitBreaker) Failure()`
Increments `failureCount`, records timestamp. If in `StateHalfOpen` OR `failureCount >= threshold` → transitions to `StateOpen`.

**Edge Cases:**
- Any failure in HalfOpen → immediately reopens (standard breaker behavior).
- Threshold reached in Closed → transitions to Open.

### `(cb *CircuitBreaker) FailureCount() int64`
Returns count under mutex.

---

## internal/reliability/ratelimit.go

### `NewTokenBucket(rate float64, burst int) *TokenBucket`
Creates with `tokens = burst`, `lastRefill = now`.

### `(tb *TokenBucket) AllowN(n int) bool`

**Flow:**
1. Lock → `refill()` → calculate elapsed seconds → add `elapsed * rate` tokens (capped at `burst`).
2. If `tokens >= n` → deduct, return `true`.
3. Else → return `false`.

**Edge Cases:**
- Burst = 0 → always denies.
- Rate = 0 → tokens never refill after initial burst.
- Concurrent calls → serialized by mutex.

### `NewRateLimiter() *RateLimiter`
Creates with empty bucket map.

### `(rl *RateLimiter) Bucket(name string) *TokenBucket`
Double-checked locking: read-lock check → if miss → write-lock create (default 100 rate, 100 burst).

### `(rl *RateLimiter) SetBucket(name, rate, burst)`
Replaces bucket entirely.

### `(rl *RateLimiter) Allow(name string) bool` / `AllowN(name, n)`
Gets/creates bucket → delegates.

---

## internal/reliability/dedup.go

### `NewDedupTracker(maxSize int, ttl time.Duration) *DedupTracker`
Defaults: `maxSize=10000`, `ttl=5min`. Uses LRU list + map.

### `(dt *DedupTracker) CheckAndMark(key string) bool`

**Flow (atomic under write-lock):**
1. If key exists → update timestamp, move to front → return `true` (duplicate).
2. If key new → `markLocked(key)` → return `false`.

**Edge Cases:**
- Eliminates TOCTOU race between separate `Seen()` + `Mark()`.
- When full → evicts LRU (back of list) before adding.

### `(dt *DedupTracker) StartCleanup(ctx, interval)`
Background goroutine: on tick, evict all entries older than TTL.

### `(dt *DedupTracker) Seen(key) bool` / `Mark(key)`
Non-atomic versions. `Seen` uses read-lock, `Mark` uses write-lock.

---

## internal/reliability/saga.go

### `NewSagaTracker(call CompensatorFunc) *SagaTracker`
In-memory only. If `call` is nil, uses no-op compensator.

### `NewSagaTrackerWithDir(call CompensatorFunc, dir string) *SagaTracker`
With disk persistence. Loads existing steps from `dir` on creation.

### `(st *SagaTracker) RegisterStep(execID string, step SagaStep)`
Appends step to in-memory map, persists to `{dir}/{execID}.json` if dir set.

### `(st *SagaTracker) Compensate(execID string) error`

**Flow:**
1. Get steps for execID, delete from map.
2. Iterate steps in reverse order.
3. For each step with `CompSvc`/`CompMethod` → call compensator.
4. **Continue on error** — collect all errors, don't stop on first failure.
5. Return aggregate error if any compensations failed.

**Edge Cases:**
- Continues compensating all steps even if individual compensators fail.
- Returns aggregate of all errors (not just the first).
- Steps without `CompSvc`/`CompMethod` are skipped.
- Steps without compensator (empty CompSvc) → skipped.
- Compensator error stops the chain.
- After successful compensation → steps cleared from memory and disk.

### `(st *SagaTracker) StepsFor(execID) []SagaStep` / `Clear(execID)` / `SetDir(dir)`

---

## internal/reliability/dlq.go

### `NewDLQ(maxSize int, opts ...DLQOption) *DLQ`
Default `maxSize=10000`. Options: `WithDLQProducer`, `WithDLQDir`. If dir set, loads existing entries.

### `(d *DLQ) Send(entry *DeadLetterEntry) error`

**Flow:**
1. Lock → if full, evict oldest → append entry → copy entry → unlock.
2. Persist entry to disk (if dir set).
3. Log warning.
4. If producer set → marshal JSON → send to Kafka topic.

**Edge Cases:**
- Kafka send failure → logged but not fatal.
- Disk persistence failure → logged but not fatal.
- Evicts oldest entry when at capacity.

### `(d *DLQ) ReplayOne(ctx) error`
Pops oldest entry → calls `replayFn` → removes on success.

### `(d *DLQ) ReplayAll(ctx) error`
Replays all entries sequentially. Removes each on success.

### `(d *DLQ) Clear()` — Empties entries, removes disk files.
### `(d *DLQ) Entries() []*DeadLetterEntry` — Snapshot under read-lock.
### `(d *DLQ) SetReplayFn(fn)` — Sets replay callback.
### `(d *DLQ) LoadFromTopic(ctx)` — **Stub** — logs "not implemented", no-op. planned for Kafka topic rebuild.
### `(d *DLQ) Replay(ctx, id)` — Replays specific entry by ID. Removes on success.
### `(d *DLQ) List() []*DeadLetterEntry` — Returns snapshot of all entries (alias for Entries).
### `(d *DLQ) Len() int` — Returns current entry count.
### `(d *DLQ) ToJSON() ([]byte, error)` — Marshals all entries to JSON.

---

## internal/reliability/pkgsupport.go (pkg/ interface adapters)

### `(cb *CircuitBreaker) Execute(ctx, name, fn) error`
Wraps `fn` in circuit breaker logic. If circuit open → returns error immediately.

### `(cb *CircuitBreaker) State(name) CircuitState`
Returns current state (Closed/Open/HalfOpen) for the named circuit.

### `(cb *CircuitBreaker) Reset(name)`
Forces circuit back to Closed state.

### `(dt *DedupTracker) IsDuplicate(ctx, id) bool`
Returns true if `id` was seen within the TTL window.

### `(dt *DedupTracker) MarkSeen(ctx, id) error`
Records `id` as seen. Evicts oldest entry if at capacity.

### `(dt *DedupTracker) StopCleanup()`
Stops the background cleanup goroutine.

### `(dt *DedupTracker) Len() int` / `Clear()`
Returns count of tracked IDs / empties the tracker.

### `(rl *RateLimiter) AllowWithCtx(ctx, key) bool`
Per-key token bucket rate limit check.

### `(rl *RateLimiter) WaitCtx(ctx, key) error`
Blocks until rate limit allows or context cancels.

### `(rl *RateLimiter) SetBucketRate(key, rate, burst)`
Configures per-key token bucket parameters.

### `(st *SagaTracker) CompensateCtx(ctx, sagaID) error`
Context-aware wrapper around `Compensate`.

### `(st *SagaTracker) StatusInfo(ctx, sagaID) (*SagaStatus, error)`
Returns current saga status (pending compensations, etc.).

---

## internal/scheduler/pkgsupport.go (pkg/ interface adapters)

### `(s *Scheduler) ID() string`
Returns node ID (for `pkg/scheduler.Scheduler` interface).

### `(s *Scheduler) Enqueue(ctx *ExecutionContext) error`
Enqueues a `pkg/scheduler.ExecutionContext` for execution.

### `(s *Scheduler) Snapshot() SchedulerSnapshot`
Returns snapshot of lane depths and active counts.

### `(s *Scheduler) ExecCount() int64`
Returns total number of executed tasks (atomic counter).

### `(t *TimerWheel) Len() int`
Returns number of pending timers.
