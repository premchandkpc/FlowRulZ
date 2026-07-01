# Flows

Every data path through FlowRulZ, from cluster formation to bytecode dispatch.

---

## 1. Cluster Membership Flow

```
Node starts
    │
    ├── Load config (node ID, seed endpoints)
    │
    ▼
Resolve seeds → dial seed peers
    │
    ├── Join request to seed → seed confirms membership
    │
    ▼
Announce on _flowrulz_members (cluster bus topic)
    ├── NodeState{id, addr, port, load, services}
    ├── Gossip protocol propagates state to all nodes
    │
    ▼
Leader election (every node runs independently)
    ├── Read all alive entries from _flowrulz_members
    ├── Sort by node_id ascending
    ├── Lowest ID = leader
    └── No Raft/Paxos — lowest-ID-alive via cluster bus gossip
    │
    ▼
Periodic heartbeat
    ├── Broadcast on _flowrulz_members every 3s via cluster bus
    ├── LeaderLease (8s) detects stale leaders
    └── Lease expiry triggers re-election
    │
    ▼
Leader responsibilities
    ├── Assign partitions → nodes (consumer group protocol)
    ├── Aggregate service registry → publish combined view
    ├── Distribute plans via _flowrulz_plans
    └── Monitor node health via heartbeat liveness
```

**Files:** `go/internal/execnode/execnode.go`, `docs/specs/cluster-model.md`

---

## 2. Node Lifecycle Flow

```
JOIN
    │
    ├── Announce on _flowrulz_members via cluster bus
    ├── Leader rebalances partitions (64 partitions, round-robin)
    ├── Catch-up: receive current rules from leader
    └── Enter normal execution loop
    │
    ▼
NORMAL
    ├── Execute rules for assigned partitions
    ├── Heartbeat periodically on _flowrulz_members
    ├── Watch for plan updates on _flowrulz_plans
    └── Watch for partition changes on _flowrulz_partitions
    │
    ▼
DRAIN (signal: SIGTERM / admin / partition revoke)
    ├── Stop consuming new messages
    ├── Signal scheduler to reject new tasks
    ├── Wait for in-flight tasks to complete
    │   └── (or timeout after grace period)
    ├── Announce departure on _flowrulz_members
    ├── Leader rebalances partitions away
    └── Shutdown HTTP server
    │
    ▼
CRASH / REJOIN
    ├── Node becomes unresponsive → lease expiry triggers rebalance
    ├── Node restarts with same node_id
    ├── Re-announce on _flowrulz_members
    ├── Catch-up from leader (missed rule versions)
    └── Resume normal execution
```

**Files:** `go/internal/execnode/execnode.go` (`Start`, `Shutdown`)

---

## 3. Plan Distribution Flow

```
LEADER                                   FOLLOWER(s)
  │                                          │
  │  POST /rules (admin API)                 │
  │  ├── engine.Deploy(id, dsl)              │
  │  ├── bridge.Compile(dsl) → plan bytes    │
  │  ├── bridge.PlanComplexity(plan) → score │
  │  ├── assign lane (fast/normal/heavy)     │
  │  └── saveRules() (atomic write)          │
  │                                          │
  │  plandist.PublishPlan(rule_id, version)  │
  │  ├── PlanMessage{type: "plan",           │
  │  │   rule_id, version, plan, dsl}        │
  │  └── Send to _flowrulz_plans             │
  │                                          │
  │                    ─────────────────────►│  Consume PlanMessage
  │                                          │  ├── Store plan (inactive)
  │                                          │  └── Send AckMessage{node_id,
  │  plandist.RecordAck(ack)  ◄──────────────│      rule_id, version, status}
  │  ├── pendingAcks[rule:ver].received++     │
  │  └── if received >= quorum → signal done  │
  │                                          │
  │  WaitForAcks(rule_id, version,           │
  │              quorum=majority(⌊N/2⌋+1),  │
  │              timeout=10s)                 │
  │  ├── blocks on done channel              │
  │  └── timeout → error (deploy fails)      │
  │                                          │
  │  plandist.ActivatePlan(rule_id, version) │
  │  └── PlanMessage{type: "activate"}       │
  │                    ─────────────────────►│  Consume "activate"
  │                                          │  └── Mark version active
```

**Files:** `go/internal/plandist/plandist.go`, `go/internal/engine/engine.go`

---

## 4. Rule Deployment Flow

```
Client                              Admin Server
  │                                      │
  │  POST /rules                         │
  │  {"id":"order-flow",                 │
  │   "dsl":"n:validate|n:process|e:out"}│
  │─────────────────────────────────────►│
  │                                      │  auth() middleware
  │                                      │  │
  │                                      │  ▼
  │                                      │  engine.Deploy("order-flow", dsl)
  │                                      │  ├── bridge.Compile(dsl, id)
  │                                      │  │   ├── C.flowrulz_compile(dsl, id)
  │                                      │  │   ├── lex → parse → optimize → compile
  │                                      │  │   └── return ExecutionPlan bytes
  │                                      │  ├── bridge.PlanComplexity(plan)
  │                                      │  │   └── C.flowrulz_plan_complexity(plan)
  │                                      │  ├── laneForScore(score) → fast/normal/heavy
  │                                      │  ├── VersionedPlan{Plan, DSL, Version, Lane}
  │                                      │  ├── store in e.rules[id].versions
  │                                      │  └── saveRules() (atomic tmp+rename)
  │                                      │
  │  ◄── 201 Created ───────────────────┤
  │                                      │
  │                                      │  (async) plandist.PublishPlan(...)
  │                                      │       WaitForAcks(...)
  │                                      │       ActivatePlan(...)
```

**Files:** `go/internal/admin/server.go`, `go/internal/engine/engine.go`,
`go/bridge/bridge.go`, `rust/src/dsl/compiler.rs`, `rust/src/ffi.rs`

---

## 5. Message Ingestion Flow

```
Cluster bus / transport ingress
    │
    ├── Consumer.Receive() → Message
    │
    ▼
Message handler
    │
    ├── RateLimiter.Allow("ingress")
    │   ├── true  → continue
    │   └── false → DLQ.Send(entry{body, error:"rate limited"})
    │                return (message dropped)
    │
    ├── Scheduler.Enqueue(Task{ID, body, Execute: handlerFn})
    │   ├── Fast lane (score < 10):  blocking send to 5k buffered chan
    │   ├── Normal lane (score ≤ 50): blocking send to 2k buffered chan
    │   └── Heavy lane (score > 50):  non-blocking send to 500 chan
    │       RejectOnFull → ErrQueueFull → DLQ.Send (if heavy lane full)
    │
    └── Return (handler completes)
```

**Scheduler dequeue (laneWorker goroutine):**
```
laneWorker loop
    │
    ├── sem <- struct{}{}     (acquire concurrency slot)
    │   max: Fast=50, Normal=20, Heavy=5
    │
    ├── task <- lane.queue    (dequeue task)
    │
    ├── go execTask(task)     (goroutine — releases sem via defer)
    │
    │   execTask:
    │   ├── executeAll(task.Body)
    │   │   ├── Engine.ActivePlanBytes() → []plan_bytes
    │   │   ├── for each plan:
    │   │   │   └── executePlan(plan, body)
    │   │   │       └── bridge.ExecuteStep loop:
    │   │   │           ├── StepDone     → collect result
    │   │   │           ├── StepPending  → callService(svc_id, body)
    │   │   │           │                  ├── circuit breaker check
    │   │   │           │                  ├── serviceResolver.Resolve()
    │   │   │           │                  └── HTTP call → response
    │   │   │           └── StepContinue → next instruction
    │   │   └── collect results
    │   │
    │   ├── on success: Metrics.RecordExec(rule_id)
    │   ├── on error:   Metrics.RecordError(rule_id)
    │   │               DLQ.Send(entry{body, error})
    │   │               CircuitBreaker.Failure()
    │   └── on success: CircuitBreaker.Success()
    │
    └── <-sem                  (release concurrency slot)
```

**Files:** `go/internal/cluster/transport.go`, `go/internal/execnode/execnode.go`,
`go/internal/scheduler/scheduler.go`, `go/internal/engine/engine.go`,
`go/bridge/bridge.go`, `go/internal/reliability/ratelimit.go`

---

## 6. Scheduling Flow

```
                            Scheduler
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
          Fast lane       Normal lane      Heavy lane
       (50 conc, 5k Q)  (20 conc, 2k Q)  (5 conc, 500 Q, reject)
              │               │               │
       ┌──────┴──────┐  ┌─────┴──────┐  ┌─────┴──────┐
       │lane.worker 1│  │lane.worker 1│  │lane.worker 1│
       │lane.worker 2│  │lane.worker 2│  │lane.worker 2│
       │...up to 50  │  │...up to 20  │  │...up to 5   │
       └─────────────┘  └────────────┘  └────────────┘
              │               │               │
              ▼               ▼               ▼
         execTask         execTask         execTask
         goroutine        goroutine        goroutine

Enqueue logic:
    Fast/Normal:  taskChan <- task     (blocking — backpressure to consumer)
    Heavy:        select { case taskChan <- task: default: return ErrQueueFull }

Lane worker:
    loop:
        sem <- struct{}{}         // acquire slot (MaxConcurrent)
        task := <-queue           // dequeue
        go execTask(task)         // release sem in defer

PriorityForScore(score):
    score < 10  → Fast
    score ≤ 50  → Normal
    score > 50  → Heavy
```

**Files:** `go/internal/scheduler/scheduler.go`

---

## 7. Execution Flow (VM)

```
ExecutionRuntime::execute(body)
    │
    ├── inspect first opcode
    │   ├── Buffer (9)  → store body in accumulator, return immediately
    │   ├── Chunk (15)  → split body into N chunks, VM each, collect as JSON array
    │   └── other       → run_vm(body)
    │
    ▼
VM::run()
    │
    └── loop ip=0..plan.instructions.len()
        │
        ├── fetch instruction{op, flags, a, b, c}
        │
        ├── dispatch(instruction)
        │   │
        │   ├── Next (1)       → caller(svc_id, body, timeout)
        │   │                    on success: replace ctx.body, inc hop_count
        │   │                    on error:   ctx.failed=true, push error
        │   │                    (with retry: exec_with_retry loop + backoff)
        │   │
        │   ├── Async (14)     → same as Next, but fire-and-forget
        │   │                    (ignore result, return empty Vec)
        │   │
        │   ├── Parallel (1*)  → fan-out: caller() for each target
        │   │                    collect results into JSON array
        │   │
        │   ├── Collect (2)    → inc hop_count (synchronization marker)
        │   │
        │   ├── Fallback (3)   → only if ctx.failed == true
        │   │                    reset failed=false, try alternative caller()
        │   │                    if also fails: set failed=true again
        │   │
        │   ├── Gate (4)       → evaluate condition on ctx.body
        │   │                    if false: skip forward (modify ip)
        │   │
        │   ├── Split (5)      → no-op (handled at runtime / plan level)
        │   │
        │   ├── Map (6)        → transform ctx.body via JMESPath expression
        │   │
        │   ├── Emit (7)       → caller(svc_id, body) — result discarded
        │   │                    (side-effect: produce to output topic)
        │   │
        │   ├── Drop (8)       → set ip to end (halt execution)
        │   │
        │   ├── Key (10)       → no-op at VM level
        │   ├── Pipe (12)      → no-op at VM level
        │   ├── Timeout (13)   → no-op at VM level
        │   ├── Label (18)     → no-op at VM level
        │   ├── SvcArg (19)    → no-op at VM level (resolved at compile time)
        │   ├── RetryData (20) → no-op at VM level (config for Next opcode)
        │   ├── JumpOffset(21) → no-op at VM level
        │   │
        │   ├── Dag (16)       → exec_dag(): layer-by-layer topo exec
        │   │                    see §10
        │   │
        │   ├── Jmp (17)       → ip = instr.a (unconditional jump)
        │   │
        │   └── TypeGuard (22) → validate ctx.body against schema
        │                        if mismatch: return Err
        │
        └── emit_span(Span{opcode, service_id, layer,
                           duration_ns, status})
                           → thread_local ring buffer
```

**Files:** `rust/src/executor/mod.rs`, `rust/src/executor/runtime.rs`,
`rust/src/executor/next.rs`, `rust/src/executor/gate.rs`, `rust/src/executor/map.rs`,
`rust/src/executor/dag.rs`, `rust/src/executor/emit.rs`, `rust/src/executor/parallel.rs`

---

## 8. Service Call Flow

```
VM::op_next / op_emit / op_dag
    │
    ├── svc_id = instr.a (service identifier)
    ├── timeout = instr.timeout_ms()
    ├── retry = instr.has_retry() → find_retry_config()
    │
    ▼
caller(svc_id, body, timeout)
    │
    ├── (Rust) closure calls C function pointer: caller_cb_t
    │   ┌──────────────────────────────────────────────┐
    │   │ int caller_cb(ctx_id, svc_id, body, len,     │
    │   │             resp, *resp_len)                  │
    │   └──────────────────────────────────────────────┘
    │
    ▼
caller_bridge.c  (static C function)
    │
    ├── Forwards to goServiceCaller (//export)
    │
    ▼
bridge.go  (goServiceCaller)
    │
    ├── Dispatches to ServiceCaller func(svc_id, body) []byte
    │
    ├── Registry.Pick(service_name_for_svc_id)
    │   ├── Lookup: healthy endpoints for service
    │   ├── Pick by strategy:
    │   │   ├── random       → rand.Intn(len(healthy))
    │   │   ├── roundrobin   → atomic counter % len(healthy)
    │   │   ├── localprefer  → same node first, fallback random
    │   │   └── leastloaded  → min(load)
    │   └── Return *Endpoint{address, port, protocol}
    │
    ├── Call external service (HTTP/gRPC/TCP)
    │
    ├── Collect response
    │
    └── Return response bytes → C → Rust → ctx.outputs[svc_id]
    │
    ▼
Result replaces ctx.body
    ├── Next/Async:     body = response
    ├── Emit:           result discarded (fire-and-forget)
    └── Dag:            result stored in results[svc_id],
                        merged later with siblings
```

**Files:** `rust/src/executor/next.rs`, `rust/src/ffi.rs`,
`go/bridge/caller_bridge.c`, `go/bridge/bridge.go`,
`go/internal/registry/registry.go`

---

## 9. Request / Reply Flow

```
Client                                              FlowRulZ Node
  │                                                      │
  │  flow.Request("payment", payload, 5s)                │
  │  ├── Serialize payload                               │
  │  ├── Generate correlation_id (UUID)                  │
  │  └── Call client SDK                                 │
  │─────────────────────────────────────────────────────►│
  │                                                      │
  │                                                      │  ReplyRouter.Send(corrID, timeout)
  │                                                      │  ├── Register PendingRequest{corrID,
  │                                                      │  │   ReplyCh, Deadline}
  │                                                      │  ├── Check: duplicate? capacity?
  │                                                      │  └── Return ReplyCh (buffered, cap 1)
  │                                                      │
    │                                                      │  Publish event to cluster bus
    │                                                      │  Mode = Request
    │                                                      │  Headers: { correlation_id, reply_to }
  │                                                      │
  │                                                      ▼
  │                                               Partition Worker
  │                                                      │
  │                                                      ├── Consume event
  │                                                      ├── Execute VM plan
  │                                                      ├── VM calls service (Next opcode)
  │                                                      └── VM emits result
  │                                                      │
    │                                                      │  Response → _flowrulz_replies (cluster bus topic)
    │                                                      │  Key = hash(correlation_id)
    │                                                      │  Routed to origin node via cluster bus
  │                                                      │
  │                                                      ▼
  │                                               ReplyRouter.Route(corrID, response)
  │                                                      │
  │                                                      ├── Lookup pending[corrID]
  │                                                      ├── Delete from map
  │                                                      └── Non-blocking send to ReplyCh
  │                                                          Close ReplyCh
  │                                                      │
  │  ◄── Response delivered on ReplyCh ──────────────────┤
  │                                                      │
  │  (cleanup goroutine, tick=1s)                        │
  │  ├── Iterate pending map                             │
  │  ├── If time.Now > Deadline:                         │
  │  │   Delete entry, close ReplyCh                     │
  │  └── (caller receives nil, detects timeout)          │
```

**Files:** `go/internal/replyrouter/replyrouter.go`,
`go/internal/cluster/node.go`, `go/internal/execnode/execnode.go`

---

## 10. DAG Execution Flow

```
exec_dag(body, instr, plan, caller, arena)
    │
    ├── Load DAGTable from plan.dag_tables[instr.a]
    │   │
    │   ├── .layers: Vec<Vec<u16>> (topologically sorted)
    │   ├── .nodes:  Vec<DAGNode>  (service_id, parent_ids, timeout)
    │   ├── .failure_policy: AbortAll | ContinueOthers | SkipDependents
    │   ├── .merge_strategy: LastWins | ArrayConcat | DeepMerge | ExplicitMap
    │   ├── .terminal_nodes: Vec<u16>
    │   └── .node_timeouts:  Vec<u64>
    │
    ▼
For each layer in layers:
    │
    ├── For each svc_id in layer:
    │   │
    │   ├── Find node index by service_id
    │   │
    │   ├── SkipDependents check:
    │   │   if failure_policy == SkipDependents
    │   │   and any parent_id in failed set:
    │   │       skip node → add to failed set → continue
    │   │
    │   ├── Build input body:
    │   │   if no parents:        use original body
    │   │   if has parents:       deep_merge(parent results) → input
    │   │
    │   ├── Get timeout: node_timeouts[node_idx] | 0
    │   │
    │   ├── caller(svc_id, input_body, timeout)
    │   │
    │   └── Failure handling:
    │       ├── AbortAll        → return Err immediately
    │       ├── ContinueOthers  → add to failed set, continue
    │       └── SkipDependents  → add to failed set, continue
    │                            (downstream skip already done)
    │
    ▼
merge_dag_results(terminal_nodes, results, failed, plan, arena, strategy)
    │
    ├── LastWins:     {"svc_name": result, ...}  (object keyed by service name)
    │                 failed nodes → null
    │
    ├── ArrayConcat:  [result1, result2, ...]     (JSON array)
    │                 failed nodes → null
    │
    ├── DeepMerge:    recursive merge of all terminal node JSON objects
    │                 failed nodes → skipped
    │
    └── ExplicitMap:  same as LastWins (explicit map config not implemented)
    │
    ▼
Result written to arena, returned as &mut [u8]
    │
    ▼
Stored in ctx.body, hop_count incremented
```

**Files:** `rust/src/executor/dag.rs`, `rust/src/bytecode/dag_table.rs`

---

## 11. DLQ Flow

```
VM execution fails (error returned from bridge.Execute)
    │
    ├── Retries exhausted (3 attempts by default)
    │
    ▼
DLQ.Send(entry)
    │
    ├── Lock
    ├── if len(entries) >= maxSize (default 10000):
    │       entries = entries[1:]     (FIFO evict oldest)
    ├── entry.FailedAt = time.Now()
    ├── entries = append(entries, entry)
    ├── log: "dlq: rule=<id> id=<entryID> error=<msg>"
    └── always returns nil (no-fail design)
    │
    ▼
Admin API replay:
    │
    ├── POST /dlq/replay/{id}
    │   │
    │   ├── DLQ.Replay(ctx, id)
    │   │   ├── Lock, linear scan → remove entry
    │   │   ├── Unlock
    │   │   ├── entry.RetryCount++
    │   │   └── replayFn(ctx, entry)
    │   │       (replayFn is set by execnode: re-runs executeAll)
    │   │
    │   └── on success: entry removed from DLQ
    │       on error:   (entry already removed — not re-added)
    │
    ├── POST /dlq/replay (replay all)
    │   │
    │   ├── DLQ.ReplayAll(ctx)
    │   │   ├── Lock, copy all entries, clear entries
    │   │   ├── Unlock
    │   │   ├── for each entry:
    │   │   │       entry.RetryCount++
    │   │   │       err = replayFn(ctx, entry)
    │   │   │       if err: DLQ.Send(entry) (re-enqueue)
    │   │   └── return success count
    │   │
    │
    ├── GET /dlq → return all entries as JSON
    │
    └── DELETE /dlq → DLQ.Clear()
```

**Files:** `go/internal/reliability/dlq.go`, `go/internal/admin/server.go`

---

## 12. Rate Limiting Flow

```
Every ingress message → RateLimiter.Allow("ingress")
    │
    ▼
RateLimiter.Allow(name)
    │
    ├── Bucket(name)
    │   ├── RLock → lookup bucket by name → RUnlock
    │   ├── if not found:
    │   │       Lock → double-check → create TokenBucket{rate=100, burst=100}
    │   │       Store → Unlock
    │   └── return bucket
    │
    ├── TokenBucket.Allow()
    │   ├── Lock
    │   ├── refill():  tokens += elapsed_seconds * rate
    │   │               if tokens > burst: tokens = burst
    │   │               lastRefill = now
    │   ├── if tokens >= 1.0:
    │   │       tokens -= 1.0
    │   │       Unlock → return true
    │   └── else:
    │           Unlock → return false
    │
    ├── true  → continue to Scheduler.Enqueue
    └── false → DLQ.Send(entry{error: "rate limited"})
                 Metrics.RecordError("rate_limited")
```

**Files:** `go/internal/reliability/ratelimit.go`, `go/internal/execnode/execnode.go`

---

## 13. Metrics Flow

```
Rust VM                           Go MetricsCollector
  │                                      │
  │  dispatch(opcode)                    │
  │    │                                 │
  │    ├── execute handler               │
  │    └── emit_span(Span{opcode,        │
  │        svc_id, duration_ns, status}) │
  │           │                          │
  │           ▼                          │
  │    thread_local SPAN_BUFFER          │
  │    (lock-free ring buffer, 1024)     │
  │           │                          │
  │           │ push:                    │
  │           │  head = atomic.load      │
  │           │  tail = atomic.load      │
  │           │  if head-tail < 1024:    │
  │           │    buffer[head%1024]=span│
  │           │    atomic.store(head+1)  │
  │           │  else: drop (full)       │
  │           │                          │
  │           ▼                          │
  │    (called by Go via FFI)            │
  │    flowrulz_get_spans(out)           │
  │           │                          │
  │           │ drain:                   │
  │           │  while tail < head:      │
  │           │    read buffer[tail%1024]│
  │           │    copy to out           │
  │           │    atomic.store(tail+1)  │
  │           │  return bytes_written    │
  │           │                          │
  │           ▼                          │
  │         raw Span bytes               │
  │           │                          │
  └───────────│──────────────────────────┘
              │
              ▼
    bridge.GetSpans()
        │
        ├── Call C.flowrulz_get_spans(buf)
        ├── Parse Span structs from bytes
        └── Update MetricsCollector counters
            │
            ├── Metrics.Counter("exec.<rule>").Inc()   (on success)
            ├── Metrics.Counter("error.<rule>").Inc()  (on error)
            ├── Metrics.Counter("exec.total").Inc()
            ├── Metrics.Counter("error.total").Inc()
            └── Metrics.Histogram("latency.<opcode>").Observe(duration_ms)

Global shortcuts:
    RecordExec(name)    → GetCounter("exec."+name).Inc()
    RecordError(name)   → GetCounter("error."+name).Inc()
    RecordTiming(name)  → GetHistogram(name).Observe(duration_seconds)
```

**Files:** `rust/src/tracing/mod.rs`, `rust/src/ffi.rs`,
`go/bridge/bridge.go`, `go/internal/observability/metrics.go`

---

## 14. Buffer / Chunk Flow

### Buffer (accumulate)

```
Runtime receives message Body
    │
    ├── first opcode == Buffer(9)?
    │
    ├── YES (Buffer mode):
    │   ├── runtime.buffer_target = instr.a (flush threshold)
    │   ├── runtime.buffer_body = Body
    │   ├── runtime.buffer_count = 1
    │   └── return (body NOT processed by VM)
    │
    ├── Subsequent message → runtime.buffer_push(Body)
    │   ├── merge_buffer_json(prev, new)
    │   │   ├── if prev is JSON array: append new
    │   │   └── else: create [prev, new]
    │   ├── buffer_count++
    │   └── if buffer_count >= buffer_target:
    │       │
    │       ├── runtime.buffer_flush()
    │       ├── runtime.execute(accumulated_body)
    │       └── (VM runs on the combined batch)
    │
    └── Each individual message without reaching target:
        └── Return (no execution until buffer full)
```

### Chunk (split)

```
Runtime receives message Body
    │
    ├── first opcode == Chunk(15)?
    │
    ├── YES (Chunk mode):
    │   ├── count = instr.a (number of chunks)
    │   ├── threshold = body.len() / count
    │   ├── split_chunks(body, count, threshold)
    │   │   ├── if can't split evenly: fallback to run_vm(body)
    │   │   └── else: return Vec<Vec<u8>> chunks
    │   │
    │   ├── for each chunk:
    │   │       run_vm(chunk) → result
    │   │
    │   └── collect results → JSON array
    │       → return as ctx.body
    │
    └── NO: delegate to run_vm(body) or other first-opcode logic
```

**Files:** `rust/src/executor/runtime.rs`, `rust/src/executor/chunk.rs`

---

## 15. Admin API Flow

```
HTTP Request
    │
    ├── mux: /admin/* (stripped prefix)
    │
    ▼
admin.ServeHTTP(w, r)
    │
    ├── auth() middleware (if API key is set)
    │   ├── Extract Bearer token from Authorization header
    │   ├── Constant-time compare with configured FLOWRULZ_API_KEY
    │   └── 401 if mismatch
    │
    ├── Route matching:
    │   │
    │   ├── POST   /rules              → handleDeploy
    │   │   ├── Parse JSON body {id, dsl}
    │   │   ├── engine.Deploy(id, dsl)
    │   │   │   ├── bridge.Compile → plan
    │   │   │   ├── lane assignment
    │   │   │   └── persist
    │   │   └── 201 {rule_id, version, lane}
    │   │
    │   ├── DELETE /rules/{id}         → handleRemove
    │   │   ├── engine.Drain(id, version)
    │   │   │   └── WaitForActiveExecs (poll until ActiveExec == 0)
    │   │   ├── engine.Remove(id) (or deactivate)
    │   │   └── 200
    │   │
    │   ├── GET    /rules              → handleList
    │   │   ├── engine.ListRules()
    │   │   └── JSON array
    │   │
    │   ├── GET    /rules/{id}         → handleGet
    │   │   ├── engine.GetRule(id)
    │   │   └── JSON with lane info
    │   │
    │   ├── GET    /rules/{id}/versions → handleVersions
    │   │   ├── engine.GetVersions(id)
    │   │   └── JSON array
    │   │
    │   ├── POST   /rules/{id}/validate → handleValidate
    │   │   ├── Parse DSL from body
    │   │   ├── bridge.Compile(dsl) → validity
    │   │   ├── bridge.PlanComplexity(plan) → score
    │   │   └── 200 {valid, complexity, lane}
    │   │
    │   ├── POST   /rules/{id}/promote  → handlePromote
    │   │   ├── Parse version from query (?version=N)
    │   │   ├── engine.Promote(id, N)
    │   │   └── 200
    │   │
    │   ├── POST   /rules/{id}/rollback → handleRollback
    │   │   ├── Same as promote
    │   │   └── 200
    │   │
    │   ├── GET    /lanes              → handleLanes
    │   │   ├── Return configured lane configs
    │   │   └── JSON
    │   │
    │   ├── GET    /dlq                → handleDLQList
    │   │   ├── DLQ.List() → []DeadLetterEntry
    │   │   └── JSON
    │   │
    │   ├── POST   /dlq/replay/{id}    → handleDLQReplay
    │   │   ├── DLQ.Replay(ctx, id)
    │   │   └── 200
    │   │
    │   ├── POST   /dlq/replay         → handleDLQReplayAll
    │   │   ├── DLQ.ReplayAll(ctx) → count
    │   │   └── 200 {replayed: count}
    │   │
    │   ├── DELETE /dlq                → handleDLQClear
    │   │   ├── DLQ.Clear()
    │   │   └── 200
    │   │
    │   ├── GET    /health             → {"status":"ok"}
    │   │
    │   └── GET    /metrics            → JSON snapshot
    │       ├── Metrics.Snapshot() → counters + gauges
    │       ├── ReplyRouter.PendingCount()
    │       └── DLQ.Len()
    │
    └── Response (JSON, appropriate status code)
```

**Files:** `go/internal/admin/server.go`, `go/internal/engine/engine.go`,
`go/internal/reliability/dlq.go`, `go/internal/observability/metrics.go`,
`go/internal/replyrouter/replyrouter.go`

---

## Flow Interconnection Map

```
                       ┌──────────────────┐
                       │  Admin API       │◄── HTTP
                       │  (flows 4, 11,   │
                       │   15)            │
                       └────────┬─────────┘
                                │ Deploy/Promote/Rollback
                                ▼
                ┌───────────────────────────────┐
                │  Cluster Membership (flow 1)  │
                │  Node Lifecycle   (flow 2)    │
                │  Plan Distribution (flow 3)   │
                └───────────┬───────────────────┘
                            │ Plan activation
                            ▼
┌──────────────┐ ┌──────────────────┐     ┌────────────┐
│ Cluster Bus  │─►│ Message          │────►│ Scheduler  │
│ / Transport  │  │ Ingestion (flow 5)│     │ (flow 6)   │
└──────────────┘  │ Rate Limit(flow12)│     └──────┬─────┘
                 └──────────────────┘            │
                                                 ▼
                                       ┌──────────────────┐
                                        │ executeAll       │
                                        │ (flow 5)         │
                                       └────────┬─────────┘
                                                │
                                                ▼
                                  ┌─────────────────────────┐
                                  │  Execution (flow 7)     │
                                  │  ├── Service Call(flow8)│
                                  │  ├── Request/Reply(f9)  │
                                  │  ├── DAG (flow 10)      │
                                  │  └── Buffer/Chunk(f14)  │
                                  └────────┬────────────────┘
                                           │
                              ┌────────────┼────────────┐
                              ▼            ▼            ▼
                    ┌─────────────┐ ┌──────────┐ ┌──────────┐
                    │ DLQ (flow11)│ │ Metrics  │ │ Emit/    │
                    │             │ │ (flow13) │ │ Produce  │
                    └─────────────┘ └──────────┘ └──────────┘
                                                      │
                                                      ▼
                                                   Cluster Bus /
                                                   Response
```

---

## Summary Table

| # | Flow | Entry Point | Key Components | Persistence |
|---|------|-------------|----------------|-------------|
| 1 | Cluster Membership | Node startup | `_flowrulz_members`, gossip protocol | Cluster bus |
| 2 | Node Lifecycle | `execnode.Start()` | Consumer, scheduler, HTTP server | In-memory |
| 3 | Plan Distribution | `plandist.PublishPlan()` | `_flowrulz_plans`, `_flowrulz_acks`, quorum | Cluster bus |
| 4 | Rule Deployment | `POST /rules` | Admin, engine, bridge, plandist | JSON file |
| 5 | Message Ingestion | Transport handler | Rate limiter, scheduler, `executeAll`/`executePlan`/`callService` | Cluster bus |
| 6 | Scheduling | `Scheduler.Enqueue()` | Lane queues, semaphore, goroutines | None |
| 7 | Execution (VM) | `VM::run()` / `VM::step()` | Instruction dispatch, opcode handlers, cooperative step loop | None |
| 8 | Service Call | `op_next` / `StepPending` | Bridge CGo, registry, external service, `callService()` | None |
| 9 | Request/Reply | `flow.Request()` | `_flowrulz_replies`, ReplyRouter | Cluster bus |
| 10 | DAG Execution | `op_dag` | Layers, parent merge, failure policy | None |
| 11 | DLQ | `bridge.Execute()`/`ExecuteStep()` error | `DLQ.Send`, admin replay | In-memory (bounded) |
| 12 | Rate Limiting | `RateLimiter.Allow()` | Token bucket refill, allow/deny | None |
| 13 | Metrics | `emit_span()` | Ring buffer, `flowrulz_get_spans`, counters | None |
| 14 | Buffer/Chunk | First opcode check | `ExecutionRuntime` accumulator/splitter | None |
| 15 | Admin API | `POST /rules` etc. | `auth()` middleware, engine, DLQ, metrics | JSON file |
