# Complete System Flow

Generic end-to-end trace of how FlowRulZ works — from node startup to event processing.

## The Big Picture

```
DSL String
  → Compiler (Rust via CGo)
  → Bytecode Plan
  → Stored in Engine (with version)
  → Distributed to cluster (Kafka/gRPC)
  → Event arrives
  → Scheduler picks lane (Fast/Normal/Heavy)
  → VM executes bytecode step-by-step
  → Service calls via Go bridge
  → Response fed back to VM
  → Result returned
```

## 1. Node Startup

```
main.go
  → loadConfig()                    read FLOWRULZ_* env vars
  → bootstrap.NewNodeBuilder(cfg)
    → WithDefaults()
      → node.DefaultDependencies()  create ALL subsystems:
          Engine, Scheduler, DLQ, Saga, Dedup, RateLimiter,
          Registry, PlanDist, Membership, Partitions, ClusterNode,
          RaftCluster, AgentPool, ReplyRouter
  → builder.Build()
    → node.NewNode(cfg, deps)       wire lifecycle hooks:
        Engine.AfterDeploy  → distribute plan to cluster
        Engine.AfterPromote → broadcast activation
  → node.Start(ctx)
    → ClusterNode.Start()           gRPC bus, peer discovery
    → startConsumers()              Kafka topics (input, plans, acks, members)
    → Scheduler.Start()             spawn lane workers
    → RaftCluster.Start()           elect leader
    → AgentPool.Start()             spawn worker agents
    → recoverInFlight()             resume interrupted executions
    → serveHTTP()                   admin API
```

## 2. Rule Registration (DSL → Plan)

```
POST /rules {id: "order", dsl: "n:validate | n:process"}
  → Engine.Deploy("order", dsl)
    → compiler.Compile(dsl, "order")
      → bridge.Compile(dsl, ruleID)
        → C.flowrulz_compile()       FFI into Rust VM
          → lexer → parser → optimizer → compiler
          → returns bincode-encoded ExecutionPlan bytes
    → bridge.PlanComplexity(plan)    score: 20 (Next=10 + Next=10)
    → LaneForScore(20) → PriorityNormal
    → Store VersionedPlan{Plan, Version:1, Lane:Normal}
    → saveRules()                    persist to disk

If leader:
  → handleEngineDeploy()
    → CaptureLeadershipToken()       fencing pattern
    → PlanDist.PublishPlan()         broadcast to cluster
      → PlanMessage{Type:"plan", RuleID, Version, Plan}
      → Kafka/ClusterNode.Send()
    → PlanDist.WaitForAcks()         quorum = majority of followers
    → PlanDist.ActivatePlan()        broadcast activation

Followers receive:
  → handlePlanMessage()
    → reject if term < currentTerm   stale leader protection
    → Engine.AddVersion(ruleID, plan)
    → SendAck() → leader
```

## 3. Event Processing

```
Message arrives (Kafka topic / gRPC / HTTP)
  → handleIncomingMessage(msg)

Step 1: Rate Limit
  → RateLimiter.Allow("ingress")
  → If denied → DLQ.Send("rate limited") → return

Step 2: Dedup
  → Dedup.CheckAndMark(hash(msg))
  → If duplicate → return

Step 3: Submit to AgentPool
  → AgentPool.SubmitAndWait(task)
    → executeAll(ctx, body)
      → Get all active plans from Engine
      → For each plan:
        → Acquire execSem (max 16 concurrent)
        → Scheduler.EnqueueAndWait(task)
          → Lane picks up task (work-stealing if idle)
          → Worker calls execTask()

Step 4: Execute Plan
  → executePlan(ctx, plan, body)
    → bridge.PlanServices(plan) → {0:"validate", 1:"process"}
    → execID = UUID()
    → Execs.Register(execID)         allow HTTP cancellation
    → execstate.Create(execID)       persist to disk

Step 5: Run Steps (loop, max 1000 iterations)
  → bridge.ExecuteStep(plan, ctx, resp)
    → C.flowrulz_execute_step()      Rust VM executes ONE bytecode instruction
    → Returns StepOutput{Result, PendingSvc, PendingBody, Output}

  Case StepDone:
    → Saga.Clear(execID)
    → return output

  Case StepPending:
    → Parse: svcName="validate", method=""
    → Saga.RegisterStep(execID, {svc:"validate", comp:"invalidate"})
    → callService(ctx, "validate", body, timeout)
      → CircuitBreaker.Allow()       if Open → error
      → Registry.LookupInstance()    get endpoint
      → serviceCaller.CallService()
        → CallServiceWithRetry()
          → switch protocol:
            HTTP → POST http://host:port/validate
            gRPC → grpc.invoke()
            TCP  → length-prefixed send/recv
          → On success: cb.Success()
          → On failure: cb.Failure(), retry if retryable
    → respBytes = response
    → Continue loop (feed response back to VM)

  Case StepContinue:
    → respBytes = nil, continue

Step 6: On Error
  → tryCompensate(execID)
    → Saga.Compensate(execID)
      → Reverse-iterate registered steps
      → Call compensator for each: invalidate(body), reverse(body)
  → DLQ.Send({Body, Error})

Step 7: On Success
  → RecordExec("completed")
  → return result
```

## 4. Service Call Flow

```
VM instruction: Next(service_id=3, timeout=5000)
  → StepPending { svc_id: 3, body: <bytes> }
  → Go bridge receives pending
  → Parse svc_id → "validate" from ServiceTable
  → Check CircuitBreaker → Open? Reject. Closed? Continue.
  → Registry.LookupInstance("validate") → {Protocol: HTTP, Host: "svc:8080"}
  → CallServiceWithRetry():
      attempt 1: POST http://svc:8080/validate → 500ms → timeout
      attempt 2: POST http://svc:8080/validate → retry after 100ms
      attempt 3: POST http://svc:8080/validate → retry after 200ms
      All failed → return error
  → On success: cb.Success(), response bytes
  → Feed response back into VM → VM continues to next instruction
```

## 5. Scheduler & Lanes

```
Task arrives
  → EnqueueTask(task)
    → Select lane by task.Priority:
        Fast    (score < 10)  → 50 workers, Q=5000
        Normal  (score ≤ 50)  → 20 workers, Q=2000
        Heavy   (score > 50)  →  5 workers, Q=500

  → Worker goroutine picks up:
    → dequeueOrSteal()
      → Try own lane queue first
      → If empty: steal from Heavy → Normal → Fast
    → execTask(task)
      → task.Execute(ctx, task)
      → Write result to task.ResultCh

  → Caller waits on task.ResultCh
```

## 6. Cluster Plan Distribution

```
[Leader]
  → PublishPlan()     → _flowrulz_plans topic
  → WaitForAcks()     → quorum = (AliveCount-1)/2 + 1
  → ActivatePlan()    → _flowrulz_plans topic

[Follower receives "plan"]
  → reject if term < currentTerm
  → Engine.AddVersion(ruleID, plan)
  → SendAck()         → _flowrulz_acks topic

[Leader receives ack]
  → increment received count
  → if received >= quorum → unblock WaitForAcks()
```

## 7. Error Layers

| Layer | What | Action |
|-------|------|--------|
| Rate Limit | Too many requests | DLQ, silent drop |
| Dedup | Duplicate message | Skip |
| Circuit Breaker | Service failing | Reject immediately |
| Service Call | HTTP 500/timeout | Retry → fallback → error |
| Execution | Step failure | Saga compensate → DLQ |
| Recovery | Node restart | Resume in-flight from StateStore |
| DLQ Replay | Admin action | Re-submit to handleIncomingMessage |

## 8. Key Interfaces

| Interface | Methods | Where Used |
|-----------|---------|------------|
| `Compiler` | `Compile(dsl, ruleID) → Result` | DSL → bytecode |
| `ServiceCaller` | `CallService(svc, method, body) → resp` | VM → service |
| `MessageProducer` | `Send(ctx, key, data)` | Plan distribution |
| `MessageConsumer` | `Start(ctx), Stop()` | Event ingestion |
| `QuorumProvider` | `AliveCount() → int` | Plan distribution |
| `Membership` | `LeaderID(), AliveNodes()` | Cluster topology |
| `Engine` | `Deploy, Promote, ActivePlanBytes` | Rule management |
| `Scheduler` | `EnqueueAndWait, Start, Stop` | Task execution |
| `DLQ` | `Send, Replay, ReplayAll` | Dead letter handling |
| `SagaTracker` | `RegisterStep, Compensate, Clear` | Compensation |

## 9. Data Flow Summary

```
                    ┌──────────────────────────────────────────┐
                    │              Go Control Plane             │
                    │                                          │
  HTTP/Kafka ──────►│  Transport ──► RateLimit ──► Dedup       │
                    │                    │                      │
                    │              AgentPool ──► Scheduler      │
                    │                    │         │            │
                    │              Engine ──────► Bridge (CGo)  │
                    │                    │         │            │
                    │              DLQ ◄─┤    ┌────▼────┐      │
                    │              Saga ◄─┤    │Rust VM  │      │
                    │                    │    │Bytecode │      │
                    │                    │    │Executor │      │
                    │                    │    └────┬────┘      │
                    │                    │         │            │
                    │              ClusterNode ◄───┘            │
                    │                    │                      │
                    │              PlanDist ◄──► Raft           │
                    └──────────────────────────────────────────┘
                                         │
                                  Service Calls
                                         │
                              ┌──────────▼──────────┐
                              │  HTTP / gRPC / TCP   │
                              │  External Services   │
                              └─────────────────────┘
```

## 10. Gotchas

- **ExecutionContext** uses `sync.Mutex` — always use `State()/SetVariable()/Variable()` accessors
- **TimerWheel.Stop()** waits for callbacks (`sync.WaitGroup`) — don't call from timer callback goroutine
- **ProdNode.Start()** refuses to start if Seeds configured without RaftCluster
- **Scheduler.Stop()** releases mutex before `wg.Wait()` to prevent deadlock if tasks call `Snapshot()`
- **DLQ** captures `replayFn` under lock before executing outside lock — prevents deadlock
- **CircuitBreaker.State()** acquires mutex — was previously racy
- **goServiceCaller** (CGo) has `defer recover()` — panics in callbacks don't crash process
- **All data files** written with `0600` permissions
- **TLS cipher suites** explicit allowlist (ECDHE+AES-GCM only)
- **Plan distribution** uses term-based fencing — stale leaders rejected
