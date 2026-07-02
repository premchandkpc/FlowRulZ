---
title: FlowRULZ DSL
tags:
  - concept
  - dsl
  - rules
---

# FlowRULZ DSL

The rule language that compiles to bytecode [[Plan Execution\|execution plans]].

## Syntax Example

```
rule "validate-and-process"
  when message.type == "order"
  map input = { user_id: message.userId, amount: message.total }
  gate amount > 0
  call "billing-api" with { user: input.user_id, charge: input.amount }
  map result = { status: "ok", txId: response.transaction_id }
end
```

## Step Types (Compiled)

| DSL Keyword | OpCode | Description |
|-------------|--------|-------------|
| `map` | MAP | Transform via JSON expression |
| `gate` | GATE | Conditional branch with comparison op |
| `call` | SERVICE_CALL | HTTP call to registered service |
| (implied) | NEXT | Advance to next instruction |
| (implied) | JUMP_OFFSET | Skip N instructions (gate failure) |
| (implied) | LABEL | Jump target |

## Compilation Pipeline

```
DSL text ──> Engine.Compile() ──> plan.Plan (bytecode)
               │
               ▼
        PlanDist.SendToCluster()
               │
               ▼
        Each node stores + ack
               │
               ▼
        Ready for execution
```

## Output Persistence

```
ExecuteAll ──> ExecState.FileStore (JSON)
```

Steps, output, and result status are saved — refer to [[ExecState#Record Lifecycle]] for details.

## Integration Points

- [[Engine]] — rule lifecycle management
- [[PlanDist]] — distribution protocol
- [[Runtime]] — bytecode VM execution
