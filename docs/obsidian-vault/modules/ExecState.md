---
title: ExecState
tags:
  - module
  - persistence
  - go
---

# ExecState

> [!info] Execution state persistence
> Path: `server/internal/execstate/`

Persists execution records to disk as JSON files. Used for history, DLQ replay, and audit.

## Key Types

| Type | File | Purpose |
|------|------|---------|
| `FileStore` | `store.go` | JSON-based storage (execution records in `data/` dir) |
| `Record` | `store.go` | Execution record with Status, Steps, Output, Error |

## Storage Format

```
data/
├── exec-<planID>-<msgID>.json   # Execution records
└── state/                        # Node state snapshots
```

## Record Lifecycle

> [!important] History retention (Gap #4)
> Completed states are saved with `StatusCompleted` + output, not deleted. This enables audit and replay.

States: `StatusPending` → `StatusRunning` → `StatusCompleted` or `StatusFailed`. On completion, the full record (steps, output, timing) is written to disk.

## Dependencies

- [[Node]] — uses FileStore to persist execution results
- [[Reliability]] — DLQ replays from stored records
