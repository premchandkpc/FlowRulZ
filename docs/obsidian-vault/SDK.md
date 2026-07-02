---
title: SDK
tags:
  - architecture
  - go
aliases:
  - Go SDK
---

# SDK

The Go SDK (`sdk/`) provides a client library for services to interact with FlowRulZ.

```
sdk/
├── flow/
│   ├── client.go     # HTTP client for rule deployment & execution
│   └── client_test.go
├── go.mod
└── go.sum
```

## Client API

The SDK client communicates with the [[Server]] admin API:

- `DeployRule(id, dsl)` — deploy or update a rule
- `Execute(body)` — send a message for processing
- `RemoveRule(id)` — delete a rule
- `ListRules()` — list deployed rules

See `sdk/flow/client.go` for the full interface.
