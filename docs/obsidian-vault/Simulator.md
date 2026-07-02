---
title: Simulator
tags:
  - architecture
  - go
aliases:
  - Load Simulator
---

# Simulator

The simulator (`simulator/`) generates load and tests timeline-based scenarios against a running FlowRulZ cluster.

```
simulator/
├── client_test.go   # Integration tests
├── loadgen/
│   └── loadgen.go   # Load generation engine
├── routes.go        # HTTP route definitions
├── services/
│   └── service.go   # Mock service implementations
└── timeline/
    └── timeline.go  # Timeline-based scenario runner
```

## Usage

> [!tip]
> Use the simulator for integration testing before production deployment.
