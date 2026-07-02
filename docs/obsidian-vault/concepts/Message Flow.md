---
title: Message Flow
tags:
  - concept
  - messaging
---

# Message Flow

End-to-end path of a message through the FlowRulZ system.

## Ingress

```
SDK Client ──HTTP──> Admin API ──> Node.ExecuteAll
                                      │
                                      ▼
                              Rate Limiter (token bucket)
                                      │
                                      ▼
                                Dedup Tracker
                                      │
                                      ▼
                              Scheduler.EnqueueAndWait
```

## Egress

```
Scheduler ──> slotWorker ──> executePlan ──> Bridge.runSteps
                                                  │
                                          ┌───────┴───────┐
                                          ▼               ▼
                                      Map / Gate    SERVICE_CALL
                                                          │
                                                          ▼
                                                  HTTP → Target Service
                                                          │
                                                          ▼
                                                  Response / Error
                                                          │
                                          ┌───────────────┘
                                          ▼
                                   Next Step or Done
```

## Transport Types

| Transport | Direction | Backend |
|-----------|-----------|---------|
| Cluster | inter-node | gRPC (inproc) |
| Service calls | node→service | HTTP |
| Kafka | cluster→external | Kafka consumer/producer |
| Admin API | external→cluster | HTTP REST |

## Dependencies

- [[Transport]] — backend implementations
- [[Node]] — ingress entry point
- [[Scheduler]] — dispatch to VM
- [[Reliability]] — DLQ, Saga on failure
