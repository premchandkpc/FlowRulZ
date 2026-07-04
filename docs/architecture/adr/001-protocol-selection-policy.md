# ADR-001: Protocol Selection Policy for Service Invocation

## Status

Accepted

## Context

FlowRulZ services can be reachable via multiple protocols (HTTP, gRPC, TCP, Kafka). The ServiceInvoker needs to decide how to select which protocol to use when calling a service.

Two options were considered:

### Option A: Preferred-protocol-per-call
The DSL/plan step (or ServiceInvoker caller) specifies which protocol it wants, and lookup filters candidates to that protocol before load-balancing.

### Option B: Preferred-protocol-per-service-with-fallback
The registry expresses an ordered protocol preference per service (e.g., "prefer gRPC, fall back to HTTP if no healthy gRPC instance"), and lookup applies that ordering, only load-balancing within the chosen protocol tier.

## Decision

**We chose Option B: Preferred-protocol-per-service-with-fallback.**

## Rationale

1. **Simplicity for callers**: The domain logic doesn't need to know about protocol selection. It just says "call payment service" and the registry handles the protocol decision.

2. **Operational flexibility**: Operators can configure protocol preferences per service without changing code. For example, prefer gRPC for low-latency services but fall back to HTTP for compatibility.

3. **Graceful degradation**: If the preferred protocol is unavailable (e.g., all gRPC instances are down), the system automatically falls back to the next available protocol.

4. **No protocol mixing**: The implementation ensures that candidates of different protocols are never mixed in the same LB pool. Protocol filtering happens BEFORE load balancing.

## Implementation

### Registry Changes

1. `Endpoint` struct now includes Kafka-specific fields (`Topic`, `ReplyTopic`, `ConsumerGroup`)
2. `LookupInstanceWithProtocol(name, method, protocol)` accepts an optional protocol filter
3. If protocol is empty, any protocol is acceptable (backward compatible)

### ServiceInvoker Changes

1. Circuit breakers are keyed by `(service, protocol)` for failure isolation
2. HTTP connections use tuned pooling (`MaxIdleConnsPerHost=20`)
3. gRPC connections use a cache with idle eviction (5-minute default)
4. Kafka support added via `KafkaInvoker` for request/reply patterns

### Configuration

Protocol preferences are configured via the `ServiceRegistry`:

```go
// Register with protocol preference
registry.Register("payment", &Endpoint{
    Address:  "payment-grpc.internal:50051",
    Protocol: ProtocolGRPC,
})

// Fallback registration
registry.Register("payment", &Endpoint{
    Address:  "payment-http.internal:8080",
    Protocol: ProtocolHTTP,
})
```

## Consequences

### Positive
- Protocol selection is explicit and configurable
- Failure isolation between protocols (gRPC circuit breaker doesn't affect HTTP)
- Graceful degradation when preferred protocol is unavailable
- No protocol mixing in LB pools

### Negative
- Slightly more complex registry configuration
- Need to maintain multiple registrations per service

## Future Work

- Add protocol preference configuration to service metadata
- Implement health checks per protocol (not just per endpoint)
- Add metrics for protocol selection and fallback events
