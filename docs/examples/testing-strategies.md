# Testing Strategies

Unit, integration, chaos, load, contract, and end-to-end testing.

## 1. Unit Testing

### Go Unit Tests

```go
func TestGateOperator(t *testing.T) {
    tests := []struct {
        name     string
        field    interface{}
        op       string
        value    interface{}
        expected bool
    }{
        {"equal", 5, "==", 5, true},
        {"not_equal", 5, "!=", 3, true},
        {"greater", 10, ">", 5, true},
        {"less", 3, "<", 5, true},
        {"contains", "hello world", "contains", "world", true},
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result := evaluateGate(tt.field, tt.op, tt.value)
            assert.Equal(t, tt.expected, result)
        })
    }
}
```

### Rust Unit Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_constant_pool() {
        let mut pool = ConstantPool::new();
        let idx1 = pool.add("hello");
        let idx2 = pool.add("world");
        let idx3 = pool.add("hello"); // duplicate
        
        assert_eq!(idx1, 0);
        assert_eq!(idx2, 1);
        assert_eq!(idx3, 0); // same as idx1
        assert_eq!(pool.len(), 2);
    }
    
    #[test]
    fn test_gate_evaluation() {
        let ctx = ExecutionContext::new();
        ctx.set_variable("amount", 1000);
        
        assert!(evaluate_gate(&ctx, "amount", GateOp::Gt, "500"));
        assert!(!evaluate_gate(&ctx, "amount", GateOp::Lt, "500"));
    }
}
```

## 2. Integration Testing

### Bridge Integration

```go
func TestBridgeCompileAndExecute(t *testing.T) {
    dsl := "n:validate | n:process"
    plan, err := bridge.Compile(dsl, "test-rule")
    require.NoError(t, err)
    require.NotEmpty(t, plan)
    
    event := createTestEvent()
    result, err := bridge.Execute(plan, event, &mockServiceCaller{})
    require.NoError(t, err)
    require.NotEmpty(t, result)
}
```

### End-to-End Flow

```go
func TestEndToEndFlow(t *testing.T) {
    // Start node
    node := startTestNode(t)
    defer node.Stop()
    
    // Register rule
    err := node.RegisterRule("test-rule", "n:validate | n:process")
    require.NoError(t, err)
    
    // Send event
    resp, err := sendEvent(node, "test-rule", map[string]interface{}{
        "key": "value",
    })
    require.NoError(t, err)
    assert.Equal(t, 200, resp.StatusCode)
}
```

## 3. Chaos Testing

### Network Partition

```go
func TestNetworkPartition(t *testing.T) {
    cluster := startTestCluster(t, 3)
    defer cluster.Stop()
    
    // Partition node 0 from 1,2
    cluster.Partition(0, []int{1, 2})
    
    // Send events during partition
    go sendEvents(cluster.Node(0), 100)
    
    // Heal partition
    time.Sleep(5 * time.Second)
    cluster.HealPartition(0)
    
    // Verify consistency
    assert.Eventually(t, func() bool {
        return cluster.AllNodesConsistent()
    }, 30*time.Second, 1*time.Second)
}
```

### Service Failure

```go
func TestServiceFailure(t *testing.T) {
    mockSvc := startMockService(t)
    node := startTestNode(t)
    
    // Register rule that calls mock service
    node.RegisterRule("test", "n:mock-service f:fallback")
    
    // Kill mock service
    mockSvc.Stop()
    
    // Send event - should fallback
    resp, err := sendEvent(node, "test", payload)
    require.NoError(t, err)
    assert.Equal(t, "fallback", resp.Result)
}
```

## 4. Load Testing

### Artillery Config

```yaml
config:
  target: "http://localhost:8080"
  phases:
    - duration: 60
      arrivalRate: 10
      name: "Warm up"
    - duration: 120
      arrivalRate: 100
      name: "Sustained load"
    - duration: 60
      arrivalRate: 1000
      name: "Peak load"

scenarios:
  - name: "Simple pipeline"
    flow:
      - post:
          url: "/event"
          json:
            topic: "test"
            payload:
              order_id: "{{ $randomNumber() }}"
              amount: "{{ $randomNumber() }}"
            mode: 0

  - name: "Parallel pipeline"
    flow:
      - post:
          url: "/event"
          json:
            topic: "parallel-test"
            payload:
              items: "{{ $randomArray() }}"
            mode: 4
```

### k6 Script

```javascript
import http from 'k6/http';
import { check, sleep } from 'k6';

export let options = {
  stages: [
    { duration: '30s', target: 100 },
    { duration: '1m', target: 500 },
    { duration: '30s', target: 0 },
  ],
  thresholds: {
    http_req_duration: ['p(95)<200'],
    http_req_failed: ['rate<0.01'],
  },
};

export default function () {
  let payload = JSON.stringify({
    topic: 'load-test',
    payload: { key: 'value', timestamp: Date.now() },
    mode: 0,
  });

  let res = http.post('http://localhost:8080/event', payload, {
    headers: { 'Content-Type': 'application/json' },
  });

  check(res, {
    'status was 200': (r) => r.status == 200,
    'response time < 200ms': (r) => r.timings.duration < 200,
  });

  sleep(0.1);
}
```

## 5. Contract Testing

### Pact Consumer Test

```go
func TestPaymentServiceConsumer(t *testing.T) {
    pact := pact.Consumer("OrderService").HasPactWith("PaymentService")
    
    pact.
        UponReceiving("a charge request").
        Given("user has sufficient balance").
        WithRequest("POST", "/charge", func(b *dsl.RequestBuilder) {
            b.JSONBody(map[string]interface{}{
                "amount": 100.00,
                "currency": "USD",
            })
        }).
        WillRespondWith(200, func(b *dsl.ResponseBuilder) {
            b.JSONBody(map[string]interface{}{
                "status": "success",
                "transaction_id": "txn_123",
            })
        })
    
    err := pact.Verify(func() error {
        client := NewPaymentClient(pact.MockServer.URL())
        result, err := client.Charge(100.00, "USD")
        assert.NoError(t, err)
        assert.Equal(t, "success", result.Status)
        return nil
    })
    
    require.NoError(t, err)
}
```

## 6. Property-Based Testing

### Go (gopter)

```go
func TestGateProperties(t *testing.T) {
    params := gopter.DefaultTestParameters()
    props := gopter.NewProperties(params)
    
    prop := forAll(
        gen.IntRange(-1000, 1000),
        gen.IntRange(-1000, 1000),
        func(a, b int) bool {
            // Property: if a > b, then !(a < b)
            if a > b {
                return !(a < b)
            }
            return true
        },
    )
    
    props.Property("gate transitivity", prop)
    props.TestingRun(t)
}
```

### Rust (proptest)

```rust
proptest! {
    #[test]
    fn test_gate_never_panics(field in any::<i64>(), op in "[!=><]", value in any::<i64>()) {
        let _ = evaluate_gate(field, &op, value);
    }
    
    #[test]
    fn test_constant_pool_dedup(s in "[a-z]{1,10}") {
        let mut pool = ConstantPool::new();
        let idx1 = pool.add(&s);
        let idx2 = pool.add(&s);
        prop_assert_eq!(idx1, idx2);
    }
}
```

## 7. Snapshot Testing

### Go (go-snaps)

```go
func TestFlowParsing(t *testing.T) {
    input := `
version 1
flow Test
service api
    type http
    url https://api.test
workflow
Start
-> api.Call
-> End
`
    ast, err := Parse(input)
    require.NoError(t, err)
    
    snaps.MatchSnapshot(t, ast)
}
```

## 8. Fuzz Testing

### Go Fuzzing

```go
func FuzzDSLCompiler(f *testing.F) {
    f.Add("n:validate")
    f.Add("p:a,b | c")
    f.Add("g:x==1 n:y f:z")
    f.Add("schema:{!x:int}")
    f.Add("dag:{a:[],b:[a]}")
    
    f.Fuzz(func(t *testing.T, dsl string) {
        plan, err := Compile(dsl, "fuzz-test")
        if err != nil {
            return // compilation error is ok
        }
        
        // Plan should always be executable
        event := createTestEvent()
        _, _ = Execute(plan, event, &noopCaller{})
    })
}
```

### Rust Fuzzing

```rust
#![no_main]
use libfuzzer_sys::fuzz_target;
use flowrulz::compiler::compile;

fuzz_target!(|data: &[u8]| {
    if let Ok(dsl) = std::str::from_utf8(data) {
        let _ = compile(dsl, "fuzz-test");
    }
});
```

## 9. Test Data Generation

### Go (go-faker)

```go
func createTestEvent() map[string]interface{} {
    return map[string]interface{}{
        "id": faker.UUIDDigit(),
        "topic": faker.Word(),
        "payload": map[string]interface{}{
            "user_id": faker.UUIDDigit(),
            "amount": faker.Amount(1, 10000),
            "email": faker.Email(),
        },
        "headers": map[string]string{
            "X-Trace-ID": faker.UUIDDigit(),
        },
        "mode": 0,
    }
}
```

## 10. CI/CD Test Pipeline

### GitHub Actions

```yaml
name: Tests
on: [push, pull_request]

jobs:
  go-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: '1.21'
      - run: cd server && go test -race ./...
      - run: cd server && golangci-lint run

  rust-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - run: cd runtime && cargo test
      - run: cd runtime && cargo clippy -- -D warnings

  e2e-tests:
    runs-on: ubuntu-latest
    needs: [go-tests, rust-tests]
    steps:
      - uses: actions/checkout@v4
      - run: docker-compose up -d
      - run: go test -race ./e2e/...
```

## Testing Gotchas

### 1. Race Detector Overhead
```
go test -race ./...
# Race detector adds 2-10x overhead
# Don't use in benchmarks — results are misleading
# Use in CI only, not in local development
```

### 2. Flaky Tests from Timing
```
# BAD — depends on timing
assert.True(t, time.Since(start) < 100*time.Millisecond)

# BETTER — use Eventually
assert.Eventually(t, func() bool {
    return status == "ready"
}, 5*time.Second, 100*time.Millisecond)
```

### 3. Test Data Isolation
```
# BAD — shared state between tests
var sharedCache = map[string]string{}

# BETTER — fresh state per test
func TestSomething(t *testing.T) {
    cache := newCache() // fresh instance
}
```

### 4. Mock Too Tightly Coupled
```
# BAD — mock implementation details
mock.On("internalMethod").Return(result)

# BETTER — mock behavior
mock.On("Process", Anything).Return(result)
```

### 5. Integration Test Order Dependency
```
# BAD — test B depends on test A
func TestA(t *testing.T) { createRecord() }
func TestB(t *testing.T) { /* assumes record exists */ }

# BETTER — independent tests
func TestB(t *testing.T) {
    createRecord() // create what you need
    // ... test
}
```

### 6. Benchmark Warmup
```
# BAD — first iteration includes JIT/compilation
b.RunParallel(func(pb *testing.PB) {
    for pb.Next() {
        execute(plan, event)
    }
})

# BETTER — warmup iteration
for i := 0; i < 1000; i++ {
    execute(plan, event)
}
b.ResetTimer()
b.RunParallel(func(pb *testing.PB) {
    for pb.Next() {
        execute(plan, event)
    }
})
```

### 7. Test Timeout
```
# BAD — no timeout, test hangs forever
func TestSlow(t *testing.T) {
    result := <-ch // blocks forever if ch never receives
}

# BETTER — use context timeout
func TestSlow(t *testing.T) {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    result := executeWithContext(ctx)
}
```

### 8. Parallel Test Isolation
```
# BAD — shared temp directory
func TestParallel(t *testing.T) {
    t.Parallel()
    os.MkdirAll("/tmp/test", 0755) // race on mkdir
}

# BETTER — unique temp directory
func TestParallel(t *testing.T) {
    t.Parallel()
    dir := t.TempDir() // unique per test
}
```
