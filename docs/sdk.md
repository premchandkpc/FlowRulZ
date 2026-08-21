# SDK Reference

Client libraries for Go, Rust, Python, Java, and JavaScript.

## Common Interface

All SDKs provide:

```go
// Core methods
Publish(topic, payload)           // fire-and-forget
Request(service, payload, timeout) // request/reply
Execute(ruleID, payload, opts)    // workflow execution
Stream(topic, handler)            // streaming
```

### Event Structure

```json
{
  "id": "optional-id",
  "topic": "service-or-topic-name",
  "payload": { "key": "value" },
  "headers": { "X-Custom": "value" },
  "mode": 0
}
```

### Modes

| Mode | Value | Description |
|------|-------|-------------|
| Publish | 0 | Fire-and-forget |
| Request | 1 | Request/reply |
| Reply | 2 | Reply to request |
| Stream | 3 | Streaming |
| Workflow | 4 | Workflow execution |
| Internal | 5 | Internal cluster |

## Go SDK

### Installation

```go
import "github.com/premchandkpc/FlowRulZ/sdk/flow"
```

### Usage

```go
client := flow.NewClient(flow.Config{
    Address: "http://localhost:8080",
    APIKey:  optional("my-api-key"),
})

// Publish
err := client.Publish("orders", map[string]interface{}{
    "order_id": "123",
    "amount":   99.99,
})

// Request
resp, err := client.Request("inventory", map[string]interface{}{
    "item_id": "widget-42",
}, nil)

// Execute
result, err := client.Execute("order-processing", map[string]interface{}{
    "order_id": "123",
    "items":    []string{"a", "b"},
}, nil)

// Stream
err := client.Stream("events", func(data []byte) {
    fmt.Printf("received: %s\n", data)
})
```

### Configuration

```go
type Config struct {
    Address string        // default: http://localhost:8080
    APIKey  *string       // optional API key
    Timeout time.Duration // default: 30s
}
```

## Rust SDK

### Installation

```toml
[dependencies]
flowrulz-sdk = { path = "../sdk/rust" }
```

### Usage

```rust
use flowrulz_sdk::{FlowRulZClient, Config, MODE_PUBLISH};

let client = FlowRulZClient::new(Config::default());

// Publish
client.publish("orders", serde_json::json!({
    "order_id": "123",
    "amount": 99.99
})).await?;

// Request
let resp = client.request("inventory", serde_json::json!({
    "item_id": "widget-42"
}), None).await?;

// Execute
let result = client.execute("order-processing", serde_json::json!({
    "order_id": "123"
}), None).await?;
```

### Error Handling

```rust
#[derive(Debug)]
pub enum SdkError {
    Serialize(serde_json::Error),
    Http(reqwest::Error),
}
```

All methods return `Result<_, SdkError>` — serialization errors propagate instead of panicking.

### Configuration

```rust
pub struct Config {
    pub address: String,        // default: http://localhost:8080
    pub api_key: Option<String>,
    pub timeout: Option<Duration>,  // default: 30s
}
```

## Python SDK

### Installation

```bash
pip install flowrulz
```

### Usage

```python
from flowrulz import FlowRulZClient

client = FlowRulZClient("http://localhost:8080")

# Publish
await client.publish("orders", {
    "order_id": "123",
    "amount": 99.99
})

# Request
resp = await client.request("inventory", {
    "item_id": "widget-42"
})

# Execute
result = await client.execute("order-processing", {
    "order_id": "123"
})
```

## Java SDK

### Installation

```xml
<dependency>
    <groupId>com.flowrulz</groupId>
    <artifactId>flowrulz-sdk</artifactId>
    <version>0.1.0</version>
</dependency>
```

### Usage

```java
FlowRulZClient client = new FlowRulZClient("http://localhost:8080");

// Publish
client.publish("orders", Map.of("order_id", "123", "amount", 99.99));

// Request
byte[] resp = client.request("inventory", Map.of("item_id", "widget-42"), null);

// Execute
byte[] result = client.execute("order-processing", Map.of("order_id", "123"), null);
```

## JavaScript/TypeScript SDK

### Installation

```bash
npm install @flowrulz/sdk
```

### Usage

```typescript
import { FlowRulZClient } from '@flowrulz/sdk';

const client = new FlowRulZClient('http://localhost:8080');

// Publish
await client.publish('orders', {
    order_id: '123',
    amount: 99.99
});

// Request
const resp = await client.request('inventory', {
    item_id: 'widget-42'
});

// Execute
const result = await client.execute('order-processing', {
    order_id: '123'
});
```

## Authentication

All SDKs support API key authentication via the `Authorization` header:

```
Authorization: Bearer <api-key>
```

### Go

```go
client := flow.NewClient(flow.Config{
    Address: "http://localhost:8080",
    APIKey:  strPtr("my-api-key"),
})
```

### Rust

```rust
let client = FlowRulZClient::new(Config {
    api_key: Some("my-api-key".into()),
    ..Default::default()
});
```

## Error Handling

| SDK | Error Type | Behavior |
|-----|-----------|----------|
| Go | `error` | Returns error, no panic |
| Rust | `SdkError` | Returns Result, no panic |
| Python | `Exception` | Raises on failure |
| Java | `FlowRulZException` | Throws on failure |
| JavaScript | `Error` | Rejects promise on failure |

## Timeouts

| SDK | Default | Configurable |
|-----|---------|-------------|
| Go | 30s | Yes, per-request |
| Rust | 30s | Yes, per-request |
| Python | 30s | Yes, per-request |
| Java | 30s | Yes, per-request |
| JavaScript | 30s | Yes, per-request |

## Testing

All SDKs include test utilities:

### Go

```go
func TestPublish(t *testing.T) {
    mock := httptest.NewServer(...)
    client := flow.NewClient(flow.Config{Address: mock.URL})
    // ...
}
```

### Rust

```rust
#[tokio::test]
async fn test_publish() {
    let mock = MockServer::start().await;
    let client = FlowRulZClient::new(Config {
        address: mock.uri(),
        ..Default::default()
    });
    // ...
}
```
