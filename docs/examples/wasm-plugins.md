# WASM Plugins

Custom logic via WebAssembly — extensible processing without modifying the VM.

## Bytecode DSL

### Basic WASM Call

```
w:my-plugin.process
```

Call the `process` function from the `my-plugin` WASM module.

### WASM in Pipeline

```
n:validate | w:custom.risk-score | g:result>0.8 f:dlq | n:process
```

Validate, run custom risk scoring via WASM, gate on the score, then process or fallback.

### WASM + Map

```
w:transform.normalize | m:{score: @.risk_score, source: "wasm"} | n:store
```

Run WASM transformation, wrap the result, then store.

## How It Works

1. WASM modules are loaded from the plugins directory
2. The VM calls the specified function with the current payload
3. The function returns a new payload
4. Execution continues with the returned payload

### Plugin Loading

```rust
// runtime/src/executor/plugin/loader.rs
pub struct PluginLoader {
    plugins_dir: PathBuf,
    loaded: HashMap<String, Plugin>,
}
```

Plugins are loaded on first access and cached.

### Plugin Interface

```rust
pub trait Plugin {
    fn call(&self, function: &str, input: &[u8]) -> Result<Vec<u8>, PluginError>;
}
```

## Use Cases

| Pattern | Example |
|---------|---------|
| Custom scoring | Risk assessment, fraud detection |
| Data transformation | Complex business logic not expressible in JMESPath |
| External integrations | Call native libraries from WASM |
| A/B testing | Different logic paths via plugin selection |
| Compliance rules | Regulatory checks as pluggable modules |

## Example: Risk Scoring Plugin

```rust
// risk-score plugin (Rust)
#[no_mangle]
pub fn process(input: *const u8, input_len: usize) -> *mut u8 {
    let data = unsafe { slice::from_raw_parts(input, input_len) };
    let payload: Value = serde_json::from_slice(data).unwrap();
    
    let score = calculate_risk(&payload);
    let result = json!({ "risk_score": score });
    
    // Return allocated memory
    let output = serde_json::to_vec(&result).unwrap();
    let ptr = output.as_mut_ptr();
    std::mem::forget(output);
    ptr
}
```

### DSL Usage

```
w:risk-score.process g:result.risk_score>0.8 f:manual-review n:auto-approve
```

## Plugin Management

| Operation | Description |
|-----------|-------------|
| Load | Plugin loaded on first `w:` reference |
| Cache | Loaded plugins cached in memory |
| Isolation | Each plugin runs in its own WASM instance |
| Memory | Plugin memory is bounded and managed by the WASM runtime |

## Limitations

- WASM plugins cannot make network calls (sandboxed)
- Plugin state is not persisted between invocations
- Plugins receive and return byte arrays (JSON serialization required)
- Plugin compilation is outside FlowRulZ scope (provide pre-compiled `.wasm` files)
