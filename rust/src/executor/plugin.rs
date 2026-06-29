use std::collections::HashMap;
use std::sync::Mutex;

use once_cell::sync::Lazy;

static PLUGIN_REGISTRY: Lazy<Mutex<HashMap<String, Vec<u8>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

pub fn register(name: &str, wasm_bytes: &[u8]) {
    let mut reg = PLUGIN_REGISTRY.lock().unwrap();
    reg.insert(name.to_string(), wasm_bytes.to_vec());
}

pub fn call(name: &str, func_name: &str, json_args: &[u8]) -> Result<Vec<u8>, String> {
    let wasm_bytes = {
        let reg = PLUGIN_REGISTRY.lock().unwrap();
        reg.get(name)
            .ok_or_else(|| format!("plugin not found: {}", name))?
            .clone()
    };

    let mut config = wasmtime::Config::new();
    config.wasm_multi_value(true);
    config.wasm_component_model(false);
    config.consume_fuel(true);

    let engine = wasmtime::Engine::new(&config)
        .map_err(|e| format!("wasm engine: {}", e))?;

    let module = wasmtime::Module::new(&engine, &wasm_bytes)
        .map_err(|e| format!("wasm module: {}", e))?;

    let mut store = wasmtime::Store::new(&engine, ());
    store.set_fuel(100_000)
        .map_err(|e| format!("fuel: {}", e))?;

    let linker = wasmtime::Linker::new(&engine);
    let instance = linker
        .instantiate(&mut store, &module)
        .map_err(|e| format!("wasm instantiate: {}", e))?;

    let func = instance
        .get_func(&mut store, func_name)
        .ok_or_else(|| format!("plugin {}: func {} not found", name, func_name))?;

    let func_ty = func.ty(&store);
    let params: Vec<wasmtime::Val> = vec![wasmtime::Val::I64(
        json_args.len() as i64,
    )];

    let mut results = vec![wasmtime::Val::I64(0)];
    func.call(&mut store, &params, &mut results)
        .map_err(|e| format!("plugin {}: call {}: {}", name, func_name, e))?;

    let out_len = match results[0] {
        wasmtime::Val::I64(len) => len as usize,
        _ => 0,
    };

    let memory = instance
        .get_memory(&mut store, "memory")
        .ok_or("plugin has no memory export")?;

    let mut data = vec![0u8; out_len];
    memory
        .read(&store, 0, &mut data)
        .map_err(|e| format!("memory read: {}", e))?;

    Ok(data)
}

pub fn call_plugin(expr: &str, body: &[u8]) -> Result<Vec<u8>, String> {
    let stripped = expr
        .strip_prefix("w:")
        .ok_or_else(|| format!("not a wasm expression: {}", expr))?;

    let (plugin_name, func_name) = if let Some(dot) = stripped.find('.') {
        let pn = &stripped[..dot];
        let fn_rest = &stripped[dot + 1..];
        let fn_name = if let Some(paren) = fn_rest.find('(') {
            &fn_rest[..paren]
        } else {
            fn_rest
        };
        (pn.to_string(), fn_name.to_string())
    } else {
        let fn_name = if let Some(paren) = stripped.find('(') {
            &stripped[..paren]
        } else {
            stripped
        };
        ("_default".to_string(), fn_name.to_string())
    };

    call(&plugin_name, &func_name, body)
}
