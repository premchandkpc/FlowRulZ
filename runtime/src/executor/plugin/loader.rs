use std::collections::HashMap;
use std::sync::Mutex;

use once_cell::sync::Lazy;
use wasmtime::{Engine, Module};

pub static PLUGIN_BYTES: Lazy<Mutex<HashMap<String, Vec<u8>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

struct CachedModule {
    engine: Engine,
    module: Module,
}

static MODULE_CACHE: Lazy<Mutex<HashMap<String, CachedModule>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

pub fn register(name: &str, wasm_bytes: &[u8]) {
    let mut reg = PLUGIN_BYTES.lock().unwrap();
    reg.insert(name.to_string(), wasm_bytes.to_vec());
}

pub fn get_or_compile(name: &str) -> Result<(Engine, Module), String> {
    {
        let cache = MODULE_CACHE.lock().unwrap();
        if let Some(entry) = cache.get(name) {
            return Ok((entry.engine.clone(), entry.module.clone()));
        }
    }

    let wasm_bytes = {
        let reg = PLUGIN_BYTES.lock().unwrap();
        reg.get(name)
            .ok_or_else(|| format!("plugin '{}' not registered", name))?
            .clone()
    };

    let mut config = wasmtime::Config::new();
    config.consume_fuel(true);

    let engine =
        Engine::new(&config).map_err(|e| format!("wasm engine for '{}': {}", name, e))?;

    let module = Module::new(&engine, &wasm_bytes)
        .map_err(|e| format!("wasm module for '{}': {}", name, e))?;

    {
        let mut cache = MODULE_CACHE.lock().unwrap();
        cache.insert(
            name.to_string(),
            CachedModule {
                engine: engine.clone(),
                module: module.clone(),
            },
        );
    }

    Ok((engine, module))
}
