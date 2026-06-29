use std::collections::HashMap;
use std::sync::Mutex;

use once_cell::sync::Lazy;
use wasmtime::{Engine, Linker, Module, Store, Val};

static PLUGIN_BYTES: Lazy<Mutex<HashMap<String, Vec<u8>>>> =
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

fn get_or_compile(name: &str) -> Result<(Engine, Module), String> {
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

pub fn call(name: &str, func_name: &str, input: &[u8]) -> Result<Vec<u8>, String> {
    let (engine, module) = get_or_compile(name)?;

    let mut store = Store::new(&engine, ());
    store
        .set_fuel(100_000)
        .map_err(|e| format!("fuel: {}", e))?;

    let linker = Linker::new(&engine);
    let instance = linker
        .instantiate(&mut store, &module)
        .map_err(|e| format!("instantiate '{}': {}", name, e))?;

    let memory = instance
        .get_memory(&mut store, "memory")
        .ok_or_else(|| format!("plugin '{}': no exported memory", name))?;

    let mem_size = memory.data_size(&store);
    let input_offset = mem_size
        .checked_sub(input.len())
        .unwrap_or(0);

    if mem_size < input_offset + input.len() {
        let needed = input_offset + input.len() - mem_size;
        let pages = (needed + 65535) / 65536;
        memory
            .grow(&mut store, pages as u64)
            .map_err(|e| format!("grow memory: {}", e))?;
    }

    memory
        .write(&mut store, input_offset, input)
        .map_err(|e| format!("write input at {}: {}", input_offset, e))?;

    let func = instance
        .get_func(&mut store, func_name)
        .ok_or_else(|| format!("plugin '{}': func '{}' not found", name, func_name))?;

    let func_ty = func.ty(&store);
    let expected_params = func_ty.params().len();
    if expected_params != 2 {
        return Err(format!(
            "plugin '{}': '{}' expects {} params (need 2: ptr, len)",
            name, func_name, expected_params
        ));
    }

    let params = [
        Val::I32(input_offset as i32),
        Val::I32(input.len() as i32),
    ];
    let mut results = [Val::I64(0)];
    func.call(&mut store, &params, &mut results)
        .map_err(|e| format!("plugin '{}': call '{}': {}", name, func_name, e))?;

    let ret = match results[0] {
        Val::I64(v) => v as u64,
        _ => return Err(format!("plugin '{}': '{}' returned non-i64", name, func_name)),
    };

    let out_ptr = (ret >> 32) as u32;
    let out_len = (ret & 0xFFFFFFFF) as u32;

    if out_len == 0 {
        return Ok(Vec::new());
    }

    let mut output = vec![0u8; out_len as usize];
    memory
        .read(&store, out_ptr as usize, &mut output)
        .map_err(|e| format!("read output at {}: {}", out_ptr, e))?;

    Ok(output)
}

pub fn call_plugin(expr: &str, body: &[u8]) -> Result<Vec<u8>, String> {
    let stripped = expr
        .strip_prefix("w:")
        .ok_or_else(|| format!("not a wasm expression: {}", expr))?;

    let (plugin_name, raw_func) = if let Some(dot) = stripped.find('.') {
        let pn = &stripped[..dot];
        let rest = &stripped[dot + 1..];
        (pn.to_string(), rest.to_string())
    } else {
        ("_default".to_string(), stripped.to_string())
    };

    let func_name = raw_func
        .trim_end_matches(')')
        .trim_end_matches('(')
        .to_string();

    call(&plugin_name, &func_name, body)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pass_through_wasm() -> Vec<u8> {
        wat::parse_str(
            r#"
(module
  (memory (export "memory") 1)
  (func (export "process") (param $ptr i32) (param $len i32) (result i64)
    (i64.or
      (i64.shl (i64.extend_i32_u (local.get $ptr)) (i64.const 32))
      (i64.extend_i32_u (local.get $len))
    )
  )
)
"#,
        )
        .expect("wat parse failed")
    }

    fn echo_json_wasm() -> Vec<u8> {
        wat::parse_str(
            r#"
(module
  (memory (export "memory") 1)
  (func (export "echo_json") (param $ptr i32) (param $len i32) (result i64)
    (i64.or
      (i64.shl (i64.extend_i32_u (local.get $ptr)) (i64.const 32))
      (i64.extend_i32_u (local.get $len))
    )
  )
)
"#,
        )
        .expect("wat parse failed")
    }

    #[test]
    fn test_parse_expr_w_dot() {
        let result = call_plugin("w:myplugin.process", b"{}");
        assert!(result.is_err(), "expected error for unregistered plugin");
        let err = result.unwrap_err();
        assert!(
            err.contains("not registered"),
            "expected 'not registered' error, got: {}",
            err
        );
    }

    #[test]
    fn test_parse_expr_w_dot_with_parens() {
        let result = call_plugin("w:myplugin.process()", b"{}");
        assert!(result.is_err(), "expected error for unregistered plugin");
        let err = result.unwrap_err();
        assert!(
            err.contains("not registered"),
            "expected 'not registered' error, got: {}",
            err
        );
    }

    #[test]
    fn test_register_and_call_missing_func() {
        register("test_plugin", b"\x00asm\x01\x00\x00\x00");
        let result = call("test_plugin", "nonexistent", b"{}");
        assert!(result.is_err(), "expected error for invalid wasm bytes");
    }

    #[test]
    fn test_register_duplicate_overwrites() {
        register("dup", b"old bytes");
        register("dup", b"new bytes");
        assert!(PLUGIN_BYTES.lock().unwrap().get("dup").unwrap().len() > 0);
    }

    #[test]
    fn test_call_pass_through_plugin() {
        let wasm = pass_through_wasm();
        register("passthru", &wasm);
        let input = br#"{"hello":"world"}"#;
        let result = call("passthru", "process", input).unwrap();
        assert_eq!(result, input);
    }

    #[test]
    fn test_call_plugin_via_call_plugin() {
        let wasm = echo_json_wasm();
        register("echo", &wasm);
        let input = br#"{"x":42}"#;
        let result = call_plugin("w:echo.echo_json", input).unwrap();
        assert_eq!(result, input);
    }

    #[test]
    fn test_plugin_integration_through_vm() {
        let wasm = pass_through_wasm();
        register("sig", &wasm);

        let plan = crate::dsl::compiler::Compiler::new(&[])
            .compile(
                &crate::dsl::optimizer::Optimizer::new().optimize(
                    &crate::dsl::parser::parse(
                        &crate::dsl::lexer::lex("w:sig.process n:svc").unwrap(),
                    )
                    .unwrap(),
                ),
                "test",
            )
            .unwrap();

        let arena = crate::memory::arena::Arena::new();
        let ctx = crate::bytecode::execution::ExecutionContext::from_body(br#"{"msg":"hi"}"#.to_vec());
        let mut vm = crate::executor::VM::new(
            &plan,
            ctx,
            arena,
            &|_svc_id: u16, body: &[u8], _timeout: u64| Ok(body.to_vec()),
        );
        vm.run().unwrap();
        assert_eq!(vm.ctx.hop_count, 1);
        let output: serde_json::Value =
            serde_json::from_slice(&vm.ctx.body).unwrap();
        assert_eq!(output["msg"], "hi");
    }
}
