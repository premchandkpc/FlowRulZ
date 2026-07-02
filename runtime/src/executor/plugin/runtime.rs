use wasmtime::{Linker, Store, Val};

use super::loader::get_or_compile;

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
