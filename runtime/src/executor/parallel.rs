use std::sync::Mutex;

use crate::bytecode::instruction::Instruction;
use crate::bytecode::plan::ExecutionPlan;

/// Service IDs to call in parallel.
struct ParallelSvc {
    svc_id: u16,
}

#[allow(clippy::type_complexity)]
pub fn exec_parallel<'a>(
    body: &[u8],
    instr: &Instruction,
    plan: &ExecutionPlan,
    caller: &(dyn Fn(u16, &[u8], u64) -> Result<Vec<u8>, String> + Sync),
    arena: &'a crate::memory::arena::Arena,
) -> Result<&'a mut [u8], String> {
    let count = instr.a as usize;
    let first_svc = instr.b as usize;

    let svcs: Vec<ParallelSvc> = (0..count)
        .map(|offset| ParallelSvc {
            svc_id: plan.services.entries()[first_svc + offset].id,
        })
        .collect();

    let parts_mtx: Mutex<Vec<Option<Vec<u8>>>> = Mutex::new((0..count).map(|_| None).collect());
    let err_mtx: Mutex<Option<String>> = Mutex::new(None);
    let parts_ref = &parts_mtx;
    let err_ref = &err_mtx;

    std::thread::scope(|s| {
        for (i, svc) in svcs.iter().enumerate() {
            let svc_id = svc.svc_id;
            s.spawn(move || {
                if err_ref.lock().unwrap().is_some() {
                    return;
                }
                match caller(svc_id, body, 0) {
                    Ok(resp) => {
                        parts_ref.lock().unwrap()[i] = Some(resp);
                    }
                    Err(e) => {
                        *err_ref.lock().unwrap() = Some(e);
                    }
                }
            });
        }
    });

    // Check for errors
    if let Some(err) = err_mtx.into_inner().unwrap() {
        return Err(err);
    }

    let parts_vec = parts_mtx.into_inner().unwrap();
    let arr: Vec<serde_json::Value> = parts_vec
        .into_iter()
        .map(|opt| {
            let p = opt.unwrap_or_default();
            serde_json::from_slice(&p).unwrap_or(serde_json::Value::String(String::from_utf8_lossy(&p).to_string()))
        })
        .collect();
    let parallel_val = serde_json::Value::Array(arr);

    let mut merged: serde_json::Value = serde_json::from_slice(body)
        .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));
    if let serde_json::Value::Object(ref mut map) = merged {
        map.insert("_parallel".to_string(), parallel_val);
    } else {
        let mut map = serde_json::Map::new();
        map.insert("_parallel".to_string(), parallel_val);
        merged = serde_json::Value::Object(map);
    }

    let out = serde_json::to_vec(&merged).unwrap_or_default();
    Ok(arena.alloc_copy(&out))
}

pub fn exec_collect<'a>(
    body: &[u8],
    _plan: &ExecutionPlan,
    arena: &'a crate::memory::arena::Arena,
) -> Result<&'a mut [u8], String> {
    let mut val: serde_json::Value = serde_json::from_slice(body)
        .map_err(|e| format!("Collect: failed to parse body: {}", e))?;

    let parallel = match val.as_object_mut().and_then(|m| m.remove("_parallel")) {
        Some(v) => v,
        None => return Err("Collect: no _parallel key in body".into()),
    };

    let out = serde_json::to_vec(&parallel).unwrap_or_default();
    Ok(arena.alloc_copy(&out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bytecode::plan::ExecutionPlan;
    use crate::dsl::{compiler::Compiler, lexer, optimizer, parser};

    fn compile_dsl(dsl: &str) -> ExecutionPlan {
        let tokens = lexer::lex(dsl).unwrap();
        let pipeline = parser::parse(&tokens).unwrap();
        let opt = optimizer::Optimizer::new();
        let optimized = opt.optimize(&pipeline);
        let compiler = Compiler::new();
        compiler.compile(&optimized, "test").unwrap()
    }

    fn mock_caller(svc_id: u16, _body: &[u8], _timeout: u64) -> Result<Vec<u8>, String> {
        Ok(serde_json::to_vec(&serde_json::json!({"result": svc_id})).unwrap())
    }

    #[test]
    fn test_parallel_stores_under_parallel_key() {
        let plan = compile_dsl("p:a,b");
        let arena = crate::memory::arena::Arena::new();
        let body = b"{\"order_id\":123,\"type\":\"test\"}";
        let instr = &plan.instructions[0];
        let result = exec_parallel(body, instr, &plan, &mock_caller, &arena).unwrap();
        let val: serde_json::Value = serde_json::from_slice(result).unwrap();
        // Original fields preserved
        assert_eq!(val["order_id"], 123);
        assert_eq!(val["type"], "test");
        // _parallel key present with array of results
        assert!(val.get("_parallel").is_some());
        assert!(val["_parallel"].is_array());
        assert_eq!(val["_parallel"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_collect_extracts_parallel_key() {
        let plan = compile_dsl("p:a,b c");
        let arena = crate::memory::arena::Arena::new();
        let body = b"{\"order_id\":123}";
        let instr = &plan.instructions[0];
        let after_parallel = exec_parallel(body, instr, &plan, &mock_caller, &arena).unwrap();

        // Collect extracts _parallel array
        let result = exec_collect(after_parallel, &plan, &arena).unwrap();
        let val: serde_json::Value = serde_json::from_slice(result).unwrap();
        assert!(val.is_array());
        assert_eq!(val.as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_collect_missing_key_errors() {
        let plan = compile_dsl("n:svc");
        let arena = crate::memory::arena::Arena::new();
        let body = b"{\"no_parallel\":true}";
        let err = exec_collect(body, &plan, &arena).unwrap_err();
        assert!(err.contains("_parallel"));
    }

    #[test]
    fn test_parallel_non_object_body_wraps() {
        let plan = compile_dsl("p:a,b");
        let arena = crate::memory::arena::Arena::new();
        let body = b"\"just_a_string\"";
        let instr = &plan.instructions[0];
        let result = exec_parallel(body, instr, &plan, &mock_caller, &arena).unwrap();
        let val: serde_json::Value = serde_json::from_slice(result).unwrap();
        // Non-object body gets wrapped in object with _parallel
        assert!(val.is_object());
        assert!(val.get("_parallel").is_some());
    }
}
