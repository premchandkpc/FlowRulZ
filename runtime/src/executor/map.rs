use super::expr;
use super::plugin;
use crate::bytecode::instruction::Instruction;
use crate::bytecode::plan::ExecutionPlan;
use crate::bytecode::resolved_type::ResolvedType;

pub fn exec_map<'a>(
    body: &[u8],
    instr: &Instruction,
    plan: &ExecutionPlan,
    arena: &'a crate::memory::arena::Arena,
) -> Result<&'a mut [u8], String> {
    let expr_str = plan.const_pool.get(instr.a);

    if expr_str.starts_with("w:") {
        let result = plugin::call_plugin(expr_str, body)?;
        return Ok(arena.alloc_copy(&result));
    }

    if let Some(ref schema) = plan.schema {
        if let Some(field) = expr_str.strip_prefix('.') {
            let field_path = field.split('=').next().unwrap_or(field);
            if matches!(schema.field_type(field_path), Some(ResolvedType::Any)) {
                eprintln!("[warn] map operates on field '{}' typed 'any' — no compile-time type checking", field_path);
            }
        }
    }

    if expr_str.is_empty() {
        return Ok(arena.alloc_copy(body));
    }

    if expr_str.contains('=') {
        let result = expr::eval_map_expression(expr_str, body)?;
        return Ok(arena.alloc_copy(&result));
    }

    if let Some(stripped) = expr_str.strip_prefix('.') {
        extract_dot_path(stripped, body, arena)
    } else {
        Ok(arena.alloc_copy(body))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bytecode::{Schema, FieldSchema};

    fn arena() -> &'static crate::memory::arena::Arena {
        Box::leak(Box::new(crate::memory::arena::Arena::new()))
    }

    fn make_plan(expr_id: u16, expr: &str) -> ExecutionPlan {
        let mut plan = ExecutionPlan::new("test");
        while plan.const_pool.len() < expr_id as usize {
            plan.const_pool.add("");
        }
        plan.const_pool.add(expr);
        plan
    }

    #[test]
    fn test_exec_map_dot_path() {
        let plan = make_plan(0, ".x");
        let instr = Instruction::map(0);
        let result = exec_map(b"{\"x\":42}", &instr, &plan, &arena()).unwrap();
        assert_eq!(result, b"42");
    }

    #[test]
    fn test_exec_map_nested_dot_path() {
        let plan = make_plan(0, ".user.name");
        let instr = Instruction::map(0);
        let result = exec_map(b"{\"user\":{\"name\":\"alice\"}}", &instr, &plan, &arena()).unwrap();
        assert_eq!(result, b"\"alice\"");
    }

    #[test]
    fn test_exec_map_empty_expr_returns_body() {
        let plan = make_plan(0, "");
        let instr = Instruction::map(0);
        let result = exec_map(b"{\"x\":1}", &instr, &plan, &arena()).unwrap();
        assert_eq!(result, b"{\"x\":1}");
    }

    #[test]
    fn test_exec_map_assignment() {
        let plan = make_plan(0, "y=x");
        let instr = Instruction::map(0);
        let result = exec_map(b"{\"x\":42}", &instr, &plan, &arena()).unwrap();
        let val: serde_json::Value = serde_json::from_slice(result).unwrap();
        assert_eq!(val["y"], 42);
    }

    #[test]
    fn test_exec_map_non_existent_field() {
        let plan = make_plan(0, ".nonexistent");
        let instr = Instruction::map(0);
        // Should return "null" string from extract_dot_path
        let result = exec_map(b"{\"x\":1}", &instr, &plan, &arena()).unwrap();
        assert_eq!(result, b"null");
    }

    #[test]
    fn test_exec_map_dot_path_missing_object() {
        let plan = make_plan(0, ".x.y");
        let instr = Instruction::map(0);
        let result = exec_map(b"{\"x\":\"string\"}", &instr, &plan, &arena()).unwrap();
        assert_eq!(result, b"null");
    }

    #[test]
    fn test_exec_map_any_type_warning() {
        let mut plan = make_plan(0, ".x");
        plan.schema = Some(Schema {
            fields: vec![FieldSchema { name: "x".into(), r#type: ResolvedType::Any, required: false }],
        });
        let instr = Instruction::map(0);
        let result = exec_map(b"{\"x\":42}", &instr, &plan, &arena()).unwrap();
        assert_eq!(result, b"42");
    }

    #[test]
    fn test_exec_map_array_index_via_dot_path() {
        let plan = make_plan(0, ".arr.[]");
        let instr = Instruction::map(0);
        let result = exec_map(b"{\"arr\":[1,2,3]}", &instr, &plan, &arena()).unwrap();
        assert_eq!(result, b"1");
    }

    #[test]
    fn test_exec_map_wildcard_object() {
        let plan = make_plan(0, ".obj.*");
        let instr = Instruction::map(0);
        let result = exec_map(b"{\"obj\":{\"a\":1,\"b\":2}}", &instr, &plan, &arena()).unwrap();
        let s = std::str::from_utf8(result).unwrap();
        assert!(s.starts_with('['));
        assert!(s.contains('1'));
        assert!(s.contains('2'));
    }

    #[test]
    fn test_exec_map_wildcard_array() {
        let plan = make_plan(0, ".arr.*");
        let instr = Instruction::map(0);
        let result = exec_map(b"{\"arr\":[10,20]}", &instr, &plan, &arena()).unwrap();
        let s = std::str::from_utf8(result).unwrap();
        assert!(s.starts_with('['));
        assert!(s.contains("10"));
        assert!(s.contains("20"));
    }

    #[test]
    fn test_exec_map_invalid_json_error() {
        let plan = make_plan(0, ".x");
        let instr = Instruction::map(0);
        let result = exec_map(b"not-json", &instr, &plan, &arena());
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("json"));
    }

    #[test]
    fn test_exec_map_invalid_utf8() {
        let plan = make_plan(0, ".x");
        let instr = Instruction::map(0);
        // Invalid UTF-8 should trigger the invalid utf8 error when trying to parse
        // But first it tries the plugin path, then the schema path, then empty, then contains '='
        // then strip_prefix '.' which it does, then it tries from_utf8
        let result = exec_map(b"\xff\xff", &instr, &plan, &arena());
        assert!(result.is_err());
    }
}

fn extract_dot_path<'a>(
    stripped: &str,
    body: &[u8],
    arena: &'a crate::memory::arena::Arena,
) -> Result<&'a mut [u8], String> {
    let parts: Vec<&str> = stripped.split('.').collect();
    let body_str = std::str::from_utf8(body).map_err(|e| format!("invalid utf8: {}", e))?;
    let mut current: serde_json::Value =
        serde_json::from_str(body_str).map_err(|e| format!("invalid json: {}", e))?;

    for part in &parts {
        if *part == "[]" {
            match current {
                serde_json::Value::Array(ref arr) => {
                    if let Some(first) = arr.first() {
                        let s = first.to_string();
                        return Ok(arena.alloc_copy(s.as_bytes()));
                    }
                    return Ok(arena.alloc_copy(b"null"));
                }
                _ => return Ok(arena.alloc_copy(b"null")),
            }
        } else if *part == "*" {
            match current {
                serde_json::Value::Object(ref map) => {
                    let vals: Vec<String> = map.values().map(|v| v.to_string()).collect();
                    let result = format!("[{}]", vals.join(","));
                    return Ok(arena.alloc_copy(result.as_bytes()));
                }
                serde_json::Value::Array(ref arr) => {
                    let vals: Vec<String> = arr.iter().map(|v| v.to_string()).collect();
                    let result = format!("[{}]", vals.join(","));
                    return Ok(arena.alloc_copy(result.as_bytes()));
                }
                _ => return Ok(arena.alloc_copy(b"null")),
            }
        } else {
            match current {
                serde_json::Value::Object(ref map) => {
                    current = map
                        .get(*part)
                        .cloned()
                        .unwrap_or(serde_json::Value::Null);
                }
                _ => return Ok(arena.alloc_copy(b"null")),
            }
        }
    }

    let result = current.to_string();
    Ok(arena.alloc_copy(result.as_bytes()))
}
