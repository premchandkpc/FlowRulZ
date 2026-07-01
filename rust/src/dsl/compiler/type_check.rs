use super::error::CompileError;
use crate::bytecode::resolved_type::{ResolvedType, Schema};

pub fn type_check_gate(
    schema: &Schema,
    field: &str,
    op: &str,
    _value: &str,
) -> Result<(), CompileError> {
    let field_type = match schema.field_type(field) {
        Some(t) => t,
        None => return Ok(()),
    };
    match op {
        "==" | "!=" => Ok(()),
        ">" | "<" | ">=" | "<=" => {
            if !field_type.supports_ordering() {
                Err(CompileError::TypeMismatch(format!(
                    "operator '{op}' requires numeric or string type, but field '{field}' has type {ft:?}",
                    op = op, field = field, ft = field_type
                )))
            } else {
                Ok(())
            }
        }
        "contains" => {
            if !field_type.supports_contains() {
                Err(CompileError::TypeMismatch(format!(
                    "operator 'contains' requires string or array type, but field '{field}' has type {ft:?}",
                    field = field, ft = field_type
                )))
            } else {
                Ok(())
            }
        }
        _ => Ok(()),
    }
}

pub fn type_check_map(schema: &Schema, expr: &str) -> Result<(), CompileError> {
    let rhs = if let Some(eq_pos) = expr.find('=') {
        expr[eq_pos + 1..].trim()
    } else {
        expr.trim()
    };
    if rhs.is_empty() {
        return Ok(());
    }

    if rhs.starts_with("concat(") && rhs.ends_with(')') {
        let args_str = &rhs[7..rhs.len() - 1];
        for arg in args_str.split(',') {
            let arg = arg.trim();
            if arg.starts_with('.') {
                let field_name = &arg[1..];
                if let Some(ft) = schema.field_type(field_name) {
                    if !matches!(ft, ResolvedType::String) {
                        return Err(CompileError::TypeMismatch(format!(
                            "concat() requires string type, but field '{field_name}' has type {ft:?}",
                            field_name = field_name, ft = ft
                        )));
                    }
                }
            }
        }
    }

    if rhs.contains('+') {
        let parts: Vec<&str> = rhs.split('+').collect();
        for part in &parts {
            let part = part.trim();
            if part.starts_with('.') {
                let field_name = &part[1..];
                if let Some(ft) = schema.field_type(field_name) {
                    if !matches!(ft, ResolvedType::String) {
                        return Err(CompileError::TypeMismatch(format!(
                            "concat '+' requires string type, but field '{field_name}' has type {ft:?}",
                            field_name = field_name, ft = ft
                        )));
                    }
                }
            }
        }
    }
    Ok(())
}
