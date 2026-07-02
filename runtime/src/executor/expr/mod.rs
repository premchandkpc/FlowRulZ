pub mod arithmetic;
pub mod collection;
pub mod conversion;
mod string;
pub mod utility;

use serde_json::Value;

#[derive(Debug, Clone)]
enum Expr {
    Field(String),
    FnCall { name: String, args: Vec<Expr> },
    Concat(Vec<Expr>),
    Literal(String),
    Number(f64),
    Raw(String),
}

fn parse_expr(input: &str) -> Result<Expr, String> {
    let input = input.trim();

    if input.starts_with('"') && input.ends_with('"') {
        return Ok(Expr::Literal(input[1..input.len() - 1].to_string()));
    }
    if input.starts_with('\'') && input.ends_with('\'') {
        return Ok(Expr::Literal(input[1..input.len() - 1].to_string()));
    }
    if let Ok(n) = input.parse::<f64>() {
        return Ok(Expr::Number(n));
    }

    if let Some(open) = input.find('(') {
        if input.ends_with(')') {
            let name = input[..open].trim().to_string();
            let args_str = &input[open + 1..input.len() - 1];
            let args = if args_str.trim().is_empty() {
                Vec::new()
            } else {
                parse_args(args_str)?
            };
            return Ok(Expr::FnCall { name, args });
        }
    }

    if input.contains('+') && !input.starts_with('+') {
        let parts: Vec<&str> = input.split('+').collect();
        if parts.iter().all(|p| !p.contains('(') && !p.contains(')')) {
            let mut exprs = Vec::new();
            for part in parts {
                exprs.push(parse_expr(part.trim())?);
            }
            return Ok(Expr::Concat(exprs));
        }
    }

    if is_valid_field(input) {
        return Ok(Expr::Field(input.to_string()));
    }

    Ok(Expr::Raw(input.to_string()))
}

fn parse_args(input: &str) -> Result<Vec<Expr>, String> {
    let mut args = Vec::new();
    let mut depth = 0;
    let mut start = 0;
    let mut in_quote = false;
    let mut quote_char = 0u8;
    let bytes = input.as_bytes();
    let len = input.len();

    for (i, &c) in bytes.iter().enumerate().take(len) {
        if in_quote {
            if c == quote_char {
                in_quote = false;
            }
            if i == len - 1 {
                let part = input[start..].trim();
                if !part.is_empty() {
                    args.push(parse_expr(part)?);
                }
            }
            continue;
        }
        if c == b'\'' || c == b'"' {
            in_quote = true;
            quote_char = c;
            continue;
        }
        if c == b'(' {
            depth += 1;
        }
        if c == b')' {
            depth -= 1;
        }
        if (c == b',' && depth == 0) || i == len - 1 {
            let end = if i == len - 1 { i + 1 } else { i };
            let part = input[start..end].trim();
            if !part.is_empty() {
                args.push(parse_expr(part)?);
            }
            start = i + 1;
        }
    }
    Ok(args)
}

fn is_valid_field(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.')
}

fn eval_expr(expr: &Expr, body: &Value) -> Result<Value, String> {
    match expr {
        Expr::Field(path) => resolve_field(body, path),
        Expr::FnCall { name, args } => {
            let evaluated: Result<Vec<Value>, String> =
                args.iter().map(|a| eval_expr(a, body)).collect();
            let evaluated = evaluated?;
            call_builtin(name, &evaluated)
        }
        Expr::Concat(parts) => {
            let mut result = String::new();
            for part in parts {
                let val = eval_expr(part, body)?;
                result.push_str(&value_to_string(&val));
            }
            Ok(Value::String(result))
        }
        Expr::Literal(s) => Ok(Value::String(s.clone())),
        Expr::Number(n) => Ok(Value::Number(
            serde_json::Number::from_f64(*n).unwrap_or(serde_json::Number::from(0)),
        )),
        Expr::Raw(s) => Ok(Value::String(s.clone())),
    }
}

fn call_builtin(name: &str, args: &[Value]) -> Result<Value, String> {
    match name {
        "abs" | "round" | "ceil" | "floor" | "min" | "max" => arithmetic::call(name, args),
        "lower" | "upper" | "trim" | "length" | "concat" | "substring" | "replace" | "split"
        | "base64" | "base64_decode" => string::call(name, args),
        "to_string" | "parse_int" | "parse_float" | "parse_bool" | "json" => {
            conversion::call(name, args)
        }
        "contains" | "keys" | "merge" => collection::call(name, args),
        "uuid" | "now" | "epoch" | "coalesce" | "default" | "hash" | "typeof" => {
            utility::call(name, args)
        }
        _ => Err(format!("unknown function: {}", name)),
    }
}

fn value_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => "null".to_string(),
        _ => v.to_string(),
    }
}

fn arg_as_str(args: &[Value], i: usize) -> String {
    args.get(i).map(value_to_string).unwrap_or_default()
}

fn arg_as_f64(args: &[Value], i: usize) -> Option<f64> {
    args.get(i).and_then(|v| match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    })
}

fn resolve_field(body: &Value, path: &str) -> Result<Value, String> {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = body.clone();
    for part in parts {
        match current {
            Value::Object(ref map) => {
                if let Some(val) = map.get(part) {
                    current = val.clone();
                } else {
                    return Err(format!(
                        "FieldNotFound: path segment '{}' not found in '{}'",
                        part, path
                    ));
                }
            }
            _ => {
                return Err(format!(
                    "FieldNotFound: cannot navigate into non-object at path '{}' segment '{}'",
                    path, part
                ));
            }
        }
    }
    Ok(current)
}

fn set_field(
    body: &mut Value,
    path: &str,
    value: Value,
) -> Result<(), String> {
    let parts: Vec<&str> = path.split('.').collect();
    if parts.is_empty() {
        return Err("empty path".to_string());
    }
    if parts.len() == 1 {
        if let Value::Object(ref mut map) = body {
            map.insert(parts[0].to_string(), value);
            return Ok(());
        }
        return Err("root is not an object".to_string());
    }
    let mut current = body;
    for part in parts.iter().take(parts.len() - 1) {
        match current {
            Value::Object(ref mut map) => {
                current = map
                    .entry(part.to_string())
                    .or_insert(Value::Object(serde_json::Map::new()));
            }
            _ => {
                return Err(format!(
                    "cannot set field in non-object at {}",
                    part
                ))
            }
        }
    }
    if let Value::Object(ref mut map) = current {
        map.insert(parts[parts.len() - 1].to_string(), value);
        Ok(())
    } else {
        Err("target is not an object".to_string())
    }
}

pub fn eval_map_expression(expr_str: &str, body: &[u8]) -> Result<Vec<u8>, String> {
    let eq_pos = expr_str
        .find('=')
        .ok_or_else(|| "not an assignment expression (missing =)".to_string())?;

    let dest = expr_str[..eq_pos].trim();
    let source_expr = expr_str[eq_pos + 1..].trim();

    if dest.is_empty() || source_expr.is_empty() {
        return Err("empty target or source in map expression".to_string());
    }

    let body_str =
        std::str::from_utf8(body).map_err(|e| format!("invalid utf8: {}", e))?;
    let mut body_json: Value =
        serde_json::from_str(body_str).map_err(|e| format!("invalid json: {}", e))?;

    let source_parsed = parse_expr(source_expr)?;
    let value = eval_expr(&source_parsed, &body_json)?;

    set_field(&mut body_json, dest, value)?;

    let result =
        serde_json::to_string(&body_json).map_err(|e| format!("serialize error: {}", e))?;
    Ok(result.into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_path() {
        let body: Value = serde_json::from_str(r#"{"user":{"name":"alice"}}"#).unwrap();
        let val = resolve_field(&body, "user.name").unwrap();
        assert_eq!(val, Value::String("alice".to_string()));
    }

    #[test]
    fn test_field_copy() {
        let body = br#"{"first":"alice","last":"smith"}"#;
        let result = eval_map_expression("fullname=concat(first,last)", body).unwrap();
        let json: Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["fullname"], "alicesmith");
    }

    #[test]
    fn test_function_lower() {
        let body = br#"{"name":"ALICE"}"#;
        let result = eval_map_expression("name=lower(name)", body).unwrap();
        let json: Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["name"], "alice");
    }

    #[test]
    fn test_function_upper() {
        let body = br#"{"name":"alice"}"#;
        let result = eval_map_expression("name=upper(name)", body).unwrap();
        let json: Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["name"], "ALICE");
    }

    #[test]
    fn test_function_trim() {
        let body = br#"{"name":"  alice  "}"#;
        let result = eval_map_expression("name=trim(name)", body).unwrap();
        let json: Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["name"], "alice");
    }

    #[test]
    fn test_function_length() {
        let body = br#"{"msg":"hello"}"#;
        let result = eval_map_expression("len=length(msg)", body).unwrap();
        let json: Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["len"], 5);
    }

    #[test]
    fn test_function_uuid() {
        let body = br#"{}"#;
        let result = eval_map_expression("id=uuid()", body).unwrap();
        let json: Value = serde_json::from_slice(&result).unwrap();
        assert!(json["id"].as_str().unwrap().len() == 36);
    }

    #[test]
    fn test_concat_expr() {
        let body = br#"{"a":"hello","b":"world"}"#;
        let result = eval_map_expression("msg=a+b", body).unwrap();
        let json: Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["msg"], "helloworld");
    }

    #[test]
    fn test_nested_field_set() {
        let body = br#"{"user":{"name":"bob"}}"#;
        let result = eval_map_expression("user.role=upper(user.name)", body).unwrap();
        let json: Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["user"]["role"], "BOB");
    }

    #[test]
    fn test_function_base64() {
        let body = br#"{"data":"hello"}"#;
        let result = eval_map_expression("encoded=base64(data)", body).unwrap();
        let json: Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["encoded"], "aGVsbG8=");
    }

    #[test]
    fn test_function_replace() {
        let body = br#"{"text":"hello world"}"#;
        let result =
            eval_map_expression("text=replace(text,'world','alice')", body).unwrap();
        let json: Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["text"], "hello alice");
    }

    #[test]
    fn test_function_substring() {
        let body = br#"{"text":"hello world"}"#;
        let result = eval_map_expression("sub=substring(text,0,5)", body).unwrap();
        let json: Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["sub"], "hello");
    }

    #[test]
    fn test_function_abs() {
        let body = br#"{"n":-42.5}"#;
        let result = eval_map_expression("v=abs(n)", body).unwrap();
        let json: Value = serde_json::from_slice(&result).unwrap();
        assert!((json["v"].as_f64().unwrap() - 42.5).abs() < 1e-10);
    }

    #[test]
    fn test_function_round() {
        let body = br#"{"n":3.7}"#;
        let result = eval_map_expression("v=round(n)", body).unwrap();
        let json: Value = serde_json::from_slice(&result).unwrap();
        assert!((json["v"].as_f64().unwrap() - 4.0).abs() < 1e-10);
    }

    #[test]
    fn test_function_ceil() {
        let body = br#"{"n":3.2}"#;
        let result = eval_map_expression("v=ceil(n)", body).unwrap();
        let json: Value = serde_json::from_slice(&result).unwrap();
        assert!((json["v"].as_f64().unwrap() - 4.0).abs() < 1e-10);
    }

    #[test]
    fn test_function_floor() {
        let body = br#"{"n":3.8}"#;
        let result = eval_map_expression("v=floor(n)", body).unwrap();
        let json: Value = serde_json::from_slice(&result).unwrap();
        assert!((json["v"].as_f64().unwrap() - 3.0).abs() < 1e-10);
    }

    #[test]
    fn test_function_min() {
        let body = br#"{"a":3,"b":7}"#;
        let result = eval_map_expression("v=min(a,b)", body).unwrap();
        let json: Value = serde_json::from_slice(&result).unwrap();
        assert!((json["v"].as_f64().unwrap() - 3.0).abs() < 1e-10);
    }

    #[test]
    fn test_function_max() {
        let body = br#"{"a":3,"b":7}"#;
        let result = eval_map_expression("v=max(a,b)", body).unwrap();
        let json: Value = serde_json::from_slice(&result).unwrap();
        assert!((json["v"].as_f64().unwrap() - 7.0).abs() < 1e-10);
    }

    #[test]
    fn test_function_base64_decode() {
        let body = br#"{"data":"aGVsbG8="}"#;
        let result =
            eval_map_expression("decoded=base64_decode(data)", body).unwrap();
        let json: Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["decoded"], "hello");
    }

    #[test]
    fn test_function_parse_bool() {
        let body = br#"{"s":"true"}"#;
        let result = eval_map_expression("b=parse_bool(s)", body).unwrap();
        let json: Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["b"], true);
    }

    #[test]
    fn test_function_parse_bool_false() {
        let body = br#"{"s":"false"}"#;
        let result = eval_map_expression("b=parse_bool(s)", body).unwrap();
        let json: Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["b"], false);
    }

    #[test]
    fn test_function_split() {
        let body = br#"{"s":"a,b,c"}"#;
        let result = eval_map_expression("parts=split(s,',')", body).unwrap();
        let json: Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["parts"][0], "a");
        assert_eq!(json["parts"][1], "b");
        assert_eq!(json["parts"][2], "c");
    }

    #[test]
    fn test_function_typeof() {
        let body = br#"{"s":"hello"}"#;
        let result = eval_map_expression("t=typeof(s)", body).unwrap();
        let json: Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["t"], "string");
    }

    #[test]
    fn test_function_typeof_number() {
        let body = br#"{"n":42}"#;
        let result = eval_map_expression("t=typeof(n)", body).unwrap();
        let json: Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["t"], "int");
    }

    #[test]
    fn test_function_typeof_bool() {
        let body = br#"{"b":true}"#;
        let result = eval_map_expression("t=typeof(b)", body).unwrap();
        let json: Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["t"], "bool");
    }

    #[test]
    fn test_function_typeof_array() {
        let body = br#"{"a":[1,2]}"#;
        let result = eval_map_expression("t=typeof(a)", body).unwrap();
        let json: Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["t"], "array");
    }
}
