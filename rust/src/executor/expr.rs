use std::time::SystemTime;

#[derive(Debug, Clone)]
enum Expr {
    Field(String),
    FnCall {
        name: String,
        args: Vec<Expr>,
    },
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

    for i in 0..len {
        let c = bytes[i];
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

fn eval_expr(expr: &Expr, body: &serde_json::Value) -> Result<serde_json::Value, String> {
    match expr {
        Expr::Field(path) => resolve_field(body, path),
        Expr::FnCall { name, args } => {
            let evaluated: Result<Vec<serde_json::Value>, String> = args
                .iter()
                .map(|a| eval_expr(a, body))
                .collect();
            let evaluated = evaluated?;
            call_builtin(name, &evaluated)
        }
        Expr::Concat(parts) => {
            let mut result = String::new();
            for part in parts {
                let val = eval_expr(part, body)?;
                result.push_str(&value_to_string(&val));
            }
            Ok(serde_json::Value::String(result))
        }
        Expr::Literal(s) => Ok(serde_json::Value::String(s.clone())),
        Expr::Number(n) => Ok(serde_json::Value::Number(
            serde_json::Number::from_f64(*n).unwrap_or(serde_json::Number::from(0)),
        )),
        Expr::Raw(s) => Ok(serde_json::Value::String(s.clone())),
    }
}

fn value_to_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => "null".to_string(),
        _ => v.to_string(),
    }
}

fn resolve_field(body: &serde_json::Value, path: &str) -> Result<serde_json::Value, String> {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = body.clone();
    for part in parts {
        match current {
            serde_json::Value::Object(ref map) => {
                if let Some(val) = map.get(part) {
                    current = val.clone();
                } else {
                    return Err(format!("FieldNotFound: path segment '{}' not found in '{}'", part, path));
                }
            }
            _ => return Err(format!("FieldNotFound: cannot navigate into non-object at path '{}' segment '{}'", path, part)),
        }
    }
    Ok(current)
}

fn set_field(
    body: &mut serde_json::Value,
    path: &str,
    value: serde_json::Value,
) -> Result<(), String> {
    let parts: Vec<&str> = path.split('.').collect();
    if parts.is_empty() {
        return Err("empty path".to_string());
    }
    if parts.len() == 1 {
        if let serde_json::Value::Object(ref mut map) = body {
            map.insert(parts[0].to_string(), value);
            return Ok(());
        }
        return Err("root is not an object".to_string());
    }
    let mut current = body;
    for i in 0..parts.len() - 1 {
        match current {
            serde_json::Value::Object(ref mut map) => {
                current = map
                    .entry(parts[i].to_string())
                    .or_insert(serde_json::Value::Object(serde_json::Map::new()));
            }
            _ => return Err(format!("cannot set field in non-object at {}", parts[i])),
        }
    }
    if let serde_json::Value::Object(ref mut map) = current {
        map.insert(parts[parts.len() - 1].to_string(), value);
        Ok(())
    } else {
        Err("target is not an object".to_string())
    }
}

fn arg_as_str(args: &[serde_json::Value], i: usize) -> String {
    args.get(i).map(value_to_string).unwrap_or_default()
}

fn arg_as_f64(args: &[serde_json::Value], i: usize) -> Option<f64> {
    args.get(i).and_then(|v| match v {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    })
}

fn call_builtin(name: &str, args: &[serde_json::Value]) -> Result<serde_json::Value, String> {
    match name {
        "uuid" => Ok(serde_json::Value::String(uuid::Uuid::new_v4().to_string())),
        "now" => Ok(serde_json::Value::String(now_iso())),
        "epoch" => {
            let d = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default();
            Ok(serde_json::Value::Number(
                serde_json::Number::from_f64(d.as_secs_f64()).unwrap_or(serde_json::Number::from(0)),
            ))
        }
        "lower" => {
            let s = arg_as_str(args, 0);
            Ok(serde_json::Value::String(s.to_lowercase()))
        }
        "upper" => {
            let s = arg_as_str(args, 0);
            Ok(serde_json::Value::String(s.to_uppercase()))
        }
        "trim" => {
            let s = arg_as_str(args, 0);
            Ok(serde_json::Value::String(s.trim().to_string()))
        }
        "length" => {
            let s = arg_as_str(args, 0);
            Ok(serde_json::Value::Number(serde_json::Number::from(s.len())))
        }
        "concat" => {
            let mut out = String::new();
            for a in args {
                out.push_str(&value_to_string(a));
            }
            Ok(serde_json::Value::String(out))
        }
        "base64" => {
            let s = arg_as_str(args, 0);
            Ok(serde_json::Value::String(base64_encode(&s)))
        }
        "json" => {
            let s = arg_as_str(args, 0);
            serde_json::from_str(&s).map_err(|e| format!("json parse error: {}", e))
        }
        "substring" => {
            let s = arg_as_str(args, 0);
            let start = arg_as_f64(args, 1).unwrap_or(0.0) as usize;
            let end = arg_as_f64(args, 2).unwrap_or(s.len() as f64) as usize;
            let end = end.min(s.len());
            Ok(serde_json::Value::String(s[start..end].to_string()))
        }
        "replace" => {
            let s = arg_as_str(args, 0);
            let from = arg_as_str(args, 1);
            let to = arg_as_str(args, 2);
            Ok(serde_json::Value::String(s.replace(&from, &to)))
        }
        "to_string" => {
            let v = args.first().cloned().unwrap_or(serde_json::Value::Null);
            Ok(serde_json::Value::String(value_to_string(&v)))
        }
        "parse_int" => {
            let s = arg_as_str(args, 0);
            let n: i64 = s.parse().map_err(|e| format!("parse_int error: {}", e))?;
            Ok(serde_json::Value::Number(serde_json::Number::from(n)))
        }
        "parse_float" => {
            let s = arg_as_str(args, 0);
            let n: f64 = s.parse().map_err(|e| format!("parse_float error: {}", e))?;
            Ok(serde_json::Number::from_f64(n)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null))
        }
        "coalesce" => {
            for a in args {
                if !a.is_null() {
                    return Ok(a.clone());
                }
            }
            Ok(serde_json::Value::Null)
        }
        "default" => {
            if let Some(val) = args.first() {
                if !val.is_null() {
                    return Ok(val.clone());
                }
            }
            Ok(args.get(1).cloned().unwrap_or(serde_json::Value::Null))
        }
        "contains" => {
            let list = args.first().ok_or("contains: missing list arg")?;
            let val = args.get(1).ok_or("contains: missing value arg")?;
            match list {
                serde_json::Value::Array(arr) => {
                    Ok(serde_json::Value::Bool(arr.contains(val)))
                }
                _ => Err("contains: first arg must be an array".to_string()),
            }
        }
        "keys" => {
            let obj = args.first().ok_or("keys: missing object arg")?;
            match obj {
                serde_json::Value::Object(map) => {
                    let k: Vec<serde_json::Value> = map
                        .keys()
                        .map(|k| serde_json::Value::String(k.clone()))
                        .collect();
                    Ok(serde_json::Value::Array(k))
                }
                _ => Err("keys: arg must be an object".to_string()),
            }
        }
        "merge" => {
            let a = args.first().ok_or("merge: missing first arg")?;
            let b = args.get(1).ok_or("merge: missing second arg")?;
            merge_json(a.clone(), b.clone())
        }
        "hash" => {
            let s = arg_as_str(args, 0);
            let hash = consistent_hash(&s);
            Ok(serde_json::Value::Number(serde_json::Number::from(hash)))
        }
        "abs" => {
            let n = arg_as_f64(args, 0).ok_or("abs: expected number")?;
            Ok(serde_json::Number::from_f64(n.abs())
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null))
        }
        "round" => {
            let n = arg_as_f64(args, 0).ok_or("round: expected number")?;
            Ok(serde_json::Value::Number(serde_json::Number::from_f64(n.round()).unwrap_or(serde_json::Number::from(0))))
        }
        "ceil" => {
            let n = arg_as_f64(args, 0).ok_or("ceil: expected number")?;
            Ok(serde_json::Value::Number(serde_json::Number::from_f64(n.ceil()).unwrap_or(serde_json::Number::from(0))))
        }
        "floor" => {
            let n = arg_as_f64(args, 0).ok_or("floor: expected number")?;
            Ok(serde_json::Value::Number(serde_json::Number::from_f64(n.floor()).unwrap_or(serde_json::Number::from(0))))
        }
        "min" => {
            let a = arg_as_f64(args, 0).ok_or("min: expected number")?;
            let b = arg_as_f64(args, 1).ok_or("min: expected number")?;
            Ok(serde_json::Number::from_f64(a.min(b))
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null))
        }
        "max" => {
            let a = arg_as_f64(args, 0).ok_or("max: expected number")?;
            let b = arg_as_f64(args, 1).ok_or("max: expected number")?;
            Ok(serde_json::Number::from_f64(a.max(b))
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null))
        }
        "base64_decode" => {
            let s = arg_as_str(args, 0);
            decode_base64(&s).map(|bytes| {
                String::from_utf8(bytes).map(serde_json::Value::String)
                    .unwrap_or(serde_json::Value::Null)
            })
        }
        "parse_bool" => {
            let s = arg_as_str(args, 0).to_lowercase();
            match s.as_str() {
                "true" | "1" | "yes" => Ok(serde_json::Value::Bool(true)),
                "false" | "0" | "no" => Ok(serde_json::Value::Bool(false)),
                _ => Err(format!("parse_bool: cannot parse '{}'", s)),
            }
        }
        "split" => {
            let s = arg_as_str(args, 0);
            let delim = arg_as_str(args, 1);
            if delim.is_empty() {
                let chars: Vec<serde_json::Value> = s.chars().map(|c| serde_json::Value::String(c.to_string())).collect();
                Ok(serde_json::Value::Array(chars))
            } else {
                let parts: Vec<serde_json::Value> = s.split(&delim).map(|p| serde_json::Value::String(p.to_string())).collect();
                Ok(serde_json::Value::Array(parts))
            }
        }
        "typeof" => {
            let v = args.first().ok_or("typeof: missing arg")?;
            let type_name = match v {
                serde_json::Value::Null => "null",
                serde_json::Value::Bool(_) => "bool",
                serde_json::Value::Number(n) if n.is_f64() && n.as_f64().unwrap().fract() != 0.0 => "float",
                serde_json::Value::Number(_) => "int",
                serde_json::Value::String(_) => "string",
                serde_json::Value::Array(_) => "array",
                serde_json::Value::Object(_) => "object",
            };
            Ok(serde_json::Value::String(type_name.to_string()))
        }
        _ => Err(format!("unknown function: {}", name)),
    }
}

fn merge_json(mut a: serde_json::Value, b: serde_json::Value) -> Result<serde_json::Value, String> {
    super::dag::deep_merge(&mut a, b);
    Ok(a)
}

fn consistent_hash(s: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut h);
    h.finish()
}

fn now_iso() -> String {
    let d = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    let total_secs = d.as_secs();
    let nanos = d.subsec_nanos();
    let days = total_secs / 86400;
    let time = total_secs % 86400;
    let hours = time / 3600;
    let mins = (time % 3600) / 60;
    let sec = time % 60;

    // civil_from_days: days since 1970-01-01
    let z = days as i64 + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };

    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:09}Z",
        y, m, d, hours as u32, mins as u32, sec as u32, nanos
    )
}

fn decode_base64(s: &str) -> Result<Vec<u8>, String> {
    const DECODE: [i8; 256] = {
        let mut t = [-1i8; 256];
        let mut i = 0u8;
        let chars = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        while i < 64 {
            t[chars[i as usize] as usize] = i as i8;
            i += 1;
        }
        t
    };
    let bytes = s.trim_end_matches('=').as_bytes();
    let mut out = Vec::with_capacity(bytes.len() * 3 / 4);
    for chunk in bytes.chunks(4) {
        let mut buf = 0u32;
        let mut bits = 0;
        for &b in chunk {
            let val = DECODE.get(b as usize).copied().unwrap_or(-1);
            if val < 0 {
                return Err(format!("base64_decode: invalid character '{}'", b as char));
            }
            buf = (buf << 6) | val as u32;
            bits += 6;
        }
        while bits >= 8 {
            bits -= 8;
            out.push((buf >> bits) as u8);
        }
    }
    Ok(out)
}

fn base64_encode(s: &str) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let bytes = s.as_bytes();
    let mut result = String::new();
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let triple = (b0 << 16) | (b1 << 8) | b2;
        result.push(CHARS[((triple >> 18) & 0x3F) as usize] as char);
        result.push(CHARS[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            result.push(CHARS[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
        if chunk.len() > 2 {
            result.push(CHARS[(triple & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
    }
    result
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

    let body_str = std::str::from_utf8(body).map_err(|e| format!("invalid utf8: {}", e))?;
    let mut body_json: serde_json::Value =
        serde_json::from_str(body_str).map_err(|e| format!("invalid json: {}", e))?;

    let source_parsed = parse_expr(source_expr)?;
    let value = eval_expr(&source_parsed, &body_json)?;

    set_field(&mut body_json, dest, value)?;

    let result = serde_json::to_string(&body_json)
        .map_err(|e| format!("serialize error: {}", e))?;
    Ok(result.into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_path() {
        let body: serde_json::Value =
            serde_json::from_str(r#"{"user":{"name":"alice"}}"#).unwrap();
        let val = resolve_field(&body, "user.name").unwrap();
        assert_eq!(val, serde_json::Value::String("alice".to_string()));
    }

    #[test]
    fn test_field_copy() {
        let body = br#"{"first":"alice","last":"smith"}"#;
        let result = eval_map_expression("fullname=concat(first,last)", body).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["fullname"], "alicesmith");
    }

    #[test]
    fn test_function_lower() {
        let body = br#"{"name":"ALICE"}"#;
        let result = eval_map_expression("name=lower(name)", body).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["name"], "alice");
    }

    #[test]
    fn test_function_upper() {
        let body = br#"{"name":"alice"}"#;
        let result = eval_map_expression("name=upper(name)", body).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["name"], "ALICE");
    }

    #[test]
    fn test_function_trim() {
        let body = br#"{"name":"  alice  "}"#;
        let result = eval_map_expression("name=trim(name)", body).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["name"], "alice");
    }

    #[test]
    fn test_function_length() {
        let body = br#"{"msg":"hello"}"#;
        let result = eval_map_expression("len=length(msg)", body).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["len"], 5);
    }

    #[test]
    fn test_function_uuid() {
        let body = br#"{}"#;
        let result = eval_map_expression("id=uuid()", body).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert!(json["id"].as_str().unwrap().len() == 36);
    }

    #[test]
    fn test_concat_expr() {
        let body = br#"{"a":"hello","b":"world"}"#;
        let result = eval_map_expression("msg=a+b", body).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["msg"], "helloworld");
    }

    #[test]
    fn test_nested_field_set() {
        let body = br#"{"user":{"name":"bob"}}"#;
        let result = eval_map_expression("user.role=upper(user.name)", body).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["user"]["role"], "BOB");
    }

    #[test]
    fn test_function_base64() {
        let body = br#"{"data":"hello"}"#;
        let result = eval_map_expression("encoded=base64(data)", body).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["encoded"], "aGVsbG8=");
    }

    #[test]
    fn test_function_replace() {
        let body = br#"{"text":"hello world"}"#;
        let result =
            eval_map_expression("text=replace(text,'world','alice')", body).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["text"], "hello alice");
    }

    #[test]
    fn test_function_substring() {
        let body = br#"{"text":"hello world"}"#;
        let result = eval_map_expression("sub=substring(text,0,5)", body).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["sub"], "hello");
    }

    #[test]
    fn test_function_abs() {
        let body = br#"{"n":-42.5}"#;
        let result = eval_map_expression("v=abs(n)", body).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert!((json["v"].as_f64().unwrap() - 42.5).abs() < 1e-10);
    }

    #[test]
    fn test_function_round() {
        let body = br#"{"n":3.7}"#;
        let result = eval_map_expression("v=round(n)", body).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert!((json["v"].as_f64().unwrap() - 4.0).abs() < 1e-10);
    }

    #[test]
    fn test_function_ceil() {
        let body = br#"{"n":3.2}"#;
        let result = eval_map_expression("v=ceil(n)", body).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert!((json["v"].as_f64().unwrap() - 4.0).abs() < 1e-10);
    }

    #[test]
    fn test_function_floor() {
        let body = br#"{"n":3.8}"#;
        let result = eval_map_expression("v=floor(n)", body).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert!((json["v"].as_f64().unwrap() - 3.0).abs() < 1e-10);
    }

    #[test]
    fn test_function_min() {
        let body = br#"{"a":3,"b":7}"#;
        let result = eval_map_expression("v=min(a,b)", body).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert!((json["v"].as_f64().unwrap() - 3.0).abs() < 1e-10);
    }

    #[test]
    fn test_function_max() {
        let body = br#"{"a":3,"b":7}"#;
        let result = eval_map_expression("v=max(a,b)", body).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert!((json["v"].as_f64().unwrap() - 7.0).abs() < 1e-10);
    }

    #[test]
    fn test_function_base64_decode() {
        let body = br#"{"data":"aGVsbG8="}"#;
        let result = eval_map_expression("decoded=base64_decode(data)", body).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["decoded"], "hello");
    }

    #[test]
    fn test_function_parse_bool() {
        let body = br#"{"s":"true"}"#;
        let result = eval_map_expression("b=parse_bool(s)", body).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["b"], true);
    }

    #[test]
    fn test_function_parse_bool_false() {
        let body = br#"{"s":"false"}"#;
        let result = eval_map_expression("b=parse_bool(s)", body).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["b"], false);
    }

    #[test]
    fn test_function_split() {
        let body = br#"{"s":"a,b,c"}"#;
        let result = eval_map_expression("parts=split(s,',')", body).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["parts"][0], "a");
        assert_eq!(json["parts"][1], "b");
        assert_eq!(json["parts"][2], "c");
    }

    #[test]
    fn test_function_typeof() {
        let body = br#"{"s":"hello"}"#;
        let result = eval_map_expression("t=typeof(s)", body).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["t"], "string");
    }

    #[test]
    fn test_function_typeof_number() {
        let body = br#"{"n":42}"#;
        let result = eval_map_expression("t=typeof(n)", body).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["t"], "int");
    }

    #[test]
    fn test_function_typeof_bool() {
        let body = br#"{"b":true}"#;
        let result = eval_map_expression("t=typeof(b)", body).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["t"], "bool");
    }

    #[test]
    fn test_function_typeof_array() {
        let body = br#"{"a":[1,2]}"#;
        let result = eval_map_expression("t=typeof(a)", body).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(json["t"], "array");
    }
}
