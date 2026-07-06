use std::time::SystemTime;

use serde_json::Value;

pub fn call(name: &str, args: &[Value]) -> Result<Value, String> {
    match name {
        "uuid" => Ok(Value::String(uuid::Uuid::new_v4().to_string())),
        "now" => Ok(Value::String(now_iso())),
        "epoch" => {
            let d = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default();
            Ok(Value::Number(
                serde_json::Number::from_f64(d.as_secs_f64())
                    .unwrap_or(serde_json::Number::from(0)),
            ))
        }
        "coalesce" => {
            for a in args {
                if !a.is_null() {
                    return Ok(a.clone());
                }
            }
            Ok(Value::Null)
        }
        "default" => {
            if let Some(val) = args.first() {
                if !val.is_null() {
                    return Ok(val.clone());
                }
            }
            Ok(args.get(1).cloned().unwrap_or(Value::Null))
        }
        "hash" => {
            let s = super::arg_as_str(args, 0);
            let hash = consistent_hash(&s);
            Ok(Value::Number(serde_json::Number::from(hash)))
        }
        "typeof" => {
            let v = args.first().ok_or("typeof: missing arg")?;
            let type_name = match v {
                Value::Null => "null",
                Value::Bool(_) => "bool",
                Value::Number(n) if n.is_f64() && n.as_f64().unwrap_or(0.0).fract() != 0.0 => "float",
                Value::Number(_) => "int",
                Value::String(_) => "string",
                Value::Array(_) => "array",
                Value::Object(_) => "object",
            };
            Ok(Value::String(type_name.to_string()))
        }
        _ => Err(format!("unknown utility function: {}", name)),
    }
}

pub fn merge_json(mut a: Value, b: Value) -> Result<Value, String> {
    crate::executor::dag::deep_merge(&mut a, b);
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

pub fn decode_base64(s: &str) -> Result<Vec<u8>, String> {
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
                return Err(format!(
                    "base64_decode: invalid character '{}'",
                    b as char
                ));
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

pub fn base64_encode(s: &str) -> String {
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
