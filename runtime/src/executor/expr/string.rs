use serde_json::Value;

pub fn call(name: &str, args: &[Value]) -> Result<Value, String> {
    match name {
        "lower" => {
            let s = super::arg_as_str(args, 0);
            Ok(Value::String(s.to_lowercase()))
        }
        "upper" => {
            let s = super::arg_as_str(args, 0);
            Ok(Value::String(s.to_uppercase()))
        }
        "trim" => {
            let s = super::arg_as_str(args, 0);
            Ok(Value::String(s.trim().to_string()))
        }
        "length" => {
            let s = super::arg_as_str(args, 0);
            Ok(Value::Number(serde_json::Number::from(s.len())))
        }
        "concat" => {
            let mut out = String::new();
            for a in args {
                out.push_str(&super::value_to_string(a));
            }
            Ok(Value::String(out))
        }
        "substring" => {
            let s = super::arg_as_str(args, 0);
            let start = super::arg_as_f64(args, 1).unwrap_or(0.0) as usize;
            let end = super::arg_as_f64(args, 2).unwrap_or(s.len() as f64) as usize;
            let end = end.min(s.len());
            Ok(Value::String(s[start..end].to_string()))
        }
        "replace" => {
            let s = super::arg_as_str(args, 0);
            let from = super::arg_as_str(args, 1);
            let to = super::arg_as_str(args, 2);
            Ok(Value::String(s.replace(&from, &to)))
        }
        "split" => {
            let s = super::arg_as_str(args, 0);
            let delim = super::arg_as_str(args, 1);
            if delim.is_empty() {
                let chars: Vec<Value> = s
                    .chars()
                    .map(|c| Value::String(c.to_string()))
                    .collect();
                Ok(Value::Array(chars))
            } else {
                let parts: Vec<Value> = s
                    .split(&delim)
                    .map(|p| Value::String(p.to_string()))
                    .collect();
                Ok(Value::Array(parts))
            }
        }
        "base64" => {
            let s = super::arg_as_str(args, 0);
            Ok(Value::String(crate::executor::expr::utility::base64_encode(&s)))
        }
        "base64_decode" => {
            let s = super::arg_as_str(args, 0);
            crate::executor::expr::utility::decode_base64(&s).map(|bytes| {
                String::from_utf8(bytes)
                    .map(Value::String)
                    .unwrap_or(Value::Null)
            })
        }
        _ => Err(format!("unknown string function: {}", name)),
    }
}
