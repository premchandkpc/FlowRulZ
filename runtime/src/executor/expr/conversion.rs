use serde_json::Value;

pub fn call(name: &str, args: &[Value]) -> Result<Value, String> {
    match name {
        "to_string" => {
            let v = args.first().cloned().unwrap_or(Value::Null);
            Ok(Value::String(super::value_to_string(&v)))
        }
        "parse_int" => {
            let s = super::arg_as_str(args, 0);
            let n: i64 = s
                .parse()
                .map_err(|e| format!("parse_int error: {}", e))?;
            Ok(Value::Number(serde_json::Number::from(n)))
        }
        "parse_float" => {
            let s = super::arg_as_str(args, 0);
            let n: f64 = s
                .parse()
                .map_err(|e| format!("parse_float error: {}", e))?;
            Ok(serde_json::Number::from_f64(n)
                .map(Value::Number)
                .unwrap_or(Value::Null))
        }
        "parse_bool" => {
            let s = super::arg_as_str(args, 0).to_lowercase();
            match s.as_str() {
                "true" | "1" | "yes" => Ok(Value::Bool(true)),
                "false" | "0" | "no" => Ok(Value::Bool(false)),
                _ => Err(format!("parse_bool: cannot parse '{}'", s)),
            }
        }
        "json" => {
            let s = super::arg_as_str(args, 0);
            serde_json::from_str(&s).map_err(|e| format!("json parse error: {}", e))
        }
        _ => Err(format!("unknown conversion function: {}", name)),
    }
}
