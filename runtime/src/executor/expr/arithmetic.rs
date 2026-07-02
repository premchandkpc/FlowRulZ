use serde_json::Value;

pub fn call(name: &str, args: &[Value]) -> Result<Value, String> {
    match name {
        "abs" => {
            let n = super::arg_as_f64(args, 0).ok_or("abs: expected number")?;
            Ok(serde_json::Number::from_f64(n.abs())
                .map(Value::Number)
                .unwrap_or(Value::Null))
        }
        "round" => {
            let n = super::arg_as_f64(args, 0).ok_or("round: expected number")?;
            Ok(Value::Number(
                serde_json::Number::from_f64(n.round())
                    .unwrap_or(serde_json::Number::from(0)),
            ))
        }
        "ceil" => {
            let n = super::arg_as_f64(args, 0).ok_or("ceil: expected number")?;
            Ok(Value::Number(
                serde_json::Number::from_f64(n.ceil())
                    .unwrap_or(serde_json::Number::from(0)),
            ))
        }
        "floor" => {
            let n = super::arg_as_f64(args, 0).ok_or("floor: expected number")?;
            Ok(Value::Number(
                serde_json::Number::from_f64(n.floor())
                    .unwrap_or(serde_json::Number::from(0)),
            ))
        }
        "min" => {
            let a = super::arg_as_f64(args, 0).ok_or("min: expected number")?;
            let b = super::arg_as_f64(args, 1).ok_or("min: expected number")?;
            Ok(serde_json::Number::from_f64(a.min(b))
                .map(Value::Number)
                .unwrap_or(Value::Null))
        }
        "max" => {
            let a = super::arg_as_f64(args, 0).ok_or("max: expected number")?;
            let b = super::arg_as_f64(args, 1).ok_or("max: expected number")?;
            Ok(serde_json::Number::from_f64(a.max(b))
                .map(Value::Number)
                .unwrap_or(Value::Null))
        }
        _ => Err(format!("unknown arithmetic function: {}", name)),
    }
}
