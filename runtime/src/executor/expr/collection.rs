use serde_json::Value;

pub fn call(name: &str, args: &[Value]) -> Result<Value, String> {
    match name {
        "contains" => {
            let list = args.first().ok_or("contains: missing list arg")?;
            let val = args.get(1).ok_or("contains: missing value arg")?;
            match list {
                Value::Array(arr) => Ok(Value::Bool(arr.contains(val))),
                _ => Err("contains: first arg must be an array".to_string()),
            }
        }
        "keys" => {
            let obj = args.first().ok_or("keys: missing object arg")?;
            match obj {
                Value::Object(map) => {
                    let k: Vec<Value> = map
                        .keys()
                        .map(|k| Value::String(k.clone()))
                        .collect();
                    Ok(Value::Array(k))
                }
                _ => Err("keys: arg must be an object".to_string()),
            }
        }
        "merge" => {
            let a = args.first().ok_or("merge: missing first arg")?;
            let b = args.get(1).ok_or("merge: missing second arg")?;
            super::utility::merge_json(a.clone(), b.clone())
        }
        _ => Err(format!("unknown collection function: {}", name)),
    }
}
