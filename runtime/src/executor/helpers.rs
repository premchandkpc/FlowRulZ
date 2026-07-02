pub fn extract_json_field<'a>(
    body: &[u8],
    field_path: &str,
    arena: &'a crate::memory::arena::Arena,
) -> Option<&'a [u8]> {
    let body_str = std::str::from_utf8(body).ok()?;
    let parts: Vec<&str> = field_path.split('.').collect();

    let mut current: serde_json::Value = serde_json::from_str(body_str).ok()?;

    for part in &parts {
        match current {
            serde_json::Value::Object(ref map) => {
                current = map.get(*part)?.clone();
            }
            _ => return None,
        }
    }

    let result_str = current.to_string();
    Some(arena.alloc_copy(result_str.as_bytes()))
}

pub fn compare_values(field_val: &[u8], op: u8, compare_val: &str) -> bool {
    let field_str = std::str::from_utf8(field_val).unwrap_or("");
    let gate_op = match op {
        0 => "==",
        1 => "!=",
        2 => ">",
        3 => "<",
        4 => ">=",
        5 => "<=",
        6 => "contains",
        _ => return false,
    };

    match gate_op {
        "==" => field_str == compare_val,
        "!=" => field_str != compare_val,
        ">" => {
            let f: f64 = field_str.parse().unwrap_or(0.0);
            let c: f64 = compare_val.parse().unwrap_or(0.0);
            f > c
        }
        "<" => {
            let f: f64 = field_str.parse().unwrap_or(0.0);
            let c: f64 = compare_val.parse().unwrap_or(0.0);
            f < c
        }
        ">=" => {
            let f: f64 = field_str.parse().unwrap_or(0.0);
            let c: f64 = compare_val.parse().unwrap_or(0.0);
            f >= c
        }
        "<=" => {
            let f: f64 = field_str.parse().unwrap_or(0.0);
            let c: f64 = compare_val.parse().unwrap_or(0.0);
            f <= c
        }
        "contains" => field_str.contains(compare_val),
        _ => false,
    }
}
