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

#[cfg(test)]
mod tests {
    use super::*;

    fn arena() -> &'static crate::memory::arena::Arena {
        Box::leak(Box::new(crate::memory::arena::Arena::new()))
    }

    #[test]
    fn test_extract_json_field_simple() {
        let result = extract_json_field(b"{\"x\":42}", "x", &arena());
        assert!(result.is_some());
        assert_eq!(result.unwrap(), b"42");
    }

    #[test]
    fn test_extract_json_field_nested() {
        let result = extract_json_field(b"{\"a\":{\"b\":\"hello\"}}", "a.b", &arena());
        assert!(result.is_some());
        assert_eq!(result.unwrap(), b"\"hello\"");
    }

    #[test]
    fn test_extract_json_field_missing() {
        let result = extract_json_field(b"{\"x\":1}", "y", &arena());
        assert!(result.is_none());
    }

    #[test]
    fn test_extract_json_field_invalid_json() {
        let result = extract_json_field(b"not-json", "x", &arena());
        assert!(result.is_none());
    }

    #[test]
    fn test_extract_json_field_non_object() {
        let result = extract_json_field(b"\"string\"", "x", &arena());
        assert!(result.is_none());
    }

    #[test]
    fn test_extract_json_field_deep_nested() {
        let result = extract_json_field(
            b"{\"a\":{\"b\":{\"c\":{\"d\":99}}}}",
            "a.b.c.d",
            &arena(),
        );
        assert!(result.is_some());
        assert_eq!(result.unwrap(), b"99");
    }

    #[test]
    fn test_compare_values_eq() {
        assert!(compare_values(b"hello", 0, "hello"));
        assert!(!compare_values(b"hello", 0, "world"));
    }

    #[test]
    fn test_compare_values_ne() {
        assert!(compare_values(b"hello", 1, "world"));
        assert!(!compare_values(b"hello", 1, "hello"));
    }

    #[test]
    fn test_compare_values_gt() {
        assert!(compare_values(b"5", 2, "3"));
        assert!(!compare_values(b"3", 2, "5"));
        assert!(!compare_values(b"5", 2, "5"));
    }

    #[test]
    fn test_compare_values_lt() {
        assert!(compare_values(b"3", 3, "5"));
        assert!(!compare_values(b"5", 3, "3"));
    }

    #[test]
    fn test_compare_values_gte() {
        assert!(compare_values(b"5", 4, "5"));
        assert!(compare_values(b"6", 4, "5"));
        assert!(!compare_values(b"4", 4, "5"));
    }

    #[test]
    fn test_compare_values_lte() {
        assert!(compare_values(b"3", 5, "5"));
        assert!(compare_values(b"5", 5, "5"));
        assert!(!compare_values(b"6", 5, "5"));
    }

    #[test]
    fn test_compare_values_contains() {
        assert!(compare_values(b"hello world", 6, "world"));
        assert!(!compare_values(b"hello world", 6, "xyz"));
    }

    #[test]
    fn test_compare_values_invalid_op() {
        assert!(!compare_values(b"anything", 255, "anything"));
    }

    #[test]
    fn test_compare_values_non_numeric_parse() {
        // When field or compare_val cannot be parsed as f64, defaults to 0.0
        assert!(!compare_values(b"abc", 2, "def")); // 0.0 > 0.0 is false
        assert!(!compare_values(b"abc", 3, "def")); // 0.0 < 0.0 is false
        assert!(compare_values(b"abc", 4, "def")); // 0.0 >= 0.0 is true
        assert!(compare_values(b"abc", 5, "def")); // 0.0 <= 0.0 is true
    }

    #[test]
    fn test_compare_values_invalid_utf8() {
        assert!(!compare_values(b"\xff\xfe", 0, "something"));
        assert!(compare_values(b"\xff\xfe", 1, "something"));
    }
}
