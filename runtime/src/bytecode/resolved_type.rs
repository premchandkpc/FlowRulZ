use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ResolvedType {
    String,
    Integer,
    Float,
    Boolean,
    Object,
    Array,
    Enum(Vec<String>),
    Null,
    Any,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldSchema {
    pub name: String,
    pub r#type: ResolvedType,
    pub required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub fields: Vec<FieldSchema>,
}

impl Schema {
    pub fn field_type(&self, name: &str) -> Option<&ResolvedType> {
        self.fields.iter().find(|f| f.name == name).map(|f| &f.r#type)
    }

    pub fn is_valid(&self, body: &serde_json::Value) -> Result<(), String> {
        let map = match body {
            serde_json::Value::Object(m) => m,
            _ => return Err("body is not an object".into()),
        };
        for field in &self.fields {
            let val = map.get(&field.name);
            if val.is_none() && field.required {
                return Err(format!("missing required field '{}'", field.name));
            }
            if let Some(v) = val {
                if !field.r#type.check(v) {
                    return Err(format!(
                        "field '{}' expected {:?}, got {:?}",
                        field.name, field.r#type, v
                    ));
                }
            }
        }
        Ok(())
    }
}

impl ResolvedType {
    pub fn check(&self, value: &serde_json::Value) -> bool {
        match self {
            ResolvedType::String => value.is_string(),
            ResolvedType::Integer => value.is_i64() || value.is_u64(),
            ResolvedType::Float => value.is_f64(),
            ResolvedType::Boolean => value.is_boolean(),
            ResolvedType::Object => value.is_object(),
            ResolvedType::Array => value.is_array(),
            ResolvedType::Enum(variants) => value
                .as_str()
                .map(|s| variants.iter().any(|v| v == s))
                .unwrap_or(false),
            ResolvedType::Null => value.is_null(),
            ResolvedType::Any => true,
        }
    }

    /// Returns true if this type supports ordering operators (>, <, >=, <=).
    pub fn supports_ordering(&self) -> bool {
        matches!(
            self,
            ResolvedType::Integer | ResolvedType::Float | ResolvedType::String
        )
    }

    /// Returns true if this type supports the `contains` operator.
    pub fn supports_contains(&self) -> bool {
        matches!(self, ResolvedType::String | ResolvedType::Array)
    }

    /// Returns true if this type is numeric (Integer or Float).
    pub fn is_numeric(&self) -> bool {
        matches!(self, ResolvedType::Integer | ResolvedType::Float)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolved_type_check_string() {
        assert!(ResolvedType::String.check(&serde_json::json!("hello")));
        assert!(!ResolvedType::String.check(&serde_json::json!(42)));
    }

    #[test]
    fn test_resolved_type_check_integer() {
        assert!(ResolvedType::Integer.check(&serde_json::json!(42)));
        assert!(ResolvedType::Integer.check(&serde_json::json!(42u64)));
        assert!(!ResolvedType::Integer.check(&serde_json::json!("42")));
    }

    #[test]
    fn test_resolved_type_check_float() {
        assert!(ResolvedType::Float.check(&serde_json::json!(3.14)));
        assert!(!ResolvedType::Float.check(&serde_json::json!(42)));
    }

    #[test]
    fn test_resolved_type_check_boolean() {
        assert!(ResolvedType::Boolean.check(&serde_json::json!(true)));
        assert!(!ResolvedType::Boolean.check(&serde_json::json!("true")));
    }

    #[test]
    fn test_resolved_type_check_object() {
        assert!(ResolvedType::Object.check(&serde_json::json!({"a": 1})));
        assert!(!ResolvedType::Object.check(&serde_json::json!([])));
    }

    #[test]
    fn test_resolved_type_check_array() {
        assert!(ResolvedType::Array.check(&serde_json::json!([1, 2, 3])));
        assert!(!ResolvedType::Array.check(&serde_json::json!({})));
    }

    #[test]
    fn test_resolved_type_check_enum() {
        let enum_type = ResolvedType::Enum(vec!["red".into(), "green".into(), "blue".into()]);
        assert!(enum_type.check(&serde_json::json!("red")));
        assert!(!enum_type.check(&serde_json::json!("yellow")));
        assert!(!enum_type.check(&serde_json::json!(42)));
    }

    #[test]
    fn test_resolved_type_check_null() {
        assert!(ResolvedType::Null.check(&serde_json::json!(null)));
        assert!(!ResolvedType::Null.check(&serde_json::json!(0)));
    }

    #[test]
    fn test_resolved_type_check_any() {
        assert!(ResolvedType::Any.check(&serde_json::json!("anything")));
        assert!(ResolvedType::Any.check(&serde_json::json!(42)));
        assert!(ResolvedType::Any.check(&serde_json::json!(null)));
    }

    #[test]
    fn test_supports_ordering() {
        assert!(ResolvedType::Integer.supports_ordering());
        assert!(ResolvedType::Float.supports_ordering());
        assert!(ResolvedType::String.supports_ordering());
        assert!(!ResolvedType::Boolean.supports_ordering());
        assert!(!ResolvedType::Array.supports_ordering());
    }

    #[test]
    fn test_supports_contains() {
        assert!(ResolvedType::String.supports_contains());
        assert!(ResolvedType::Array.supports_contains());
        assert!(!ResolvedType::Integer.supports_contains());
        assert!(!ResolvedType::Object.supports_contains());
    }

    #[test]
    fn test_is_numeric() {
        assert!(ResolvedType::Integer.is_numeric());
        assert!(ResolvedType::Float.is_numeric());
        assert!(!ResolvedType::String.is_numeric());
    }

    #[test]
    fn test_schema_field_type() {
        let schema = Schema {
            fields: vec![
                FieldSchema { name: "name".into(), r#type: ResolvedType::String, required: true },
                FieldSchema { name: "age".into(), r#type: ResolvedType::Integer, required: false },
            ],
        };
        assert_eq!(schema.field_type("name"), Some(&ResolvedType::String));
        assert_eq!(schema.field_type("age"), Some(&ResolvedType::Integer));
        assert_eq!(schema.field_type("missing"), None);
    }

    #[test]
    fn test_schema_is_valid() {
        let schema = Schema {
            fields: vec![
                FieldSchema { name: "name".into(), r#type: ResolvedType::String, required: true },
                FieldSchema { name: "age".into(), r#type: ResolvedType::Integer, required: true },
            ],
        };
        let valid = serde_json::json!({"name": "alice", "age": 30});
        assert!(schema.is_valid(&valid).is_ok());

        let missing = serde_json::json!({"name": "alice"});
        assert!(schema.is_valid(&missing).is_err());
        assert!(schema.is_valid(&missing).unwrap_err().contains("age"));

        let wrong_type = serde_json::json!({"name": "alice", "age": "thirty"});
        assert!(schema.is_valid(&wrong_type).is_err());
        assert!(schema.is_valid(&wrong_type).unwrap_err().contains("age"));
    }

    #[test]
    fn test_schema_is_valid_non_object() {
        let schema = Schema { fields: vec![] };
        let result = schema.is_valid(&serde_json::json!("string"));
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not an object"));
    }

    #[test]
    fn test_schema_serialization() {
        let schema = Schema {
            fields: vec![
                FieldSchema { name: "x".into(), r#type: ResolvedType::Integer, required: true },
            ],
        };
        let bytes = bincode::serialize(&schema).unwrap();
        let deserialized: Schema = bincode::deserialize(&bytes).unwrap();
        assert_eq!(deserialized.fields.len(), 1);
        assert_eq!(deserialized.fields[0].name, "x");
    }
}
