use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ResolvedType {
    String,
    Integer,
    Float,
    Boolean,
    Object,
    Array,
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
