use super::error::CompileError;
use crate::bytecode::resolved_type::{FieldSchema, ResolvedType, Schema};

pub fn compile_schema(body: &str) -> Result<Schema, CompileError> {
    let content = body
        .trim_start_matches('{')
        .trim_end_matches('}')
        .trim();
    if content.is_empty() {
        return Err(CompileError::SchemaParseError("empty schema body".into()));
    }
    let mut fields = Vec::new();
    for segment in content.split(',') {
        let seg = segment.trim();
        if seg.is_empty() {
            continue;
        }
        let required = seg.starts_with('!');
        let name_part = if required { &seg[1..] } else { seg };
        let parts: Vec<&str> = name_part.split(':').collect();
        if parts.len() < 2 || parts[0].is_empty() || parts[1].is_empty() {
            return Err(CompileError::SchemaParseError(format!(
                "invalid field spec: '{}'",
                seg
            )));
        }
        let name = parts[0].trim().to_string();
        let type_str = parts[1].trim().to_lowercase();
        let r#type = if type_str.starts_with("enum[") && type_str.ends_with(']') {
            let inner = &type_str[5..type_str.len() - 1];
            let variants: Vec<String> = inner
                .split('|')
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .collect();
            if variants.is_empty() {
                return Err(CompileError::SchemaParseError(format!(
                    "empty enum variants for field '{}'",
                    name
                )));
            }
            ResolvedType::Enum(variants)
        } else {
            match type_str.as_str() {
                "string" => ResolvedType::String,
                "int" => ResolvedType::Integer,
                "float" => ResolvedType::Float,
                "bool" => ResolvedType::Boolean,
                "object" => ResolvedType::Object,
                "array" => ResolvedType::Array,
                "null" => ResolvedType::Null,
                "any" => ResolvedType::Any,
                _ => {
                    return Err(CompileError::SchemaParseError(format!(
                        "unknown type '{}' for field '{}'",
                        type_str, name
                    )));
                }
            }
        };
        fields.push(FieldSchema { name, r#type, required });
    }
    Ok(Schema { fields })
}
