#![cfg(test)]

use super::*;
use crate::bytecode::opcode::OpCode;
use crate::bytecode::plan::ExecutionPlan;
use crate::bytecode::resolved_type::ResolvedType;
use crate::dsl::lexer;
use crate::dsl::optimizer::Optimizer;
use crate::dsl::parser;

fn compile_str(dsl: &str, rule_id: &str) -> Result<ExecutionPlan, CompileError> {
    let tokens = lexer::lex(dsl).unwrap();
    let pipeline = parser::parse(&tokens).unwrap();
    let opt = Optimizer::new();
    let optimized = opt.optimize(&pipeline);
    let compiler = Compiler::new();
    compiler.compile(&optimized, rule_id)
}

#[test]
fn test_compile_next() {
    let plan = compile_str("n:validate", "test").unwrap();
    assert!(plan.instr_count > 0);
}

#[test]
fn test_compile_dag() {
    let plan = compile_str("dag:{A:[B,C],D:[A]} e:audit", "dag-test").unwrap();
    assert_eq!(plan.dag_tables.len(), 1);
    let dag = &plan.dag_tables[0];
    assert!(!dag.layers.is_empty());
}

#[test]
fn test_dag_cycle_detection() {
    let result = compile_str("dag:{A:[B],B:[A]} e:audit", "cycle-test");
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), CompileError::DagCycle(_)));
}

#[test]
fn test_dag_empty_error() {
    let result = compile_str("dag:{} e:audit", "empty-test");
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), CompileError::DagEmpty(_)));
}

#[test]
fn test_dag_unknown_service() {
    let result = compile_str("dag:{A:[X]} e:audit", "unknown-test");
    assert!(result.is_ok());
}

#[test]
fn test_compile_timeout_hoisted() {
    let plan = compile_str("t500 n:validate", "timeout-test").unwrap();
    let next_instr = plan
        .instructions
        .iter()
        .find(|i| i.op == OpCode::Next)
        .expect("should have Next instruction");
    assert_eq!(next_instr.timeout_ms(), 500);
}

#[test]
fn test_compile_retry_attached() {
    let plan = compile_str("n:validate r3", "retry-test").unwrap();
    let next_instr = plan
        .instructions
        .iter()
        .find(|i| i.op == OpCode::Next)
        .expect("should have Next instruction");
    assert!(next_instr.has_retry());
    assert_eq!(plan.retry_configs.len(), 1);
}

#[test]
fn test_compile_chunk() {
    let plan = compile_str("chunk:5:par n:storage", "chunk-test").unwrap();
    let chunk_instrs: Vec<&Instruction> = plan
        .instructions
        .iter()
        .filter(|i| i.op == OpCode::Chunk)
        .collect();
    assert_eq!(chunk_instrs.len(), 1);
    assert_eq!(chunk_instrs[0].a, 5);
}

#[test]
fn test_compile_async() {
    let plan = compile_str("a:job-queue e:analytics", "async-test").unwrap();
    let async_instrs: Vec<&Instruction> = plan
        .instructions
        .iter()
        .filter(|i| i.op == OpCode::Async)
        .collect();
    assert_eq!(async_instrs.len(), 1);
}

#[test]
fn test_compile_schema() {
    let plan = compile_str("schema:{name:string,!age:int} n:svc", "schema-test").unwrap();
    let schema = plan.schema.as_ref().expect("should have schema");
    assert_eq!(schema.fields.len(), 2);
    assert_eq!(schema.fields[0].name, "name");
    assert_eq!(schema.fields[0].r#type, ResolvedType::String);
    assert!(!schema.fields[0].required);
    assert_eq!(schema.fields[1].name, "age");
    assert_eq!(schema.fields[1].r#type, ResolvedType::Integer);
    assert!(schema.fields[1].required);
    let has_type_guard = plan.instructions.iter().any(|i| i.op == OpCode::TypeGuard);
    assert!(has_type_guard);
}

#[test]
fn test_compile_schema_empty_error() {
    let result = compile_str("schema:{} n:svc", "schema-empty");
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), CompileError::SchemaParseError(_)));
}

#[test]
fn test_compile_full_pipeline() {
    let result = compile_str(
        "t500 n:validate t1000 p:fraud,inventory c f:dlq n:fulfill e:notify,analytics",
        "full-test",
    );
    assert!(result.is_ok());
}

// --- Type-checking tests ---

#[test]
fn test_type_check_gate_ordering_on_numeric() {
    let result = compile_str(
        "schema:{amount:float} g:amount>10000 n:svc",
        "tc-ordering-num",
    );
    assert!(result.is_ok());
}

#[test]
fn test_type_check_gate_ordering_on_string() {
    let result = compile_str(
        "schema:{name:string} g:name>\"m\" n:svc",
        "tc-ordering-str",
    );
    assert!(result.is_ok());
}

#[test]
fn test_type_check_gate_ordering_on_bool_error() {
    let result = compile_str(
        "schema:{flag:bool} g:flag>true n:svc",
        "tc-ordering-bool",
    );
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), CompileError::TypeMismatch(_)));
}

#[test]
fn test_type_check_gate_ordering_on_object_error() {
    let result = compile_str(
        "schema:{data:object} g:data>x n:svc",
        "tc-ordering-obj",
    );
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), CompileError::TypeMismatch(_)));
}

#[test]
fn test_type_check_gate_equals_on_any_type() {
    let result = compile_str(
        "schema:{flag:bool} g:flag==true n:svc",
        "tc-eq-bool",
    );
    assert!(result.is_ok());

    let result = compile_str(
        "schema:{data:object} g:data==null n:svc",
        "tc-eq-obj",
    );
    assert!(result.is_ok());

    let result = compile_str(
        "schema:{tags:array} g:tags!=null n:svc",
        "tc-eq-arr",
    );
    assert!(result.is_ok());
}

#[test]
fn test_type_check_gate_contains_on_string() {
    let result = compile_str(
        "schema:{name:string} g:name.containssmith n:svc",
        "tc-contains-str",
    );
    assert!(result.is_ok());
}

#[test]
fn test_type_check_gate_contains_on_array() {
    let result = compile_str(
        "schema:{tags:array} g:tags.containsurgent n:svc",
        "tc-contains-arr",
    );
    assert!(result.is_ok());
}

#[test]
fn test_type_check_gate_contains_on_numeric_error() {
    let result = compile_str(
        "schema:{amount:float} g:amount.containsx n:svc",
        "tc-contains-num",
    );
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), CompileError::TypeMismatch(_)));
}

#[test]
fn test_type_check_gate_field_not_in_schema() {
    let result = compile_str(
        "schema:{name:string} g:unknown>5 n:svc",
        "tc-unknown-field",
    );
    assert!(result.is_ok());
}

#[test]
fn test_type_check_no_schema_skips_validation() {
    let result = compile_str(
        "g:amount>10000 n:svc",
        "tc-no-schema-ordering",
    );
    assert!(result.is_ok());

    let result = compile_str(
        "g:flag==true n:svc",
        "tc-no-schema-eq",
    );
    assert!(result.is_ok());
}

#[test]
fn test_type_check_map_concat_string() {
    let result = compile_str(
        "schema:{first:string,last:string} m:full=concat(.first,.last) n:svc",
        "tc-map-concat-str",
    );
    assert!(result.is_ok());
}

#[test]
fn test_type_check_map_concat_non_string_error() {
    let result = compile_str(
        "schema:{amount:float,last:string} m:full=concat(.amount,.last) n:svc",
        "tc-map-concat-num",
    );
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), CompileError::TypeMismatch(_)));
}

#[test]
fn test_type_check_map_no_concat_skips() {
    let result = compile_str(
        "schema:{x:float} m:out=.x n:svc",
        "tc-map-no-concat",
    );
    assert!(result.is_ok());
}

#[test]
fn test_type_check_with_dag_and_schema() {
    let result = compile_str(
        "schema:{id:string} dag:{A:[B]} g:id!=null e:audit",
        "tc-dag-schema",
    );
    assert!(result.is_ok());
}

// --- Enum type tests ---

#[test]
fn test_compile_schema_enum() {
    let plan = compile_str(
        "schema:{role:enum[admin|user|guest]} n:svc",
        "schema-enum",
    )
    .unwrap();
    let schema = plan.schema.as_ref().expect("should have schema");
    assert_eq!(schema.fields.len(), 1);
    assert_eq!(schema.fields[0].name, "role");
    match &schema.fields[0].r#type {
        ResolvedType::Enum(variants) => {
            assert_eq!(variants.len(), 3);
            assert!(variants.contains(&"admin".to_string()));
            assert!(variants.contains(&"user".to_string()));
            assert!(variants.contains(&"guest".to_string()));
        }
        _ => panic!("expected Enum type"),
    }
}

#[test]
fn test_compile_schema_enum_empty_error() {
    let result = compile_str(
        "schema:{role:enum[]} n:svc",
        "schema-enum-empty",
    );
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), CompileError::SchemaParseError(_)));
}

#[test]
fn test_compile_schema_enum_required() {
    let plan = compile_str(
        "schema:{!role:enum[admin|user]} n:svc",
        "schema-enum-req",
    )
    .unwrap();
    let schema = plan.schema.as_ref().expect("should have schema");
    assert!(schema.fields[0].required);
    match &schema.fields[0].r#type {
        ResolvedType::Enum(variants) => {
            assert_eq!(variants.len(), 2);
        }
        _ => panic!("expected Enum type"),
    }
}

#[test]
fn test_type_check_gate_enum_ordering_error() {
    let result = compile_str(
        "schema:{role:enum[admin|user]} g:role>admin n:svc",
        "tc-enum-ordering",
    );
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), CompileError::TypeMismatch(_)));
}

#[test]
fn test_type_check_gate_enum_eq_ok() {
    let result = compile_str(
        "schema:{role:enum[admin|user]} g:role==admin n:svc",
        "tc-enum-eq",
    );
    assert!(result.is_ok());
}
