use std::collections::{HashMap, HashSet};
use std::fmt;

use super::parser::ASTNode;
use super::optimizer::OptimizedPipeline;
use crate::bytecode::instruction::Instruction;
use crate::bytecode::opcode::{ChunkMode, GateOp, OpCode, RetryStrategy};
use crate::bytecode::plan::{ChunkConfig, ExecutionPlan, RetryConfig};
use crate::bytecode::resolved_type::{FieldSchema, ResolvedType, Schema};

#[derive(Debug)]
pub enum CompileError {
    UnknownTarget(String),
    DagCycle(String),
    DagEmpty(String),
    DagUnknownService(String),
    DuplicateLabel(String),
    UnknownLabel(String),
    UnterminatedPipeline(String),
    SchemaParseError(String),
    TypeMismatch(String),
}

impl fmt::Display for CompileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompileError::UnknownTarget(t) => write!(f, "unknown target: {}", t),
            CompileError::DagCycle(d) => write!(f, "DAG contains cycle: {}", d),
            CompileError::DagEmpty(_) => write!(f, "DAG is empty (use p:+c: instead)"),
            CompileError::DagUnknownService(s) => {
                write!(f, "DAG references unknown service: {}", s)
            }
            CompileError::DuplicateLabel(l) => write!(f, "duplicate label: {}", l),
            CompileError::UnknownLabel(l) => write!(f, "unknown label: {}", l),
            CompileError::UnterminatedPipeline(s) => write!(f, "unterminated pipeline: {}", s),
            CompileError::SchemaParseError(s) => write!(f, "schema parse error: {}", s),
            CompileError::TypeMismatch(s) => write!(f, "type mismatch: {}", s),
        }
    }
}

impl std::error::Error for CompileError {}

pub struct Compiler {
    pub targets: Vec<String>,
}

impl Compiler {
    pub fn new(targets: &[String]) -> Self {
        Compiler {
            targets: targets.to_vec(),
        }
    }

    pub fn compile(
        &self,
        pipeline: &OptimizedPipeline,
        rule_id: &str,
    ) -> Result<ExecutionPlan, CompileError> {
        let mut plan = ExecutionPlan::new(rule_id);

        // Pre-pass: extract and parse schema for type checking
        let mut parsed_schema: Option<Schema> = None;
        for node in &pipeline.nodes {
            if let ASTNode::Schema(body) = node {
                let schema = self.compile_schema(body)?;
                parsed_schema = Some(schema);
                break;
            }
        }

        // Type-checking pass: validate Gate and Map compatibility against schema
        if let Some(ref schema) = parsed_schema {
            for node in &pipeline.nodes {
                match node {
                    ASTNode::Gate { field, op, value } => {
                        Self::type_check_gate(schema, field, op, value)?;
                    }
                    ASTNode::Map(expr) => {
                        Self::type_check_map(schema, expr)?;
                    }
                    _ => {}
                }
            }
        }

        let mut labels: HashMap<String, usize> = HashMap::new();
        let mut instructions: Vec<Instruction> = Vec::new();
        let mut pending_retry: Option<RetryConfig> = None;
        let mut pending_timeout_ms: Option<u64> = None;

        for node in &pipeline.nodes {
            match node {
                ASTNode::Label(name) => {
                    if labels.contains_key(name) {
                        return Err(CompileError::DuplicateLabel(name.clone()));
                    }
                    labels.insert(name.clone(), instructions.len());
                    instructions.push(Instruction::label());
                }
                ASTNode::Jmp(target) => {
                    instructions.push(Instruction::jmp(0));
                    instructions.push(Instruction::jump_offset(0));
                    let label_idx = instructions.len() - 1;
                    let target_idx = labels.get(target).copied();
                    instructions[label_idx] = Instruction::jump_offset(
                        target_idx.unwrap_or(0) as u16,
                    );
                    instructions[label_idx - 1] = Instruction::jmp(
                        target_idx.unwrap_or(0) as u16,
                    );
                }
                ASTNode::Next(target) => {
                    let svc_id = self.resolve_service(&mut plan, target);
                    let timeout = pending_timeout_ms.take().unwrap_or(0);
                    instructions.push(Instruction::next(svc_id, timeout));
                }
                ASTNode::Async(target) => {
                    let svc_id = self.resolve_service(&mut plan, target);
                    let timeout = pending_timeout_ms.take().unwrap_or(0);
                    instructions.push(Instruction::async_svc(svc_id, timeout));
                }
                ASTNode::Parallel(targets) => {
                    let ids: Vec<u16> = targets
                        .iter()
                        .map(|t| self.resolve_service(&mut plan, t))
                        .collect();
                    instructions.push(Instruction::parallel(ids.len() as u8, ids[0]));
                    for &id in &ids {
                        instructions.push(Instruction::svc_arg(id));
                    }
                }
                ASTNode::Collect => {
                    instructions.push(Instruction::collect());
                }
                ASTNode::Fallback(target) => {
                    let svc_id = self.resolve_service(&mut plan, target);
                    instructions.push(Instruction::fallback(svc_id));
                }
                ASTNode::Gate { field, op, value } => {
                    let field_id = plan.const_pool.add(field);
                    let value_id = plan.const_pool.add(value);
                    let gate_op = GateOp::from_str(op).unwrap_or(GateOp::Eq);
                    let mut instr = Instruction::gate(field_id, gate_op as u8, value_id);
                    instr.flags = gate_op as u8;
                    instructions.push(instr);
                    instructions.push(Instruction::jump_offset(0));
                }
                ASTNode::Emit(targets) => {
                    let ids: Vec<u16> = targets
                        .iter()
                        .map(|t| self.resolve_service(&mut plan, t))
                        .collect();
                    instructions.push(Instruction::emit(ids.len() as u8, ids[0]));
                    for &id in &ids {
                        instructions.push(Instruction::svc_arg(id));
                    }
                }
                ASTNode::Drop => {
                    instructions.push(Instruction::drop());
                }
                ASTNode::Buffer(n) => {
                    instructions.push(Instruction::buffer(*n as u8));
                }
                ASTNode::Key(field) => {
                    let field_id = plan.const_pool.add(field);
                    instructions.push(Instruction::set_key(field_id));
                }
                ASTNode::Split(field) => {
                    let field_id = plan.const_pool.add(field);
                    instructions.push(Instruction::set_key(field_id));
                }
                ASTNode::Map(expr) => {
                    let expr_id = plan.const_pool.add(expr);
                    instructions.push(Instruction::map(expr_id));
                }
                ASTNode::Timeout(ms) => {
                    pending_timeout_ms = Some(*ms);
                }
                ASTNode::Retry {
                    count,
                    strategy,
                    fixed_ms,
                } => {
                    let strategy_enum = match strategy.as_deref() {
                        Some("exp") | None => RetryStrategy::Exponential,
                        Some("linear") => RetryStrategy::Linear,
                        Some("fixed") => RetryStrategy::Fixed,
                        _ => RetryStrategy::Exponential,
                    };
                    pending_retry = Some(RetryConfig {
                        max_attempts: *count,
                        strategy: strategy_enum,
                        fixed_ms: fixed_ms.unwrap_or(0),
                    });
                }
                ASTNode::Chunk { count, mode } => {
                    let cm = match mode.as_str() {
                        "par" => ChunkMode::Parallel,
                        _ => ChunkMode::Sequential,
                    };
                    let cfg = ChunkConfig {
                        count: *count,
                        mode: cm,
                    };
                    plan.chunk_configs.push(cfg);
                    instructions.push(Instruction::chunk(*count, cm as u8));
                }
                ASTNode::Dag(body) => {
                    let dag_id = self.compile_dag(&mut plan, body)?;
                    instructions.push(Instruction::dag(dag_id));
                }
                ASTNode::Schema(_body) => {
                    // Schema already parsed in pre-pass; just emit type guard
                    instructions.push(Instruction::type_guard(1));
                }
                ASTNode::Pipe => {}
            }
        }

        // Set schema from pre-pass
        if let Some(schema) = parsed_schema {
            plan.schema = Some(schema);
        }

        // Second pass: attach pending retry to preceding Next/Async
        if let Some(retry_cfg) = pending_retry {
            for instr in instructions.iter_mut().rev() {
                match instr.op {
                    OpCode::Next | OpCode::Async => {
                        instr.flags |= 0x01;
                        let cfg_id = plan.retry_configs.len() as u16;
                        plan.retry_configs.push(retry_cfg);
                        instr.c = cfg_id;
                        break;
                    }
                    OpCode::Timeout | OpCode::RetryData => continue,
                    _ => break,
                }
            }
        }

        for instr in instructions {
            plan.add_instr(instr);
        }

        plan.complexity_score = calc_complexity(&plan);
        Ok(plan)
    }

    /// Type-check a Gate node against schema.
    /// Returns TypeMismatch error if the operator is incompatible with the field type.
    fn type_check_gate(
        schema: &Schema,
        field: &str,
        op: &str,
        _value: &str,
    ) -> Result<(), CompileError> {
        let field_type = match schema.field_type(field) {
            Some(t) => t,
            None => return Ok(()), // field not in schema, skip check
        };
        match op {
            "==" | "!=" => Ok(()), // equality works on all types
            ">" | "<" | ">=" | "<=" => {
                if !field_type.supports_ordering() {
                    Err(CompileError::TypeMismatch(format!(
                        "operator '{op}' requires numeric or string type, but field '{field}' has type {ft:?}",
                        op = op, field = field, ft = field_type
                    )))
                } else {
                    Ok(())
                }
            }
            "contains" => {
                if !field_type.supports_contains() {
                    Err(CompileError::TypeMismatch(format!(
                        "operator 'contains' requires string or array type, but field '{field}' has type {ft:?}",
                        field = field, ft = field_type
                    )))
                } else {
                    Ok(())
                }
            }
            _ => Ok(()), // unknown operator, let runtime handle it
        }
    }

    /// Type-check a Map expression against schema for basic compatibility.
    fn type_check_map(schema: &Schema, expr: &str) -> Result<(), CompileError> {
        // Extract the right-hand side of the map (after '=')
        let rhs = if let Some(eq_pos) = expr.find('=') {
            expr[eq_pos + 1..].trim()
        } else {
            expr.trim()
        };
        if rhs.is_empty() {
            return Ok(());
        }

        // Check for `concat()` function calls: all field arguments must be strings
        if rhs.starts_with("concat(") && rhs.ends_with(')') {
            let args_str = &rhs[7..rhs.len() - 1];
            for arg in args_str.split(',') {
                let arg = arg.trim();
                if arg.starts_with('.') {
                    let field_name = &arg[1..];
                    if let Some(ft) = schema.field_type(field_name) {
                        if !matches!(ft, ResolvedType::String) {
                            return Err(CompileError::TypeMismatch(format!(
                                "concat() requires string type, but field '{field_name}' has type {ft:?}",
                                field_name = field_name, ft = ft
                            )));
                        }
                    }
                }
            }
        }

        // Also check `+` operator (used in expression syntax without spaces)
        if rhs.contains('+') {
            let parts: Vec<&str> = rhs.split('+').collect();
            for part in &parts {
                let part = part.trim();
                if part.starts_with('.') {
                    let field_name = &part[1..];
                    if let Some(ft) = schema.field_type(field_name) {
                        if !matches!(ft, ResolvedType::String) {
                            return Err(CompileError::TypeMismatch(format!(
                                "concat '+' requires string type, but field '{field_name}' has type {ft:?}",
                                field_name = field_name, ft = ft
                            )));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn resolve_service(&self, plan: &mut ExecutionPlan, name: &str) -> u16 {
        plan.services.add(name)
    }

    fn compile_dag(&self, plan: &mut ExecutionPlan, body: &str) -> Result<u16, CompileError> {
        if body.is_empty() || body == "{}" {
            return Err(CompileError::DagEmpty(body.to_string()));
        }

        let content = body
            .trim_start_matches('{')
            .trim_end_matches('}')
            .trim();
        if content.is_empty() {
            return Err(CompileError::DagEmpty(body.to_string()));
        }

        let mut deps: HashMap<String, Vec<String>> = HashMap::new();
        for segment in content.split(',') {
            let seg = segment.trim();
            if seg.is_empty() {
                continue;
            }
            if let Some(colon_pos) = seg.find(':') {
                let node = seg[..colon_pos].trim().to_string();
                let dep_list = seg[colon_pos + 1..].trim();
                let inner = dep_list.trim_start_matches('[').trim_end_matches(']').trim();
                let node_deps: Vec<String> = if inner.is_empty() {
                    Vec::new()
                } else {
                    inner
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect()
                };
                for dep in &node_deps {
                    deps.entry(dep.clone()).or_insert_with(Vec::new);
                }
                deps.insert(node, node_deps);
            } else {
                let node = seg.to_string();
                deps.entry(node).or_insert_with(Vec::new);
            }
        }

        if deps.is_empty() {
            return Err(CompileError::DagEmpty(body.to_string()));
        }

        for node_deps in deps.values() {
            for dep in node_deps {
                if !deps.contains_key(dep) {
                    return Err(CompileError::DagUnknownService(dep.clone()));
                }
            }
        }

        if self.detect_cycle(&deps) {
            return Err(CompileError::DagCycle(body.to_string()));
        }

        let layers = self.topological_sort(&deps);
        let mut dag_table = crate::bytecode::dag_table::DAGTable::new();

        for (layer_idx, layer) in layers.iter().enumerate() {
            for node_name in layer {
                let svc_id = self.resolve_service(plan, node_name);
                dag_table.nodes.push(crate::bytecode::dag_table::DAGNode {
                    service_id: svc_id,
                    layer: layer_idx as u8,
                });
            }
            let svc_layer: Vec<u16> = layer
                .iter()
                .map(|n| plan.services.get_by_name(n).map(|e| e.id).unwrap_or(0))
                .collect();
            dag_table.layers.push(svc_layer);
        }

        let all_nodes: Vec<String> = deps.keys().cloned().collect();
        let depended_upon: HashSet<&str> =
            deps.values()
                .flat_map(|v| v.iter().map(|s| s.as_str()))
                .collect();
        for node in &all_nodes {
            if !depended_upon.contains(node.as_str()) {
                let svc_id = plan
                    .services
                    .get_by_name(node)
                    .map(|e| e.id)
                    .unwrap_or(0);
                dag_table.terminal_nodes.push(svc_id);
            }
        }

        let id = plan.dag_tables.len() as u16;
        plan.dag_tables.push(dag_table);
        Ok(id)
    }

    fn detect_cycle(&self, deps: &HashMap<String, Vec<String>>) -> bool {
        let mut visited: HashMap<&str, bool> = HashMap::new();
        let mut in_stack: HashMap<&str, bool> = HashMap::new();

        for node in deps.keys() {
            visited.insert(node.as_str(), false);
            in_stack.insert(node.as_str(), false);
        }

        for node in deps.keys() {
            if !visited.get(node.as_str()).copied().unwrap_or(false)
                && self.dfs_cycle(node, deps, &mut visited, &mut in_stack)
            {
                return true;
            }
        }
        false
    }

    fn dfs_cycle<'a>(
        &self,
        node: &'a str,
        deps: &'a HashMap<String, Vec<String>>,
        visited: &mut HashMap<&'a str, bool>,
        in_stack: &mut HashMap<&'a str, bool>,
    ) -> bool {
        visited.insert(node, true);
        in_stack.insert(node, true);

        if let Some(children) = deps.get(node) {
            for child in children {
                if let Some(&is_visited) = visited.get(child.as_str()) {
                    if !is_visited {
                        if self.dfs_cycle(child, deps, visited, in_stack) {
                            return true;
                        }
                    } else if let Some(&in_st) = in_stack.get(child.as_str()) {
                        if in_st {
                            return true;
                        }
                    }
                }
            }
        }

        in_stack.insert(node, false);
        false
    }

    fn topological_sort(
        &self,
        deps: &HashMap<String, Vec<String>>,
    ) -> Vec<Vec<String>> {
        let mut in_degree: HashMap<String, usize> = HashMap::new();
        let mut adj: HashMap<String, Vec<String>> = HashMap::new();

        for (node, node_deps) in deps {
            in_degree.entry(node.clone()).or_insert(0);
            adj.entry(node.clone()).or_insert_with(Vec::new);

            for dep in node_deps {
                in_degree.entry(dep.clone()).or_insert(0);
                adj.entry(dep.clone())
                    .or_insert_with(Vec::new)
                    .push(node.clone());
            }
        }

        let mut layers = Vec::new();
        let mut current_layer: Vec<String> = in_degree
            .iter()
            .filter(|(_, &deg)| deg == 0)
            .map(|(n, _)| n.clone())
            .collect();
        current_layer.sort();

        while !current_layer.is_empty() {
            layers.push(current_layer.clone());

            let mut next_layer = Vec::new();
            for node in &current_layer {
                if let Some(children) = adj.get(node) {
                    for child in children {
                        if let Some(deg) = in_degree.get_mut(child) {
                            if *deg == 0 {
                                continue;
                            }
                            *deg -= 1;
                            if *deg == 0 {
                                next_layer.push(child.clone());
                            }
                        }
                    }
                }
            }
            next_layer.sort();
            current_layer = next_layer;
        }

        layers
    }

    fn compile_schema(&self, body: &str) -> Result<Schema, CompileError> {
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
            if parts.len() != 2 || parts[0].is_empty() || parts[1].is_empty() {
                return Err(CompileError::SchemaParseError(format!(
                    "invalid field spec: '{}'",
                    seg
                )));
            }
            let name = parts[0].trim().to_string();
            let type_str = parts[1].trim().to_lowercase();
            let r#type = match type_str.as_str() {
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
            };
            fields.push(FieldSchema { name, r#type, required });
        }
        Ok(Schema { fields })
    }
}

fn calc_complexity(plan: &ExecutionPlan) -> u32 {
    let mut score: u32 = 0;
    for instr in &plan.instructions {
        use crate::bytecode::opcode::OpCode;
        match instr.op {
            OpCode::Next | OpCode::Async => score += 10,
            OpCode::Parallel | OpCode::Dag => score += 20,
            OpCode::Chunk => score += 25,
            OpCode::Gate => score += 5,
            OpCode::Map => score += 3,
            OpCode::Emit => score += 8,
            OpCode::Buffer => score += 15,
            _ => score += 1,
        }
    }
    score
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bytecode::opcode::OpCode;
    use crate::dsl::lexer;
    use crate::dsl::optimizer::Optimizer;
    use crate::dsl::parser;

    fn compile_str(dsl: &str, rule_id: &str) -> Result<ExecutionPlan, CompileError> {
        let tokens = lexer::lex(dsl).unwrap();
        let pipeline = parser::parse(&tokens).unwrap();
        let opt = Optimizer::new();
        let optimized = opt.optimize(&pipeline);
        let compiler = Compiler::new(&[]);
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
        // ==/!= must work on all types
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
        // Field not in schema should be allowed (dynamic)
        let result = compile_str(
            "schema:{name:string} g:unknown>5 n:svc",
            "tc-unknown-field",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_type_check_no_schema_skips_validation() {
        // Without schema, all gates should compile
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
        // concat() is how map expressions combine strings (not +, which doesn't lex)
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
}
