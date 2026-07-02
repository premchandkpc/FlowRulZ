use std::collections::HashMap;

use super::optimizer::OptimizedPipeline;
use super::parser::ASTNode;
use crate::bytecode::instruction::Instruction;
use crate::bytecode::opcode::{ChunkMode, GateOp, OpCode, RetryStrategy};
use crate::bytecode::plan::{ChunkConfig, ExecutionPlan, RetryConfig};

mod complexity;
mod dag;
mod error;
mod schema;
mod type_check;

pub use error::CompileError;

#[cfg(test)]
mod tests;

pub struct Compiler;

impl Default for Compiler {
    fn default() -> Self {
        Self::new()
    }
}

impl Compiler {
    pub fn new() -> Self {
        Compiler
    }

    pub fn compile(
        &self,
        pipeline: &OptimizedPipeline,
        rule_id: &str,
    ) -> Result<ExecutionPlan, CompileError> {
        let mut plan = ExecutionPlan::new(rule_id);

        let mut parsed_schema: Option<crate::bytecode::resolved_type::Schema> = None;
        for node in &pipeline.nodes {
            if let ASTNode::Schema(body) = node {
                let schema = schema::compile_schema(body)?;
                parsed_schema = Some(schema);
                break;
            }
        }

        if let Some(ref schema) = parsed_schema {
            for node in &pipeline.nodes {
                match node {
                    ASTNode::Gate { field, op, value } => {
                        type_check::type_check_gate(schema, field, op, value)?;
                    }
                    ASTNode::Map(expr) => {
                        type_check::type_check_map(schema, expr)?;
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
                    let gate_op = GateOp::parse(op).unwrap_or(GateOp::Eq);
                    instructions.push(Instruction::gate(field_id, gate_op as u8, value_id));
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
                    instructions.push(Instruction::type_guard(1));
                }
                ASTNode::Delay(ms) => {
                    instructions.push(Instruction::delay(*ms));
                }
                ASTNode::Pipe => {}
            }
        }

        if let Some(schema) = parsed_schema {
            plan.schema = Some(schema);
        }

        if let Some(retry_cfg) = pending_retry {
            if let Some(instr) = instructions.iter_mut().next_back() {
                match instr.op {
                    OpCode::Next | OpCode::Async => {
                        instr.flags |= 0x01;
                        let cfg_id = plan.retry_configs.len() as u16;
                        plan.retry_configs.push(retry_cfg);
                        instr.c = cfg_id;
                    }
                    _ => {}
                }
            }
        }

        for instr in instructions {
            plan.add_instr(instr);
        }

        plan.complexity_score = complexity::calc_complexity(&plan);
        Ok(plan)
    }

    pub fn resolve_service(&self, plan: &mut ExecutionPlan, name: &str) -> u16 {
        plan.services.add(name)
    }
}
