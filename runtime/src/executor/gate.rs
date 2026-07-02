use crate::bytecode::instruction::Instruction;
use crate::bytecode::opcode::OpCode;
use crate::bytecode::plan::ExecutionPlan;
use crate::bytecode::resolved_type::ResolvedType;
use crate::executor::helpers;

/// Evaluate a Gate condition. Returns `true` if the condition passes (fall through),
/// `false` if the true branch should be skipped.
pub fn evaluate(body: &[u8], instr: &Instruction, plan: &ExecutionPlan, arena: &crate::memory::arena::Arena) -> bool {
    let field_path = plan.const_pool.get(instr.a);

    if matches!(plan.schema.as_ref().and_then(|s| s.field_type(field_path)), Some(ResolvedType::Any)) {
        eprintln!("[warn] gate operates on field '{}' typed 'any' — no compile-time type checking", field_path);
    }
    let compare_val_str = plan.const_pool.get(instr.b);
    let gate_op = instr.flags;

    let field_val = helpers::extract_json_field(body, field_path, arena);
    match field_val {
        Some(val) => helpers::compare_values(val, gate_op, compare_val_str),
        None => false,
    }
}

/// Compute the number of instructions to skip when a Gate condition fails.
/// Scans forward from `ip` (which points to the JumpOffset after the Gate)
/// to the next Gate, Label, or end of instructions.
pub fn skip_count(plan: &ExecutionPlan, ip: usize) -> usize {
    let mut count = 1;
    for i in (ip + 1)..plan.instructions.len() {
        if plan.instructions[i].op == OpCode::Gate || plan.instructions[i].op == OpCode::Label {
            break;
        }
        count += 1;
    }
    count
}
