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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bytecode::{Schema, FieldSchema};

    fn make_plan(schema_type: Option<ResolvedType>) -> ExecutionPlan {
        let mut plan = ExecutionPlan::new("test");
        plan.const_pool.add("x");
        plan.const_pool.add("1");
        plan.const_pool.add("hello");
        if let Some(t) = schema_type {
            plan.schema = Some(Schema {
                fields: vec![FieldSchema { name: "x".into(), r#type: t, required: false }],
            });
        }
        plan
    }

    fn arena() -> &'static crate::memory::arena::Arena {
        Box::leak(Box::new(crate::memory::arena::Arena::new()))
    }

    fn gate_instr(op: u8, field_const: u16, val_const: u16) -> Instruction {
        Instruction::new(OpCode::Gate, op, field_const, val_const, 0)
    }

    #[test]
    fn test_gate_eq_true() {
        let plan = make_plan(None);
        let instr = gate_instr(0, 0, 1); // x == 1
        let result = evaluate(b"{\"x\":1}", &instr, &plan, &arena());
        assert!(result);
    }

    #[test]
    fn test_gate_eq_false() {
        let plan = make_plan(None);
        let instr = gate_instr(0, 0, 1); // x == 1
        let result = evaluate(b"{\"x\":2}", &instr, &plan, &arena());
        assert!(!result);
    }

    #[test]
    fn test_gate_ne_true() {
        let plan = make_plan(None);
        let instr = gate_instr(1, 0, 1); // x != 1
        let result = evaluate(b"{\"x\":2}", &instr, &plan, &arena());
        assert!(result);
    }

    #[test]
    fn test_gate_gt() {
        let plan = make_plan(None);
        let instr = gate_instr(2, 0, 1); // x > 1
        assert!(evaluate(b"{\"x\":2}", &instr, &plan, &arena()));
        assert!(!evaluate(b"{\"x\":0}", &instr, &plan, &arena()));
        assert!(!evaluate(b"{\"x\":1}", &instr, &plan, &arena()));
    }

    #[test]
    fn test_gate_lt() {
        let plan = make_plan(None);
        let instr = gate_instr(3, 0, 1); // x < 1
        assert!(evaluate(b"{\"x\":0}", &instr, &plan, &arena()));
        assert!(!evaluate(b"{\"x\":2}", &instr, &plan, &arena()));
    }

    #[test]
    fn test_gate_gte() {
        let plan = make_plan(None);
        let instr = gate_instr(4, 0, 1); // x >= 1
        assert!(evaluate(b"{\"x\":1}", &instr, &plan, &arena()));
        assert!(evaluate(b"{\"x\":2}", &instr, &plan, &arena()));
        assert!(!evaluate(b"{\"x\":0}", &instr, &plan, &arena()));
    }

    #[test]
    fn test_gate_lte() {
        let plan = make_plan(None);
        let instr = gate_instr(5, 0, 1); // x <= 1
        assert!(evaluate(b"{\"x\":1}", &instr, &plan, &arena()));
        assert!(evaluate(b"{\"x\":0}", &instr, &plan, &arena()));
        assert!(!evaluate(b"{\"x\":2}", &instr, &plan, &arena()));
    }

    #[test]
    fn test_gate_contains() {
        let mut p = ExecutionPlan::new("test");
        p.const_pool.add("x");
        p.const_pool.add("ell");
        let instr2 = gate_instr(6, 0, 1); // x contains "ell"
        assert!(evaluate(b"{\"x\":\"hello\"}", &instr2, &p, &arena()));
        assert!(!evaluate(b"{\"x\":\"world\"}", &instr2, &p, &arena()));
    }

    #[test]
    fn test_gate_missing_field() {
        let plan = make_plan(None);
        let instr = gate_instr(0, 0, 1);
        assert!(!evaluate(b"{}", &instr, &plan, &arena()));
    }

    #[test]
    fn test_gate_with_any_schema_type() {
        let plan = make_plan(Some(ResolvedType::Any));
        let instr = gate_instr(0, 0, 1);
        assert!(evaluate(b"{\"x\":1}", &instr, &plan, &arena()));
    }

    #[test]
    fn test_skip_count_to_next_gate() {
        let mut plan = ExecutionPlan::new("test");
        plan.add_instr(Instruction::jump_offset(0)); // index 0 = after gate, ip starts here
        plan.add_instr(Instruction::next(1, 0));     // index 1
        plan.add_instr(Instruction::gate(0, 0, 1));  // index 2 - stop here
        let count = skip_count(&plan, 0);
        // count=1 (initial), i=1: next not Gate/Label, count=2, i=2: gate -> break
        assert_eq!(count, 2);
    }

    #[test]
    fn test_skip_count_to_label() {
        let mut plan = ExecutionPlan::new("test");
        plan.add_instr(Instruction::jump_offset(0)); // index 0
        plan.add_instr(Instruction::next(1, 0));     // index 1
        plan.add_instr(Instruction::label());        // index 2 - stop here
        let count = skip_count(&plan, 0);
        assert_eq!(count, 2);
    }

    #[test]
    fn test_skip_count_end_of_instructions() {
        let mut plan = ExecutionPlan::new("test");
        plan.add_instr(Instruction::jump_offset(0));
        plan.add_instr(Instruction::next(1, 0));

        let count = skip_count(&plan, 0);
        // i=1: next, not Gate/Label, count=2
        // i=2: out of bounds, loop ends
        assert_eq!(count, 2);
    }
}
