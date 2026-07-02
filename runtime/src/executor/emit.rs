use crate::bytecode::instruction::Instruction;
use crate::bytecode::plan::ExecutionPlan;

pub fn exec_emit(
    body: &[u8],
    instr: &Instruction,
    plan: &ExecutionPlan,
    caller: &dyn Fn(u16, &[u8], u64) -> Result<Vec<u8>, String>,
) -> Result<(), String> {
    let first_svc = instr.b as usize;
    let count = instr.a as u8;

    for offset in 0..count as usize {
        let svc_idx = first_svc + offset;
        caller(
            plan.services.entries()[svc_idx as usize].id,
            body,
            0,
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mock_caller(_svc_id: u16, body: &[u8], _timeout: u64) -> Result<Vec<u8>, String> {
        Ok(body.to_vec())
    }

    fn make_plan_with_services(names: &[&str]) -> ExecutionPlan {
        let mut plan = ExecutionPlan::new("test");
        for name in names {
            plan.services.add(name);
        }
        plan
    }

    #[test]
    fn test_emit_single_service() {
        let plan = make_plan_with_services(&["notify"]);
        let instr = Instruction::emit(1, 0); // count=1, first_svc=0
        let result = exec_emit(b"hello", &instr, &plan, &mock_caller);
        assert!(result.is_ok());
    }

    #[test]
    fn test_emit_multiple_services() {
        let plan = make_plan_with_services(&["a", "b", "c"]);
        let call_count = std::cell::Cell::new(0u32);
        let counter = &call_count;
        let caller = |_svc_id: u16, _body: &[u8], _timeout: u64| -> Result<Vec<u8>, String> {
            counter.set(counter.get() + 1);
            Ok(vec![])
        };
        let instr = Instruction::emit(3, 0);
        exec_emit(b"hello", &instr, &plan, &caller).unwrap();
        assert_eq!(call_count.get(), 3);
    }

    #[test]
    fn test_emit_with_offset() {
        let plan = make_plan_with_services(&["first", "second", "third"]);
        let called = std::cell::RefCell::new(Vec::new());
        let caller = |svc_id: u16, _body: &[u8], _timeout: u64| -> Result<Vec<u8>, String> {
            called.borrow_mut().push(svc_id);
            Ok(vec![])
        };
        let instr = Instruction::emit(2, 1); // count=2, starting at index 1
        exec_emit(b"hello", &instr, &plan, &caller).unwrap();
        assert_eq!(called.borrow().len(), 2);
        assert_eq!(called.borrow()[0], 1); // "second"
        assert_eq!(called.borrow()[1], 2); // "third"
    }

    #[test]
    fn test_emit_propagates_caller_error() {
        let plan = make_plan_with_services(&["fail"]);
        let caller = |_svc_id: u16, _body: &[u8], _timeout: u64| -> Result<Vec<u8>, String> {
            Err("emit error".to_string())
        };
        let instr = Instruction::emit(1, 0);
        let result = exec_emit(b"hello", &instr, &plan, &caller);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "emit error");
    }
}
