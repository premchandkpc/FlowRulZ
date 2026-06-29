pub mod chunk;
pub mod context;
pub mod dag;
pub mod emit;
pub mod expr;
pub mod gate;
pub mod helpers;
pub mod map;
pub mod next;
pub mod parallel;
pub mod runtime;

use std::sync::Arc;

use crate::bytecode::execution::ExecutionContext;
use crate::bytecode::instruction::Instruction;
use crate::bytecode::opcode::OpCode;
use crate::bytecode::plan::ExecutionPlan;

pub struct VM<'a> {
    pub ip: usize,
    pub plan: &'a ExecutionPlan,
    pub arena: crate::memory::arena::Arena,
    pub caller: Arc<dyn Fn(u16, &[u8], u64) -> Result<Vec<u8>, String> + Send + Sync + 'a>,
    pub ctx: ExecutionContext,
}

impl<'a> VM<'a> {
    pub fn new<F>(
        plan: &'a ExecutionPlan,
        ctx: ExecutionContext,
        arena: crate::memory::arena::Arena,
        caller: F,
    ) -> Self
    where
        F: Fn(u16, &[u8], u64) -> Result<Vec<u8>, String> + Send + Sync + 'a,
    {
        VM {
            ip: 0,
            plan,
            arena,
            caller: Arc::new(caller),
            ctx,
        }
    }

    pub fn run(&mut self) -> Result<(), String> {
        self.ip = 0;
        while self.ip < self.plan.instructions.len() {
            let instr = &self.plan.instructions[self.ip];
            self.ip += 1;
            self.dispatch(instr)?;
        }
        Ok(())
    }

    fn dispatch(&mut self, instr: &Instruction) -> Result<(), String> {
        let start = std::time::Instant::now();
        let caller = self.caller.clone();
        let result = match instr.op {
            OpCode::Next => self.op_next(instr, &*caller, false),
            OpCode::Async => self.op_next(instr, &*caller, true),
            OpCode::Parallel => self.op_parallel(instr, &*caller),
            OpCode::Collect => self.op_collect(),
            OpCode::Fallback => self.op_fallback(instr, &*caller),
            OpCode::Gate => self.op_gate(instr),
            OpCode::Emit => self.op_emit(instr, &*caller),
            OpCode::Drop => self.op_drop(),
            OpCode::Map => self.op_map(instr),
            OpCode::Dag => self.op_dag(instr, &*caller),
            OpCode::Jmp => self.op_jmp(instr),
            OpCode::Key | OpCode::Split => Ok(()),
            OpCode::Retry => Ok(()),
            OpCode::Buffer => Err("Buffer must be handled at engine level".to_string()),
            OpCode::Timeout => Ok(()),
            OpCode::Chunk => Ok(()),
            OpCode::Pipe => Ok(()),
            OpCode::Label => Ok(()),
            OpCode::SvcArg | OpCode::RetryData | OpCode::JumpOffset => Ok(()),
            OpCode::TypeGuard => self.op_type_guard(instr),
        };

        let duration_ns = start.elapsed().as_nanos() as u64;
        let status: u8 = match &result {
            Ok(()) => 0,
            Err(_) => 1,
        };
        let svc_id = instr.b;
        let layer = 0u8;
        crate::tracing::emit_span(crate::tracing::Span {
            opcode: instr.op as u8,
            service_id: svc_id,
            layer,
            duration_ns,
            status,
        });

        result
    }

    fn op_next(
        &mut self,
        instr: &Instruction,
        caller: &dyn Fn(u16, &[u8], u64) -> Result<Vec<u8>, String>,
        is_async: bool,
    ) -> Result<(), String> {
        match next::exec_next(&self.ctx.body, instr, self.plan, caller, is_async) {
            Ok(resp) => {
                self.ctx.body = resp;
                self.ctx.hop_count += 1;
                Ok(())
            }
            Err(e) => {
                self.ctx.failed = true;
                self.ctx.errors.push(e);
                Ok(())
            }
        }
    }

    fn op_parallel(
        &mut self,
        instr: &Instruction,
        caller: &dyn Fn(u16, &[u8], u64) -> Result<Vec<u8>, String>,
    ) -> Result<(), String> {
        let result =
            parallel::exec_parallel(&self.ctx.body, instr, self.plan, caller, &self.arena)?;
        self.ctx.body = result.to_vec();
        Ok(())
    }

    fn op_collect(&mut self) -> Result<(), String> {
        let result = parallel::exec_collect(&self.ctx.body, self.plan, &self.arena)?;
        self.ctx.body = result.to_vec();
        self.ctx.hop_count += 1;
        Ok(())
    }

    fn op_fallback(
        &mut self,
        instr: &Instruction,
        caller: &dyn Fn(u16, &[u8], u64) -> Result<Vec<u8>, String>,
    ) -> Result<(), String> {
        if self.ctx.failed {
            self.ctx.failed = false;
            match next::exec_next(&self.ctx.body, instr, self.plan, caller, false) {
                Ok(resp) => {
                    self.ctx.body = resp;
                    self.ctx.hop_count += 1;
                }
                Err(e) => {
                    self.ctx.failed = true;
                    self.ctx.errors.push(e);
                }
            }
        }
        Ok(())
    }

    fn op_gate(&mut self, instr: &Instruction) -> Result<(), String> {
        let mut skip = 0usize;
        gate::exec_jmp_if_false(&self.ctx.body, instr, self.plan, &self.arena, &mut skip);
        Ok(())
    }

    fn op_emit(
        &mut self,
        instr: &Instruction,
        caller: &dyn Fn(u16, &[u8], u64) -> Result<Vec<u8>, String>,
    ) -> Result<(), String> {
        emit::exec_emit(&self.ctx.body, instr, self.plan, caller)
    }

    fn op_drop(&mut self) -> Result<(), String> {
        self.ip = self.plan.instructions.len();
        Ok(())
    }

    fn op_map(&mut self, instr: &Instruction) -> Result<(), String> {
        let result = map::exec_map(&self.ctx.body, instr, self.plan, &self.arena)?;
        self.ctx.body = result.to_vec();
        Ok(())
    }

    fn op_dag(
        &mut self,
        instr: &Instruction,
        caller: &dyn Fn(u16, &[u8], u64) -> Result<Vec<u8>, String>,
    ) -> Result<(), String> {
        let result = dag::exec_dag(&self.ctx.body, instr, self.plan, caller, &self.arena)?;
        self.ctx.body = result.to_vec();
        self.ctx.hop_count += 1;
        Ok(())
    }

    fn op_jmp(&mut self, instr: &Instruction) -> Result<(), String> {
        self.ip = instr.a as usize;
        Ok(())
    }

    fn op_type_guard(&mut self, instr: &Instruction) -> Result<(), String> {
        let schema = match &self.plan.schema {
            Some(s) => s,
            None => {
                if instr.a == 0 {
                    return Ok(());
                }
                return Err("schema required but not provided".into());
            }
        };
        let body: serde_json::Value = serde_json::from_slice(&self.ctx.body)
            .map_err(|e| format!("TypeGuard: failed to parse body: {}", e))?;
        schema.is_valid(&body).map_err(|e| format!("TypeGuard: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bytecode::plan::ExecutionPlan;

    use crate::dsl::{compiler::Compiler, lexer, optimizer, parser};

    fn compile_dsl(dsl: &str) -> ExecutionPlan {
        let tokens = lexer::lex(dsl).unwrap();
        let pipeline = parser::parse(&tokens).unwrap();
        let opt = optimizer::Optimizer::new();
        let optimized = opt.optimize(&pipeline);
        let compiler = Compiler::new(&[]);
        compiler.compile(&optimized, "test").unwrap()
    }

    fn mock_caller(_svc_id: u16, body: &[u8], _timeout: u64) -> Result<Vec<u8>, String> {
        Ok(body.to_vec())
    }

    fn mock_failing_caller(_svc_id: u16, _body: &[u8], _timeout: u64) -> Result<Vec<u8>, String> {
        Err("mock failure".to_string())
    }

    fn make_ctx(body: &[u8]) -> ExecutionContext {
        ExecutionContext::from_body(body.to_vec())
    }

    #[test]
    fn test_vm_simple_next() {
        let plan = compile_dsl("n:validate");
        let arena = crate::memory::arena::Arena::new();
        let mut vm = VM::new(&plan, make_ctx(b"hello"), arena, &mock_caller);
        vm.run().unwrap();
        assert_eq!(vm.ctx.hop_count, 1);
    }

    #[test]
    fn test_vm_chain() {
        let plan = compile_dsl("n:a n:b n:c");
        let arena = crate::memory::arena::Arena::new();
        let mut vm = VM::new(&plan, make_ctx(b"hello"), arena, &mock_caller);
        vm.run().unwrap();
        assert_eq!(vm.ctx.hop_count, 3);
    }

    #[test]
    fn test_vm_drop_halt() {
        let plan = compile_dsl("n:a d n:b");
        let arena = crate::memory::arena::Arena::new();
        let mut vm = VM::new(&plan, make_ctx(b"hello"), arena, &mock_caller);
        vm.run().unwrap();
        assert_eq!(vm.ctx.hop_count, 1);
    }

    #[test]
    fn test_vm_fallback_after_failure() {
        let plan = compile_dsl("n:a f:b");
        let arena = crate::memory::arena::Arena::new();
        let mut vm = VM::new(&plan, make_ctx(b"hello"), arena, &mock_failing_caller);
        vm.run().unwrap();
        assert!(vm.ctx.failed);
    }

    #[test]
    fn test_vm_async() {
        let plan = compile_dsl("a:svc e:analytics");
        let arena = crate::memory::arena::Arena::new();
        let mut vm = VM::new(&plan, make_ctx(b"hello"), arena, &mock_caller);
        vm.run().unwrap();
        assert_eq!(vm.ctx.hop_count, 1);
    }

    #[test]
    fn test_vm_parallel_collect() {
        let plan = compile_dsl("p:a,b c");
        let arena = crate::memory::arena::Arena::new();
        let mut vm = VM::new(&plan, make_ctx(b"{\"x\":1}"), arena, &mock_caller);
        vm.run().unwrap();
        assert!(vm.ctx.hop_count > 0);
    }

    #[test]
    fn test_vm_gate_true() {
        let dsl = "g:x==1 n:svc";
        let plan = compile_dsl(dsl);
        let arena = crate::memory::arena::Arena::new();
        let mut vm = VM::new(&plan, make_ctx(b"{\"x\":1}"), arena, &mock_caller);
        vm.run().unwrap();
        assert_eq!(vm.ctx.hop_count, 1);
    }

    #[test]
    fn test_vm_emit() {
        let plan = compile_dsl("e:a,b,c");
        let arena = crate::memory::arena::Arena::new();
        let mut vm = VM::new(&plan, make_ctx(b"hello"), arena, &mock_caller);
        vm.run().unwrap();
        assert_eq!(vm.ctx.hop_count, 0);
    }

    #[test]
    fn test_vm_dag() {
        let dsl = "dag:{A:[B,C],D:[A]} e:audit";
        let plan = compile_dsl(dsl);
        let arena = crate::memory::arena::Arena::new();
        let mut vm = VM::new(&plan, make_ctx(b"{\"x\":1}"), arena, &mock_caller);
        vm.run().unwrap();
        assert!(vm.ctx.hop_count > 0);
    }

    #[test]
    fn test_vm_map() {
        let dsl = "m:.x n:svc";
        let plan = compile_dsl(dsl);
        let arena = crate::memory::arena::Arena::new();
        let mut vm = VM::new(&plan, make_ctx(b"{\"x\":42}"), arena, &mock_caller);
        vm.run().unwrap();
        assert_eq!(vm.ctx.hop_count, 1);
    }

    #[test]
    fn test_vm_type_guard_valid() {
        let plan = compile_dsl("schema:{name:string,!age:int} n:validate");
        let arena = crate::memory::arena::Arena::new();
        let mut vm = VM::new(&plan, make_ctx(b"{\"name\":\"alice\",\"age\":30}"), arena, &mock_caller);
        vm.run().unwrap();
        assert_eq!(vm.ctx.hop_count, 1);
    }

    #[test]
    fn test_vm_type_guard_invalid() {
        let plan = compile_dsl("schema:{!age:int} n:validate");
        let arena = crate::memory::arena::Arena::new();
        let mut vm = VM::new(&plan, make_ctx(b"{\"age\":\"bad\"}"), arena, &mock_caller);
        let err = vm.run();
        assert!(err.is_err());
        assert!(err.unwrap_err().contains("TypeGuard"));
    }

    #[test]
    fn test_vm_full_pipeline() {
        let dsl = "t500 n:validate t1000 p:fraud,inventory c f:dlq n:fulfill e:notify,analytics";
        let plan = compile_dsl(dsl);
        let arena = crate::memory::arena::Arena::new();
        let mut vm = VM::new(&plan, make_ctx(b"{\"type\":\"ORDER\"}"), arena, &mock_caller);
        vm.run().unwrap();
        assert!(vm.ctx.hop_count > 0);
    }
}
