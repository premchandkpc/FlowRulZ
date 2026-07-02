use crate::bytecode::execution::ExecutionContext;
use crate::bytecode::plan::ExecutionPlan;
use crate::executor::VM;
use std::sync::Arc;

pub struct ExecutionRuntime {
    plan: ExecutionPlan,
    #[allow(clippy::type_complexity)]
    caller: Arc<dyn Fn(u16, &[u8], u64) -> Result<Vec<u8>, String> + Send + Sync>,
    buffer_body: Vec<u8>,
    buffer_count: u8,
    buffer_target: u8,
}

impl ExecutionRuntime {
    pub fn new<F>(plan: ExecutionPlan, caller: F) -> Self
    where
        F: Fn(u16, &[u8], u64) -> Result<Vec<u8>, String> + Send + Sync + 'static,
    {
        ExecutionRuntime {
            plan,
            caller: Arc::new(caller),
            buffer_body: Vec::new(),
            buffer_count: 0,
            buffer_target: 0,
        }
    }

    pub fn plan(&self) -> &ExecutionPlan {
        &self.plan
    }

    /// Execute a message through the full pipeline.
    pub fn execute(
        &mut self,
        body: &[u8],
    ) -> Result<Vec<u8>, String> {
        if let Some(first) = self.plan.instructions.first() {
            match first.op {
                crate::bytecode::opcode::OpCode::Buffer => {
                    self.buffer_target = first.a as u8;
                    self.buffer_body = body.to_vec();
                    self.buffer_count = 1;
                    return Ok(body.to_vec());
                }
                crate::bytecode::opcode::OpCode::Chunk => {
                    return self.exec_chunked(body, first.a as u8);
                }
                _ => {}
            }
        }

        self.run_vm(body)
    }

    /// Push another message into the buffer.
    /// Returns true when the buffer is full (caller should flush).
    pub fn buffer_push(&mut self, body: &[u8]) -> bool {
        if self.buffer_target == 0 {
            return false;
        }
        let prev = std::mem::take(&mut self.buffer_body);
        self.buffer_body = merge_buffer_json(&prev, body);
        self.buffer_count += 1;
        self.buffer_count >= self.buffer_target
    }

    /// Flush accumulated buffer and reset state.
    pub fn buffer_flush(&mut self) -> Vec<u8> {
        let body = std::mem::take(&mut self.buffer_body);
        self.buffer_count = 0;
        self.buffer_target = 0;
        body
    }

    pub fn buffer_remaining(&self) -> u8 {
        self.buffer_target.saturating_sub(self.buffer_count)
    }

    fn run_vm(
        &self,
        body: &[u8],
    ) -> Result<Vec<u8>, String> {
        let arena = crate::memory::arena::Arena::new();
        let caller_ref = &*self.caller;
        let ctx = ExecutionContext::from_body(body.to_vec());
        let mut vm = VM::new(&self.plan, ctx, arena, caller_ref);
        vm.run()?;
        Ok(vm.ctx.body)
    }

    fn exec_chunked(
        &self,
        body: &[u8],
        count: u8,
    ) -> Result<Vec<u8>, String> {
        let threshold = body.len() / count.max(1) as usize;
        let chunks = match crate::executor::chunk::split_chunks(body, count, threshold) {
            Some(c) => c,
            None => return self.run_vm(body),
        };

        let mut results = Vec::new();
        for chunk in &chunks {
            results.push(self.run_vm(chunk)?);
        }

        let arr: Vec<serde_json::Value> = results
            .into_iter()
            .map(|r| serde_json::from_slice(&r).unwrap_or(serde_json::Value::Null))
            .collect();
        Ok(serde_json::to_vec(&serde_json::Value::Array(arr)).unwrap_or_default())
    }
}

fn merge_buffer_json(a: &[u8], b: &[u8]) -> Vec<u8> {
    let a_val: serde_json::Value = serde_json::from_slice(a).unwrap_or(serde_json::Value::Null);
    let b_val: serde_json::Value = serde_json::from_slice(b).unwrap_or(serde_json::Value::Null);
    match (a_val, b_val) {
        (serde_json::Value::Array(mut arr), b) => {
            arr.push(b);
            serde_json::to_vec(&serde_json::Value::Array(arr)).unwrap_or_default()
        }
        (a, b) => serde_json::to_vec(&serde_json::Value::Array(vec![a, b])).unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::{compiler::Compiler, lexer, optimizer, parser};

    fn compile_dsl(dsl: &str) -> ExecutionPlan {
        let tokens = lexer::lex(dsl).unwrap();
        let pipeline = parser::parse(&tokens).unwrap();
        let opt = optimizer::Optimizer::new();
        let optimized = opt.optimize(&pipeline);
        let compiler = Compiler::new();
        compiler.compile(&optimized, "test").unwrap()
    }

    fn mock_caller(_svc_id: u16, body: &[u8], _timeout: u64) -> Result<Vec<u8>, String> {
        Ok(body.to_vec())
    }

    #[test]
    fn test_runtime_simple_next() {
        let plan = compile_dsl("n:svc");
        let mut rt = ExecutionRuntime::new(plan, mock_caller);
        let result = rt.execute(b"hello").unwrap();
        assert_eq!(result, b"hello");
    }

    #[test]
    fn test_runtime_gate_true() {
        let plan = compile_dsl("g:x==1 n:svc");
        let mut rt = ExecutionRuntime::new(plan, mock_caller);
        let result = rt.execute(b"{\"x\":1}").unwrap();
        assert_eq!(result, b"{\"x\":1}");
    }

    #[test]
    fn test_runtime_full_pipeline() {
        let dsl = "t500 n:validate t1000 p:a,b c f:dlq n:fulfill e:notify";
        let plan = compile_dsl(dsl);
        let mut rt = ExecutionRuntime::new(plan, mock_caller);
        let result = rt.execute(b"{\"type\":\"ORDER\"}").unwrap();
        assert!(!result.is_empty());
    }

    #[test]
    fn test_runtime_dag() {
        let dsl = "dag:{A:[B],C:[A]} e:audit";
        let plan = compile_dsl(dsl);
        let mut rt = ExecutionRuntime::new(plan, mock_caller);
        let result = rt.execute(b"{\"x\":1}").unwrap();
        assert!(!result.is_empty());
    }

    #[test]
    fn test_buffer_push_flush() {
        let plan = compile_dsl("b5 n:svc");
        let mut rt = ExecutionRuntime::new(plan, mock_caller);
        let result = rt.execute(b"msg1").unwrap();
        assert_eq!(result, b"msg1");
        assert_eq!(rt.buffer_target, 5);
        assert_eq!(rt.buffer_remaining(), 4);

        assert!(!rt.buffer_push(b"msg2"));
        assert!(!rt.buffer_push(b"msg3"));
        assert!(!rt.buffer_push(b"msg4"));
        assert!(rt.buffer_push(b"msg5"));

        let flushed = rt.buffer_flush();
        assert!(!flushed.is_empty());
        assert_eq!(rt.buffer_remaining(), 0);
    }

    #[test]
    fn test_runtime_chunk_seq() {
        let dsl = "chunk:2:seq n:process";
        let plan = compile_dsl(dsl);
        let mut rt = ExecutionRuntime::new(plan, mock_caller);
        let result = rt.execute(b"large_body_data_here").unwrap();
        assert!(!result.is_empty());
        let val: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert!(val.is_array());
    }
}
