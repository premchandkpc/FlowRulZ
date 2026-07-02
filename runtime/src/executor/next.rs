use crate::bytecode::instruction::Instruction;
use crate::bytecode::opcode::RetryStrategy;
use crate::bytecode::plan::{ExecutionPlan, RetryConfig};
use rand::Rng;
use std::time::Duration;

pub fn exec_next(
    body: &[u8],
    instr: &Instruction,
    plan: &ExecutionPlan,
    caller: &dyn Fn(u16, &[u8], u64) -> Result<Vec<u8>, String>,
    async_ack: bool,
) -> Result<Vec<u8>, String> {
    let svc_id = instr.a;
    let timeout_ms = instr.timeout_ms();
    let has_retry = instr.has_retry();

    if has_retry {
        let retry_cfg = find_retry_config(instr, plan);
        exec_with_retry(svc_id, body, timeout_ms, &retry_cfg, caller, async_ack)
    } else {
        caller(svc_id, body, timeout_ms)
    }
}

fn exec_with_retry(
    svc_id: u16,
    body: &[u8],
    timeout_ms: u64,
    retry_cfg: &RetryConfig,
    caller: &dyn Fn(u16, &[u8], u64) -> Result<Vec<u8>, String>,
    async_ack: bool,
) -> Result<Vec<u8>, String> {
    let max = retry_cfg.max_attempts as usize + 1;
    let mut last_err = None;

    for attempt in 0..max {
        if attempt > 0 {
            let delay = match retry_cfg.strategy {
                RetryStrategy::Exponential => {
                    let base = Duration::from_millis(100 * (1u64 << (attempt - 1)));
                    let jitter = Duration::from_millis(rand::thread_rng().gen_range(0..50));
                    base.min(Duration::from_secs(10)) + jitter
                }
                RetryStrategy::Linear => Duration::from_millis(100 * attempt as u64),
                RetryStrategy::Fixed => Duration::from_millis(retry_cfg.fixed_ms as u64),
            };
            std::thread::sleep(delay);
        }

        match caller(svc_id, body, timeout_ms) {
            Ok(resp) => {
                if async_ack {
                    return Ok(Vec::new());
                }
                return Ok(resp);
            }
            Err(e) => {
                last_err = Some(e);
            }
        }
    }

    Err(last_err.unwrap_or_else(|| "all retries exhausted".to_string()))
}

fn find_retry_config(instr: &Instruction, plan: &ExecutionPlan) -> RetryConfig {
    let cfg_idx = instr.c as usize;
    if cfg_idx < plan.retry_configs.len() {
        plan.retry_configs[cfg_idx].clone()
    } else {
        RetryConfig {
            max_attempts: 3,
            strategy: RetryStrategy::Exponential,
            fixed_ms: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bytecode::opcode::OpCode;

    fn mock_ok_caller(_svc_id: u16, body: &[u8], _timeout: u64) -> Result<Vec<u8>, String> {
        Ok(body.to_vec())
    }

    fn mock_err_caller(_svc_id: u16, _body: &[u8], _timeout: u64) -> Result<Vec<u8>, String> {
        Err("service error".to_string())
    }

    fn make_plan_with_retry() -> ExecutionPlan {
        let mut plan = ExecutionPlan::new("test");
        let cfg = RetryConfig {
            max_attempts: 2,
            strategy: RetryStrategy::Fixed,
            fixed_ms: 1, // minimal delay for tests
        };
        plan.retry_configs.push(cfg);
        plan
    }

    #[test]
    fn test_exec_next_no_retry() {
        let plan = ExecutionPlan::new("test");
        let instr = Instruction::next(1, 5000);
        let result = exec_next(b"hello", &instr, &plan, &mock_ok_caller, false);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), b"hello");
    }

    #[test]
    fn test_exec_next_no_retry_error() {
        let plan = ExecutionPlan::new("test");
        let instr = Instruction::next(1, 5000);
        let result = exec_next(b"hello", &instr, &plan, &mock_err_caller, false);
        assert!(result.is_err());
    }

    #[test]
    fn test_exec_next_with_retry_success() {
        let plan = make_plan_with_retry();
        let mut instr = Instruction::next(1, 5000);
        instr.flags = 0x01; // has_retry
        instr.c = 0; // retry config index
        let result = exec_next(b"hello", &instr, &plan, &mock_ok_caller, false);
        assert!(result.is_ok());
    }

    #[test]
    fn test_exec_next_with_retry_all_fail() {
        let plan = make_plan_with_retry();
        let mut instr = Instruction::next(1, 5000);
        instr.flags = 0x01;
        instr.c = 0;
        let result = exec_next(b"hello", &instr, &plan, &mock_err_caller, false);
        assert!(result.is_err());
    }

    #[test]
    fn test_exec_next_async_ack_with_retry() {
        let mut plan = make_plan_with_retry();
        let mut instr = Instruction::next(1, 5000);
        instr.flags = 0x01; // has_retry
        instr.c = 0;
        let result = exec_next(b"hello", &instr, &plan, &mock_ok_caller, true);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_find_retry_config_valid() {
        let plan = make_plan_with_retry();
        let instr = Instruction { op: OpCode::Next, flags: 0x01, a: 0, b: 0, c: 0 };
        let cfg = find_retry_config(&instr, &plan);
        assert_eq!(cfg.max_attempts, 2);
        assert_eq!(cfg.strategy, RetryStrategy::Fixed);
    }

    #[test]
    fn test_find_retry_config_out_of_bounds() {
        let plan = ExecutionPlan::new("test");
        let instr = Instruction { op: OpCode::Next, flags: 0x01, a: 0, b: 0, c: 99 };
        let cfg = find_retry_config(&instr, &plan);
        assert_eq!(cfg.max_attempts, 3);
        assert_eq!(cfg.strategy, RetryStrategy::Exponential);
    }
}
