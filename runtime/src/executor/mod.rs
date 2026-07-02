pub mod chunk;
pub mod dag;
pub mod emit;
pub mod expr;
pub mod gate;
pub mod helpers;
pub mod map;
pub mod next;
pub mod parallel;
pub mod plugin;
pub mod runtime;
mod vm;

pub use vm::VM;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepResult {
    Done,
    Continue,
    Pending {
        svc_id: u16,
        body: Vec<u8>,
        timeout_ms: u64,
    },
    Delay(u64),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_step_result_done() {
        assert_eq!(StepResult::Done, StepResult::Done);
        assert_ne!(StepResult::Done, StepResult::Continue);
    }

    #[test]
    fn test_step_result_continue() {
        assert_eq!(StepResult::Continue, StepResult::Continue);
    }

    #[test]
    fn test_step_result_pending() {
        let p1 = StepResult::Pending { svc_id: 1, body: vec![], timeout_ms: 5000 };
        let p2 = StepResult::Pending { svc_id: 1, body: vec![], timeout_ms: 5000 };
        assert_eq!(p1, p2);
    }

    #[test]
    fn test_step_result_pending_ne() {
        let p1 = StepResult::Pending { svc_id: 1, body: vec![], timeout_ms: 5000 };
        let p2 = StepResult::Pending { svc_id: 2, body: vec![], timeout_ms: 5000 };
        assert_ne!(p1, p2);
    }

    #[test]
    fn test_step_result_delay() {
        assert_eq!(StepResult::Delay(1000), StepResult::Delay(1000));
        assert_ne!(StepResult::Delay(1000), StepResult::Delay(2000));
    }

    #[test]
    fn test_step_result_debug() {
        let d = format!("{:?}", StepResult::Done);
        assert_eq!(d, "Done");

        let c = format!("{:?}", StepResult::Continue);
        assert_eq!(c, "Continue");
    }
}
