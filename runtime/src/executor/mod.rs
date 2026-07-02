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
