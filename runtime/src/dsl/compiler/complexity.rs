use crate::bytecode::opcode::OpCode;
use crate::bytecode::plan::ExecutionPlan;

pub fn calc_complexity(plan: &ExecutionPlan) -> u32 {
    let mut score: u32 = 0;
    for instr in &plan.instructions {
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
