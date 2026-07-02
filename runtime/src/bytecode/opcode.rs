use std::fmt;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum OpCode {
    Next = 0,
    Parallel = 1,
    Collect = 2,
    Fallback = 3,
    Gate = 4,
    Map = 6,
    Emit = 7,
    Drop = 8,
    Buffer = 9,
    Key = 10,
    Async = 14,
    Chunk = 15,
    Dag = 16,
    Jmp = 17,
    Label = 18,
    SvcArg = 19,
    JumpOffset = 21,
    TypeGuard = 22,
    SvcCall = 23,
    Delay = 24,
}

impl OpCode {
    pub fn from_u8(v: u8) -> Option<OpCode> {
        match v {
            0 => Some(OpCode::Next),
            1 => Some(OpCode::Parallel),
            2 => Some(OpCode::Collect),
            3 => Some(OpCode::Fallback),
            4 => Some(OpCode::Gate),
            6 => Some(OpCode::Map),
            7 => Some(OpCode::Emit),
            8 => Some(OpCode::Drop),
            9 => Some(OpCode::Buffer),
            10 => Some(OpCode::Key),
            14 => Some(OpCode::Async),
            15 => Some(OpCode::Chunk),
            16 => Some(OpCode::Dag),
            17 => Some(OpCode::Jmp),
            18 => Some(OpCode::Label),
            19 => Some(OpCode::SvcArg),
            21 => Some(OpCode::JumpOffset),
            22 => Some(OpCode::TypeGuard),
            23 => Some(OpCode::SvcCall),
            24 => Some(OpCode::Delay),
            _ => None,
        }
    }
}

impl fmt::Display for OpCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum GateOp {
    Eq = 0,
    Ne = 1,
    Gt = 2,
    Lt = 3,
    Gte = 4,
    Lte = 5,
    Contains = 6,
}

impl GateOp {
    pub fn from_str(s: &str) -> Option<GateOp> {
        match s {
            "==" => Some(GateOp::Eq),
            "!=" => Some(GateOp::Ne),
            ">" => Some(GateOp::Gt),
            "<" => Some(GateOp::Lt),
            ">=" => Some(GateOp::Gte),
            "<=" => Some(GateOp::Lte),
            "contains" => Some(GateOp::Contains),
            _ => None,
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ChunkMode {
    Sequential = 0,
    Parallel = 1,
}

impl ChunkMode {
    pub fn from_str(s: &str) -> Option<ChunkMode> {
        match s {
            "seq" => Some(ChunkMode::Sequential),
            "par" => Some(ChunkMode::Parallel),
            _ => None,
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RetryStrategy {
    Exponential = 0,
    Linear = 1,
    Fixed = 2,
}

impl RetryStrategy {
    pub fn from_str(s: &str) -> Option<RetryStrategy> {
        match s {
            "exp" => Some(RetryStrategy::Exponential),
            "linear" => Some(RetryStrategy::Linear),
            "fixed" => Some(RetryStrategy::Fixed),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_opcode_from_u8_all_variants() {
        assert_eq!(OpCode::from_u8(0), Some(OpCode::Next));
        assert_eq!(OpCode::from_u8(1), Some(OpCode::Parallel));
        assert_eq!(OpCode::from_u8(2), Some(OpCode::Collect));
        assert_eq!(OpCode::from_u8(3), Some(OpCode::Fallback));
        assert_eq!(OpCode::from_u8(4), Some(OpCode::Gate));
        assert_eq!(OpCode::from_u8(6), Some(OpCode::Map));
        assert_eq!(OpCode::from_u8(7), Some(OpCode::Emit));
        assert_eq!(OpCode::from_u8(8), Some(OpCode::Drop));
        assert_eq!(OpCode::from_u8(9), Some(OpCode::Buffer));
        assert_eq!(OpCode::from_u8(10), Some(OpCode::Key));
        assert_eq!(OpCode::from_u8(14), Some(OpCode::Async));
        assert_eq!(OpCode::from_u8(15), Some(OpCode::Chunk));
        assert_eq!(OpCode::from_u8(16), Some(OpCode::Dag));
        assert_eq!(OpCode::from_u8(17), Some(OpCode::Jmp));
        assert_eq!(OpCode::from_u8(18), Some(OpCode::Label));
        assert_eq!(OpCode::from_u8(19), Some(OpCode::SvcArg));
        assert_eq!(OpCode::from_u8(21), Some(OpCode::JumpOffset));
        assert_eq!(OpCode::from_u8(22), Some(OpCode::TypeGuard));
        assert_eq!(OpCode::from_u8(23), Some(OpCode::SvcCall));
        assert_eq!(OpCode::from_u8(24), Some(OpCode::Delay));
        assert_eq!(OpCode::from_u8(255), None);
        assert_eq!(OpCode::from_u8(5), None);
    }

    #[test]
    fn test_opcode_display() {
        assert_eq!(format!("{}", OpCode::Next), "Next");
        assert_eq!(format!("{}", OpCode::Delay), "Delay");
    }

    #[test]
    fn test_gate_op_from_str() {
        assert_eq!(GateOp::from_str("=="), Some(GateOp::Eq));
        assert_eq!(GateOp::from_str("!="), Some(GateOp::Ne));
        assert_eq!(GateOp::from_str(">"), Some(GateOp::Gt));
        assert_eq!(GateOp::from_str("<"), Some(GateOp::Lt));
        assert_eq!(GateOp::from_str(">="), Some(GateOp::Gte));
        assert_eq!(GateOp::from_str("<="), Some(GateOp::Lte));
        assert_eq!(GateOp::from_str("contains"), Some(GateOp::Contains));
        assert_eq!(GateOp::from_str("???"), None);
    }

    #[test]
    fn test_chunk_mode_from_str() {
        assert_eq!(ChunkMode::from_str("seq"), Some(ChunkMode::Sequential));
        assert_eq!(ChunkMode::from_str("par"), Some(ChunkMode::Parallel));
        assert_eq!(ChunkMode::from_str("invalid"), None);
    }

    #[test]
    fn test_retry_strategy_from_str() {
        assert_eq!(RetryStrategy::from_str("exp"), Some(RetryStrategy::Exponential));
        assert_eq!(RetryStrategy::from_str("linear"), Some(RetryStrategy::Linear));
        assert_eq!(RetryStrategy::from_str("fixed"), Some(RetryStrategy::Fixed));
        assert_eq!(RetryStrategy::from_str("unknown"), None);
    }

    #[test]
    fn test_gate_op_equality() {
        assert_eq!(GateOp::Eq as u8, 0);
        assert_eq!(GateOp::Ne as u8, 1);
        assert_eq!(GateOp::Gt as u8, 2);
        assert_eq!(GateOp::Lt as u8, 3);
        assert_eq!(GateOp::Gte as u8, 4);
        assert_eq!(GateOp::Lte as u8, 5);
        assert_eq!(GateOp::Contains as u8, 6);
    }

    #[test]
    fn test_chunk_mode_values() {
        assert_eq!(ChunkMode::Sequential as u8, 0);
        assert_eq!(ChunkMode::Parallel as u8, 1);
    }
}
