use super::{
    ChunkMode, ConstantPool, DAGTable, Instruction, RetryStrategy, Schema, ServiceTable,
};

/// Current bytecode version. Bump when the serialization format changes.
pub const BYTECODE_VERSION: u64 = 1;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RetryConfig {
    pub max_attempts: u8,
    pub strategy: RetryStrategy,
    pub fixed_ms: u32,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChunkConfig {
    pub count: u8,
    pub mode: ChunkMode,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ExecutionPlan {
    pub rule_id: String,
    pub version: u64,
    pub instr_count: u32,
    pub complexity_score: u32,
    pub instructions: Vec<Instruction>,
    pub const_pool: ConstantPool,
    pub services: ServiceTable,
    pub dag_tables: Vec<DAGTable>,
    pub retry_configs: Vec<RetryConfig>,
    pub chunk_configs: Vec<ChunkConfig>,
    pub schema: Option<Schema>,
}

impl ExecutionPlan {
    pub fn new(rule_id: &str) -> Self {
        ExecutionPlan {
            rule_id: rule_id.to_string(),
            version: 1,
            instr_count: 0,
            complexity_score: 0,
            instructions: Vec::new(),
            const_pool: ConstantPool::new(),
            services: ServiceTable::new(),
            dag_tables: Vec::new(),
            retry_configs: Vec::new(),
            chunk_configs: Vec::new(),
            schema: None,
        }
    }

    pub fn add_instr(&mut self, instr: Instruction) {
        self.instructions.push(instr);
        self.instr_count = self.instructions.len() as u32;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bytecode::opcode::OpCode;

    #[test]
    fn test_execution_plan_new() {
        let plan = ExecutionPlan::new("my_rule");
        assert_eq!(plan.rule_id, "my_rule");
        assert_eq!(plan.version, 1);
        assert_eq!(plan.instr_count, 0);
        assert!(plan.instructions.is_empty());
        assert!(plan.services.is_empty());
        assert!(plan.dag_tables.is_empty());
        assert!(plan.retry_configs.is_empty());
        assert!(plan.chunk_configs.is_empty());
        assert!(plan.schema.is_none());
    }

    #[test]
    fn test_add_instr() {
        let mut plan = ExecutionPlan::new("test");
        let instr = Instruction::next(1, 5000);
        plan.add_instr(instr);
        assert_eq!(plan.instr_count, 1);
        assert_eq!(plan.instructions.len(), 1);
        assert_eq!(plan.instructions[0].op, OpCode::Next);
        plan.add_instr(Instruction::drop());
        assert_eq!(plan.instr_count, 2);
    }

    #[test]
    fn test_serialization_roundtrip() {
        let mut plan = ExecutionPlan::new("roundtrip");
        plan.add_instr(Instruction::next(1, 5000));
        plan.complexity_score = 42;
        let bytes = bincode::serialize(&plan).unwrap();
        let deserialized: ExecutionPlan = bincode::deserialize(&bytes).unwrap();
        assert_eq!(deserialized.rule_id, "roundtrip");
        assert_eq!(deserialized.version, 1);
        assert_eq!(deserialized.complexity_score, 42);
        assert_eq!(deserialized.instructions.len(), 1);
    }

    #[test]
    fn test_retry_config_defaults() {
        let cfg = RetryConfig {
            max_attempts: 3,
            strategy: RetryStrategy::Exponential,
            fixed_ms: 0,
        };
        assert_eq!(cfg.max_attempts, 3);
        assert_eq!(cfg.strategy, RetryStrategy::Exponential);
    }

    #[test]
    fn test_chunk_config_defaults() {
        let cfg = ChunkConfig {
            count: 4,
            mode: ChunkMode::Sequential,
        };
        assert_eq!(cfg.count, 4);
        assert_eq!(cfg.mode, ChunkMode::Sequential);
    }
}
