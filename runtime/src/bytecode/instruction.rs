use super::opcode::OpCode;

#[repr(C)]
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct Instruction {
    pub op: OpCode,
    pub flags: u8,
    pub a: u16,
    pub b: u16,
    pub c: u16,
}

const _: () = assert!(std::mem::size_of::<Instruction>() == 8);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_size_is_8_bytes() {
        assert_eq!(std::mem::size_of::<Instruction>(), 8);
    }

    #[test]
    fn test_instruction_new() {
        let i = Instruction::new(OpCode::Next, 0x01, 1, 2, 3);
        assert_eq!(i.op, OpCode::Next);
        assert_eq!(i.flags, 0x01);
        assert_eq!(i.a, 1);
        assert_eq!(i.b, 2);
        assert_eq!(i.c, 3);
    }

    #[test]
    fn test_next_encodes_timeout() {
        let i = Instruction::next(42, 0xDEADBEEF);
        assert_eq!(i.op, OpCode::Next);
        assert_eq!(i.a, 42);
        assert_eq!(i.timeout_ms(), 0xDEADBEEF);
    }

    #[test]
    fn test_parallel_instruction() {
        let i = Instruction::parallel(5, 100);
        assert_eq!(i.op, OpCode::Parallel);
        assert_eq!(i.a, 5);
        assert_eq!(i.b, 100);
    }

    #[test]
    fn test_collect_instruction() {
        let i = Instruction::collect();
        assert_eq!(i.op, OpCode::Collect);
    }

    #[test]
    fn test_fallback_instruction() {
        let i = Instruction::fallback(77);
        assert_eq!(i.op, OpCode::Fallback);
        assert_eq!(i.a, 77);
    }

    #[test]
    fn test_gate_instruction() {
        let i = Instruction::gate(10, 2, 20);
        assert_eq!(i.op, OpCode::Gate);
        assert_eq!(i.flags, 2);
        assert_eq!(i.a, 10);
        assert_eq!(i.b, 20);
        assert_eq!(i.gate_op(), 2);
    }

    #[test]
    fn test_jmp_instruction() {
        let i = Instruction::jmp(99);
        assert_eq!(i.op, OpCode::Jmp);
        assert_eq!(i.a, 99);
    }

    #[test]
    fn test_label_instruction() {
        let i = Instruction::label();
        assert_eq!(i.op, OpCode::Label);
    }

    #[test]
    fn test_svc_arg_instruction() {
        let i = Instruction::svc_arg(55);
        assert_eq!(i.op, OpCode::SvcArg);
        assert_eq!(i.a, 55);
    }

    #[test]
    fn test_jump_offset_instruction() {
        let i = Instruction::jump_offset(33);
        assert_eq!(i.op, OpCode::JumpOffset);
        assert_eq!(i.a, 33);
    }

    #[test]
    fn test_emit_instruction() {
        let i = Instruction::emit(3, 200);
        assert_eq!(i.op, OpCode::Emit);
        assert_eq!(i.a, 3);
        assert_eq!(i.b, 200);
    }

    #[test]
    fn test_map_instruction() {
        let i = Instruction::map(42);
        assert_eq!(i.op, OpCode::Map);
        assert_eq!(i.a, 42);
    }

    #[test]
    fn test_set_key_instruction() {
        let i = Instruction::set_key(7);
        assert_eq!(i.op, OpCode::Key);
        assert_eq!(i.a, 7);
    }

    #[test]
    fn test_chunk_instruction() {
        let i = Instruction::chunk(4, 1);
        assert_eq!(i.op, OpCode::Chunk);
        assert_eq!(i.a, 4);
        assert_eq!(i.b, 1);
    }

    #[test]
    fn test_drop_instruction() {
        let i = Instruction::drop();
        assert_eq!(i.op, OpCode::Drop);
    }

    #[test]
    fn test_buffer_instruction() {
        let i = Instruction::buffer(10);
        assert_eq!(i.op, OpCode::Buffer);
        assert_eq!(i.a, 10);
    }

    #[test]
    fn test_async_svc_instruction() {
        let i = Instruction::async_svc(88, 0xFF00FF);
        assert_eq!(i.op, OpCode::Async);
        assert_eq!(i.a, 88);
        assert_eq!(i.timeout_ms(), 0xFF00FF);
    }

    #[test]
    fn test_dag_instruction() {
        let i = Instruction::dag(3);
        assert_eq!(i.op, OpCode::Dag);
        assert_eq!(i.a, 3);
    }

    #[test]
    fn test_type_guard_instruction() {
        let i = Instruction::type_guard(1);
        assert_eq!(i.op, OpCode::TypeGuard);
        assert_eq!(i.a, 1);
    }

    #[test]
    fn test_delay_instruction() {
        let i = Instruction::delay(0xAABBCCDD);
        assert_eq!(i.op, OpCode::Delay);
        assert_eq!(i.delay_ms(), 0xAABBCCDD);
    }

    #[test]
    fn test_has_retry_flag() {
        let with_retry = Instruction { op: OpCode::Next, flags: 0x01, a: 0, b: 0, c: 0 };
        let without = Instruction { op: OpCode::Next, flags: 0x00, a: 0, b: 0, c: 0 };
        assert!(with_retry.has_retry());
        assert!(!without.has_retry());
    }

    #[test]
    fn test_timeout_roundtrip() {
        let i = Instruction::next(1, 0x12345678);
        assert_eq!(i.timeout_ms(), 0x12345678);
    }

    #[test]
    fn test_delay_roundtrip() {
        let i = Instruction::delay(0xDEADBEAF);
        assert_eq!(i.delay_ms(), 0xDEADBEAF);
    }

    #[test]
    fn test_serialization_roundtrip() {
        let i = Instruction::next(1, 5000);
        let bytes = bincode::serialize(&i).unwrap();
        let deserialized: Instruction = bincode::deserialize(&bytes).unwrap();
        assert_eq!(i.op, deserialized.op);
        assert_eq!(i.flags, deserialized.flags);
        assert_eq!(i.a, deserialized.a);
        assert_eq!(i.b, deserialized.b);
        assert_eq!(i.c, deserialized.c);
    }
}

impl Instruction {
    pub fn new(op: OpCode, flags: u8, a: u16, b: u16, c: u16) -> Self {
        Instruction { op, flags, a, b, c }
    }

    pub fn next(target_id: u16, timeout_ms: u64) -> Self {
        let hi = (timeout_ms >> 16) as u16;
        let lo = (timeout_ms & 0xFFFF) as u16;
        Instruction::new(OpCode::Next, 0, target_id, hi, lo)
    }

    pub fn parallel(count: u8, first_svc: u16) -> Self {
        Instruction::new(OpCode::Parallel, 0, u16::from(count), first_svc, 0)
    }

    pub fn collect() -> Self {
        Instruction::new(OpCode::Collect, 0, 0, 0, 0)
    }

    pub fn fallback(target_id: u16) -> Self {
        Instruction::new(OpCode::Fallback, 0, target_id, 0, 0)
    }

    pub fn gate(field_const: u16, gate_op: u8, value_const: u16) -> Self {
        Instruction::new(OpCode::Gate, gate_op, field_const, value_const, 0)
    }

    pub fn jmp(ip_offset: u16) -> Self {
        Instruction::new(OpCode::Jmp, 0, ip_offset, 0, 0)
    }

    pub fn label() -> Self {
        Instruction::new(OpCode::Label, 0, 0, 0, 0)
    }

    pub fn svc_arg(svc_id: u16) -> Self {
        Instruction::new(OpCode::SvcArg, 0, svc_id, 0, 0)
    }

    pub fn jump_offset(offset: u16) -> Self {
        Instruction::new(OpCode::JumpOffset, 0, offset, 0, 0)
    }

    pub fn emit(count: u8, first_svc: u16) -> Self {
        Instruction::new(OpCode::Emit, 0, u16::from(count), first_svc, 0)
    }

    pub fn map(expr_id: u16) -> Self {
        Instruction::new(OpCode::Map, 0, expr_id, 0, 0)
    }

    pub fn set_key(field_const: u16) -> Self {
        Instruction::new(OpCode::Key, 0, field_const, 0, 0)
    }

    pub fn chunk(count: u8, mode: u8) -> Self {
        Instruction::new(OpCode::Chunk, 0, u16::from(count), u16::from(mode), 0)
    }

    pub fn drop() -> Self {
        Instruction::new(OpCode::Drop, 0, 0, 0, 0)
    }

    pub fn buffer(n: u8) -> Self {
        Instruction::new(OpCode::Buffer, 0, u16::from(n), 0, 0)
    }

    pub fn async_svc(target_id: u16, timeout_ms: u64) -> Self {
        let hi = (timeout_ms >> 16) as u16;
        let lo = (timeout_ms & 0xFFFF) as u16;
        Instruction::new(OpCode::Async, 0, target_id, hi, lo)
    }

    pub fn dag(dag_table_id: u16) -> Self {
        Instruction::new(OpCode::Dag, 0, dag_table_id, 0, 0)
    }

    pub fn type_guard(strict: u8) -> Self {
        Instruction::new(OpCode::TypeGuard, 0, u16::from(strict), 0, 0)
    }

    pub fn delay(ms: u64) -> Self {
        let hi = (ms >> 16) as u16;
        let lo = (ms & 0xFFFF) as u16;
        Instruction::new(OpCode::Delay, 0, 0, hi, lo)
    }

    pub fn delay_ms(&self) -> u64 {
        ((self.b as u64) << 16) | (self.c as u64)
    }

    pub fn has_retry(&self) -> bool {
        (self.flags & 0x01) != 0
    }

    pub fn gate_op(&self) -> u8 {
        self.flags
    }

    pub fn timeout_ms(&self) -> u64 {
        ((self.b as u64) << 16) | (self.c as u64)
    }
}
