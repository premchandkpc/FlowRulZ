use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Next(String),
    Async(String),
    Parallel(Vec<String>),
    Collect,
    Fallback(String),
    Gate {
        field: String,
        op: String,
        value: String,
    },
    Split(String),
    Map(String),
    Emit(Vec<String>),
    Drop,
    Buffer(u64),
    Key(String),
    Retry {
        count: u8,
        strategy: Option<String>,
        fixed_ms: Option<u32>,
    },
    Pipe,
    Timeout(u64),
    Chunk {
        count: u8,
        mode: String,
    },
    Dag(String),
    Schema(String),
    Label(String),
    Jmp(String),
    Delay(u64),
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Token::Next(t) => write!(f, "n:{}", t),
            Token::Async(t) => write!(f, "a:{}", t),
            Token::Parallel(ts) => write!(f, "p:{}", ts.join(",")),
            Token::Collect => write!(f, "c"),
            Token::Fallback(t) => write!(f, "f:{}", t),
            Token::Gate { field, op, value } => {
                write!(f, "g:{}{}{}", field, op, value)
            }
            Token::Split(field) => write!(f, "s:{}", field),
            Token::Map(e) => write!(f, "m:{}", e),
            Token::Emit(ts) => write!(f, "e:{}", ts.join(",")),
            Token::Drop => write!(f, "d"),
            Token::Buffer(n) => write!(f, "b{}", n),
            Token::Key(k) => write!(f, "k:{}", k),
            Token::Retry {
                count,
                strategy,
                fixed_ms: _,
            } => {
                if let Some(s) = strategy {
                    write!(f, "r{}:{}", count, s)
                } else {
                    write!(f, "r{}", count)
                }
            }
            Token::Pipe => write!(f, "|"),
            Token::Timeout(ms) => write!(f, "t{}", ms),
            Token::Chunk { count, mode } => write!(f, "chunk:{}:{}", count, mode),
            Token::Dag(body) => write!(f, "dag:{}", body),
            Token::Schema(body) => write!(f, "schema:{}", body),
            Token::Label(l) => write!(f, "{}:", l),
            Token::Jmp(l) => write!(f, "j:{}", l),
            Token::Delay(ms) => write!(f, "delay:{}", ms),
        }
    }
}

#[derive(Debug)]
pub enum LexError {
    UnknownToken(String),
    InvalidTimeout(String),
    InvalidBuffer(String),
    InvalidRetry(String),
    InvalidChunk(String),
    InvalidDag(String),
    InvalidSchema(String),
    EmptyOperand(String),
    InvalidGateOp(String),
    InvalidLabel(String),
    InvalidJmp(String),
    InvalidAsync(String),
    InvalidSplit(String),
    InvalidKey(String),
    InvalidMap(String),
    InvalidEmit(String),
    InvalidDelay(String),
}

impl fmt::Display for LexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LexError::UnknownToken(t) => write!(f, "unknown token: {}", t),
            LexError::InvalidTimeout(t) => write!(f, "invalid timeout: {}", t),
            LexError::InvalidBuffer(t) => write!(f, "invalid buffer: {}", t),
            LexError::InvalidRetry(t) => write!(f, "invalid retry: {}", t),
            LexError::InvalidChunk(t) => write!(f, "invalid chunk: {}", t),
            LexError::InvalidDag(t) => write!(f, "invalid dag: {}", t),
            LexError::InvalidSchema(t) => write!(f, "invalid schema: {}", t),
            LexError::EmptyOperand(t) => write!(f, "empty operand for: {}", t),
            LexError::InvalidGateOp(t) => write!(f, "invalid gate operator: {}", t),
            LexError::InvalidLabel(t) => write!(f, "invalid label: {}", t),
            LexError::InvalidJmp(t) => write!(f, "invalid jmp: {}", t),
            LexError::InvalidAsync(t) => write!(f, "invalid async: {}", t),
            LexError::InvalidSplit(t) => write!(f, "invalid split: {}", t),
            LexError::InvalidKey(t) => write!(f, "invalid key: {}", t),
            LexError::InvalidMap(t) => write!(f, "invalid map: {}", t),
            LexError::InvalidEmit(t) => write!(f, "invalid emit: {}", t),
            LexError::InvalidDelay(t) => write!(f, "invalid delay: {}", t),
        }
    }
}

impl std::error::Error for LexError {}
