use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum ASTNode {
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

impl fmt::Display for ASTNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ASTNode::Next(t) => write!(f, "n:{}", t),
            ASTNode::Async(t) => write!(f, "a:{}", t),
            ASTNode::Parallel(ts) => write!(f, "p:{}", ts.join(",")),
            ASTNode::Collect => write!(f, "c"),
            ASTNode::Fallback(t) => write!(f, "f:{}", t),
            ASTNode::Gate { field, op, value } => write!(f, "g:{}{}{}", field, op, value),
            ASTNode::Split(field) => write!(f, "s:{}", field),
            ASTNode::Map(e) => write!(f, "m:{}", e),
            ASTNode::Emit(ts) => write!(f, "e:{}", ts.join(",")),
            ASTNode::Drop => write!(f, "d"),
            ASTNode::Buffer(n) => write!(f, "b{}", n),
            ASTNode::Key(k) => write!(f, "k:{}", k),
            ASTNode::Retry {
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
            ASTNode::Pipe => write!(f, "|"),
            ASTNode::Timeout(ms) => write!(f, "t{}", ms),
            ASTNode::Chunk { count, mode } => write!(f, "chunk:{}:{}", count, mode),
            ASTNode::Dag(body) => write!(f, "dag:{}", body),
            ASTNode::Schema(body) => write!(f, "schema:{}", body),
            ASTNode::Label(l) => write!(f, "{}:", l),
            ASTNode::Jmp(l) => write!(f, "j:{}", l),
            ASTNode::Delay(ms) => write!(f, "delay:{}", ms),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Pipeline {
    pub nodes: Vec<ASTNode>,
}

#[derive(Debug)]
pub enum ParseError {
    RetryWithoutPrecedingService(String),
    RetryAfterParallel(String),
    RetryAfterCollect(String),
    RetryAfterFallback(String),
    RetryAfterEmit(String),
    RetryAfterDrop(String),
    RetryAfterGate(String),
    RetryAfterPipe(String),
    RetryAfterLabel(String),
    RetryAfterJmp(String),
    RetryAfterRetry(String),
    CollectWithoutParallel(String),
    ChunkWithoutFollowingService(String),
    ChunkAfterDrop(String),
    DagAfterDrop(String),
    MultipleTimeoutsBetweenServices(String),
    UnexpectedToken(String),
    EmptyPipeline,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::RetryWithoutPrecedingService(s) => {
                write!(f, "retry without preceding service: {}", s)
            }
            ParseError::RetryAfterParallel(s) => {
                write!(f, "retry after parallel is not allowed: {}", s)
            }
            ParseError::RetryAfterCollect(s) => {
                write!(f, "retry after collect is not allowed: {}", s)
            }
            ParseError::RetryAfterFallback(s) => {
                write!(f, "retry after fallback is not allowed: {}", s)
            }
            ParseError::RetryAfterEmit(s) => {
                write!(f, "retry after emit is not allowed: {}", s)
            }
            ParseError::RetryAfterDrop(s) => {
                write!(f, "retry after drop is not allowed: {}", s)
            }
            ParseError::RetryAfterGate(s) => {
                write!(f, "retry after gate is not allowed: {}", s)
            }
            ParseError::RetryAfterPipe(s) => {
                write!(f, "retry after pipe is not allowed: {}", s)
            }
            ParseError::RetryAfterLabel(s) => {
                write!(f, "retry after label is not allowed: {}", s)
            }
            ParseError::RetryAfterJmp(s) => {
                write!(f, "retry after jmp is not allowed: {}", s)
            }
            ParseError::RetryAfterRetry(s) => {
                write!(f, "retry after retry is not allowed: {}", s)
            }
            ParseError::CollectWithoutParallel(s) => {
                write!(f, "collect without preceding parallel: {}", s)
            }
            ParseError::ChunkWithoutFollowingService(s) => {
                write!(f, "chunk must be followed by next/parallel: {}", s)
            }
            ParseError::ChunkAfterDrop(s) => write!(f, "chunk after drop is useless: {}", s),
            ParseError::DagAfterDrop(s) => write!(f, "dag after drop is useless: {}", s),
            ParseError::MultipleTimeoutsBetweenServices(s) => {
                write!(f, "multiple timeouts without service between: {}", s)
            }
            ParseError::UnexpectedToken(s) => write!(f, "unexpected token: {}", s),
            ParseError::EmptyPipeline => write!(f, "empty pipeline"),
        }
    }
}

impl std::error::Error for ParseError {}
