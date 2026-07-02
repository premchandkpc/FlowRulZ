use std::fmt;

#[derive(Debug)]
pub enum CompileError {
    UnknownTarget(String),
    DagCycle(String),
    DagEmpty(String),
    DagUnknownService(String),
    DuplicateLabel(String),
    UnknownLabel(String),
    UnterminatedPipeline(String),
    SchemaParseError(String),
    TypeMismatch(String),
}

impl fmt::Display for CompileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompileError::UnknownTarget(t) => write!(f, "unknown target: {}", t),
            CompileError::DagCycle(d) => write!(f, "DAG contains cycle: {}", d),
            CompileError::DagEmpty(_) => write!(f, "DAG is empty (use p:+c: instead)"),
            CompileError::DagUnknownService(s) => {
                write!(f, "DAG references unknown service: {}", s)
            }
            CompileError::DuplicateLabel(l) => write!(f, "duplicate label: {}", l),
            CompileError::UnknownLabel(l) => write!(f, "unknown label: {}", l),
            CompileError::UnterminatedPipeline(s) => write!(f, "unterminated pipeline: {}", s),
            CompileError::SchemaParseError(s) => write!(f, "schema parse error: {}", s),
            CompileError::TypeMismatch(s) => write!(f, "type mismatch: {}", s),
        }
    }
}

impl std::error::Error for CompileError {}
