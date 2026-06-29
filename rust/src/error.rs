use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FfiError {
    NullPointer = -1,
    InvalidUtf8 = -2,
    Lex = -3,
    Parse = -4,
    Compile = -5,
    Serialize = -6,
    BufferTooSmall = -7,
    Deserialize = -8,
    Exec = -9,
}

impl FfiError {
    pub fn code(self) -> i32 {
        self as i32
    }
}

impl fmt::Display for FfiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ffi error: {:?}", self)
    }
}
