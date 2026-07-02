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
    VersionMismatch = -10,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ffi_error_code() {
        assert_eq!(FfiError::NullPointer.code(), -1);
        assert_eq!(FfiError::InvalidUtf8.code(), -2);
        assert_eq!(FfiError::Lex.code(), -3);
        assert_eq!(FfiError::Parse.code(), -4);
        assert_eq!(FfiError::Compile.code(), -5);
        assert_eq!(FfiError::Serialize.code(), -6);
        assert_eq!(FfiError::BufferTooSmall.code(), -7);
        assert_eq!(FfiError::Deserialize.code(), -8);
        assert_eq!(FfiError::Exec.code(), -9);
        assert_eq!(FfiError::VersionMismatch.code(), -10);
    }

    #[test]
    fn test_ffi_error_display() {
        assert_eq!(format!("{}", FfiError::NullPointer), "ffi error: NullPointer");
        assert_eq!(format!("{}", FfiError::Exec), "ffi error: Exec");
        assert_eq!(format!("{}", FfiError::Serialize), "ffi error: Serialize");
    }

    #[test]
    fn test_ffi_error_equality() {
        assert_eq!(FfiError::NullPointer, FfiError::NullPointer);
        assert_ne!(FfiError::Lex, FfiError::Parse);
    }
}
