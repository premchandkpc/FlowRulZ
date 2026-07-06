use crate::dsl::{compiler::Compiler, lexer, optimizer, parser};
use crate::error::FfiError;

use super::{ffi_catch_unwind, read_str, write_error};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bytecode::plan::{BYTECODE_VERSION, ExecutionPlan};

    fn compile_dsl(dsl: &str) -> Result<Vec<u8>, i32> {
        let mut out_buf = vec![0u8; 65536];
        let mut out_len: usize = 0;
        let mut err_buf = vec![0u8; 256];
        let mut err_len: usize = 0;

        let rc = unsafe {
            flowrulz_compile(
                dsl.as_ptr(),
                dsl.len(),
                std::ptr::null(), // default rule_id
                0,
                out_buf.as_mut_ptr(),
                out_buf.len(),
                &mut out_len as *mut usize,
                err_buf.as_mut_ptr(),
                err_buf.len(),
                &mut err_len as *mut usize,
            )
        };

        if rc == 0 {
            out_buf.truncate(out_len);
            Ok(out_buf)
        } else {
            err_buf.truncate(err_len);
            Err(rc)
        }
    }

    #[test]
    fn test_compile_simple_dsl() {
        let result = compile_dsl("n:validate");
        assert!(result.is_ok());
        let bytes = result.unwrap();
        assert!(!bytes.is_empty());

        // Verify the result deserializes to an ExecutionPlan
        let plan: ExecutionPlan = bincode::deserialize(&bytes).unwrap();
        assert_eq!(plan.rule_id, "default");
        assert_eq!(plan.version, BYTECODE_VERSION);
        assert_eq!(plan.instructions.len(), 1);
    }

    #[test]
    fn test_compile_with_rule_id() {
        let dsl = "n:svc";
        let mut out_buf = vec![0u8; 65536];
        let mut out_len: usize = 0;
        let mut err_buf = vec![0u8; 256];
        let mut err_len: usize = 0;
        let rule_id = b"my_rule";

        let rc = unsafe {
            flowrulz_compile(
                dsl.as_ptr(),
                dsl.len(),
                rule_id.as_ptr(),
                rule_id.len(),
                out_buf.as_mut_ptr(),
                out_buf.len(),
                &mut out_len as *mut usize,
                err_buf.as_mut_ptr(),
                err_buf.len(),
                &mut err_len as *mut usize,
            )
        };

        assert_eq!(rc, 0);
        out_buf.truncate(out_len);
        let plan: ExecutionPlan = bincode::deserialize(&out_buf).unwrap();
        assert_eq!(plan.rule_id, "my_rule");
    }

    #[test]
    fn test_compile_lex_error() {
        let result = compile_dsl("n:");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), -3); // FfiError::Lex
    }

    #[test]
    fn test_compile_parse_error() {
        // A DSL string that passes lexer but fails parser
        let result = compile_dsl("invalid_operator!");
        assert!(result.is_err());
    }

    #[test]
    fn test_compile_buffer_too_small() {
        let dsl = "n:a n:b n:c n:d n:e";
        let mut out_buf = vec![0u8; 4]; // too small
        let mut out_len: usize = 0;
        let mut err_buf = vec![0u8; 256];
        let mut err_len: usize = 0;

        let rc = unsafe {
            flowrulz_compile(
                dsl.as_ptr(),
                dsl.len(),
                std::ptr::null(),
                0,
                out_buf.as_mut_ptr(),
                out_buf.len(),
                &mut out_len as *mut usize,
                err_buf.as_mut_ptr(),
                err_buf.len(),
                &mut err_len as *mut usize,
            )
        };

        assert_eq!(rc, -7); // FfiError::BufferTooSmall
    }

    #[test]
    fn test_compile_null_dsl() {
        let mut out_buf = vec![0u8; 65536];
        let mut out_len: usize = 0;
        let mut err_buf = vec![0u8; 256];
        let mut err_len: usize = 0;

        let rc = unsafe {
            // Note: this is technically UB, but it matches the test pattern
            flowrulz_compile(
                std::ptr::null(),
                0,
                std::ptr::null(),
                0,
                out_buf.as_mut_ptr(),
                out_buf.len(),
                &mut out_len as *mut usize,
                err_buf.as_mut_ptr(),
                err_buf.len(),
                &mut err_len as *mut usize,
            )
        };

        assert_eq!(rc, -1); // FfiError::NullPointer
    }
}

/// # Safety
/// Caller must ensure that `dsl_ptr` points to a valid UTF-8 string of length `dsl_len`,
/// and that `rule_id_ptr` points to a valid UTF-8 string of length `rule_id_len` (or both are null).
/// Output buffers must be valid with sufficient capacity.
#[no_mangle]
pub unsafe extern "C" fn flowrulz_compile(
    dsl_ptr: *const u8,
    dsl_len: usize,
    rule_id_ptr: *const u8,
    rule_id_len: usize,
    out_ptr: *mut u8,
    out_cap: usize,
    out_len: *mut usize,
    err_ptr: *mut u8,
    err_cap: usize,
    err_len: *mut usize,
) -> i32 {
    ffi_catch_unwind(|| {
        let dsl_str = match read_str(dsl_ptr, dsl_len) {
            Some(s) => s,
            None => return FfiError::NullPointer.code(),
        };

        let rule_id = read_str(rule_id_ptr, rule_id_len).unwrap_or("default");

        let tokens = match lexer::lex(dsl_str) {
            Ok(t) => t,
            Err(e) => {
                write_error(err_ptr, err_cap, err_len, &format!("flowrulz_compile lex: {}", e));
                return FfiError::Lex.code();
            }
        };

        let pipeline = match parser::parse(&tokens) {
            Ok(p) => p,
            Err(e) => {
                write_error(err_ptr, err_cap, err_len, &format!("flowrulz_compile parse: {}", e));
                return FfiError::Parse.code();
            }
        };

        let opt = optimizer::Optimizer::new();
        let optimized = opt.optimize(&pipeline);

        let compiler = Compiler::new();
        let plan = match compiler.compile(&optimized, rule_id) {
            Ok(p) => p,
            Err(e) => {
                write_error(err_ptr, err_cap, err_len, &format!("flowrulz_compile: {}", e));
                return FfiError::Compile.code();
            }
        };

        let plan_bytes = match bincode::serialize(&plan) {
            Ok(b) => b,
            Err(e) => {
                write_error(err_ptr, err_cap, err_len, &format!("flowrulz_compile serialize: {}", e));
                return FfiError::Serialize.code();
            }
        };

        if plan_bytes.len() > out_cap {
            write_error(
                err_ptr,
                err_cap,
                err_len,
                &format!(
                    "flowrulz_compile: output buffer too small ({} > {})",
                    plan_bytes.len(),
                    out_cap
                ),
            );
            return FfiError::BufferTooSmall.code();
        }

        unsafe {
            std::ptr::copy_nonoverlapping(plan_bytes.as_ptr(), out_ptr, plan_bytes.len());
            *out_len = plan_bytes.len();
        }

        0
    })
}
