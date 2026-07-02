use crate::dsl::{compiler::Compiler, lexer, optimizer, parser};
use crate::error::FfiError;

use super::{read_str, write_error};

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
    let dsl_str = match read_str(dsl_ptr, dsl_len) {
        Some(s) => s,
        None => return FfiError::NullPointer.code(),
    };

    let rule_id = match read_str(rule_id_ptr, rule_id_len) {
        Some(s) => s,
        None => "default",
    };

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
}
