use crate::bytecode::plan::ExecutionPlan;
use crate::error::FfiError;

use super::{check_plan_version, read_slice};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::{compiler::Compiler, lexer, optimizer, parser};

    fn compile_to_bytes(dsl: &str) -> Vec<u8> {
        let tokens = lexer::lex(dsl).unwrap();
        let pipeline = parser::parse(&tokens).unwrap();
        let opt = optimizer::Optimizer::new();
        let optimized = opt.optimize(&pipeline);
        let compiler = Compiler::new();
        let plan = compiler.compile(&optimized, "test").unwrap();
        bincode::serialize(&plan).unwrap()
    }

    #[test]
    fn test_plan_services_success() {
        let plan_bytes = compile_to_bytes("n:validate e:notify");
        let mut out_buf = [0u8; 1024];
        let mut out_len: usize = 0;

        let rc = unsafe {
            flowrulz_plan_services(
                plan_bytes.as_ptr(),
                plan_bytes.len(),
                out_buf.as_mut_ptr(),
                out_buf.len(),
                &mut out_len as *mut usize,
            )
        };

        assert_eq!(rc, 0);
        assert!(out_len > 0);
        let json = std::str::from_utf8(&out_buf[..out_len]).unwrap();
        // Should be a JSON array of service entries
        assert!(json.contains("validate") || json.contains("notify"));
    }

    #[test]
    fn test_plan_services_null_output_ptr() {
        let plan_bytes = compile_to_bytes("n:svc");
        let rc = unsafe {
            flowrulz_plan_services(
                plan_bytes.as_ptr(),
                plan_bytes.len(),
                std::ptr::null_mut(),
                0,
                std::ptr::null_mut(),
            )
        };
        assert_eq!(rc, -1); // NullPointer
    }

    #[test]
    fn test_plan_services_invalid_plan() {
        let mut out_buf = [0u8; 1024];
        let mut out_len: usize = 0;
        let bad_plan = b"not_a_valid_plan";

        let rc = unsafe {
            flowrulz_plan_services(
                bad_plan.as_ptr(),
                bad_plan.len(),
                out_buf.as_mut_ptr(),
                out_buf.len(),
                &mut out_len as *mut usize,
            )
        };
        assert_eq!(rc, -8); // Deserialize
    }

    #[test]
    fn test_plan_services_version_mismatch() {
        let mut plan = ExecutionPlan::new("test");
        plan.version = 999;
        let plan_bytes = bincode::serialize(&plan).unwrap();
        let mut out_buf = [0u8; 1024];
        let mut out_len: usize = 0;

        let rc = unsafe {
            flowrulz_plan_services(
                plan_bytes.as_ptr(),
                plan_bytes.len(),
                out_buf.as_mut_ptr(),
                out_buf.len(),
                &mut out_len as *mut usize,
            )
        };
        assert_eq!(rc, -10); // VersionMismatch
    }

    #[test]
    fn test_plan_complexity_success() {
        let plan_bytes = compile_to_bytes("n:svc1 n:svc2 n:svc3");
        let complexity = unsafe { flowrulz_plan_complexity(plan_bytes.as_ptr(), plan_bytes.len()) };
        assert!(complexity > 0);
    }

    #[test]
    fn test_plan_complexity_zero_invalid() {
        let bad_plan = b"invalid";
        let complexity = unsafe { flowrulz_plan_complexity(bad_plan.as_ptr(), bad_plan.len()) };
        assert_eq!(complexity, 0);
    }

    #[test]
    fn test_plan_complexity_version_mismatch() {
        let mut plan = ExecutionPlan::new("test");
        plan.version = 999;
        let plan_bytes = bincode::serialize(&plan).unwrap();
        let complexity = unsafe { flowrulz_plan_complexity(plan_bytes.as_ptr(), plan_bytes.len()) };
        assert_eq!(complexity, 0);
    }

    #[test]
    fn test_plan_complexity_null_plan() {
        let complexity = unsafe { flowrulz_plan_complexity(std::ptr::null(), 0) };
        assert_eq!(complexity, 0);
    }

    #[test]
    fn test_plan_services_with_real_services() {
        let dsl = "p:a,b,c e:d,f";
        let plan_bytes = compile_to_bytes(dsl);
        let mut out_buf = [0u8; 1024];
        let mut out_len: usize = 0;

        let rc = unsafe {
            flowrulz_plan_services(
                plan_bytes.as_ptr(),
                plan_bytes.len(),
                out_buf.as_mut_ptr(),
                out_buf.len(),
                &mut out_len as *mut usize,
            )
        };

        assert_eq!(rc, 0);
        let json = std::str::from_utf8(&out_buf[..out_len]).unwrap();
        // Services should include a, b, c, d, f
        for svc in &["a", "b", "c", "d", "f"] {
            assert!(json.contains(svc), "missing service {}", svc);
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn flowrulz_plan_services(
    plan_ptr: *const u8,
    plan_len: usize,
    out_ptr: *mut u8,
    out_cap: usize,
    out_len: *mut usize,
) -> i32 {
    if out_ptr.is_null() || out_len.is_null() {
        return FfiError::NullPointer.code();
    }
    let plan_slice = match read_slice(plan_ptr, plan_len) {
        Some(s) => s,
        None => return FfiError::NullPointer.code(),
    };
    let plan: ExecutionPlan = match bincode::deserialize(plan_slice) {
        Ok(p) => p,
        Err(_) => return FfiError::Deserialize.code(),
    };
    if !check_plan_version(&plan) {
        return FfiError::VersionMismatch.code();
    }
    let json = serde_json::to_string(&plan.services.entries()).unwrap_or_default();
    let bytes = json.as_bytes();
    let n = bytes.len().min(out_cap);
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), out_ptr, n);
        *out_len = n;
    }
    0
}

#[no_mangle]
pub unsafe extern "C" fn flowrulz_plan_complexity(plan_ptr: *const u8, plan_len: usize) -> u32 {
    let plan_slice = match read_slice(plan_ptr, plan_len) {
        Some(s) => s,
        None => return 0,
    };
    match bincode::deserialize::<ExecutionPlan>(plan_slice) {
        Ok(plan) => {
            if !check_plan_version(&plan) {
                return 0;
            }
            plan.complexity_score
        }
        Err(_) => 0,
    }
}
