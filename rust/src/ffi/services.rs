use crate::bytecode::plan::ExecutionPlan;
use crate::error::FfiError;

use super::{check_plan_version, read_slice};

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
