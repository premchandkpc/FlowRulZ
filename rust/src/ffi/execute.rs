use std::sync::Arc;

use crate::bytecode::execution::ExecutionContext;
use crate::bytecode::plan::ExecutionPlan;
use crate::error::FfiError;
use crate::executor::VM;
use crate::memory::arena::Arena;

use super::{
    check_plan_version, hash_bytes, read_slice, read_str, with_resp_buf, write_error, PLAN_CACHE,
};

#[no_mangle]
pub unsafe extern "C" fn flowrulz_execute(
    ctx_id: u64,
    plan_ptr: *const u8,
    plan_len: usize,
    body_ptr: *const u8,
    body_len: usize,
    caller_cb: extern "C" fn(u64, u16, *const u8, usize, *mut u8, *mut usize) -> i32,
    out_ptr: *mut u8,
    out_cap: usize,
    out_len: *mut usize,
    err_ptr: *mut u8,
    err_cap: usize,
    err_len: *mut usize,
    msg_id_ptr: *const u8,
    msg_id_len: usize,
    corr_id_ptr: *const u8,
    corr_id_len: usize,
    trace_id_ptr: *const u8,
    trace_id_len: usize,
    partition: u32,
    offset: i64,
) -> i32 {
    let plan_slice = match read_slice(plan_ptr, plan_len) {
        Some(s) => s,
        None => return FfiError::NullPointer.code(),
    };

    let plan_key = hash_bytes(plan_slice);
    let plan: Arc<ExecutionPlan> = {
        let mut cache = PLAN_CACHE.lock().unwrap();
        if let Some(cached) = cache.get(&plan_key) {
            Arc::clone(cached)
        } else {
            match bincode::deserialize(plan_slice) {
                Ok(p) => {
                    if !check_plan_version(&p) {
                        let msg = format!(
                            "bytecode version mismatch: expected {}, got {}",
                            crate::bytecode::plan::BYTECODE_VERSION,
                            p.version
                        );
                        write_error(err_ptr, err_cap, err_len, &msg);
                        return FfiError::VersionMismatch.code();
                    }
                    let arc = Arc::new(p);
                    if cache.len() >= 64 {
                        cache.clear();
                    }
                    cache.insert(plan_key, Arc::clone(&arc));
                    arc
                }
                Err(e) => {
                    write_error(
                        err_ptr,
                        err_cap,
                        err_len,
                        &format!("flowrulz_execute deserialize: {}", e),
                    );
                    return FfiError::Deserialize.code();
                }
            }
        }
    };

    let body = match read_slice(body_ptr, body_len) {
        Some(s) => s,
        None => return FfiError::NullPointer.code(),
    };

    let arena = Arena::new();
    let caller_wrapper =
        move |svc_id: u16, b: &[u8], _timeout: u64| -> Result<Vec<u8>, String> {
            with_resp_buf(|resp_buf| {
                let mut resp_len: usize = 0;

                let rc = caller_cb(
                    ctx_id,
                    svc_id,
                    b.as_ptr(),
                    b.len(),
                    resp_buf.as_mut_ptr(),
                    &mut resp_len as *mut usize,
                );

                if rc != 0 {
                    Err(format!("caller returned {}", rc))
                } else {
                    resp_buf.truncate(resp_len);
                    Ok(std::mem::take(resp_buf))
                }
            })
        };

    let mut ctx = ExecutionContext::from_body(body.to_vec());

    if !msg_id_ptr.is_null() {
        if let Some(s) = read_str(msg_id_ptr, msg_id_len) {
            ctx.event.id = s.to_string();
        }
    }
    if !corr_id_ptr.is_null() {
        if let Some(s) = read_str(corr_id_ptr, corr_id_len) {
            ctx.event.metadata.correlation_id = s.to_string();
        }
    }
    if !trace_id_ptr.is_null() {
        if let Some(s) = read_str(trace_id_ptr, trace_id_len) {
            ctx.event.metadata.trace_id = s.to_string();
        }
    }
    ctx.event.metadata.partition = partition;
    ctx.event.metadata.offset = offset;

    let mut vm = VM::new(&plan, ctx, arena, &caller_wrapper);

    match vm.run() {
        Ok(()) => {
            let result = &vm.ctx.body;
            if result.len() <= out_cap {
                unsafe {
                    std::ptr::copy_nonoverlapping(result.as_ptr(), out_ptr, result.len());
                    *out_len = result.len();
                }
            }
            if !err_ptr.is_null() && err_cap > 0 {
                unsafe {
                    *err_len = 0;
                }
            }
            0
        }
        Err(e) => {
            write_error(
                err_ptr,
                err_cap,
                err_len,
                &format!("flowrulz_execute: {}", e),
            );
            FfiError::Exec.code()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn flowrulz_execute_step(
    ctx_id: u64,
    plan_ptr: *const u8,
    plan_len: usize,
    ctx_bytes_ptr: *const u8,
    ctx_bytes_len: usize,
    resp_ptr: *const u8,
    resp_len: usize,
    caller_cb: extern "C" fn(u64, u16, *const u8, usize, *mut u8, *mut usize) -> i32,
    out_ptr: *mut u8,
    out_cap: usize,
    out_len: *mut usize,
    err_ptr: *mut u8,
    err_cap: usize,
    err_len: *mut usize,
    pending_svc_id: *mut u16,
    pending_body_ptr: *mut u8,
    pending_body_cap: usize,
    pending_body_len: *mut usize,
    pending_timeout_ms: *mut u64,
    ctx_out_ptr: *mut u8,
    ctx_out_cap: usize,
    ctx_out_len: *mut usize,
) -> i32 {
    use crate::executor::StepResult;

    let plan_slice = match read_slice(plan_ptr, plan_len) {
        Some(s) => s,
        None => return FfiError::NullPointer.code(),
    };
    let plan_key = hash_bytes(plan_slice);
    let plan: Arc<ExecutionPlan> = {
        let mut cache = PLAN_CACHE.lock().unwrap();
        if let Some(cached) = cache.get(&plan_key) {
            Arc::clone(cached)
        } else {
            let p: ExecutionPlan = match bincode::deserialize(plan_slice) {
                Ok(p) => p,
                Err(_) => return FfiError::Deserialize.code(),
            };
            if !check_plan_version(&p) {
                return FfiError::VersionMismatch.code();
            }
            let arc = Arc::new(p);
            if cache.len() >= 64 {
                cache.clear();
            }
            cache.insert(plan_key, Arc::clone(&arc));
            arc
        }
    };

    let ctx: ExecutionContext =
        if ctx_bytes_len > 0 && !ctx_bytes_ptr.is_null() {
            let slice = match read_slice(ctx_bytes_ptr, ctx_bytes_len) {
                Some(s) => s,
                None => return FfiError::NullPointer.code(),
            };
            match bincode::deserialize(slice) {
                Ok(c) => c,
                Err(_) => return FfiError::Deserialize.code(),
            }
        } else {
            let body = Vec::new();
            ExecutionContext::from_body(body)
        };

    let arena = Arena::new();
    let caller_wrapper =
        move |svc_id: u16, b: &[u8], _timeout: u64| -> Result<Vec<u8>, String> {
            with_resp_buf(|resp_buf| {
                let mut resp_len: usize = 0;
                let rc = caller_cb(
                    ctx_id,
                    svc_id,
                    b.as_ptr(),
                    b.len(),
                    resp_buf.as_mut_ptr(),
                    &mut resp_len as *mut usize,
                );
                if rc != 0 {
                    Err(format!("caller returned {}", rc))
                } else {
                    resp_buf.truncate(resp_len);
                    Ok(std::mem::take(resp_buf))
                }
            })
        };

    let mut vm = VM::new(&plan, ctx, arena, &caller_wrapper);
    let response = if !resp_ptr.is_null() {
        Some(read_slice(resp_ptr, resp_len).unwrap_or(&[]))
    } else {
        None
    };

    match vm.step(response) {
        Ok(step_result) => match step_result {
            StepResult::Done => {
                let result = &vm.ctx.body;
                if !out_ptr.is_null() && out_cap > 0 {
                    let n = result.len().min(out_cap);
                    unsafe {
                        std::ptr::copy_nonoverlapping(result.as_ptr(), out_ptr, n);
                        *out_len = n;
                    }
                }
                let ctx_bytes = bincode::serialize(&vm.ctx).unwrap_or_default();
                if !ctx_out_ptr.is_null() && ctx_out_cap > 0 {
                    let n = ctx_bytes.len().min(ctx_out_cap);
                    unsafe {
                        std::ptr::copy_nonoverlapping(ctx_bytes.as_ptr(), ctx_out_ptr, n);
                        *ctx_out_len = n;
                    }
                }
                0
            }
            StepResult::Continue => {
                let ctx_bytes = bincode::serialize(&vm.ctx).unwrap_or_default();
                if !ctx_out_ptr.is_null() && ctx_out_cap > 0 {
                    let n = ctx_bytes.len().min(ctx_out_cap);
                    unsafe {
                        std::ptr::copy_nonoverlapping(ctx_bytes.as_ptr(), ctx_out_ptr, n);
                        *ctx_out_len = n;
                    }
                }
                2
            }
            StepResult::Pending {
                svc_id,
                body,
                timeout_ms,
            } => {
                if !pending_svc_id.is_null() {
                    unsafe { *pending_svc_id = svc_id }
                }
                if !pending_body_ptr.is_null()
                    && !pending_body_len.is_null()
                    && pending_body_cap > 0
                {
                    let n = body.len().min(pending_body_cap);
                    unsafe {
                        std::ptr::copy_nonoverlapping(body.as_ptr(), pending_body_ptr, n);
                        *pending_body_len = n;
                    }
                }
                if !pending_timeout_ms.is_null() {
                    unsafe { *pending_timeout_ms = timeout_ms }
                }
                let ctx_bytes = bincode::serialize(&vm.ctx).unwrap_or_default();
                if !ctx_out_ptr.is_null() && ctx_out_cap > 0 {
                    let n = ctx_bytes.len().min(ctx_out_cap);
                    unsafe {
                        std::ptr::copy_nonoverlapping(ctx_bytes.as_ptr(), ctx_out_ptr, n);
                        *ctx_out_len = n;
                    }
                }
                1
            }
            StepResult::Delay(ms) => {
                if !pending_svc_id.is_null() {
                    unsafe { *pending_svc_id = ms as u16 }
                }
                if !pending_body_ptr.is_null()
                    && !pending_body_len.is_null()
                    && pending_body_cap >= 8
                {
                    unsafe {
                        std::ptr::write(pending_body_ptr as *mut u64, ms);
                        *pending_body_len = 8;
                    }
                }
                let ctx_bytes = bincode::serialize(&vm.ctx).unwrap_or_default();
                if !ctx_out_ptr.is_null() && ctx_out_cap > 0 {
                    let n = ctx_bytes.len().min(ctx_out_cap);
                    unsafe {
                        std::ptr::copy_nonoverlapping(ctx_bytes.as_ptr(), ctx_out_ptr, n);
                        *ctx_out_len = n;
                    }
                }
                3
            }
        },
        Err(e) => {
            write_error(
                err_ptr,
                err_cap,
                err_len,
                &format!("step: {}", e),
            );
            FfiError::Exec.code()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn flowrulz_init_context(
    body_ptr: *const u8,
    body_len: usize,
    out_ptr: *mut u8,
    out_cap: usize,
    out_len: *mut usize,
    err_ptr: *mut u8,
    err_cap: usize,
    err_len: *mut usize,
) -> i32 {
    let body = match read_slice(body_ptr, body_len) {
        Some(s) => s.to_vec(),
        None => return FfiError::NullPointer.code(),
    };
    let ctx = ExecutionContext::from_body(body);
    let bytes = match bincode::serialize(&ctx) {
        Ok(b) => b,
        Err(e) => {
            write_error(err_ptr, err_cap, err_len, &format!("serialize: {}", e));
            return FfiError::Serialize.code();
        }
    };
    let n = bytes.len().min(out_cap);
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), out_ptr, n);
        *out_len = n;
    }
    0
}
