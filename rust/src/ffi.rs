use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

use crate::bytecode::plan::ExecutionPlan;
use crate::dsl::{compiler::Compiler, lexer, optimizer, parser};
use crate::error::FfiError;
use crate::executor::plugin;
use crate::executor::{StepResult, VM};
use crate::memory::{arena::Arena, intern::InternTable};

static INTERN_TABLE: once_cell::sync::Lazy<InternTable> = once_cell::sync::Lazy::new(|| {
    let table = InternTable::new();
    table.prefill(&[
        "content-type",
        "content-length",
        "x-correlation-id",
        "x-trace-id",
        "x-flowrulz-chunk-id",
        "x-flowrulz-chunk-index",
        "x-flowrulz-chunk-total",
    ]);
    table
});

static PLAN_CACHE: once_cell::sync::Lazy<Mutex<HashMap<u64, Arc<ExecutionPlan>>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(HashMap::new()));

thread_local! {
    static RESP_BUF: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };
}

fn with_resp_buf<F, R>(f: F) -> R
where
    F: FnOnce(&mut Vec<u8>) -> R,
{
    RESP_BUF.with(|cell| {
        let mut buf = cell.borrow_mut();
        buf.resize(65536, 0);
        let r = f(&mut buf);
        buf.clear();
        r
    })
}

fn check_plan_version(plan: &ExecutionPlan) -> bool {
    plan.version == crate::bytecode::plan::BYTECODE_VERSION
}

fn hash_bytes(data: &[u8]) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    data.hash(&mut h);
    h.finish()
}

fn write_error(ptr: *mut u8, cap: usize, len: *mut usize, msg: &str) {
    if ptr.is_null() || cap == 0 || len.is_null() {
        return;
    }
    let bytes = msg.as_bytes();
    let n = bytes.len().min(cap);
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr, n);
        *len = n;
    }
}

fn read_slice<'a>(ptr: *const u8, len: usize) -> Option<&'a [u8]> {
    if ptr.is_null() {
        return None;
    }
    Some(unsafe { std::slice::from_raw_parts(ptr, len) })
}

fn read_str<'a>(ptr: *const u8, len: usize) -> Option<&'a str> {
    let slice = read_slice(ptr, len)?;
    std::str::from_utf8(slice).ok()
}

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
                        let msg = format!("bytecode version mismatch: expected {}, got {}", crate::bytecode::plan::BYTECODE_VERSION, p.version);
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
                    write_error(err_ptr, err_cap, err_len, &format!("flowrulz_execute deserialize: {}", e));
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
    let caller_wrapper = move |svc_id: u16, b: &[u8], _timeout: u64| -> Result<Vec<u8>, String> {
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

    let mut ctx = crate::bytecode::execution::ExecutionContext::from_body(body.to_vec());

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
            write_error(err_ptr, err_cap, err_len, &format!("flowrulz_execute: {}", e));
            FfiError::Exec.code()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn flowrulz_msg_alloc(size: usize) -> *mut u8 {
    if size == 0 {
        return std::ptr::null_mut();
    }
    // Store size in header before returned pointer so release() can reconstruct the layout
    let header_size = std::mem::size_of::<usize>();
    let total = header_size.checked_add(size).unwrap_or(usize::MAX);
    let layout = std::alloc::Layout::from_size_align(total, std::mem::align_of::<usize>()).unwrap();
    let base = std::alloc::alloc(layout) as *mut usize;
    if base.is_null() {
        return std::ptr::null_mut();
    }
    base.write(size);
    base.add(1) as *mut u8
}

#[no_mangle]
pub unsafe extern "C" fn flowrulz_msg_release(ptr: *mut u8) {
    if ptr.is_null() {
        return;
    }
    let base = (ptr as *mut usize).sub(1);
    let size = base.read();
    let header_size = std::mem::size_of::<usize>();
    let total = header_size.checked_add(size).unwrap_or(usize::MAX);
    let layout = std::alloc::Layout::from_size_align(total, std::mem::align_of::<usize>()).unwrap();
    std::alloc::dealloc(base as *mut u8, layout);
}

#[no_mangle]
pub unsafe extern "C" fn flowrulz_intern(s_ptr: *const u8, s_len: usize) -> u16 {
    let s = match read_str(s_ptr, s_len) {
        Some(s) => s,
        None => return 0,
    };
    INTERN_TABLE.intern(s)
}

#[no_mangle]
pub unsafe extern "C" fn flowrulz_intern_lookup(id: u16, out_ptr: *mut u8, out_len: *mut usize) {
	if out_ptr.is_null() || out_len.is_null() {
		return;
	}
	if let Some(s) = INTERN_TABLE.lookup(id) {
		let bytes = s.as_bytes();
		unsafe {
			std::ptr::copy_nonoverlapping(bytes.as_ptr(), out_ptr, bytes.len());
			*out_len = bytes.len();
		}
	}
}

#[no_mangle]
pub unsafe extern "C" fn flowrulz_span_size() -> usize {
    std::mem::size_of::<crate::tracing::Span>()
}

#[no_mangle]
pub unsafe extern "C" fn flowrulz_get_spans(out_ptr: *mut u8, out_cap: usize) -> usize {
    if out_ptr.is_null() || out_cap == 0 {
        return 0;
    }
    let out_slice = unsafe { std::slice::from_raw_parts_mut(out_ptr, out_cap) };
    crate::tracing::SPAN_BUFFER.with(|buf| {
        buf.borrow_mut().drain(out_slice)
    })
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

    let ctx: crate::bytecode::execution::ExecutionContext = if ctx_bytes_len > 0 && !ctx_bytes_ptr.is_null() {
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
        crate::bytecode::execution::ExecutionContext::from_body(body)
    };

    let arena = crate::memory::arena::Arena::new();
    let caller_wrapper = move |svc_id: u16, b: &[u8], _timeout: u64| -> Result<Vec<u8>, String> {
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
            StepResult::Pending { svc_id, body, timeout_ms } => {
                if !pending_svc_id.is_null() {
                    unsafe { *pending_svc_id = svc_id; }
                }
                if !pending_body_ptr.is_null() && !pending_body_len.is_null() && pending_body_cap > 0 {
                    let n = body.len().min(pending_body_cap);
                    unsafe {
                        std::ptr::copy_nonoverlapping(body.as_ptr(), pending_body_ptr, n);
                        *pending_body_len = n;
                    }
                }
                if !pending_timeout_ms.is_null() {
                    unsafe { *pending_timeout_ms = timeout_ms; }
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
                    unsafe { *pending_svc_id = ms as u16; }
                }
                if !pending_body_ptr.is_null() && !pending_body_len.is_null() && pending_body_cap >= 8 {
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
            write_error(err_ptr, err_cap, err_len, &format!("step: {}", e));
            FfiError::Exec.code()
        }
    }
}

/// Creates an initial serialized ExecutionContext from a body payload.
/// Returns bincode-encoded bytes in out_ptr. Caller must provide a
/// sufficiently large buffer (usually 256KB is safe).
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
    let ctx = crate::bytecode::execution::ExecutionContext::from_body(body);
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

#[no_mangle]
pub unsafe extern "C" fn flowrulz_register_plugin(
    name_ptr: *const u8,
    name_len: usize,
    wasm_ptr: *const u8,
    wasm_len: usize,
) -> i32 {
    let name = match read_str(name_ptr, name_len) {
        Some(s) => s,
        None => return FfiError::InvalidUtf8.code(),
    };
    let wasm_bytes = match read_slice(wasm_ptr, wasm_len) {
        Some(s) => s,
        None => return FfiError::NullPointer.code(),
    };
    plugin::register(name, wasm_bytes);
    0
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
