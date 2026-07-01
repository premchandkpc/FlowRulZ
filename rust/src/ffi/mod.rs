use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

use crate::bytecode::plan::ExecutionPlan;
use crate::memory::intern::InternTable;

pub mod compile;
pub mod execute;
pub mod lifecycle;
pub mod memory;
pub mod services;
pub mod tracing;

pub(crate) static INTERN_TABLE: once_cell::sync::Lazy<InternTable> =
    once_cell::sync::Lazy::new(|| {
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

pub(crate) static PLAN_CACHE: once_cell::sync::Lazy<Mutex<HashMap<u64, Arc<ExecutionPlan>>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(HashMap::new()));

thread_local! {
    static RESP_BUF: std::cell::RefCell<Vec<u8>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

pub(crate) fn with_resp_buf<F, R>(f: F) -> R
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

pub(crate) fn check_plan_version(plan: &ExecutionPlan) -> bool {
    plan.version == crate::bytecode::plan::BYTECODE_VERSION
}

pub(crate) fn hash_bytes(data: &[u8]) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    data.hash(&mut h);
    h.finish()
}

pub(crate) fn write_error(ptr: *mut u8, cap: usize, len: *mut usize, msg: &str) {
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

pub(crate) fn read_slice<'a>(ptr: *const u8, len: usize) -> Option<&'a [u8]> {
    if ptr.is_null() {
        return None;
    }
    Some(unsafe { std::slice::from_raw_parts(ptr, len) })
}

pub(crate) fn read_str<'a>(ptr: *const u8, len: usize) -> Option<&'a str> {
    let slice = read_slice(ptr, len)?;
    std::str::from_utf8(slice).ok()
}
