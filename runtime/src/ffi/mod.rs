use std::num::NonZeroUsize;
use std::hash::{Hash, Hasher};
use std::panic;
use std::sync::{Arc, Mutex};

use lru::LruCache;

use crate::bytecode::plan::ExecutionPlan;
use crate::error::FfiError;
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

pub(crate) static PLAN_CACHE: once_cell::sync::Lazy<Mutex<LruCache<u64, Arc<ExecutionPlan>>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(LruCache::new(NonZeroUsize::new(64).unwrap())));

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

/// Wraps an `extern "C"` function body in `catch_unwind` to prevent Rust panics
/// from unwinding across the FFI boundary (which is undefined behavior).
///
/// If the closure panics, returns `FfiError::Panic.code()` (-11).
/// The panic message is logged to stderr before returning.
pub(crate) fn ffi_catch_unwind<F: FnOnce() -> i32 + panic::UnwindSafe>(f: F) -> i32 {
    match panic::catch_unwind(f) {
        Ok(code) => code,
        Err(payload) => {
            let msg = if let Some(s) = payload.downcast_ref::<&str>() {
                s.to_string()
            } else if let Some(s) = payload.downcast_ref::<String>() {
                s.clone()
            } else {
                "unknown panic".to_string()
            };
            eprintln!("[flowrulz] FFI panic caught: {}", msg);
            FfiError::Panic.code()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bytecode::plan::BYTECODE_VERSION;

    #[test]
    fn test_check_plan_version_match() {
        let mut plan = ExecutionPlan::new("test");
        plan.version = BYTECODE_VERSION;
        assert!(check_plan_version(&plan));
    }

    #[test]
    fn test_check_plan_version_mismatch() {
        let mut plan = ExecutionPlan::new("test");
        plan.version = BYTECODE_VERSION + 1;
        assert!(!check_plan_version(&plan));
    }

    #[test]
    fn test_hash_bytes_deterministic() {
        let data = b"hello";
        let h1 = hash_bytes(data);
        let h2 = hash_bytes(data);
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_hash_bytes_different_inputs() {
        let h1 = hash_bytes(b"hello");
        let h2 = hash_bytes(b"world");
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_read_slice_valid() {
        let data = b"hello";
        let slice = read_slice(data.as_ptr(), data.len());
        assert_eq!(slice, Some(b"hello" as &[u8]));
    }

    #[test]
    fn test_read_slice_null_ptr() {
        let slice = read_slice(std::ptr::null(), 5);
        assert_eq!(slice, None);
    }

    #[test]
    fn test_read_slice_zero_len() {
        let data = b"hello";
        let slice = read_slice(data.as_ptr(), 0);
        assert_eq!(slice, Some(b"" as &[u8]));
    }

    #[test]
    fn test_read_str_valid_utf8() {
        let data = b"hello";
        let s = read_str(data.as_ptr(), data.len());
        assert_eq!(s, Some("hello"));
    }

    #[test]
    fn test_read_str_invalid_utf8() {
        let data = b"\xff\xfe";
        let s = read_str(data.as_ptr(), data.len());
        assert_eq!(s, None);
    }

    #[test]
    fn test_read_str_null_ptr() {
        let s = read_str(std::ptr::null(), 5);
        assert_eq!(s, None);
    }

    #[test]
    fn test_write_error_normal() {
        let mut buf = [0u8; 32];
        let mut written: usize = 0;
        write_error(buf.as_mut_ptr(), buf.len(), &mut written as *mut usize, "test error");
        assert_eq!(&buf[..written], b"test error");
    }

    #[test]
    fn test_write_error_truncated() {
        let mut buf = [0u8; 4];
        let mut written: usize = 0;
        write_error(buf.as_mut_ptr(), buf.len(), &mut written as *mut usize, "too long error message");
        assert_eq!(&buf[..written], b"too ");
    }

    #[test]
    fn test_write_error_null_ptr() {
        // Should not panic
        write_error(std::ptr::null_mut(), 0, std::ptr::null_mut(), "test");
        write_error(std::ptr::null_mut(), 10, std::ptr::null_mut(), "test");
        let mut buf = [0u8; 10];
        write_error(buf.as_mut_ptr(), 0, std::ptr::null_mut(), "test");
    }

    #[test]
    fn test_with_resp_buf() {
        let result = with_resp_buf(|buf| {
            buf[..5].copy_from_slice(b"hello");
            buf.len()
        });
        assert!(result >= 5);
    }

    #[test]
    fn test_plan_cache_basic() {
        let mut cache = PLAN_CACHE.lock().unwrap();
        let plan = Arc::new(ExecutionPlan::new("cached_rule"));
        let key = 42u64;
        cache.put(key, Arc::clone(&plan));
        let retrieved = cache.get(&key).cloned();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().rule_id, "cached_rule");
        cache.clear();
    }

    #[test]
    fn test_plan_cache_eviction() {
        let mut cache = PLAN_CACHE.lock().unwrap();
        for i in 0..100u64 {
            cache.put(i, Arc::new(ExecutionPlan::new(&format!("rule_{}", i))));
        }
        assert!(cache.len() <= 64);
        cache.clear();
    }

    #[test]
    fn test_intern_table_prefilled() {
        let table = &INTERN_TABLE;
        let id = table.intern("content-type");
        assert!(table.lookup(id).is_some());
        // Should be < the number of prefilled entries
        assert!(id < 10);
    }
}
