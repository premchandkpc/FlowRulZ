use std::panic;

use super::{read_str, INTERN_TABLE};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_msg_alloc_and_release() {
        let ptr = unsafe { flowrulz_msg_alloc(100) };
        assert!(!ptr.is_null());
        // Write some data
        unsafe {
            std::ptr::write_bytes(ptr, 0xAB, 100);
            assert_eq!(std::ptr::read(ptr), 0xAB);
        }
        unsafe { flowrulz_msg_release(ptr) };
    }

    #[test]
    fn test_msg_alloc_zero_size() {
        let ptr = unsafe { flowrulz_msg_alloc(0) };
        assert!(ptr.is_null());
    }

    #[test]
    fn test_msg_release_null() {
        // Should not panic
        unsafe { flowrulz_msg_release(std::ptr::null_mut()) };
    }

    #[test]
    fn test_msg_alloc_large_size() {
        let ptr = unsafe { flowrulz_msg_alloc(1024 * 1024) }; // 1MB
        assert!(!ptr.is_null());
        unsafe { flowrulz_msg_release(ptr) };
    }

    #[test]
    fn test_intern_and_lookup() {
        let s = b"my-header";
        let id = unsafe { flowrulz_intern(s.as_ptr(), s.len()) };
        assert!(id > 0); // Not 0, which is reserved for null/invalid

        let mut out_buf = [0u8; 64];
        let mut out_len: usize = 0;
        unsafe {
            flowrulz_intern_lookup(id, out_buf.as_mut_ptr(), &mut out_len as *mut usize);
        }
        assert_eq!(&out_buf[..out_len], b"my-header");
    }

    #[test]
    fn test_intern_empty_string() {
        let id = unsafe { flowrulz_intern(std::ptr::null(), 0) };
        assert_eq!(id, 0); // Invalid input returns 0
    }

    #[test]
    fn test_intern_lookup_invalid_id() {
        let mut out_buf = [0u8; 64];
        let mut out_len: usize = 0;
        unsafe {
            flowrulz_intern_lookup(999, out_buf.as_mut_ptr(), &mut out_len as *mut usize);
        }
        assert_eq!(out_len, 0); // Nothing written
    }

    #[test]
    fn test_intern_lookup_null_out_ptr() {
        unsafe {
            flowrulz_intern_lookup(0, std::ptr::null_mut(), std::ptr::null_mut());
        }
        // Should not panic
    }

    #[test]
    fn test_intern_prefilled_headers() {
        let s = b"content-type";
        let id = unsafe { flowrulz_intern(s.as_ptr(), s.len()) };
        // Prefilled entries start at id=0, so "content-type" should have a valid id
        let mut out_buf = [0u8; 64];
        let mut out_len: usize = 0;
        unsafe {
            flowrulz_intern_lookup(id, out_buf.as_mut_ptr(), &mut out_len as *mut usize);
        }
        assert_eq!(&out_buf[..out_len], b"content-type");
    }

    #[test]
    fn test_intern_lookup_miss_sets_zero_len() {
        let mut out_buf = [0u8; 64];
        let mut out_len: usize = 42; // pre-set to non-zero
        unsafe {
            flowrulz_intern_lookup(9999, out_buf.as_mut_ptr(), &mut out_len as *mut usize);
        }
        assert_eq!(out_len, 0);
    }

    #[test]
    fn test_intern_lookup_valid_id() {
        let s = b"test-value";
        let id = unsafe { flowrulz_intern(s.as_ptr(), s.len()) };
        assert!(id > 0);

        let mut out_buf = [0u8; 64];
        let mut out_len: usize = 0;
        unsafe {
            flowrulz_intern_lookup(id, out_buf.as_mut_ptr(), &mut out_len as *mut usize);
        }
        assert_eq!(&out_buf[..out_len], b"test-value");
    }

    #[test]
    fn test_intern_lookup_does_not_write_on_hit() {
        let s = b"another-value";
        let id = unsafe { flowrulz_intern(s.as_ptr(), s.len()) };

        let mut out_buf = [0xFFu8; 64];
        let mut out_len: usize = 0;
        unsafe {
            flowrulz_intern_lookup(id, out_buf.as_mut_ptr(), &mut out_len as *mut usize);
        }
        assert_eq!(out_len, s.len());
        assert_eq!(&out_buf[..out_len], s);
    }

    #[test]
    fn test_msg_alloc_and_release_roundtrip() {
        let sizes = [1, 16, 256, 1024, 65536];
        for size in sizes {
            let ptr = unsafe { flowrulz_msg_alloc(size) };
            assert!(!ptr.is_null(), "alloc({}) returned null", size);
            unsafe {
                std::ptr::write_bytes(ptr, 0xAA, size);
                assert_eq!(*ptr, 0xAA);
            }
            unsafe { flowrulz_msg_release(ptr) };
        }
    }

    #[test]
    fn test_msg_release_does_not_panic_on_arbitrary_ptr() {
        let ptr = unsafe { flowrulz_msg_alloc(8) };
        assert!(!ptr.is_null());
        unsafe {
            std::ptr::write_bytes(ptr, 0xBB, 8);
        }
        unsafe { flowrulz_msg_release(ptr) };
    }
}

/// # Safety
/// Allocates a buffer of `size` bytes. Returns null if size is 0 or allocation fails.
#[no_mangle]
pub unsafe extern "C" fn flowrulz_msg_alloc(size: usize) -> *mut u8 {
    match panic::catch_unwind(panic::AssertUnwindSafe(|| {
        if size == 0 {
            return std::ptr::null_mut();
        }
        let header_size = std::mem::size_of::<usize>();
        let total = header_size.checked_add(size).unwrap_or(usize::MAX);
        let layout = match std::alloc::Layout::from_size_align(total, std::mem::align_of::<usize>()) {
            Ok(l) => l,
            Err(_) => return std::ptr::null_mut(),
        };
        let base = std::alloc::alloc(layout) as *mut usize;
        if base.is_null() {
            return std::ptr::null_mut();
        }
        base.write(size);
        base.add(1) as *mut u8
    })) {
        Ok(ptr) => ptr,
        Err(_) => {
            eprintln!("[flowrulz] panic in flowrulz_msg_alloc");
            std::ptr::null_mut()
        }
    }
}

/// # Safety
/// `ptr` must have been returned by `flowrulz_msg_alloc` and not yet freed.
#[no_mangle]
pub unsafe extern "C" fn flowrulz_msg_release(ptr: *mut u8) {
    let _ = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        if ptr.is_null() {
            return;
        }
        let base = (ptr as *mut usize).sub(1);
        let size = base.read();
        let header_size = std::mem::size_of::<usize>();
        let total = header_size.checked_add(size).unwrap_or(usize::MAX);
        let layout = match std::alloc::Layout::from_size_align(total, std::mem::align_of::<usize>()) {
            Ok(l) => l,
            Err(_) => return,
        };
        std::alloc::dealloc(base as *mut u8, layout);
    }));
}

/// # Safety
/// `s_ptr` must point to a valid UTF-8 string of length `s_len`.
#[no_mangle]
pub unsafe extern "C" fn flowrulz_intern(s_ptr: *const u8, s_len: usize) -> u16 {
    match panic::catch_unwind(panic::AssertUnwindSafe(|| {
        let s = match read_str(s_ptr, s_len) {
            Some(s) => s,
            None => return 0u16,
        };
        INTERN_TABLE.intern(s)
    })) {
        Ok(id) => id,
        Err(_) => {
            eprintln!("[flowrulz] panic in flowrulz_intern");
            0
        }
    }
}

/// # Safety
/// `out_ptr` and `out_len` must be valid pointers with sufficient capacity.
#[no_mangle]
pub unsafe extern "C" fn flowrulz_intern_lookup(
    id: u16,
    out_ptr: *mut u8,
    out_len: *mut usize,
) {
    let _ = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        if out_ptr.is_null() || out_len.is_null() {
            return;
        }
        if let Some(s) = INTERN_TABLE.lookup(id) {
            let bytes = s.as_bytes();
            unsafe {
                std::ptr::copy_nonoverlapping(bytes.as_ptr(), out_ptr, bytes.len());
                *out_len = bytes.len();
            }
        } else {
            unsafe {
                *out_len = 0;
            }
        }
    }));
}
