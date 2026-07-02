use super::{read_str, INTERN_TABLE};

#[no_mangle]
pub unsafe extern "C" fn flowrulz_msg_alloc(size: usize) -> *mut u8 {
    if size == 0 {
        return std::ptr::null_mut();
    }
    let header_size = std::mem::size_of::<usize>();
    let total = header_size.checked_add(size).unwrap_or(usize::MAX);
    let layout =
        std::alloc::Layout::from_size_align(total, std::mem::align_of::<usize>()).unwrap();
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
    let layout =
        std::alloc::Layout::from_size_align(total, std::mem::align_of::<usize>()).unwrap();
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
pub unsafe extern "C" fn flowrulz_intern_lookup(
    id: u16,
    out_ptr: *mut u8,
    out_len: *mut usize,
) {
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
