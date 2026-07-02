use crate::error::FfiError;
use crate::executor::plugin;

use super::{read_slice, read_str};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_plugin_invalid_name() {
        let wasm_bytes = b"fake_wasm";
        let rc = unsafe {
            flowrulz_register_plugin(
                std::ptr::null(),
                0,
                wasm_bytes.as_ptr(),
                wasm_bytes.len(),
            )
        };
        assert_eq!(rc, -2); // InvalidUtf8
    }

    #[test]
    fn test_register_plugin_null_wasm() {
        let name = b"test_plugin";
        let rc = unsafe {
            flowrulz_register_plugin(
                name.as_ptr(),
                name.len(),
                std::ptr::null(),
                0,
            )
        };
        assert_eq!(rc, -1); // NullPointer
    }

    #[test]
    fn test_register_plugin_success() {
        let name = b"empty_plugin";
        let wasm_bytes = b"";
        let rc = unsafe {
            flowrulz_register_plugin(
                name.as_ptr(),
                name.len(),
                wasm_bytes.as_ptr(),
                wasm_bytes.len(),
            )
        };
        assert_eq!(rc, 0);
    }
}

/// # Safety
/// `name_ptr` must point to a valid UTF-8 string of length `name_len`.
/// `wasm_ptr` must point to valid data of length `wasm_len`.
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
