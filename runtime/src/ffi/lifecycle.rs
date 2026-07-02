use crate::error::FfiError;
use crate::executor::plugin;

use super::{read_slice, read_str};

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
