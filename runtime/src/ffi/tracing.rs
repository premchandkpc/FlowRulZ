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
    crate::tracing::SPAN_BUFFER.with(|buf| buf.borrow_mut().drain(out_slice))
}
