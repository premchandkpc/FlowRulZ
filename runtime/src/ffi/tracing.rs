/// # Safety
/// Returns the size of the Span struct in bytes.
#[no_mangle]
pub unsafe extern "C" fn flowrulz_span_size() -> usize {
    std::mem::size_of::<crate::tracing::Span>()
}

/// # Safety
/// `out_ptr` must point to a valid buffer of at least `out_cap` bytes.
#[no_mangle]
pub unsafe extern "C" fn flowrulz_get_spans(out_ptr: *mut u8, out_cap: usize) -> usize {
    if out_ptr.is_null() || out_cap == 0 {
        return 0;
    }
    let out_slice = unsafe { std::slice::from_raw_parts_mut(out_ptr, out_cap) };
    match crate::tracing::SPAN_BUFFER.lock() {
        Ok(mut buf) => buf.drain(out_slice),
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_span_size() {
        let size = unsafe { flowrulz_span_size() };
        assert_eq!(size, std::mem::size_of::<crate::tracing::Span>());
    }

    #[test]
    fn test_get_spans_empty() {
        crate::tracing::drain_global_buffer();
        let mut buf = [0u8; 128];
        let written = unsafe { flowrulz_get_spans(buf.as_mut_ptr(), buf.len()) };
        assert_eq!(written, 0);
    }

    #[test]
    fn test_get_spans_null_ptr() {
        let written = unsafe { flowrulz_get_spans(std::ptr::null_mut(), 100) };
        assert_eq!(written, 0);
    }

    #[test]
    fn test_get_spans_zero_cap() {
        let mut buf = [0u8; 128];
        let written = unsafe { flowrulz_get_spans(buf.as_mut_ptr(), 0) };
        assert_eq!(written, 0);
    }

    #[test]
    fn test_get_spans_after_emit() {
        crate::tracing::emit_span(crate::tracing::Span {
            opcode: 7,
            service_id: 99,
            layer: 1,
            duration_ns: 1234,
            status: 0,
        });

        let mut buf = [0u8; 256];
        let written = unsafe { flowrulz_get_spans(buf.as_mut_ptr(), buf.len()) };
        assert!(written >= std::mem::size_of::<crate::tracing::Span>());

        // Read the span back
        let span: crate::tracing::Span = unsafe { std::ptr::read(buf.as_ptr() as *const crate::tracing::Span) };
        assert_eq!(span.opcode, 7);
        assert_eq!(span.service_id, 99);
        assert_eq!(span.duration_ns, 1234);
    }

    #[test]
    fn test_get_spans_multiple() {
        for i in 0..3 {
            crate::tracing::emit_span(crate::tracing::Span {
                opcode: i,
                service_id: i as u16,
                layer: 0,
                duration_ns: i as u64 * 100,
                status: 0,
            });
        }

        let span_size = std::mem::size_of::<crate::tracing::Span>();
        let mut buf = vec![0u8; span_size * 10];
        let written = unsafe { flowrulz_get_spans(buf.as_mut_ptr(), buf.len()) };
        assert_eq!(written, span_size * 3);
    }
}
