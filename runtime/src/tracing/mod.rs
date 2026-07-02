use std::sync::atomic::{AtomicU64, Ordering};

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct Span {
    pub opcode: u8,
    pub service_id: u16,
    pub layer: u8,
    pub duration_ns: u64,
    pub status: u8,
}

pub const SPAN_BUFFER_CAPACITY: usize = 1024;

pub struct SpanRingBuffer {
    buffer: Box<[Span; SPAN_BUFFER_CAPACITY]>,
    head: AtomicU64,
    tail: AtomicU64,
}

impl SpanRingBuffer {
    pub fn new() -> Self {
        SpanRingBuffer {
            buffer: Box::new([Span {
                opcode: 0,
                service_id: 0,
                layer: 0,
                duration_ns: 0,
                status: 0,
            }; SPAN_BUFFER_CAPACITY]),
            head: AtomicU64::new(0),
            tail: AtomicU64::new(0),
        }
    }

    pub fn push(&mut self, span: Span) {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Acquire);

        if head - tail >= SPAN_BUFFER_CAPACITY as u64 {
            return;
        }

        let idx = (head % SPAN_BUFFER_CAPACITY as u64) as usize;
        // Safety: we own the buffer and idx is bounded
        unsafe {
            *self.buffer.as_mut_ptr().add(idx) = span;
        }
        self.head.store(head + 1, Ordering::Release);
    }

    pub fn drain(&mut self, out: &mut [u8]) -> usize {
        let span_size = std::mem::size_of::<Span>();
        let max_spans = out.len() / span_size;
        let mut written = 0usize;

        loop {
            let tail = self.tail.load(Ordering::Acquire);
            let head = self.head.load(Ordering::Relaxed);
            if tail >= head || written >= max_spans {
                break;
            }
            let idx = (tail % SPAN_BUFFER_CAPACITY as u64) as usize;
            let span = unsafe { *self.buffer.as_mut_ptr().add(idx) };
            let dest_start = written * span_size;
            if dest_start + span_size > out.len() {
                break;
            }
            unsafe {
                std::ptr::copy_nonoverlapping(
                    &span as *const Span as *const u8,
                    out.as_mut_ptr().add(dest_start),
                    span_size,
                );
            }
            written += 1;
            self.tail.store(tail + 1, Ordering::Release);
        }

        written * span_size
    }
}

thread_local! {
    pub static SPAN_BUFFER: std::cell::RefCell<SpanRingBuffer> =
        std::cell::RefCell::new(SpanRingBuffer::new());
}

pub fn emit_span(span: Span) {
    SPAN_BUFFER.with(|buf| {
        buf.borrow_mut().push(span);
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_span(opcode: u8, svc_id: u16, duration_ns: u64, status: u8) -> Span {
        Span { opcode, service_id: svc_id, layer: 0, duration_ns, status }
    }

    #[test]
    fn test_span_ring_buffer_new() {
        let buf = SpanRingBuffer::new();
        assert_eq!(buf.head.load(std::sync::atomic::Ordering::Relaxed), 0);
        assert_eq!(buf.tail.load(std::sync::atomic::Ordering::Relaxed), 0);
    }

    #[test]
    fn test_span_ring_buffer_push_and_drain() {
        let mut buf = SpanRingBuffer::new();
        buf.push(make_span(1, 100, 1000, 0));
        buf.push(make_span(2, 200, 2000, 1));

        let span_size = std::mem::size_of::<Span>();
        let mut out = vec![0u8; span_size * 10];
        let written = buf.drain(&mut out);

        // Should have written 2 spans
        assert_eq!(written, span_size * 2);

        // Read back spans from output bytes
        let span1: Span = unsafe { std::ptr::read(out.as_ptr() as *const Span) };
        let span2: Span = unsafe { std::ptr::read(out.as_ptr().add(span_size) as *const Span) };

        assert_eq!(span1.opcode, 1);
        assert_eq!(span1.service_id, 100);
        assert_eq!(span1.duration_ns, 1000);
        assert_eq!(span2.opcode, 2);
        assert_eq!(span2.service_id, 200);
    }

    #[test]
    fn test_span_ring_buffer_empty_drain() {
        let mut buf = SpanRingBuffer::new();
        let span_size = std::mem::size_of::<Span>();
        let mut out = vec![0u8; span_size * 10];
        let written = buf.drain(&mut out);
        assert_eq!(written, 0);
    }

    #[test]
    fn test_span_ring_buffer_boundary() {
        let mut buf = SpanRingBuffer::new();
        // Fill up to capacity
        for i in 0..SPAN_BUFFER_CAPACITY {
            buf.push(make_span(i as u8, i as u16, i as u64, 0));
        }
        // This one should be dropped (full)
        buf.push(make_span(255, 999, 999, 0));

        let span_size = std::mem::size_of::<Span>();
        let mut out = vec![0u8; span_size * (SPAN_BUFFER_CAPACITY + 10)];
        let written = buf.drain(&mut out);
        // Should drain exactly capacity
        assert_eq!(written / span_size, SPAN_BUFFER_CAPACITY);
    }

    #[test]
    fn test_drain_with_small_output_buffer() {
        let mut buf = SpanRingBuffer::new();
        buf.push(make_span(1, 1, 1, 0));
        buf.push(make_span(2, 2, 2, 0));

        let span_size = std::mem::size_of::<Span>();
        let mut out = vec![0u8; span_size - 1]; // smaller than one span
        let written = buf.drain(&mut out);
        assert_eq!(written, 0);
    }

    #[test]
    fn test_emit_span() {
        emit_span(make_span(42, 7, 500, 1));
        let span_size = std::mem::size_of::<Span>();
        let mut out = vec![0u8; span_size];
        let written = SPAN_BUFFER.with(|buf| buf.borrow_mut().drain(&mut out));
        assert_eq!(written, span_size);
    }

    #[test]
    fn test_span_size_value() {
        assert_eq!(std::mem::size_of::<Span>(), 24);
    }
}
