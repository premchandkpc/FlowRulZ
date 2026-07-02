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
