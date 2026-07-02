use bumpalo::Bump;

pub struct Arena {
    bump: Bump,
}

unsafe impl Send for Arena {}

impl Arena {
    pub fn new() -> Self {
        Arena { bump: Bump::new() }
    }

    pub fn alloc(&self, n: usize) -> &mut [u8] {
        self.bump.alloc_slice_fill_default(n)
    }

    pub fn alloc_copy(&self, src: &[u8]) -> &mut [u8] {
        let buf = self.alloc(src.len());
        buf.copy_from_slice(src);
        buf
    }

    pub fn reset(&mut self) {
        self.bump.reset();
    }

    pub fn allocated_bytes(&self) -> usize {
        self.bump.allocated_bytes()
    }
}

impl Default for Arena {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arena_alloc() {
        let arena = Arena::new();
        let buf = arena.alloc(10);
        assert_eq!(buf.len(), 10);
        buf[0] = 42;
        assert_eq!(buf[0], 42);
    }

    #[test]
    fn test_arena_alloc_copy() {
        let arena = Arena::new();
        let src = b"hello world";
        let copied = arena.alloc_copy(src);
        assert_eq!(copied, src);
        // Modifying copy should not affect original
        copied[0] = b'j';
        assert_eq!(src[0], b'h');
    }

    #[test]
    fn test_arena_alloc_multiple() {
        let arena = Arena::new();
        let a = arena.alloc_copy(b"aaa");
        let b = arena.alloc_copy(b"bbbb");
        assert_eq!(a, b"aaa");
        assert_eq!(b, b"bbbb");
    }

    #[test]
    fn test_arena_reset() {
        let mut arena = Arena::new();
        let a = arena.alloc_copy(b"data");
        assert_eq!(a, b"data");
        arena.reset();
        let b = arena.alloc_copy(b"new");
        assert_eq!(b, b"new");
    }

    #[test]
    fn test_arena_allocated_bytes() {
        let arena = Arena::new();
        let before = arena.allocated_bytes();
        arena.alloc_copy(b"some data here");
        let after = arena.allocated_bytes();
        assert!(after > before);
    }

    #[test]
    fn test_arena_default() {
        let arena = Arena::default();
        assert!(arena.allocated_bytes() >= 0);
    }
}
