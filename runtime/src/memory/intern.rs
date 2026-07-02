use std::collections::HashMap;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::RwLock;

use boxcar::Vec as BoxcarVec;

pub struct InternTable {
    fwd: RwLock<HashMap<String, u16>>,
    rev: BoxcarVec<String>,
    next: AtomicU16,
}

impl InternTable {
    pub fn new() -> Self {
        let rev = BoxcarVec::new();
        InternTable {
            fwd: RwLock::new(HashMap::new()),
            rev,
            next: AtomicU16::new(0),
        }
    }

    pub fn prefill(&self, strings: &[&str]) {
        let mut fwd = self.fwd.write().unwrap();
        for &s in strings {
            let id = self.next.fetch_add(1, Ordering::Relaxed);
            fwd.insert(s.to_string(), id);
            self.rev.push(s.to_string());
        }
    }

    pub fn intern(&self, s: &str) -> u16 {
        {
            let fwd = self.fwd.read().unwrap();
            if let Some(&id) = fwd.get(s) {
                return id;
            }
        }
        let mut fwd = self.fwd.write().unwrap();
        if let Some(&id) = fwd.get(s) {
            return id;
        }
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        fwd.insert(s.to_string(), id);
        self.rev.push(s.to_string());
        id
    }

    pub fn lookup(&self, id: u16) -> Option<&str> {
        self.rev.get(id as usize).map(|s| s.as_str())
    }

    pub fn len(&self) -> u16 {
        self.next.load(Ordering::Relaxed)
    }
}

impl Default for InternTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_intern_and_lookup() {
        let table = InternTable::new();
        let id = table.intern("hello");
        assert_eq!(table.lookup(id), Some("hello"));
    }

    #[test]
    fn test_intern_dedup() {
        let table = InternTable::new();
        let id1 = table.intern("hello");
        let id2 = table.intern("hello");
        assert_eq!(id1, id2);
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn test_intern_multiple() {
        let table = InternTable::new();
        let a = table.intern("foo");
        let b = table.intern("bar");
        assert_ne!(a, b);
        assert_eq!(table.len(), 2);
        assert_eq!(table.lookup(a), Some("foo"));
        assert_eq!(table.lookup(b), Some("bar"));
    }

    #[test]
    fn test_prefill() {
        let table = InternTable::new();
        table.prefill(&["content-type", "content-length"]);
        assert_eq!(table.len(), 2);
        let id1 = table.intern("content-type");
        let id2 = table.intern("content-length");
        // Should return same IDs as prefill
        assert_eq!(table.len(), 2);
        assert!(id1 < 2);
        assert!(id2 < 2);
    }

    #[test]
    fn test_lookup_invalid_id() {
        let table = InternTable::new();
        assert_eq!(table.lookup(999), None);
    }

    #[test]
    fn test_intern_empty_string() {
        let table = InternTable::new();
        let id = table.intern("");
        assert_eq!(table.lookup(id), Some(""));
    }

    #[test]
    fn test_intern_unicode() {
        let table = InternTable::new();
        let id = table.intern("héllo wörld 🎉");
        assert_eq!(table.lookup(id), Some("héllo wörld 🎉"));
    }

    #[test]
    fn test_new_table_is_empty() {
        let table = InternTable::new();
        assert_eq!(table.len(), 0);
    }
}
