use std::collections::HashMap;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ConstantPool {
    entries: Vec<String>,
    index: HashMap<String, u16>,
}

impl ConstantPool {
    pub fn new() -> Self {
        ConstantPool {
            entries: Vec::new(),
            index: HashMap::new(),
        }
    }

    pub fn add(&mut self, s: &str) -> u16 {
        if let Some(&id) = self.index.get(s) {
            return id;
        }
        let id = self.entries.len() as u16;
        self.entries.push(s.to_string());
        self.index.insert(s.to_string(), id);
        id
    }

    pub fn get(&self, id: u16) -> &str {
        &self.entries[id as usize]
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn entries(&self) -> &[String] {
        &self.entries
    }
}

impl Default for ConstantPool {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constant_pool_new() {
        let pool = ConstantPool::new();
        assert!(pool.is_empty());
        assert_eq!(pool.len(), 0);
    }

    #[test]
    fn test_constant_pool_add_and_get() {
        let mut pool = ConstantPool::new();
        let id = pool.add("hello");
        assert_eq!(pool.get(id), "hello");
        assert_eq!(pool.len(), 1);
    }

    #[test]
    fn test_constant_pool_dedup() {
        let mut pool = ConstantPool::new();
        let id1 = pool.add("hello");
        let id2 = pool.add("hello");
        assert_eq!(id1, id2);
        assert_eq!(pool.len(), 1);
    }

    #[test]
    fn test_constant_pool_multiple_entries() {
        let mut pool = ConstantPool::new();
        let a = pool.add("foo");
        let b = pool.add("bar");
        let c = pool.add("baz");
        assert_ne!(a, b);
        assert_ne!(b, c);
        assert_eq!(pool.len(), 3);
        assert_eq!(pool.get(a), "foo");
        assert_eq!(pool.get(b), "bar");
        assert_eq!(pool.get(c), "baz");
    }

    #[test]
    fn test_constant_pool_entries() {
        let mut pool = ConstantPool::new();
        pool.add("first");
        pool.add("second");
        let entries = pool.entries();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], "first");
        assert_eq!(entries[1], "second");
    }

    #[test]
    fn test_serialization_roundtrip() {
        let mut pool = ConstantPool::new();
        pool.add("x");
        pool.add("y");
        let bytes = bincode::serialize(&pool).unwrap();
        let deserialized: ConstantPool = bincode::deserialize(&bytes).unwrap();
        assert_eq!(deserialized.len(), 2);
        assert_eq!(deserialized.get(0), "x");
        assert_eq!(deserialized.get(1), "y");
    }

    #[test]
    fn test_default_is_empty() {
        let pool = ConstantPool::default();
        assert!(pool.is_empty());
    }
}
