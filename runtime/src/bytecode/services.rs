use std::collections::HashMap;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ServiceEntry {
    pub id: u16,
    pub name: String,
}

impl ServiceEntry {
    pub fn new(id: u16, name: &str) -> Self {
        ServiceEntry {
            id,
            name: name.to_string(),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ServiceTable {
    entries: Vec<ServiceEntry>,
    index: HashMap<String, u16>,
}

impl ServiceTable {
    pub fn new() -> Self {
        ServiceTable {
            entries: Vec::new(),
            index: HashMap::new(),
        }
    }

    pub fn add(&mut self, name: &str) -> u16 {
        if let Some(&id) = self.index.get(name) {
            return id;
        }
        let id = self.entries.len() as u16;
        self.entries.push(ServiceEntry::new(id, name));
        self.index.insert(name.to_string(), id);
        id
    }

    pub fn get(&self, id: u16) -> &ServiceEntry {
        &self.entries[id as usize]
    }

    pub fn get_by_name(&self, name: &str) -> Option<&ServiceEntry> {
        self.index.get(name).map(|&id| &self.entries[id as usize])
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn entries(&self) -> &[ServiceEntry] {
        &self.entries
    }
}

impl Default for ServiceTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_service_table_new() {
        let table = ServiceTable::new();
        assert!(table.is_empty());
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn test_service_table_add_and_get() {
        let mut table = ServiceTable::new();
        let id = table.add("validate");
        assert_eq!(id, 0);
        assert_eq!(table.len(), 1);

        let entry = table.get(0);
        assert_eq!(entry.name, "validate");
        assert_eq!(entry.id, 0);
    }

    #[test]
    fn test_service_table_dedup() {
        let mut table = ServiceTable::new();
        let id1 = table.add("validate");
        let id2 = table.add("validate");
        assert_eq!(id1, id2);
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn test_service_table_get_by_name() {
        let mut table = ServiceTable::new();
        table.add("fulfill");
        let entry = table.get_by_name("fulfill");
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().name, "fulfill");

        assert!(table.get_by_name("unknown").is_none());
    }

    #[test]
    fn test_service_table_entries() {
        let mut table = ServiceTable::new();
        table.add("a");
        table.add("b");
        table.add("c");
        assert_eq!(table.entries().len(), 3);
        assert_eq!(table.entries()[1].name, "b");
    }

    #[test]
    fn test_service_entry_new() {
        let entry = ServiceEntry::new(5, "my_svc");
        assert_eq!(entry.id, 5);
        assert_eq!(entry.name, "my_svc");
    }

    #[test]
    fn test_serialization_roundtrip() {
        let mut table = ServiceTable::new();
        table.add("svc1");
        table.add("svc2");
        let bytes = bincode::serialize(&table).unwrap();
        let deserialized: ServiceTable = bincode::deserialize(&bytes).unwrap();
        assert_eq!(deserialized.len(), 2);
        assert_eq!(deserialized.get(0).name, "svc1");
        assert_eq!(deserialized.get_by_name("svc2").unwrap().id, 1);
    }

    #[test]
    fn test_default_is_empty() {
        let table = ServiceTable::default();
        assert!(table.is_empty());
    }
}
