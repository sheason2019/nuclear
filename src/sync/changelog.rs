use crate::core::VectorClock;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Operation {
    Create,
    Update,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeEntry {
    pub id: String,
    pub timestamp: u64,
    pub collection: String,
    pub record_id: String,
    pub operation: Operation,
    pub data: Option<serde_json::Value>,
    pub vector_clock: VectorClock,
}

pub struct ChangeLog {
    entries: Vec<ChangeEntry>,
}

impl ChangeLog {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn add_entry(&mut self, entry: ChangeEntry) {
        self.entries.push(entry);
    }

    pub fn get_entries(&self) -> &[ChangeEntry] {
        &self.entries
    }

    pub fn get_entries_since(&self, since: &VectorClock) -> Vec<&ChangeEntry> {
        self.entries
            .iter()
            .filter(|entry| entry.vector_clock.happens_before(since))
            .collect()
    }

    pub fn get_entries_for_collection(&self, collection: &str) -> Vec<&ChangeEntry> {
        self.entries
            .iter()
            .filter(|entry| entry.collection == collection)
            .collect()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_changelog_new() {
        let log = ChangeLog::new();
        assert!(log.is_empty());
        assert_eq!(log.len(), 0);
    }

    #[test]
    fn test_changelog_add_entry() {
        let mut log = ChangeLog::new();
        let entry = ChangeEntry {
            id: "1".to_string(),
            timestamp: 1000,
            collection: "users".to_string(),
            record_id: "user1".to_string(),
            operation: Operation::Create,
            data: Some(json!({"name": "Alice"})),
            vector_clock: VectorClock::new(),
        };

        log.add_entry(entry);
        assert_eq!(log.len(), 1);
    }

    #[test]
    fn test_changelog_get_entries() {
        let mut log = ChangeLog::new();

        for i in 0..5 {
            let entry = ChangeEntry {
                id: format!("{}", i),
                timestamp: 1000 + i as u64,
                collection: "users".to_string(),
                record_id: format!("user{}", i),
                operation: Operation::Create,
                data: Some(json!({"name": format!("User{}", i)})),
                vector_clock: VectorClock::new(),
            };
            log.add_entry(entry);
        }

        assert_eq!(log.len(), 5);
        assert_eq!(log.get_entries().len(), 5);
    }

    #[test]
    fn test_changelog_get_entries_for_collection() {
        let mut log = ChangeLog::new();

        log.add_entry(ChangeEntry {
            id: "1".to_string(),
            timestamp: 1000,
            collection: "users".to_string(),
            record_id: "user1".to_string(),
            operation: Operation::Create,
            data: None,
            vector_clock: VectorClock::new(),
        });

        log.add_entry(ChangeEntry {
            id: "2".to_string(),
            timestamp: 1001,
            collection: "posts".to_string(),
            record_id: "post1".to_string(),
            operation: Operation::Create,
            data: None,
            vector_clock: VectorClock::new(),
        });

        log.add_entry(ChangeEntry {
            id: "3".to_string(),
            timestamp: 1002,
            collection: "users".to_string(),
            record_id: "user2".to_string(),
            operation: Operation::Create,
            data: None,
            vector_clock: VectorClock::new(),
        });

        let users = log.get_entries_for_collection("users");
        assert_eq!(users.len(), 2);

        let posts = log.get_entries_for_collection("posts");
        assert_eq!(posts.len(), 1);
    }

    #[test]
    fn test_changelog_operations() {
        let mut log = ChangeLog::new();

        log.add_entry(ChangeEntry {
            id: "1".to_string(),
            timestamp: 1000,
            collection: "users".to_string(),
            record_id: "user1".to_string(),
            operation: Operation::Create,
            data: Some(json!({"name": "Alice"})),
            vector_clock: VectorClock::new(),
        });

        log.add_entry(ChangeEntry {
            id: "2".to_string(),
            timestamp: 1001,
            collection: "users".to_string(),
            record_id: "user1".to_string(),
            operation: Operation::Update,
            data: Some(json!({"name": "Alice Updated"})),
            vector_clock: VectorClock::new(),
        });

        log.add_entry(ChangeEntry {
            id: "3".to_string(),
            timestamp: 1002,
            collection: "users".to_string(),
            record_id: "user1".to_string(),
            operation: Operation::Delete,
            data: None,
            vector_clock: VectorClock::new(),
        });

        assert_eq!(log.len(), 3);
    }

    #[test]
    fn test_changelog_serialization() {
        let mut log = ChangeLog::new();

        log.add_entry(ChangeEntry {
            id: "1".to_string(),
            timestamp: 1000,
            collection: "users".to_string(),
            record_id: "user1".to_string(),
            operation: Operation::Create,
            data: Some(json!({"name": "Alice"})),
            vector_clock: VectorClock::new(),
        });

        let json = serde_json::to_string(&log.get_entries()[0]).unwrap();
        let deserialized: ChangeEntry = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.id, "1");
        assert_eq!(deserialized.collection, "users");
        assert_eq!(deserialized.record_id, "user1");
        assert_eq!(deserialized.operation, Operation::Create);
    }
}
