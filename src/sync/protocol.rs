use crate::core::VectorClock;
use crate::sync::changelog::ChangeEntry;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncMessage {
    SyncRequest {
        from: String,
        clock: VectorClock,
    },

    SyncResponse {
        from: String,
        clock: VectorClock,
        changes: Vec<ChangeEntry>,
    },

    SyncAck {
        from: String,
        clock: VectorClock,
    },

    ChangeNotification {
        from: String,
        changes: Vec<ChangeEntry>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataChange {
    pub collection: String,
    pub key: String,
    pub value: Option<serde_json::Value>,
    pub clock: VectorClock,
    pub timestamp: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::changelog::Operation;
    use serde_json::json;

    #[test]
    fn test_sync_message_serialization() {
        let mut clock = VectorClock::new();
        clock.increment("node1");

        let msg = SyncMessage::SyncRequest {
            from: "node1".to_string(),
            clock: clock.clone(),
        };

        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: SyncMessage = serde_json::from_str(&json).unwrap();

        match deserialized {
            SyncMessage::SyncRequest { from, clock: c } => {
                assert_eq!(from, "node1");
                assert_eq!(c.get("node1"), 1);
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_sync_response_serialization() {
        let mut clock = VectorClock::new();
        clock.increment("node1");

        let entry = ChangeEntry {
            id: "1".to_string(),
            timestamp: 1000,
            collection: "users".to_string(),
            record_id: "user1".to_string(),
            operation: Operation::Create,
            data: Some(json!({"name": "Alice"})),
            vector_clock: clock.clone(),
        };

        let msg = SyncMessage::SyncResponse {
            from: "node2".to_string(),
            clock: clock.clone(),
            changes: vec![entry],
        };

        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: SyncMessage = serde_json::from_str(&json).unwrap();

        match deserialized {
            SyncMessage::SyncResponse { from, changes, .. } => {
                assert_eq!(from, "node2");
                assert_eq!(changes.len(), 1);
                assert_eq!(changes[0].collection, "users");
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_sync_ack_serialization() {
        let mut clock = VectorClock::new();
        clock.increment("node1");

        let msg = SyncMessage::SyncAck {
            from: "node1".to_string(),
            clock: clock.clone(),
        };

        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: SyncMessage = serde_json::from_str(&json).unwrap();

        match deserialized {
            SyncMessage::SyncAck { from, clock: c } => {
                assert_eq!(from, "node1");
                assert_eq!(c.get("node1"), 1);
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_change_notification_serialization() {
        let mut clock = VectorClock::new();
        clock.increment("node1");

        let entry = ChangeEntry {
            id: "1".to_string(),
            timestamp: 1000,
            collection: "users".to_string(),
            record_id: "user1".to_string(),
            operation: Operation::Update,
            data: Some(json!({"name": "Alice Updated"})),
            vector_clock: clock.clone(),
        };

        let msg = SyncMessage::ChangeNotification {
            from: "node1".to_string(),
            changes: vec![entry],
        };

        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: SyncMessage = serde_json::from_str(&json).unwrap();

        match deserialized {
            SyncMessage::ChangeNotification { from, changes } => {
                assert_eq!(from, "node1");
                assert_eq!(changes.len(), 1);
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_data_change_serialization() {
        let mut clock = VectorClock::new();
        clock.increment("node1");

        let change = DataChange {
            collection: "users".to_string(),
            key: "user1".to_string(),
            value: Some(json!({"name": "Alice"})),
            clock: clock.clone(),
            timestamp: 1000,
        };

        let json = serde_json::to_string(&change).unwrap();
        let deserialized: DataChange = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.collection, "users");
        assert_eq!(deserialized.key, "user1");
        assert_eq!(deserialized.timestamp, 1000);
    }
}
