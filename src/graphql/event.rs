use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventType {
    Created,
    Updated,
    Deleted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub event_type: EventType,
    pub collection: String,
    pub record_id: String,
    pub data: Option<serde_json::Value>,
}

pub struct EventBus {
    sender: broadcast::Sender<Event>,
}

impl EventBus {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(100);
        Self { sender }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.sender.subscribe()
    }

    pub fn publish(&self, event: Event) -> Result<usize, broadcast::error::SendError<Event>> {
        self.sender.send(event)
    }
}

impl Clone for EventBus {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_event_bus_new() {
        let bus = EventBus::new();
        let _rx = bus.subscribe();
    }

    #[test]
    fn test_event_bus_publish_and_receive() {
        let bus = EventBus::new();
        let mut rx = bus.subscribe();

        let event = Event {
            event_type: EventType::Created,
            collection: "users".to_string(),
            record_id: "123".to_string(),
            data: Some(json!({"name": "Alice"})),
        };

        bus.publish(event.clone()).unwrap();
        let received = rx.try_recv().unwrap();

        assert_eq!(received.collection, "users");
        assert_eq!(received.record_id, "123");
    }

    #[test]
    fn test_event_bus_multiple_subscribers() {
        let bus = EventBus::new();
        let mut rx1 = bus.subscribe();
        let mut rx2 = bus.subscribe();

        let event = Event {
            event_type: EventType::Created,
            collection: "users".to_string(),
            record_id: "123".to_string(),
            data: None,
        };

        bus.publish(event).unwrap();

        let received1 = rx1.try_recv().unwrap();
        let received2 = rx2.try_recv().unwrap();

        assert_eq!(received1.record_id, "123");
        assert_eq!(received2.record_id, "123");
    }

    #[test]
    fn test_event_bus_clone() {
        let bus1 = EventBus::new();
        let bus2 = bus1.clone();
        let mut rx = bus2.subscribe();

        let event = Event {
            event_type: EventType::Updated,
            collection: "users".to_string(),
            record_id: "456".to_string(),
            data: None,
        };

        bus1.publish(event).unwrap();
        let received = rx.try_recv().unwrap();

        assert_eq!(received.record_id, "456");
    }

    #[test]
    fn test_event_types() {
        let bus = EventBus::new();
        let mut rx = bus.subscribe();

        bus.publish(Event {
            event_type: EventType::Created,
            collection: "users".to_string(),
            record_id: "1".to_string(),
            data: None,
        })
        .unwrap();

        bus.publish(Event {
            event_type: EventType::Updated,
            collection: "users".to_string(),
            record_id: "2".to_string(),
            data: None,
        })
        .unwrap();

        bus.publish(Event {
            event_type: EventType::Deleted,
            collection: "users".to_string(),
            record_id: "3".to_string(),
            data: None,
        })
        .unwrap();

        let e1 = rx.try_recv().unwrap();
        let e2 = rx.try_recv().unwrap();
        let e3 = rx.try_recv().unwrap();

        assert!(matches!(e1.event_type, EventType::Created));
        assert!(matches!(e2.event_type, EventType::Updated));
        assert!(matches!(e3.event_type, EventType::Deleted));
    }

    #[test]
    fn test_event_bus_empty_receive() {
        let bus = EventBus::new();
        let mut rx = bus.subscribe();

        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_event_with_data() {
        let bus = EventBus::new();
        let mut rx = bus.subscribe();

        let data = json!({"name": "Alice", "age": 25});
        bus.publish(Event {
            event_type: EventType::Created,
            collection: "users".to_string(),
            record_id: "123".to_string(),
            data: Some(data.clone()),
        })
        .unwrap();

        let received = rx.try_recv().unwrap();
        assert_eq!(received.data, Some(data));
    }

    #[test]
    fn test_event_bus_different_collections() {
        let bus = EventBus::new();
        let mut rx = bus.subscribe();

        bus.publish(Event {
            event_type: EventType::Created,
            collection: "users".to_string(),
            record_id: "1".to_string(),
            data: None,
        })
        .unwrap();

        bus.publish(Event {
            event_type: EventType::Created,
            collection: "posts".to_string(),
            record_id: "2".to_string(),
            data: None,
        })
        .unwrap();

        let e1 = rx.try_recv().unwrap();
        let e2 = rx.try_recv().unwrap();

        assert_eq!(e1.collection, "users");
        assert_eq!(e2.collection, "posts");
    }
}
