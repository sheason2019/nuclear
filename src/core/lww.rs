use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LWWRegister<T> {
    value: T,
    timestamp: u64,
    node_id: String,
}

impl<T: Clone + Default> LWWRegister<T> {
    pub fn new(node_id: &str) -> Self {
        Self {
            value: T::default(),
            timestamp: 0,
            node_id: node_id.to_string(),
        }
    }

    pub fn set(&mut self, value: T) {
        self.value = value;
        self.timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
    }

    pub fn get(&self) -> Option<&T> {
        if self.timestamp == 0 {
            None
        } else {
            Some(&self.value)
        }
    }

    pub fn merge(&mut self, other: &LWWRegister<T>) {
        if other.timestamp > self.timestamp {
            self.value = other.value.clone();
            self.timestamp = other.timestamp;
            self.node_id = other.node_id.clone();
        } else if other.timestamp == self.timestamp && other.node_id > self.node_id {
            self.value = other.value.clone();
            self.node_id = other.node_id.clone();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lww_register_new_returns_none() {
        let reg: LWWRegister<String> = LWWRegister::new("node1");
        assert_eq!(reg.get(), None);
    }

    #[test]
    fn test_lww_register_set() {
        let mut reg = LWWRegister::new("node1");
        reg.set("value1");
        assert_eq!(reg.get(), Some(&"value1"));
    }

    #[test]
    fn test_lww_register_set_overwrites() {
        let mut reg = LWWRegister::new("node1");
        reg.set("value1");
        reg.set("value2");
        assert_eq!(reg.get(), Some(&"value2"));
    }

    #[test]
    fn test_lww_register_merge_later_wins() {
        let mut reg1 = LWWRegister::new("node1");
        reg1.set("value1");

        let mut reg2 = LWWRegister::new("node2");
        std::thread::sleep(std::time::Duration::from_millis(10));
        reg2.set("value2");

        reg1.merge(&reg2);
        assert_eq!(reg1.get(), Some(&"value2"));
    }

    #[test]
    fn test_lww_register_merge_earlier_loses() {
        let mut reg1 = LWWRegister::new("node1");
        reg1.set("value1");

        std::thread::sleep(std::time::Duration::from_millis(10));

        let mut reg2 = LWWRegister::new("node2");
        reg2.set("value2");

        reg1.merge(&reg2);
        assert_eq!(reg1.get(), Some(&"value2"));
    }

    #[test]
    fn test_lww_register_merge_equal_timestamp() {
        let mut reg1 = LWWRegister::new("node1");
        reg1.set("value1");

        let mut reg2 = LWWRegister::new("node2");
        reg2.set("value2");

        reg1.merge(&reg2);
        assert_eq!(reg1.get(), Some(&"value2"));
    }

    #[test]
    fn test_lww_register_merge_equal_timestamp_lower_node_id() {
        let mut reg1 = LWWRegister::new("node2");
        reg1.set("value1");

        let mut reg2 = LWWRegister::new("node1");
        reg2.set("value2");

        reg1.merge(&reg2);
        assert_eq!(reg1.get(), Some(&"value1"));
    }

    #[test]
    fn test_lww_register_merge_with_uninitialized() {
        let mut reg1 = LWWRegister::new("node1");
        reg1.set("value1");

        let reg2: LWWRegister<&str> = LWWRegister::new("node2");

        reg1.merge(&reg2);
        assert_eq!(reg1.get(), Some(&"value1"));
    }

    #[test]
    fn test_lww_register_merge_uninitialized_with_initialized() {
        let mut reg1: LWWRegister<&str> = LWWRegister::new("node1");

        let mut reg2 = LWWRegister::new("node2");
        reg2.set("value2");

        reg1.merge(&reg2);
        assert_eq!(reg1.get(), Some(&"value2"));
    }

    #[test]
    fn test_lww_register_merge_self() {
        let mut reg = LWWRegister::new("node1");
        reg.set("value1");

        let reg_clone = reg.clone();
        reg.merge(&reg_clone);

        assert_eq!(reg.get(), Some(&"value1"));
    }

    #[test]
    fn test_lww_register_merge_multiple_times() {
        let mut reg1 = LWWRegister::new("node1");
        reg1.set("value1");

        let mut reg2 = LWWRegister::new("node2");
        std::thread::sleep(std::time::Duration::from_millis(10));
        reg2.set("value2");

        let mut reg3 = LWWRegister::new("node3");
        std::thread::sleep(std::time::Duration::from_millis(10));
        reg3.set("value3");

        reg1.merge(&reg2);
        reg1.merge(&reg3);

        assert_eq!(reg1.get(), Some(&"value3"));
    }

    #[test]
    fn test_lww_register_with_different_types() {
        let mut reg = LWWRegister::new("node1");
        reg.set(42);
        assert_eq!(reg.get(), Some(&42));

        let mut reg = LWWRegister::new("node1");
        reg.set(vec![1, 2, 3]);
        assert_eq!(reg.get(), Some(&vec![1, 2, 3]));
    }

    #[test]
    fn test_lww_register_serialization() {
        let mut reg = LWWRegister::new("node1");
        reg.set("value1");

        let json = serde_json::to_string(&reg).unwrap();
        let deserialized: LWWRegister<&str> = serde_json::from_str(&json).unwrap();

        assert_eq!(reg.get(), deserialized.get());
    }
}
