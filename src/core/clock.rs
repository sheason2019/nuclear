use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VectorClock {
    clocks: HashMap<String, u64>,
}

impl VectorClock {
    pub fn new() -> Self {
        Self {
            clocks: HashMap::new(),
        }
    }

    pub fn increment(&mut self, node_id: &str) {
        let counter = self.clocks.entry(node_id.to_string()).or_insert(0);
        *counter += 1;
    }

    pub fn get(&self, node_id: &str) -> u64 {
        self.clocks.get(node_id).copied().unwrap_or(0)
    }

    pub fn merge(&mut self, other: &VectorClock) {
        for (node_id, &counter) in &other.clocks {
            let entry = self.clocks.entry(node_id.clone()).or_insert(0);
            *entry = (*entry).max(counter);
        }
    }

    pub fn happens_before(&self, other: &VectorClock) -> bool {
        let mut at_least_one_less = false;

        let all_nodes: std::collections::HashSet<_> =
            self.clocks.keys().chain(other.clocks.keys()).collect();

        for node in all_nodes {
            let self_count = self.get(node);
            let other_count = other.get(node);

            if self_count > other_count {
                return false;
            }
            if self_count < other_count {
                at_least_one_less = true;
            }
        }

        at_least_one_less
    }

    pub fn concurrent(&self, other: &VectorClock) -> bool {
        !self.happens_before(other) && !other.happens_before(self)
    }
}

impl PartialOrd for VectorClock {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.happens_before(other) {
            Some(std::cmp::Ordering::Less)
        } else if other.happens_before(self) {
            Some(std::cmp::Ordering::Greater)
        } else if self == other {
            Some(std::cmp::Ordering::Equal)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_clock_new_is_empty() {
        let clock = VectorClock::new();
        assert_eq!(clock.get("node1"), 0);
        assert_eq!(clock.get("node2"), 0);
    }

    #[test]
    fn test_vector_clock_increment() {
        let mut clock = VectorClock::new();
        clock.increment("node1");
        assert_eq!(clock.get("node1"), 1);
    }

    #[test]
    fn test_vector_clock_increment_multiple_times() {
        let mut clock = VectorClock::new();
        clock.increment("node1");
        clock.increment("node1");
        clock.increment("node1");
        assert_eq!(clock.get("node1"), 3);
    }

    #[test]
    fn test_vector_clock_increment_different_nodes() {
        let mut clock = VectorClock::new();
        clock.increment("node1");
        clock.increment("node2");
        clock.increment("node1");
        assert_eq!(clock.get("node1"), 2);
        assert_eq!(clock.get("node2"), 1);
    }

    #[test]
    fn test_vector_clock_merge() {
        let mut clock1 = VectorClock::new();
        clock1.increment("node1");

        let mut clock2 = VectorClock::new();
        clock2.increment("node2");

        clock1.merge(&clock2);
        assert_eq!(clock1.get("node1"), 1);
        assert_eq!(clock1.get("node2"), 1);
    }

    #[test]
    fn test_vector_clock_merge_with_higher_values() {
        let mut clock1 = VectorClock::new();
        clock1.increment("node1");
        clock1.increment("node1");

        let mut clock2 = VectorClock::new();
        clock2.increment("node1");
        clock2.increment("node2");

        clock1.merge(&clock2);
        assert_eq!(clock1.get("node1"), 2);
        assert_eq!(clock1.get("node2"), 1);
    }

    #[test]
    fn test_vector_clock_merge_idempotent() {
        let mut clock1 = VectorClock::new();
        clock1.increment("node1");

        let mut clock2 = VectorClock::new();
        clock2.increment("node2");

        clock1.merge(&clock2);
        let val1 = clock1.get("node1");
        let val2 = clock1.get("node2");

        clock1.merge(&clock2);
        assert_eq!(clock1.get("node1"), val1);
        assert_eq!(clock1.get("node2"), val2);
    }

    #[test]
    fn test_vector_clock_merge_self() {
        let mut clock = VectorClock::new();
        clock.increment("node1");
        clock.increment("node2");

        let clock_clone = clock.clone();
        clock.merge(&clock_clone);

        assert_eq!(clock.get("node1"), 1);
        assert_eq!(clock.get("node2"), 1);
    }

    #[test]
    fn test_vector_clock_happens_before_empty() {
        let clock1 = VectorClock::new();
        let mut clock2 = VectorClock::new();
        clock2.increment("node1");

        assert!(clock1.happens_before(&clock2));
        assert!(!clock2.happens_before(&clock1));
    }

    #[test]
    fn test_vector_clock_happens_before_equal() {
        let mut clock1 = VectorClock::new();
        clock1.increment("node1");

        let mut clock2 = VectorClock::new();
        clock2.increment("node1");

        assert!(!clock1.happens_before(&clock2));
        assert!(!clock2.happens_before(&clock1));
    }

    #[test]
    fn test_vector_clock_happens_before_greater() {
        let mut clock1 = VectorClock::new();
        clock1.increment("node1");
        clock1.increment("node1");

        let mut clock2 = VectorClock::new();
        clock2.increment("node1");

        assert!(!clock1.happens_before(&clock2));
        assert!(clock2.happens_before(&clock1));
    }

    #[test]
    fn test_vector_clock_concurrent() {
        let mut clock1 = VectorClock::new();
        clock1.increment("node1");

        let mut clock2 = VectorClock::new();
        clock2.increment("node2");

        assert!(clock1.concurrent(&clock2));
        assert!(clock2.concurrent(&clock1));
    }

    #[test]
    fn test_vector_clock_not_concurrent() {
        let mut clock1 = VectorClock::new();
        clock1.increment("node1");

        let mut clock2 = VectorClock::new();
        clock2.increment("node1");
        clock2.increment("node2");

        assert!(!clock1.concurrent(&clock2));
        assert!(!clock2.concurrent(&clock1));
    }

    #[test]
    fn test_vector_clock_partial_ord_less() {
        let mut clock1 = VectorClock::new();
        clock1.increment("node1");

        let mut clock2 = VectorClock::new();
        clock2.increment("node1");
        clock2.increment("node1");

        assert!(clock1 < clock2);
        assert!(clock2 > clock1);
    }

    #[test]
    fn test_vector_clock_partial_ord_equal() {
        let mut clock1 = VectorClock::new();
        clock1.increment("node1");

        let mut clock2 = VectorClock::new();
        clock2.increment("node1");

        assert!(clock1 == clock2);
    }

    #[test]
    fn test_vector_clock_partial_ord_concurrent_returns_none() {
        let mut clock1 = VectorClock::new();
        clock1.increment("node1");

        let mut clock2 = VectorClock::new();
        clock2.increment("node2");

        assert!(clock1.partial_cmp(&clock2).is_none());
    }

    #[test]
    fn test_vector_clock_serialization() {
        let mut clock = VectorClock::new();
        clock.increment("node1");
        clock.increment("node2");
        clock.increment("node1");

        let json = serde_json::to_string(&clock).unwrap();
        let deserialized: VectorClock = serde_json::from_str(&json).unwrap();

        assert_eq!(clock, deserialized);
    }

    #[test]
    fn test_vector_clock_multiple_nodes_happens_before() {
        let mut clock1 = VectorClock::new();
        clock1.increment("node1");
        clock1.increment("node2");

        let mut clock2 = VectorClock::new();
        clock2.increment("node1");
        clock2.increment("node2");
        clock2.increment("node3");

        assert!(clock1.happens_before(&clock2));
        assert!(!clock2.happens_before(&clock1));
    }

    #[test]
    fn test_vector_clock_complex_merge() {
        let mut clock1 = VectorClock::new();
        clock1.increment("node1");
        clock1.increment("node1");
        clock1.increment("node2");

        let mut clock2 = VectorClock::new();
        clock2.increment("node1");
        clock2.increment("node2");
        clock2.increment("node2");
        clock2.increment("node3");

        clock1.merge(&clock2);
        assert_eq!(clock1.get("node1"), 2);
        assert_eq!(clock1.get("node2"), 2);
        assert_eq!(clock1.get("node3"), 1);
    }
}
