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
    fn test_vector_clock_increment() {
        let mut clock = VectorClock::new();
        clock.increment("node1");
        assert_eq!(clock.get("node1"), 1);
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
    fn test_vector_clock_compare() {
        let mut clock1 = VectorClock::new();
        clock1.increment("node1");

        let mut clock2 = VectorClock::new();
        clock2.increment("node1");
        clock2.increment("node1");

        assert!(clock1 < clock2);
    }
}
