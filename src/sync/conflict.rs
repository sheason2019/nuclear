use super::protocol::DataChange;
use crate::core::VectorClock;

pub struct ConflictResolver;

impl ConflictResolver {
    pub fn resolve(change1: &DataChange, change2: &DataChange) -> DataChange {
        if change1.timestamp > change2.timestamp {
            change1.clone()
        } else if change2.timestamp > change1.timestamp {
            change2.clone()
        } else {
            if change1.clock > change2.clock {
                change1.clone()
            } else {
                change2.clone()
            }
        }
    }
}
