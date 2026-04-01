// API module - public interface
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Database;

impl Database {
    pub fn new() -> Self {
        Self
    }
}
