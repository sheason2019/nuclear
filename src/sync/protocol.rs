use crate::core::VectorClock;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncMessage {
    /// 请求同步
    SyncRequest { from: String, clock: VectorClock },

    /// 同步响应
    SyncResponse {
        from: String,
        clock: VectorClock,
        changes: Vec<DataChange>,
    },

    /// 数据变更通知
    ChangeNotification {
        from: String,
        changes: Vec<DataChange>,
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
