use wasm_bindgen::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);
}

macro_rules! console_log {
    ($($t:tt)*) => (log(&format_args!($($t)*).to_string()))
}

#[derive(Serialize, Deserialize)]
pub struct JsDatabaseOptions {
    pub node_id: Option<String>,
    pub base_path: Option<String>,
}

#[wasm_bindgen]
pub struct JsDatabase {
    inner: Arc<crate::api::Database<crate::storage::WasiStorage>>,
}

#[wasm_bindgen]
impl JsDatabase {
    #[wasm_bindgen(js_name = "create")]
    pub async fn create(options: JsValue) -> Result<JsDatabase, JsValue> {
        let opts: JsDatabaseOptions = serde_wasm_bindgen::from_value(options)
            .unwrap_or(JsDatabaseOptions { node_id: None, base_path: None });
        
        let node_id = opts.node_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let base_path = opts.base_path.unwrap_or_else(|| "./data".to_string());
        
        let storage = crate::storage::WasiStorage::new(&base_path);
        let db = crate::api::DatabaseBuilder::new(storage)
            .node_id(node_id)
            .base_path(base_path)
            .build()
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        
        Ok(JsDatabase { inner: Arc::new(db) })
    }

    pub async fn query(&self, query: String) -> Result<JsValue, JsValue> {
        let result = self.inner.query(&query).await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        
        serde_wasm_bindgen::to_value(&result)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    pub async fn mutation(&self, mutation: String) -> Result<JsValue, JsValue> {
        let result = self.inner.mutation(&mutation).await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        
        serde_wasm_bindgen::to_value(&result)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    #[wasm_bindgen(js_name = "getSyncRequest")]
    pub async fn get_sync_request(&self) -> Result<JsValue, JsValue> {
        let request = self.inner.get_sync_request().await;
        serde_wasm_bindgen::to_value(&request)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    #[wasm_bindgen(js_name = "getChangesSince")]
    pub async fn get_changes_since(&self, clock: JsValue) -> Result<JsValue, JsValue> {
        let vector_clock: crate::core::VectorClock = serde_wasm_bindgen::from_value(clock)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        
        let changes = self.inner.get_changes_since(&vector_clock).await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        
        serde_wasm_bindgen::to_value(&changes)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    #[wasm_bindgen(js_name = "applySyncResponse")]
    pub async fn apply_sync_response(&self, response: JsValue) -> Result<(), JsValue> {
        let sync_response: crate::sync::SyncMessage = serde_wasm_bindgen::from_value(response)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        
        self.inner.apply_sync_response(sync_response).await
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    #[wasm_bindgen(js_name = "getClock")]
    pub async fn get_clock(&self) -> Result<JsValue, JsValue> {
        let clock = self.inner.get_clock().await;
        serde_wasm_bindgen::to_value(&clock)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }
}

#[wasm_bindgen(start)]
pub fn main() {
    console_log!("Nuclear WASM module initialized");
}
