use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use web_sys::{window, FileSystemDirectoryHandle, FileSystemFileHandle, FileSystemWritableFileStream};

use super::{Storage, FileHandle, OpenOptions, Result, StorageError};

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = navigator, js_name = storage)]
    fn storage() -> JsValue;
}

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(method, js_name = getDirectory)]
    fn get_directory(this: &JsValue) -> JsValue;
}

pub struct OpfsStorage {
    root: Arc<RwLock<Option<FileSystemDirectoryHandle>>>,
    files: Arc<RwLock<HashMap<u64, FileSystemFileHandle>>>,
    next_handle: Arc<RwLock<u64>>,
}

impl OpfsStorage {
    pub async fn new() -> Result<Self> {
        let window = window().ok_or(StorageError::WasmError("No window".to_string()))?;
        let navigator = window.navigator();
        let storage = navigator.storage();
        
        let dir_handle = JsFuture::from(storage.get_directory())
            .await
            .map_err(|e| StorageError::WasmError(format!("{:?}", e)))?;
        
        Ok(Self {
            root: Arc::new(RwLock::new(Some(dir_handle.into()))),
            files: Arc::new(RwLock::new(HashMap::new())),
            next_handle: Arc::new(RwLock::new(1)),
        })
    }
}

#[async_trait::async_trait]
impl Storage for OpfsStorage {
    async fn open(&self, path: &str, options: OpenOptions) -> Result<FileHandle> {
        let root = self.root.read().await;
        let root = root.as_ref().ok_or(StorageError::WasmError("No root".to_string()))?;
        
        let parts: Vec<&str> = path.split('/').collect();
        let mut current = root.clone();
        
        for part in &parts[..parts.len()-1] {
            let dir = JsFuture::from(current.get_directory_handle_with_options(part, &JsValue::from_serde(&serde_json::json!({
                "create": options.create
            })).map_err(|e| StorageError::WasmError(format!("{:?}", e)))?))
                .await
                .map_err(|e| StorageError::WasmError(format!("{:?}", e)))?;
            current = dir.into();
        }
        
        let file_name = parts.last().ok_or(StorageError::WasmError("No file name".to_string()))?;
        let file_handle = JsFuture::from(current.get_file_handle_with_options(file_name, &JsValue::from_serde(&serde_json::json!({
            "create": options.create
        })).map_err(|e| StorageError::WasmError(format!("{:?}", e)))?))
            .await
            .map_err(|e| StorageError::WasmError(format!("{:?}", e)))?;
        
        let file_handle: FileSystemFileHandle = file_handle.into();
        
        let mut handle_id = self.next_handle.write().await;
        let handle = FileHandle(*handle_id);
        *handle_id += 1;
        
        self.files.write().await.insert(handle.0, file_handle);
        
        Ok(handle)
    }

    async fn close(&self, handle: FileHandle) -> Result<()> {
        self.files.write().await.remove(&handle.0)
            .ok_or(StorageError::InvalidHandle)?;
        Ok(())
    }

    async fn read(&self, handle: FileHandle, offset: u64, buf: &mut [u8]) -> Result<usize> {
        let files = self.files.read().await;
        let file_handle = files.get(&handle.0)
            .ok_or(StorageError::InvalidHandle)?;
        
        let file = JsFuture::from(file_handle.get_file())
            .await
            .map_err(|e| StorageError::WasmError(format!("{:?}", e)))?;
        
        let file: web_sys::File = file.into();
        let slice = file.slice_with_i32_and_i32(offset as i32, (offset + buf.len() as u64) as i32)
            .map_err(|e| StorageError::WasmError(format!("{:?}", e)))?;
        
        let array_buffer = JsFuture::from(slice.array_buffer())
            .await
            .map_err(|e| StorageError::WasmError(format!("{:?}", e)))?;
        
        let uint8_array = js_sys::Uint8Array::new(&array_buffer);
        uint8_array.copy_to(buf);
        
        Ok(uint8_array.length() as usize)
    }

    async fn write(&self, handle: FileHandle, offset: u64, buf: &[u8]) -> Result<usize> {
        let files = self.files.read().await;
        let file_handle = files.get(&handle.0)
            .ok_or(StorageError::InvalidHandle)?;
        
        let writable = JsFuture::from(file_handle.create_writable())
            .await
            .map_err(|e| StorageError::WasmError(format!("{:?}", e)))?;
        
        let writable: FileSystemWritableFileStream = writable.into();
        
        if offset > 0 {
            JsFuture::from(writable.seek_with_u32(offset as u32))
                .await
                .map_err(|e| StorageError::WasmError(format!("{:?}", e)))?;
        }
        
        let uint8_array = js_sys::Uint8Array::from(buf);
        JsFuture::from(writable.write_with_buffer_source(&uint8_array))
            .await
            .map_err(|e| StorageError::WasmError(format!("{:?}", e)))?;
        
        JsFuture::from(writable.close())
            .await
            .map_err(|e| StorageError::WasmError(format!("{:?}", e)))?;
        
        Ok(buf.len())
    }

    async fn sync(&self, _handle: FileHandle) -> Result<()> {
        Ok(())
    }

    async fn size(&self, handle: FileHandle) -> Result<u64> {
        let files = self.files.read().await;
        let file_handle = files.get(&handle.0)
            .ok_or(StorageError::InvalidHandle)?;
        
        let file = JsFuture::from(file_handle.get_file())
            .await
            .map_err(|e| StorageError::WasmError(format!("{:?}", e)))?;
        
        let file: web_sys::File = file.into();
        Ok(file.size() as u64)
    }
}
