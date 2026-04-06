use serde::{Deserialize, Serialize};

/// 页面大小，默认 4KB（与 SQLite 相同）
pub const PAGE_SIZE: u64 = 4096;

/// 文件头魔数
pub const MAGIC_NUMBER: u32 = 0x4E55434C; // "NUCL"

/// 文件头版本
pub const VERSION: u32 = 1;

/// 页面头大小
pub const PAGE_HEADER_SIZE: u16 = 32;

/// 单元格指针大小（每个指针 2 字节）
pub const CELL_POINTER_SIZE: u16 = 2;

/// 页面类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum PageType {
    Free = 0,
    Data = 1,
    Overflow = 2,
    Index = 3,
}

impl Default for PageType {
    fn default() -> Self {
        PageType::Free
    }
}

pub type PageNumber = u64;

/// 页面头结构（固定 32 字节）
/// 布局: [page_type:1][record_count:2][used_space:2][cell_content_start:2][cell_ptr_array_end:2][next_free_page:8][parent_page:8][reserved:7]
#[derive(Debug, Clone)]
#[repr(C)]
pub struct PageHeader {
    pub page_type: PageType,
    pub record_count: u16,
    pub used_space: u16,
    pub cell_content_start: u16,
    pub cell_ptr_array_end: u16,
    pub next_free_page: PageNumber,
    pub parent_page: PageNumber,
    pub reserved: [u8; 7],
}

impl Default for PageHeader {
    fn default() -> Self {
        Self {
            page_type: PageType::Free,
            record_count: 0,
            used_space: 0,
            cell_content_start: (PAGE_SIZE - PAGE_HEADER_SIZE as u64) as u16, // 相对于 data 数组
            cell_ptr_array_end: 0, // 相对于 data 数组
            next_free_page: 0,
            parent_page: 0,
            reserved: [0; 7],
        }
    }
}

impl PageHeader {
    pub fn to_bytes(&self) -> [u8; 32] {
        let mut buf = [0u8; 32];
        buf[0] = self.page_type as u8;
        buf[1..3].copy_from_slice(&self.record_count.to_le_bytes());
        buf[3..5].copy_from_slice(&self.used_space.to_le_bytes());
        buf[5..7].copy_from_slice(&self.cell_content_start.to_le_bytes());
        buf[7..9].copy_from_slice(&self.cell_ptr_array_end.to_le_bytes());
        buf[9..17].copy_from_slice(&self.next_free_page.to_le_bytes());
        buf[17..25].copy_from_slice(&self.parent_page.to_le_bytes());
        buf
    }

    pub fn from_bytes(buf: &[u8]) -> Result<Self, &'static str> {
        if buf.len() < 32 {
            return Err("Buffer too small for PageHeader");
        }
        Ok(Self {
            page_type: match buf[0] {
                0 => PageType::Free,
                1 => PageType::Data,
                2 => PageType::Overflow,
                3 => PageType::Index,
                _ => return Err("Invalid page type"),
            },
            record_count: u16::from_le_bytes([buf[1], buf[2]]),
            used_space: u16::from_le_bytes([buf[3], buf[4]]),
            cell_content_start: u16::from_le_bytes([buf[5], buf[6]]),
            cell_ptr_array_end: u16::from_le_bytes([buf[7], buf[8]]),
            next_free_page: u64::from_le_bytes(buf[9..17].try_into().unwrap()),
            parent_page: u64::from_le_bytes(buf[17..25].try_into().unwrap()),
            reserved: buf[25..32]
                .try_into()
                .map_err(|_| "Invalid reserved bytes")?,
        })
    }

    /// 可用空间 = cell_content_start - cell_ptr_array_end
    pub fn free_space(&self) -> u16 {
        self.cell_content_start
            .saturating_sub(self.cell_ptr_array_end)
    }
}

/// 记录头结构（固定 8 字节）
/// 布局: [key_len:2][value_len:4][deleted:1][reserved:1]
#[derive(Debug, Clone)]
#[repr(C)]
pub struct RecordHeader {
    pub key_len: u16,
    pub value_len: u32,
    pub deleted: bool,
    pub reserved: u8,
}

impl RecordHeader {
    pub fn to_bytes(&self) -> [u8; 8] {
        let mut buf = [0u8; 8];
        buf[0..2].copy_from_slice(&self.key_len.to_le_bytes());
        buf[2..6].copy_from_slice(&self.value_len.to_le_bytes());
        buf[6] = if self.deleted { 1 } else { 0 };
        buf
    }

    pub fn from_bytes(buf: &[u8]) -> Result<Self, &'static str> {
        if buf.len() < 8 {
            return Err("Buffer too small for RecordHeader");
        }
        Ok(Self {
            key_len: u16::from_le_bytes([buf[0], buf[1]]),
            value_len: u32::from_le_bytes(buf[2..6].try_into().unwrap()),
            deleted: buf[6] != 0,
            reserved: buf[7],
        })
    }

    pub fn total_size(&self) -> usize {
        8 + self.key_len as usize + self.value_len as usize
    }
}

/// 数据库文件头
///
/// 布局 (128 bytes):
/// [magic:4][version:4][page_size:4][total_pages:8][first_free_page:8]
/// [free_page_count:8][data_page_count:8][next_page_number:8][metadata_page:8]
/// [reserved:68]
#[derive(Debug, Clone)]
pub struct FileHeader {
    pub magic: u32,
    pub version: u32,
    pub page_size: u32,
    pub total_pages: PageNumber,
    pub first_free_page: PageNumber,
    pub free_page_count: PageNumber,
    pub data_page_count: PageNumber,
    /// 下一个可分配的页面号（由 BufferPool 维护）
    pub next_page_number: PageNumber,
    /// 元数据页面号（存储集合-页面映射）
    pub metadata_page: PageNumber,
    reserved: [u8; 68],
}

impl Default for FileHeader {
    fn default() -> Self {
        Self {
            magic: MAGIC_NUMBER,
            version: VERSION,
            page_size: PAGE_SIZE as u32,
            total_pages: 1,
            first_free_page: 0,
            free_page_count: 0,
            data_page_count: 0,
            next_page_number: 2, // page 0 = header, page 1 = metadata
            metadata_page: 1,
            reserved: [0; 68],
        }
    }
}

impl FileHeader {
    /// 创建一个仅更新指定字段的 FileHeader（用于 sync_file_header）
    pub fn with_next_page_number(next_page_number: PageNumber) -> Self {
        Self {
            next_page_number,
            ..Default::default()
        }
    }

    pub fn to_bytes(&self) -> [u8; 128] {
        let mut buf = [0u8; 128];
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..8].copy_from_slice(&self.version.to_le_bytes());
        buf[8..12].copy_from_slice(&self.page_size.to_le_bytes());
        buf[12..20].copy_from_slice(&self.total_pages.to_le_bytes());
        buf[20..28].copy_from_slice(&self.first_free_page.to_le_bytes());
        buf[28..36].copy_from_slice(&self.free_page_count.to_le_bytes());
        buf[36..44].copy_from_slice(&self.data_page_count.to_le_bytes());
        buf[44..52].copy_from_slice(&self.next_page_number.to_le_bytes());
        buf[52..60].copy_from_slice(&self.metadata_page.to_le_bytes());
        buf
    }

    pub fn from_bytes(buf: &[u8]) -> Result<Self, &'static str> {
        if buf.len() < 128 {
            return Err("Buffer too small for FileHeader");
        }
        let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        if magic != MAGIC_NUMBER {
            return Err("Invalid magic number");
        }
        Ok(Self {
            magic,
            version: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
            page_size: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
            total_pages: u64::from_le_bytes(buf[12..20].try_into().unwrap()),
            first_free_page: u64::from_le_bytes(buf[20..28].try_into().unwrap()),
            free_page_count: u64::from_le_bytes(buf[28..36].try_into().unwrap()),
            data_page_count: u64::from_le_bytes(buf[36..44].try_into().unwrap()),
            next_page_number: u64::from_le_bytes(buf[44..52].try_into().unwrap()),
            metadata_page: u64::from_le_bytes(buf[52..60].try_into().unwrap()),
            reserved: buf[60..128].try_into().unwrap(),
        })
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        if self.magic != MAGIC_NUMBER {
            return Err("Invalid magic number");
        }
        if self.version != VERSION {
            return Err("Unsupported version");
        }
        if self.page_size != PAGE_SIZE as u32 {
            return Err("Invalid page size");
        }
        Ok(())
    }
}

/// 页面数据结构
///
/// 页面布局:
/// [PageHeader: 32B] [Cell Pointer Array: N*2B] [Free Space] [Cell Content: grows backward]
///
/// 每个 Cell 格式:
/// [RecordHeader: 8B] [Key: key_len B] [Value: value_len B]
#[derive(Debug, Clone)]
pub struct Page {
    pub page_number: PageNumber,
    pub header: PageHeader,
    pub data: Vec<u8>,
    pub is_dirty: bool,
}

impl Page {
    pub fn new(page_number: PageNumber, page_type: PageType) -> Self {
        let data_len = (PAGE_SIZE - PAGE_HEADER_SIZE as u64) as usize;
        Self {
            page_number,
            header: PageHeader {
                page_type,
                cell_content_start: data_len as u16,
                cell_ptr_array_end: 0,
                ..Default::default()
            },
            data: vec![0u8; data_len],
            is_dirty: true,
        }
    }

    pub fn from_bytes(page_number: PageNumber, data: &[u8]) -> Result<Self, &'static str> {
        if data.len() != PAGE_SIZE as usize {
            return Err("Invalid page size");
        }
        let header = PageHeader::from_bytes(&data[..32])?;
        // 只存储数据部分（跳过 PageHeader），to_bytes 会从偏移 32 写回
        let page_data = data[32..].to_vec();
        Ok(Self {
            page_number,
            header,
            data: page_data,
            is_dirty: false,
        })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut result = vec![0u8; PAGE_SIZE as usize];
        let header_bytes = self.header.to_bytes();
        result[..32].copy_from_slice(&header_bytes);
        // data 只包含 payload 部分，直接写入偏移 32 之后
        result[32..].copy_from_slice(&self.data);
        result
    }

    /// 写入记录到页面
    /// 使用单元格指针数组方案：
    /// 1. 将记录内容写入页面末尾的可用空间
    /// 2. 在单元格指针数组中添加指向该记录的偏移
    pub fn write_record(&mut self, key: &[u8], value: &[u8]) -> Result<u16, &'static str> {
        let record_header = RecordHeader {
            key_len: key.len() as u16,
            value_len: value.len() as u32,
            deleted: false,
            reserved: 0,
        };

        let cell_size = record_header.total_size();
        let total_needed = cell_size + CELL_POINTER_SIZE as usize;

        if total_needed > self.header.free_space() as usize {
            return Err("Not enough space in page");
        }

        // 计算记录写入位置（从页面末尾向前）
        let cell_offset = (self.header.cell_content_start as usize - cell_size) as u16;

        // 写入记录内容
        let header_bytes = record_header.to_bytes();
        let data_start = cell_offset as usize;
        self.data[data_start..data_start + 8].copy_from_slice(&header_bytes);
        let key_start = data_start + 8;
        self.data[key_start..key_start + key.len()].copy_from_slice(key);
        let value_start = key_start + key.len();
        self.data[value_start..value_start + value.len()].copy_from_slice(value);

        // 更新 cell_content_start
        self.header.cell_content_start = cell_offset;

        // 在单元格指针数组末尾写入指向该记录的指针
        let ptr_offset = self.header.cell_ptr_array_end as usize;
        self.data[ptr_offset..ptr_offset + 2].copy_from_slice(&cell_offset.to_le_bytes());
        self.header.cell_ptr_array_end += CELL_POINTER_SIZE;

        // 更新页面头
        self.header.record_count += 1;
        self.header.used_space += cell_size as u16;
        self.is_dirty = true;

        Ok(cell_offset)
    }

    /// 通过单元格索引获取记录偏移
    fn get_cell_offset(&self, cell_index: u16) -> Result<u16, &'static str> {
        if cell_index >= self.header.record_count {
            return Err("Cell index out of range");
        }
        let ptr_pos = (cell_index * CELL_POINTER_SIZE) as usize;
        Ok(u16::from_le_bytes(
            self.data[ptr_pos..ptr_pos + 2].try_into().unwrap(),
        ))
    }

    /// 通过单元格索引读取记录
    pub fn read_record_by_index(
        &self,
        cell_index: u16,
    ) -> Result<(Vec<u8>, Vec<u8>, bool), &'static str> {
        let offset = self.get_cell_offset(cell_index)?;
        self.read_record(offset as u64)
    }

    /// 从指定偏移读取记录
    pub fn read_record(&self, offset: u64) -> Result<(Vec<u8>, Vec<u8>, bool), &'static str> {
        let offset = offset as usize;
        if offset + 8 > PAGE_SIZE as usize {
            return Err("Invalid record offset");
        }

        let record_header = RecordHeader::from_bytes(&self.data[offset..offset + 8])?;
        let key_start = offset + 8;
        let value_start = key_start + record_header.key_len as usize;

        if value_start + record_header.value_len as usize > PAGE_SIZE as usize {
            return Err("Record extends beyond page");
        }

        let key = self.data[key_start..value_start].to_vec();
        let value = self.data[value_start..value_start + record_header.value_len as usize].to_vec();

        Ok((key, value, record_header.deleted))
    }

    /// 标记指定单元格的记录为已删除
    pub fn delete_record_by_index(&mut self, cell_index: u16) -> Result<(), &'static str> {
        let offset = self.get_cell_offset(cell_index)?;
        let offset = offset as usize;
        if offset + 8 > PAGE_SIZE as usize {
            return Err("Invalid record offset");
        }
        self.data[offset + 6] = 1;
        self.is_dirty = true;
        Ok(())
    }

    /// 获取所有记录的单元格索引列表
    pub fn iter_cells(&self) -> impl Iterator<Item = u16> {
        0..self.header.record_count
    }
}

/// 页面迭代器
pub struct PageRecordIterator<'a> {
    page: &'a Page,
    current: u16,
}

impl<'a> PageRecordIterator<'a> {
    pub fn new(page: &'a Page) -> Self {
        Self { page, current: 0 }
    }
}

impl<'a> Iterator for PageRecordIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>, bool), &'static str>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.page.header.record_count {
            return None;
        }
        let result = self.page.read_record_by_index(self.current);
        self.current += 1;
        Some(result)
    }
}

impl Page {
    pub fn iter_records(&self) -> PageRecordIterator<'_> {
        PageRecordIterator::new(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_header_serialization() {
        let header = PageHeader {
            page_type: PageType::Data,
            record_count: 5,
            used_space: 100,
            cell_content_start: 4000,
            cell_ptr_array_end: 42,
            next_free_page: 0,
            parent_page: 0,
            reserved: [0; 7],
        };

        let bytes = header.to_bytes();
        let restored = PageHeader::from_bytes(&bytes).unwrap();

        assert_eq!(restored.page_type, PageType::Data);
        assert_eq!(restored.record_count, 5);
        assert_eq!(restored.used_space, 100);
        assert_eq!(restored.cell_content_start, 4000);
        assert_eq!(restored.cell_ptr_array_end, 42);
    }

    #[test]
    fn test_record_header_serialization() {
        let header = RecordHeader {
            key_len: 10,
            value_len: 100,
            deleted: false,
            reserved: 0,
        };

        let bytes = header.to_bytes();
        let restored = RecordHeader::from_bytes(&bytes).unwrap();

        assert_eq!(restored.key_len, 10);
        assert_eq!(restored.value_len, 100);
        assert!(!restored.deleted);
        assert_eq!(restored.total_size(), 118);
    }

    #[test]
    fn test_file_header_serialization() {
        let header = FileHeader {
            magic: MAGIC_NUMBER,
            version: VERSION,
            page_size: PAGE_SIZE as u32,
            total_pages: 100,
            first_free_page: 5,
            free_page_count: 10,
            data_page_count: 89,
            next_page_number: 50,
            metadata_page: 1,
            reserved: [0; 68],
        };

        let bytes = header.to_bytes();
        let restored = FileHeader::from_bytes(&bytes).unwrap();

        assert_eq!(restored.total_pages, 100);
        assert_eq!(restored.first_free_page, 5);
        assert_eq!(restored.free_page_count, 10);
        assert_eq!(restored.next_page_number, 50);
        assert_eq!(restored.metadata_page, 1);
    }

    #[test]
    fn test_file_header_validation() {
        let valid_header = FileHeader::default();
        assert!(valid_header.validate().is_ok());

        let invalid_magic = FileHeader {
            magic: 0xDEADBEEF,
            ..Default::default()
        };
        assert!(invalid_magic.validate().is_err());
    }

    #[test]
    fn test_page_new() {
        let page = Page::new(1, PageType::Data);
        assert_eq!(page.page_number, 1);
        assert_eq!(page.header.page_type, PageType::Data);
        assert_eq!(page.data.len(), (PAGE_SIZE - PAGE_HEADER_SIZE as u64) as usize);
        assert!(page.is_dirty);
    }

    #[test]
    fn test_page_write_read_record() {
        let mut page = Page::new(1, PageType::Data);

        let offset = page.write_record(b"key", b"value").unwrap();

        let (read_key, read_value, deleted) = page.read_record(offset as u64).unwrap();

        assert_eq!(read_key, b"key");
        assert_eq!(read_value, b"value");
        assert!(!deleted);
    }

    #[test]
    fn test_page_write_multiple_records() {
        let mut page = Page::new(1, PageType::Data);

        let records = vec![
            (b"key1".to_vec(), b"value1".to_vec()),
            (b"key2".to_vec(), b"value2".to_vec()),
            (b"key3".to_vec(), b"value3".to_vec()),
        ];

        for (key, value) in &records {
            page.write_record(key, value).unwrap();
        }

        assert_eq!(page.header.record_count, 3);

        for i in 0..3u16 {
            let (key, value, _) = page.read_record_by_index(i).unwrap();
            assert_eq!(key, records[i as usize].0);
            assert_eq!(value, records[i as usize].1);
        }
    }

    #[test]
    fn test_page_delete_record() {
        let mut page = Page::new(1, PageType::Data);

        page.write_record(b"key", b"value").unwrap();

        page.delete_record_by_index(0).unwrap();

        let (_, _, deleted) = page.read_record_by_index(0).unwrap();
        assert!(deleted);
    }

    #[test]
    fn test_page_free_space() {
        let page = Page::new(1, PageType::Data);
        assert_eq!(page.header.free_space(), (PAGE_SIZE - 32) as u16);

        let mut page = Page::new(1, PageType::Data);
        page.write_record(b"key", b"value").unwrap();
        assert!(page.header.free_space() < (PAGE_SIZE - 32) as u16);
    }

    #[test]
    fn test_page_to_from_bytes() {
        let mut page = Page::new(1, PageType::Data);
        page.write_record(b"key", b"value").unwrap();

        let bytes = page.to_bytes();
        let restored = Page::from_bytes(1, &bytes).unwrap();

        assert_eq!(restored.header.record_count, 1);
        assert_eq!(restored.header.page_type, PageType::Data);
    }

    #[test]
    fn test_page_record_overflow() {
        let mut page = Page::new(1, PageType::Data);

        // Value larger than available page space (PAGE_SIZE - 32 header = 4064)
        let large_value = vec![0u8; (PAGE_SIZE + 100) as usize];
        let result = page.write_record(b"key", &large_value);

        assert!(result.is_err());
    }

    #[test]
    fn test_page_iteration() {
        let mut page = Page::new(1, PageType::Data);

        page.write_record(b"key1", b"value1").unwrap();
        page.write_record(b"key2", b"value2").unwrap();
        page.write_record(b"key3", b"value3").unwrap();

        let records: Vec<_> = page.iter_records().collect();
        assert_eq!(records.len(), 3);

        for result in records {
            let (key, value, _) = result.unwrap();
            assert!(key.starts_with(b"key"));
            assert!(value.starts_with(b"value"));
        }
    }

    #[test]
    fn test_page_serialization_roundtrip() {
        let mut page = Page::new(1, PageType::Data);

        page.write_record(b"key1", b"value1").unwrap();
        page.write_record(b"key2", b"value2").unwrap();

        let bytes = page.to_bytes();
        assert_eq!(bytes.len(), PAGE_SIZE as usize);

        let restored = Page::from_bytes(1, &bytes).unwrap();
        assert_eq!(restored.header.record_count, 2);
        assert_eq!(restored.header.used_space, page.header.used_space);
    }
}
