use crate::{error::Result, fio};
use parking_lot::RwLock;
use std::sync::Arc;

/// 数据文件
pub struct DataFile {
    /// 数据文件id
    file_id: Arc<RwLock<u32>>,

    /// 当前写偏移，记录数据文件写到哪个位置了
    write_off: Arc<RwLock<u64>>,

    /// IO 管理接口
    io_manager: Box<dyn fio::IOManager>,
}

impl DataFile {
    pub fn get_file_off(&self) -> u64 {
        let read_guard = self.write_off.read();
        *read_guard
    }

    pub fn get_file_id(&self) -> u32 {
        let read_guard = self.file_id.read();
        *read_guard
    }

    pub fn sync(&self) -> Result<()> {
        todo!()
    }
}
