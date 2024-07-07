use std::sync::Arc;

use parking_lot::RwLock;

use crate::fio;

/// 数据文件
pub struct DataFile {
    /// 数据文件id
    file_id: Arc<RwLock<u32>>,

    /// 当前写偏移，记录数据文件写到哪个位置了
    write_off: Arc<RwLock<u64>>,

    /// IO 管理接口
    io_manager: Box<dyn fio::IOManager>,
}
