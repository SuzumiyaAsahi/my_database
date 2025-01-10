use super::log_record::{LogRecord, ReadLogRecord};
use crate::{
    data::log_record::max_log_record_header_size,
    error::Result,
    fio::{self, new_io_manager},
};
use bytes::{Buf, BytesMut};
use parking_lot::RwLock;
use prost::decode_length_delimiter;
use std::{path::PathBuf, sync::Arc};

pub const DATA_FILE_NAME_SUFFIX: &str = ".data";

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
    /// 创建或打开一个新的数据文件
    pub fn new(dir_path: PathBuf, file_id: u32) -> Result<DataFile> {
        // 根据 path 和 id 构建出完整的文件名称
        let file_name = get_data_file_name(dir_path, file_id);
        // 初始化 io manager
        let io_manager = new_io_manager(file_name)?;

        Ok(DataFile {
            file_id: Arc::new(RwLock::new(file_id)),
            write_off: Arc::new(RwLock::new(0)),
            io_manager: Box::new(io_manager),
        })
    }

    pub fn get_file_off(&self) -> u64 {
        let read_guard = self.write_off.read();
        *read_guard
    }

    pub fn set_write_off(&self, offset: u64) {
        let mut write_guard = self.write_off.write();
        *write_guard = offset;
    }

    pub fn get_file_id(&self) -> u32 {
        let read_guard = self.file_id.read();
        *read_guard
    }

    /// 根据 offset 从数据文件中读取 LogRecord
    pub fn read_log_record(&self, offset: u64) -> Result<ReadLogRecord> {
        // 先读取出 header 部分的数据
        let mut header_buf = BytesMut::zeroed(max_log_record_header_size());
        self.io_manager.read(&mut header_buf, offset)?;

        // 取出 type，在第一个字节
        let rec_type = header_buf.get_u8();

        // 取出 key 和 value 的长度
        let key_size = decode_length_delimiter(&mut header_buf).unwrap();
        let value_size = decode_length_delimiter(&mut header_buf).unwrap();
        todo!()
    }

    pub fn write(&self, buf: &[u8]) -> Result<usize> {
        todo!()
    }

    pub fn sync(&self) -> Result<()> {
        todo!()
    }
}

fn get_data_file_name(path: PathBuf, file_id: u32) -> PathBuf {
    let v = std::format!("{:09}", file_id) + DATA_FILE_NAME_SUFFIX;
    path.to_path_buf().join(v)
}
