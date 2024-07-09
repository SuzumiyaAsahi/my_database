use crate::{
    data::{
        data_file::DataFile,
        log_record::{LogRecorPos, LogRecord, LogRecordType},
    },
    error::{Errors, Result},
    options::Options,
};
use bytes::Bytes;
use parking_lot::RwLock;
use std::{collections::HashMap, sync::Arc};

/// bitcask 存储引擎实例结构体
pub struct Engine {
    options: Arc<Options>,
    /// 当前活跃数据文件
    active_file: Arc<RwLock<DataFile>>,
    /// 旧的数据文件
    older_files: Arc<RwLock<HashMap<u32, DataFile>>>,
}

impl Engine {
    /// 存储 key/value 数据，key 不能为空
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        // 判断 key 的有效性
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        // 构造 LogRecord
        let record = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            rec_type: LogRecordType::NORMAL,
        };

        // 追加写活跃文件到数据文件中
        Ok(())
    }

    // 追加写数据到当前活跃文件中
    fn append_log_record(&self, log_record: &mut LogRecord) -> Result<LogRecorPos> {
        let dir_path = self.options.dir_path.clone();

        // 输入数据进行编码
        let enc_record = log_record.encode();
        let record_len: u64 = enc_record.len() as u64;

        // 获取当前活跃文件
        let mut active_file = self.active_file.write();

        // 判断当前活跃文件是否达到了阈值
        if active_file.get_file_off() + record_len > self.options.data_file_size {
            // 将当前活跃文件进行持久化
            active_file.sync()?;

            let current_fid = active_file.get_file_id();

            // 旧的数据文件存储到 map 中
            let mut older_files = self.older_files.write();
            let old_file = DataFile::new(dir_path.clone(), current_fid)?;
            older_files.insert(current_fid, old_file);

            // 打开新的数据文件
            let new_file = DataFile::new(dir_path.clone(), current_fid + 1)?;
            *active_file = new_file;
        }

        // 追加写数据到当前活跃文件中
        let write_off = active_file.get_file_off();
        active_file.write(&enc_record)?;

        // 根据配置项决定是否持久化
        if self.options.sync_writes {
            active_file.sync()?;
        }

        // 构造数据索引信息
        Ok(LogRecorPos {
            file_id: active_file.get_file_id(),
            offset: write_off,
        })
    }
}
