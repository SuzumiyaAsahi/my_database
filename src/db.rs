use crate::{
    data::log_record::{self, LogRecord, LogRecordType},
    error::{Errors, Result},
    options::Options,
};
use bytes::Bytes;
use std::sync::Arc;

/// bitcask 存储引擎实例结构体
pub struct Engine {
    options: Arc<Options>,
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
    fn append_log_record(&self, log_record: &mut LogRecord) -> Result<()> {
        let dir_path = self.options.dir_path.clone();

        // 输入数据进行编码
        let enc_record = log_record.encode();
        let record_len: u64 = enc_record.len() as u64;

        // 获取当前活跃文件

        todo!()
    }
}
