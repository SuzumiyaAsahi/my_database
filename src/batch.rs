use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::Mutex;
use prost::{decode_length_delimiter, encode_length_delimiter};
use std::{collections::HashMap, sync::Arc};

use crate::{
    data::log_record::{LogRecord, LogRecordType},
    db::Engine,
    error::{Errors, Result},
    options::{IndexType, WriteBatchOptions},
};

const TXN_FINISHED: &[u8] = "legacy".as_bytes();
pub(crate) const NON_TRANSCATION_SEQ_NO: usize = 0;

pub struct WriteBatch<'a> {
    /// 暂存用户写入的数据
    pending_writes: Arc<Mutex<HashMap<Vec<u8>, LogRecord>>>,
    engine: &'a Engine,
    options: WriteBatchOptions,
}

impl Engine {
    /// 初始化 WriteBatch
    pub fn new_write_batch(&self, options: WriteBatchOptions) -> Result<WriteBatch> {
        if self.options.index_type == IndexType::BPlusTree
            && !self.seq_file_exists
            && !self.is_initial
        {
            return Err(Errors::UnableToUserWriteBatch);
        }
        Ok(WriteBatch {
            pending_writes: Arc::new(Mutex::new(HashMap::new())),
            engine: self,
            options,
        })
    }
}

impl WriteBatch<'_> {
    /// 批量操作写数据
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        // 暂存数据
        let record = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            rec_type: LogRecordType::Normal,
        };

        let mut pending_writes = self.pending_writes.lock();
        pending_writes.insert(key.to_vec(), record);
        Ok(())
    }

    /// 批量操作删除数据
    pub fn delete(&self, key: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        let mut pending_writes = self.pending_writes.lock();
        // 如果数据不存在直接返回
        let index_pos = self.engine.index.get(key.to_vec());
        if index_pos.is_none() {
            // 如果暂存文件中有的话，删除
            if pending_writes.contains_key(&key.to_vec()) {
                pending_writes.remove(&key.to_vec());
            }
            return Ok(());
        }

        // 暂存数据
        let record = LogRecord {
            key: key.to_vec(),
            value: Default::default(),
            rec_type: LogRecordType::Deleted,
        };

        pending_writes.insert(key.to_vec(), record);
        Ok(())
    }

    pub fn commit(&self) -> Result<()> {
        let mut pending_writes = self.pending_writes.lock();
        if pending_writes.len() == 0 {
            return Ok(());
        }
        if pending_writes.len() > self.options.max_batch_num {
            return Err(Errors::ExceedMaxBatchNum);
        }

        // 加锁保证事务提交串行化
        let _lock = self.engine.batch_commit_lock.lock();

        // 获取全局事务序列号
        let seq_no = self
            .engine
            .seq_no
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let mut positions = HashMap::new();

        // 开始写数据到数据文件当中
        for (_, item) in pending_writes.iter() {
            let mut record = LogRecord {
                key: log_record_key_with_seq(item.key.clone(), seq_no),
                value: item.value.clone(),
                rec_type: item.rec_type,
            };

            let pos = self.engine.append_log_record(&mut record)?;
            positions.insert(item.key.clone(), pos);
        }

        // 写最后一条标识事务完成的数据
        let mut finish_record = LogRecord {
            key: log_record_key_with_seq(TXN_FINISHED.to_vec(), seq_no),
            value: Default::default(),
            rec_type: LogRecordType::Txnfinished,
        };

        self.engine.append_log_record(&mut finish_record)?;

        // 如果配置了持久化，则 sync
        if self.options.sync_writes {
            self.engine.sync()?;
        }

        // 数据全部写完之后更新内存索引
        for (_, item) in pending_writes.iter() {
            if item.rec_type == LogRecordType::Normal {
                let record_pos = positions.get(&item.key).unwrap();
                if let Some(old_pos) = self.engine.index.put(item.key.clone(), *record_pos) {
                    self.engine
                        .reclaim_size
                        .fetch_add(old_pos.size as usize, std::sync::atomic::Ordering::SeqCst);
                }
            }
            if item.rec_type == LogRecordType::Deleted {
                if let Some(old_pos) = self.engine.index.delete(item.key.clone()) {
                    self.engine
                        .reclaim_size
                        .fetch_add(old_pos.size as usize, std::sync::atomic::Ordering::SeqCst);
                }
            }
        }
        // 清空暂存数据
        pending_writes.clear();

        Ok(())
    }
}

/// 编码 seq no 和 key
pub(crate) fn log_record_key_with_seq(key: Vec<u8>, seq_no: usize) -> Vec<u8> {
    let mut enc_key = BytesMut::new();
    encode_length_delimiter(seq_no, &mut enc_key).unwrap();
    enc_key.extend_from_slice(&key.to_vec());
    enc_key.to_vec()
}

/// 解析 LogRecord 和 key。拿到实际的 key 和 seq no
pub(crate) fn parse_log_record_key(key: Vec<u8>) -> (Vec<u8>, usize) {
    let mut buf = BytesMut::new();
    buf.put_slice(&key);
    let seq_no = decode_length_delimiter(&mut buf).unwrap();
    (buf.to_vec(), seq_no)
}

#[cfg(test)]
mod tests {
    use crate::{options::Options, util};
    use std::path::PathBuf;
    use std::sync::atomic::Ordering;

    use super::*;

    #[test]
    fn test_write_batch_1() {
        let opts = Options {
            dir_path: PathBuf::from("/tmp/bitcask-rs-batch-1"),
            data_file_size: 64 * 1024 * 1024,
            ..Default::default()
        };
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let wb = engine
            .new_write_batch(WriteBatchOptions::default())
            .expect("failed to create write batch");
        // 写数据之后未提交
        let put_res1 = wb.put(
            util::rand_kv::get_test_key(1),
            util::rand_kv::get_test_value(10),
        );
        assert!(put_res1.is_ok());
        let put_res2 = wb.put(
            util::rand_kv::get_test_key(2),
            util::rand_kv::get_test_value(10),
        );
        assert!(put_res2.is_ok());

        let res1 = engine.get(util::rand_kv::get_test_key(1));
        assert_eq!(Errors::KeyNotFound, res1.err().unwrap());

        // 事务提交之后进行查询
        let commit_res = wb.commit();
        assert!(commit_res.is_ok());

        let res2 = engine.get(util::rand_kv::get_test_key(1));
        assert!(res2.is_ok());

        // 验证事务序列号
        let seq_no = wb.engine.seq_no.load(Ordering::SeqCst);
        assert_eq!(2, seq_no);

        // 删除测试的文件夹
        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_write_batch_2() {
        let opts = Options {
            dir_path: PathBuf::from("/tmp/bitcask-rs-batch-2"),
            data_file_size: 64 * 1024 * 1024,
            ..Default::default()
        };
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let wb = engine
            .new_write_batch(WriteBatchOptions::default())
            .expect("failed to create write batch");
        let put_res1 = wb.put(
            util::rand_kv::get_test_key(1),
            util::rand_kv::get_test_value(10),
        );
        assert!(put_res1.is_ok());
        let put_res2 = wb.put(
            util::rand_kv::get_test_key(2),
            util::rand_kv::get_test_value(10),
        );
        assert!(put_res2.is_ok());
        let commit_res1 = wb.commit();
        assert!(commit_res1.is_ok());

        let put_res3 = wb.put(
            util::rand_kv::get_test_key(1),
            util::rand_kv::get_test_value(10),
        );
        assert!(put_res3.is_ok());

        let commit_res2 = wb.commit();
        assert!(commit_res2.is_ok());

        // 重启之后进行校验
        engine.close().expect("failed to close");
        std::mem::drop(engine);

        let engine2 = Engine::open(opts.clone()).expect("failed to open engine");
        let keys = engine2.list_keys();
        assert_eq!(2, keys.ok().unwrap().len());

        // 验证事务序列号
        let seq_no = engine2.seq_no.load(Ordering::SeqCst);
        assert_eq!(3, seq_no);

        // 删除测试的文件夹
        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }
}
