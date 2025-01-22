use super::Indexer;
use crate::{
    data::log_record::{decode_log_record_pos, LogRecordPos},
    error::{Errors, Result},
    options::IteratorOptions,
};
use bytes::Bytes;
use jammdb::{Error, DB};
use std::{path::PathBuf, sync::Arc};

const BPTREE_INDEX_FINE_NAME: &str = "bptree-index";
const BPTREE_BUCKET_NAME: &str = "bitcask-index";

pub struct BPlusTree {
    tree: Arc<DB>,
}

impl BPlusTree {
    pub fn new(dir_path: PathBuf) -> Self {
        // 打开 B+ 树实例，并创建对应的 bucket
        let bptree =
            DB::open(dir_path.join(BPTREE_INDEX_FINE_NAME)).expect("failed to open bptree");
        let tree = Arc::new(bptree);
        let tx = tree.tx(true).expect("failed to begin tx");
        tx.get_or_create_bucket(BPTREE_BUCKET_NAME).unwrap();
        tx.commit().unwrap();

        Self { tree: tree.clone() }
    }
}

impl Indexer for BPlusTree {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> Option<LogRecordPos> {
        let mut result = None;
        let tx = self.tree.tx(true).expect("failed to begin tx");
        let bucket = tx.get_bucket(BPTREE_BUCKET_NAME).unwrap();

        // 先获取到旧的值
        if let Some(kv) = bucket.get_kv(&key) {
            let pos = decode_log_record_pos(kv.value().to_vec());
            result = Some(pos);
        }

        // put 新值
        bucket
            .put(key, pos.encode())
            .expect("failed to put value in bptree");
        tx.commit().unwrap();

        result
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        let tx = self.tree.tx(false).expect("failed to begin tx");
        let bucket = tx.get_bucket(BPTREE_BUCKET_NAME).unwrap();
        if let Some(kv) = bucket.get_kv(key) {
            return Some(decode_log_record_pos(kv.value().to_vec()));
        }
        None
    }

    fn delete(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        let mut result = None;
        let tx = self.tree.tx(true).expect("failed to begin tx");
        let bucket = tx.get_bucket(BPTREE_BUCKET_NAME).unwrap();
        if let Ok(kv) = bucket.delete(key) {
            let pos = decode_log_record_pos(kv.value().to_vec());
            result = Some(pos);
        }
        tx.commit().unwrap();
        result
    }

    fn list_keys(&self) -> Result<Vec<bytes::Bytes>> {
        let tx = self.tree.tx(false).expect("failed to begin tx");
        let bucket = tx.get_bucket(BPTREE_BUCKET_NAME).unwrap();
        let mut keys = Vec::new();

        for data in bucket.cursor() {
            keys.push(Bytes::copy_from_slice(data.key()));
        }

        Ok(keys)
    }

    fn iterator(&self, options: IteratorOptions) -> Box<dyn super::IndexIterator> {
        let mut items = Vec::new();
        let tx = self.tree.tx(false).expect("failed to begin tx");
        let bucket = tx.get_bucket(BPTREE_BUCKET_NAME).unwrap();

        for data in bucket.cursor() {
            let key = data.key().to_vec();
            let pos = decode_log_record_pos(data.kv().value().to_vec());
            items.push((key, pos));
        }
        if options.reverse {
            items.reverse();
        }

        Box::new(BPTreeIterator {
            items,
            curr_index: 0,
            options,
        })
    }
}

pub struct BPTreeIterator {
    items: Vec<(Vec<u8>, LogRecordPos)>, // 存储 key+索引
    curr_index: usize,                   // 当前遍历的位置下标
    options: IteratorOptions,            // 配置项
}

impl crate::index::IndexIterator for BPTreeIterator {
    fn rewind(&mut self) {
        self.curr_index = 0;
    }

    fn seek(&mut self, key: Vec<u8>) {
        self.curr_index = match self.items.binary_search_by(|(x, _)| {
            if self.options.reverse {
                x.cmp(&key).reverse()
            } else {
                x.cmp(&key)
            }
        }) {
            Ok(equal_val) => equal_val,
            Err(insert_val) => insert_val,
        };
    }

    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPos)> {
        if self.curr_index >= self.items.len() {
            return None;
        }

        while let Some(item) = self.items.get(self.curr_index) {
            self.curr_index += 1;
            let prefix = &self.options.prefix;
            if prefix.is_empty() || item.0.starts_with(prefix) {
                return Some((&item.0, &item.1));
            }
        }
        None
    }
}
