use crate::{
    data::log_record::LogRecordPos,
    error::{Errors, Result},
    index::Indexer,
    options::IteratorOptions,
};
use bytes::Bytes;
use parking_lot::RwLock;
use std::{collections::BTreeMap, sync::Arc};

use super::IndexIterator;

/// BTree 索引，主要封装了标准库中的 BTreeMap 结构
pub struct BTree {
    tree: Arc<RwLock<BTreeMap<Vec<u8>, LogRecordPos>>>,
}

impl BTree {
    pub fn new() -> Self {
        Self {
            tree: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

impl Indexer for BTree {
    fn delete(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        let mut write_guard = self.tree.write();
        write_guard.remove(&key)
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        let read_guard = self.tree.read();
        read_guard.get(&key).copied()
    }

    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> Option<LogRecordPos> {
        let mut write_guard = self.tree.write();
        write_guard.insert(key, pos)
    }

    fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexIterator> {
        let read_guard = self.tree.read();
        let mut items = Vec::with_capacity(read_guard.len());

        // 将 BTree 中的数据存储到数组中
        for (key, value) in read_guard.iter() {
            items.push((key.clone(), *value));
        }

        if options.reverse {
            items.reverse();
        }
        Box::new({
            BTreeIterator {
                items,
                curr_index: 0,
                options,
            }
        })
    }

    fn list_keys(&self) -> Result<Vec<bytes::Bytes>> {
        let read_guard = self.tree.read();
        let mut keys = Vec::with_capacity(read_guard.len());
        for (k, _) in read_guard.iter() {
            keys.push(Bytes::copy_from_slice(k));
        }
        Ok(keys)
    }
}

pub struct BTreeIterator {
    items: Vec<(Vec<u8>, LogRecordPos)>, // 存储 key 和 索引
    curr_index: usize,                   // 当前遍历的位置下标
    options: IteratorOptions,            // 配置项
}

impl IndexIterator for BTreeIterator {
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
