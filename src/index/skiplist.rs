use super::Indexer;
use crate::{data::log_record::LogRecordPos, error::Result, index::IteratorOptions};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use std::sync::Arc;

pub struct SkipList {
    skl: Arc<SkipMap<Vec<u8>, LogRecordPos>>,
}

impl SkipList {
    pub fn new() -> Self {
        Self {
            skl: Arc::new(SkipMap::new()),
        }
    }
}

impl Indexer for SkipList {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> Option<LogRecordPos> {
        let mut result = None;
        if let Some(entry) = self.skl.get(&key) {
            result = Some(*entry.value());
        }
        self.skl.insert(key, pos);
        result
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        if let Some(entry) = self.skl.get(&key) {
            return Some(*entry.value());
        }
        None
    }

    fn delete(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        if let Some(entry) = self.skl.remove(&key) {
            return Some(*entry.value());
        }
        None
    }

    fn iterator(&self, options: IteratorOptions) -> Box<dyn super::IndexIterator> {
        let mut items = Vec::with_capacity(self.skl.len());

        // 将 SkipList 中的数据存储到数组中
        for entry in self.skl.iter() {
            items.push((entry.key().clone(), *entry.value()));
        }

        if options.reverse {
            items.reverse();
        }
        Box::new({
            SkipListIterator {
                items,
                curr_index: 0,
                options,
            }
        })
    }

    fn list_keys(&self) -> Result<Vec<bytes::Bytes>> {
        let mut keys = Vec::with_capacity(self.skl.len());
        for e in self.skl.iter() {
            keys.push(Bytes::copy_from_slice(e.key()));
        }
        Ok(keys)
    }
}

pub struct SkipListIterator {
    items: Vec<(Vec<u8>, LogRecordPos)>, // 存储 key 和 索引
    curr_index: usize,                   // 当前遍历的位置下标
    options: IteratorOptions,            // 配置项
}

impl crate::index::IndexIterator for SkipListIterator {
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
