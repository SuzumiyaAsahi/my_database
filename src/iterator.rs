use crate::data::log_record::LogRecordPos;
use parking_lot::RwLock;
use std::sync::Arc;

use crate::{db::Engine, index::IndexIterator, options::IteratorOptions};

/// 迭代器接口
pub struct Iterator<'a> {
    /// 索引迭代器
    index_iter: Arc<RwLock<Box<dyn IndexIterator>>>,
    engine: &'a Engine,
}

impl Engine {
    /// 获取迭代器
    pub fn iter(&self, options: IteratorOptions) -> Iterator {
        Iterator {
            index_iter: Arc::new(RwLock::new(self.index.iterator(options))),
            engine: self,
        }
    }
}

impl Iterator<'_> {
    /// Rewind 重新回到迭代器的起点，即第一个数据
    fn rewind(&self) {
        let mut index_iter = self.index_iter.write();
        index_iter.rewind();
    }

    /// Seek 根据传入的 key 查找第一个大于（或小于）等于的目标 key，根据从这个 key 开始的遍历
    fn seek(&mut self, key: Vec<u8>) {
        let mut index_iter = self.index_iter.write();
        index_iter.seek(key);
    }

    /// Next 跳转到下一个 key，返回 None 则说明迭代完毕
    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPos)> {
        todo!()
    }
}
