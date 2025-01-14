pub mod btree;
use crate::{
    data::log_record::LogRecordPos,
    error::Result,
    options::{IndexType, IteratorOptions},
};

/// Indexer 抽象索引接口，后续如果想要接入其他的数据结构，则直接实现这个接口即可
pub trait Indexer: Sync + Send {
    /// 向索引中存储 key 对应的数据位置信息
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> Result<()>;

    /// 根据 key 取出对应的索引信息位置
    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos>;

    /// 根据 key 删除对应的索引信息信息
    fn delete(&self, key: Vec<u8>) -> Result<()>;

    /// 返回索引迭代器
    fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexIterator>;
}

/// 根据类型打开内存索引
pub fn new_indexer(index_type: IndexType) -> impl Indexer {
    match index_type {
        IndexType::BTree => btree::BTree::new(),
        IndexType::SkipList => todo!(),
    }
}

pub trait IndexIterator: Sync + Send {
    /// Rewind 重新回到迭代器的起点，即第一个数据
    fn rewind(&mut self);

    /// Seek 根据传入的 key 查找第一个大于（或小于）等于的目标 key，根据从这个 key 开始的遍历
    fn seek(&mut self, key: Vec<u8>);

    /// Next 跳转到下一个 key，返回 None 则说明迭代完毕
    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPos)>;
}
