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
    fn delete(&self, key: Vec<u8>) -> Result<()> {
        let mut write_guard = self.tree.write();
        let remove_res = write_guard.remove(&key);
        match remove_res {
            Some(_) => Ok(()),
            None => Err(Errors::IndexDeleteFailed),
        }
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        let read_guard = self.tree.read();
        read_guard.get(&key).copied()
    }

    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> Result<()> {
        let mut write_guard = self.tree.write();
        write_guard.insert(key, pos);
        Ok(())
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

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_btree_put() {
        let bt = BTree::new();
        let res1 = bt.put(
            "".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        assert!(res1.is_ok());

        let res2 = bt.put(
            "aa".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 11,
                offset: 22,
            },
        );
        assert!(res2.is_ok());
    }

    #[test]
    fn test_bree_get() {
        let bt = BTree::new();
        let res1 = bt.put(
            "".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        assert!(res1.is_ok());

        let res2 = bt.put(
            "aa".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 11,
                offset: 22,
            },
        );
        assert!(res2.is_ok());

        let del1 = bt.delete("".as_bytes().to_vec());
        assert!(del1.is_ok());

        let del2 = bt.delete("aa".as_bytes().to_vec());
        assert!(del2.is_ok());

        let del3 = bt.delete("not exist".as_bytes().to_vec());
        assert!(del3.is_err());
    }

    #[test]
    fn test_btree_iterator_seek() -> Result<()> {
        let bt = BTree::new();

        // 没有数据的情况
        let mut iter1 = bt.iterator(IteratorOptions::default());
        iter1.seek("aa".as_bytes().to_vec());
        let res1 = iter1.next();
        assert!(res1.is_none());

        // 有一条数据的情况
        bt.put(
            "ccde".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        )?;
        let mut iter2 = bt.iterator(IteratorOptions::default());
        iter2.seek("aa".as_bytes().to_vec());
        let res2 = iter2.next();
        assert!(res2.is_some());

        let mut iter3 = bt.iterator(IteratorOptions::default());
        iter3.seek("zz".as_bytes().to_vec());
        let res3 = iter3.next();
        assert!(res3.is_none());

        // 有多条数据的情况
        bt.put(
            "bbed".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        )?;
        bt.put(
            "aaed".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        )?;
        bt.put(
            "cadd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        )?;

        let mut iter4 = bt.iterator(IteratorOptions::default());
        iter4.seek("b".as_bytes().to_vec());
        while let Some(item) = iter4.next() {
            assert!(!item.0.is_empty());
        }

        let mut iter5 = bt.iterator(IteratorOptions::default());
        iter5.seek("cadd".as_bytes().to_vec());
        while let Some(item) = iter5.next() {
            assert!(!item.0.is_empty());
        }

        let mut iter6 = bt.iterator(IteratorOptions::default());
        iter6.seek("zzz".as_bytes().to_vec());
        let res6 = iter6.next();
        assert!(res6.is_none());

        // 反向迭代
        let iter_opts = IteratorOptions {
            reverse: true,
            ..Default::default()
        };
        let mut iter7 = bt.iterator(iter_opts);
        iter7.seek("bb".as_bytes().to_vec());
        while let Some(item) = iter7.next() {
            assert!(!item.0.is_empty());
        }
        Ok(())
    }

    #[test]
    fn test_btree_iterator_next() -> Result<()> {
        let bt = BTree::new();
        let mut iter1 = bt.iterator(IteratorOptions::default());
        assert!(iter1.next().is_none());

        // 有一条数据的情况
        bt.put(
            "cadd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        )?;
        let iter_opt1 = IteratorOptions {
            reverse: true,
            ..Default::default()
        };
        let mut iter2 = bt.iterator(iter_opt1);
        assert!(iter2.next().is_some());

        // 有多条数据的情况
        bt.put(
            "bbed".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        )?;
        bt.put(
            "aaed".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        )?;
        bt.put(
            "cdea".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        )?;

        let iter_opt2 = IteratorOptions {
            reverse: true,
            ..Default::default()
        };
        let mut iter3 = bt.iterator(iter_opt2);
        while let Some(item) = iter3.next() {
            assert!(!item.0.is_empty());
        }

        // 有前缀的情况
        let iter_opt3 = IteratorOptions {
            reverse: true,
            ..Default::default()
        };
        let mut iter4 = bt.iterator(iter_opt3);
        while let Some(item) = iter4.next() {
            assert!(!item.0.is_empty());
        }

        Ok(())
    }
}
