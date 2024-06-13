use std::{collections::BTreeMap, sync::Arc};

use parking_lot::RwLock;

use crate::{data::log_record::LogRecorPos, index::Indexer};

/// BTree 索引，主要封装了标准库中的 BTreeMap 结构
pub struct BTree {
    tree: Arc<RwLock<BTreeMap<Vec<u8>, LogRecorPos>>>,
}

impl BTree {
    pub fn new() -> Self {
        Self {
            tree: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

impl Indexer for BTree {
    fn delete(&self, key: Vec<u8>) -> bool {
        let mut write_guard = self.tree.write();
        let remove_res = write_guard.remove(&key);
        remove_res.is_some()
    }
    fn get(&self, key: Vec<u8>) -> Option<LogRecorPos> {
        let read_guard = self.tree.write();
        read_guard.get(&key).copied()
    }
    fn put(&self, key: Vec<u8>, pos: LogRecorPos) -> bool {
        let mut write_guard = self.tree.write();
        write_guard.insert(key, pos);
        true
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
            LogRecorPos {
                file_id: 1,
                offset: 10,
            },
        );
        assert!(res1);

        let res2 = bt.put(
            "aa".as_bytes().to_vec(),
            LogRecorPos {
                file_id: 11,
                offset: 22,
            },
        );
        assert!(res2);
    }

    #[test]
    fn test_bree_get() {
        let bt = BTree::new();
        let res1 = bt.put(
            "".as_bytes().to_vec(),
            LogRecorPos {
                file_id: 1,
                offset: 10,
            },
        );
        assert!(res1);

        let res2 = bt.put(
            "aa".as_bytes().to_vec(),
            LogRecorPos {
                file_id: 11,
                offset: 22,
            },
        );
        assert!(res2);

        let del1 = bt.delete("".as_bytes().to_vec());
        assert!(del1);

        let del2 = bt.delete("aa".as_bytes().to_vec());
        assert!(del2);

        let del3 = bt.delete("not exist".as_bytes().to_vec());
        assert!(!del3);
    }
}
