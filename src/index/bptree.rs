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
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> Result<()> {
        let tx = self.tree.tx(true).expect("failed to begin tx");
        let bucket = tx.get_bucket(BPTREE_BUCKET_NAME).unwrap();
        bucket
            .put(key, pos.encode())
            .expect("failed to put value in bptree");
        tx.commit().unwrap();
        Ok(())
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        let tx = self.tree.tx(false).expect("failed to begin tx");
        let bucket = tx.get_bucket(BPTREE_BUCKET_NAME).unwrap();
        if let Some(kv) = bucket.get_kv(key) {
            return Some(decode_log_record_pos(kv.value().to_vec()));
        }
        None
    }

    fn delete(&self, key: Vec<u8>) -> Result<()> {
        let tx = self.tree.tx(true).expect("failed to begin tx");
        let bucket = tx.get_bucket(BPTREE_BUCKET_NAME).unwrap();
        if let Err(e) = bucket.delete(key) {
            if e == Error::KeyValueMissing {
                return Err(Errors::IndexDeleteFailed);
            }
        }
        tx.commit().unwrap();
        Ok(())
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

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    #[test]
    fn test_bptree_put() {
        let path = PathBuf::from("/tmp/bptree-put");
        fs::create_dir_all(path.clone()).unwrap();
        let bpt = BPlusTree::new(path.clone());

        let res1 = bpt.put(
            b"ccbde".to_vec(),
            LogRecordPos {
                file_id: 123,
                offset: 883,
            },
        );
        assert!(res1.is_ok());
        let res2 = bpt.put(
            b"bbed".to_vec(),
            LogRecordPos {
                file_id: 123,
                offset: 883,
            },
        );
        assert!(res2.is_ok());
        let res3 = bpt.put(
            b"aeer".to_vec(),
            LogRecordPos {
                file_id: 123,
                offset: 883,
            },
        );
        assert!(res3.is_ok());
        let res4 = bpt.put(
            b"cccd".to_vec(),
            LogRecordPos {
                file_id: 123,
                offset: 883,
            },
        );
        assert!(res4.is_ok());

        let res5 = bpt.put(
            b"cccd".to_vec(),
            LogRecordPos {
                file_id: 77,
                offset: 11,
            },
        );
        assert!(res5.is_ok());

        fs::remove_dir_all(path.clone()).unwrap();
    }

    #[test]
    fn test_bptree_get() {
        let path = PathBuf::from("/tmp/bptree-get");
        fs::create_dir_all(path.clone()).unwrap();
        let bpt = BPlusTree::new(path.clone());

        let v1 = bpt.get(b"not exist".to_vec());
        assert!(v1.is_none());

        bpt.put(
            b"ccbde".to_vec(),
            LogRecordPos {
                file_id: 123,
                offset: 883,
            },
        )
        .unwrap();
        let v2 = bpt.get(b"ccbde".to_vec());
        assert!(v2.is_some());

        bpt.put(
            b"ccbde".to_vec(),
            LogRecordPos {
                file_id: 125,
                offset: 77773,
            },
        )
        .unwrap();
        let v3 = bpt.get(b"ccbde".to_vec());
        assert!(v3.is_some());

        fs::remove_dir_all(path.clone()).unwrap();
    }

    #[test]
    fn test_bptree_delete() {
        let path = PathBuf::from("/tmp/bptree-delete");
        fs::create_dir_all(path.clone()).unwrap();
        let bpt = BPlusTree::new(path.clone());

        let r1 = bpt.delete(b"not exist".to_vec());
        assert!(r1.is_err());

        bpt.put(
            b"ccbde".to_vec(),
            LogRecordPos {
                file_id: 123,
                offset: 883,
            },
        )
        .unwrap();
        let r2 = bpt.delete(b"ccbde".to_vec());
        assert!(r2.is_ok());

        let v2 = bpt.get(b"ccbde".to_vec());
        assert!(v2.is_none());

        fs::remove_dir_all(path.clone()).unwrap();
    }

    #[test]
    fn test_bptree_list_keys() {
        let path = PathBuf::from("/tmp/bptree-list-keys");
        fs::create_dir_all(path.clone()).unwrap();
        let bpt = BPlusTree::new(path.clone());

        let keys1 = bpt.list_keys();
        assert_eq!(keys1.ok().unwrap().len(), 0);

        bpt.put(
            b"ccbde".to_vec(),
            LogRecordPos {
                file_id: 123,
                offset: 883,
            },
        )
        .unwrap();
        bpt.put(
            b"bbed".to_vec(),
            LogRecordPos {
                file_id: 123,
                offset: 883,
            },
        )
        .unwrap();
        bpt.put(
            b"aeer".to_vec(),
            LogRecordPos {
                file_id: 123,
                offset: 883,
            },
        )
        .unwrap();
        bpt.put(
            b"cccd".to_vec(),
            LogRecordPos {
                file_id: 123,
                offset: 883,
            },
        )
        .unwrap();

        let keys2 = bpt.list_keys();
        assert_eq!(keys2.ok().unwrap().len(), 4);

        fs::remove_dir_all(path.clone()).unwrap();
    }

    #[test]
    fn test_bptree_itreator() {
        let path = PathBuf::from("/tmp/bptree-iterator");
        fs::create_dir_all(path.clone()).unwrap();
        let bpt = BPlusTree::new(path.clone());

        bpt.put(
            b"ccbde".to_vec(),
            LogRecordPos {
                file_id: 123,
                offset: 883,
            },
        )
        .unwrap();
        bpt.put(
            b"bbed".to_vec(),
            LogRecordPos {
                file_id: 123,
                offset: 883,
            },
        )
        .unwrap();
        bpt.put(
            b"aeer".to_vec(),
            LogRecordPos {
                file_id: 123,
                offset: 883,
            },
        )
        .unwrap();
        bpt.put(
            b"cccd".to_vec(),
            LogRecordPos {
                file_id: 123,
                offset: 883,
            },
        )
        .unwrap();

        let opts = IteratorOptions {
            reverse: true,
            ..Default::default()
        };
        let mut iter = bpt.iterator(opts);
        while let Some((key, _)) = iter.next() {
            assert!(!key.is_empty());
        }

        fs::remove_dir_all(path.clone()).unwrap();
    }
}
