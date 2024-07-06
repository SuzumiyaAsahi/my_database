use crate::error::{Errors, Result};
use bytes::Bytes;

/// bitcask 存储引擎实例结构体
pub struct Engine {}

impl Engine {
    /// 存储 key/value 数据，key 不能为空
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        // 判断 key 的有效性
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }
        Ok(())
    }
}
