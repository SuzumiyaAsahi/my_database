use crate::{
    db::Engine,
    error::{Errors, Result},
};

impl Engine {
    /// merge 数据目录，处理无效数据，并生成 hint 索引文件
    pub fn merge(&self) -> Result<()> {
        // 如果正在 merge，则直接返回
        let lock = self.merging_lock.try_lock();
        if lock.is_none() {
            return Err(Errors::MergeInProgress);
        }
        todo!()
    }
}
