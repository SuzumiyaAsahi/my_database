/// 数据位置索引信息， 描述数据存储到了哪个位置
#[derive(Debug, Clone, Copy)]
pub struct LogRecorPos {
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
}
