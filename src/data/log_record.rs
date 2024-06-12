// 数据位置索引信息， 描述数据存储到了哪个位置
pub struct LogRecorPos {
    file_id: u32,
    offset: u64,
}
