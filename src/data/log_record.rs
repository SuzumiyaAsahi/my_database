/// LogRecord 写入到数据文件的记录
/// 之所以叫日志，是因为数据文件中的数据是追加写入的，类似日志的格式
pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) rec_type: LogRecordType,
}

pub enum LogRecordType {
    // 正常 put 的数据
    NORMAL,

    // 被删除的数据标识，墓碑值
    DELETED,
}

/// 数据位置索引信息， 描述数据存储到了哪个位置
#[derive(Debug, Clone, Copy)]
pub struct LogRecorPos {
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
}
