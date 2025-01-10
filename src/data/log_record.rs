/// LogRecord 写入到数据文件的记录
/// 之所以叫日志，是因为数据文件中的数据是追加写入的，类似日志的格式
pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) rec_type: LogRecordType,
}

#[derive(PartialEq)]
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

/// 从数据文件中读取的 log_record 信息，包含其 size
pub struct ReadLogRecord {
    pub(crate) record: LogRecord,
    pub(crate) size: u64,
}

impl LogRecord {
    pub fn encode(&mut self) -> Vec<u8> {
        todo!()
    }
}

/// 获得 LogRecord header 部分的最大长度
pub fn max_log_record_header_size() -> usize {
    use prost::length_delimiter_len;
    size_of::<u8>() + length_delimiter_len(std::u32::MAX as usize) * 2
}
