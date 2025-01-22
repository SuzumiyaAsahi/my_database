use crate::{
    batch::{log_record_key_with_seq, parse_log_record_key, NON_TRANSCATION_SEQ_NO},
    data::{
        data_file::{DataFile, DATA_FILE_NAME_SUFFIX, MERGE_FINISHED_FILE_NAME, SEQ_FILE_NAME},
        log_record::{LogRecord, LogRecordPos, LogRecordType, TransactionRecord},
    },
    error::{Errors, Result},
    index,
    merge::load_merge_files,
    options::{IOType, IndexType, Options},
};
use bytes::Bytes;
use fs2::FileExt;
use log::warn;
use parking_lot::{Mutex, RwLock};
use std::{
    collections::HashMap,
    fs::{self, File},
    path::PathBuf,
    sync::{atomic::AtomicUsize, Arc},
};

const INITIAL_FILE_ID: u32 = 0;
const SEQ_NO_KEY: &str = "seq.no";
pub(crate) const FILE_LOCK_NAME: &str = "flock";

/// 存储引擎相关统计信息
#[derive(Debug)]
pub struct Stat {
    /// key 的总数量
    pub key_num: usize,
    /// 数据文件的数量
    pub data_file_num: usize,
    /// 可以回收的数据量
    pub reclaim_size: usize,
    /// 数据目录占据的磁盘空间大小
    pub disk_size: u64,
}

/// bitcask 存储引擎实例结构体
pub struct Engine {
    pub(crate) options: Arc<Options>,
    /// 当前活跃数据文件
    pub(crate) active_file: Arc<RwLock<DataFile>>,
    /// 旧的数据文件
    pub(crate) older_files: Arc<RwLock<HashMap<u32, DataFile>>>,
    /// 数据内存索引
    pub(crate) index: Box<dyn index::Indexer>,
    /// 数据库启动时的文件 id，只用于加载索引时使用，不能在其他的地方更新或使用
    file_ids: Vec<u32>,
    /// 事务提交保证串行化
    pub(crate) batch_commit_lock: Mutex<()>,
    /// 事务序列号，全局递增
    pub(crate) seq_no: Arc<AtomicUsize>,
    /// 防止多个线程同时 merge
    pub(crate) merging_lock: Mutex<()>,
    /// 事务序列号是否存在
    pub(crate) seq_file_exists: bool,
    /// 是否第一词初始化该目录
    pub(crate) is_initial: bool,
    /// 文件锁，保证只能在数据目录上打开一个实例
    lock_file: File,
    /// 累计写入了多少字节
    bytes_write: Arc<AtomicUsize>,
    /// 累计有多少空间可以 merge
    pub(crate) reclaim_size: Arc<AtomicUsize>,
}

impl Engine {
    /// 打开 bitcask 存储引擎实例
    pub fn open(opts: Options) -> Result<Self> {
        // 校验用户传递过来的配置项
        check_options(&opts)?;

        let options = opts.clone();

        let mut is_initial = false;
        // 判断数据目录是够存在，如果不存在的话则创建这个目录
        let dir_path = options.dir_path.clone();

        if !dir_path.is_dir() {
            is_initial = true;
            if let Err(e) = fs::create_dir_all(dir_path.clone()) {
                warn!("create database directory err: {}", e);
                return Err(Errors::FailedCreateDatabaseDir);
            }
        }

        // 判断数据目录是否已经被使用了
        let lock_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(dir_path.join(FILE_LOCK_NAME))
            .unwrap();

        if lock_file.try_lock_exclusive().is_err() {
            return Err(Errors::DatabaseIsUsing);
        }

        let entries = fs::read_dir(dir_path.clone()).unwrap();
        if entries.count() == 0 {
            is_initial = true;
        }

        // 加载 merge 数据目录
        load_merge_files(dir_path.clone())?;

        // 加载数据文件
        let mut data_files = load_data_files(dir_path.clone(), options.mmap_at_startup)?;

        // 设置 file id 信息
        let mut file_ids = Vec::new();
        for v in data_files.iter() {
            file_ids.push(v.get_file_id());
        }

        // 因为我们这里把 data_files 里面的文件 id 都排好了顺序
        // 文件号最大的，也就是 Vec 中最后的文件 id 就是活跃文件 id
        // 其余的都是不活跃文件 id

        data_files.reverse();

        // 将旧的数据文件保存到 older_files 中
        let mut older_files = HashMap::new();
        if data_files.len() > 1 {
            for _ in 0..data_files.len() - 1 {
                let file = data_files.pop().unwrap();
                older_files.insert(file.get_file_id(), file);
            }
        }

        // 拿到当前活跃文件，即列表中最后一个文件
        let active_file = match data_files.pop() {
            Some(v) => v,
            None => DataFile::new(
                dir_path.clone(),
                INITIAL_FILE_ID,
                crate::options::IOType::StandardFIO,
            )?,
        };

        let mut engine = Self {
            options: Arc::new(opts),
            active_file: Arc::new(RwLock::new(active_file)),
            older_files: Arc::new(RwLock::new(older_files)),
            index: index::new_indexer(options.index_type, options.dir_path),
            file_ids,
            batch_commit_lock: Mutex::new(()),
            seq_no: Arc::new(AtomicUsize::new(1)),
            merging_lock: Mutex::new(()),
            seq_file_exists: false,
            is_initial,
            lock_file,
            bytes_write: Arc::new(AtomicUsize::new(0)),
            reclaim_size: Arc::new(AtomicUsize::new(0)),
        };

        // B+ 树不需要从数据文件中加载索引
        if engine.options.index_type != IndexType::BPlusTree {
            // 从 hint 文件中加载索引
            engine.load_index_from_hint_file()?;

            // 从数据文件中加载索引
            let current_seq_no = engine.load_index_from_data_files()?;

            // 更新当前事务序列号
            if current_seq_no > 0 {
                engine
                    .seq_no
                    .store(current_seq_no + 1, std::sync::atomic::Ordering::SeqCst);
            }

            // 重置 IO 类型
            if engine.options.mmap_at_startup {
                engine.reset_io_type()?;
            }
        } else {
            // 加载事务序列号
            let (exists, seq_no) = engine.load_seq_no();
            if exists {
                engine
                    .seq_no
                    .store(seq_no, std::sync::atomic::Ordering::SeqCst);
                engine.seq_file_exists = exists;
            }

            // 设置当前活跃文件的偏移
            let active_file = engine.active_file.write();
            active_file.set_write_off(active_file.file_size());
        }

        Ok(engine)
    }

    /// 关闭数据库，释放相关资源
    pub fn close(&self) -> Result<()> {
        // 如果语句目录不存在则返回
        if !self.options.dir_path.is_dir() {
            return Ok(());
        }
        // 记录当前事务序列号
        let seq_no_file = DataFile::new_seq_no_file(self.options.dir_path.clone())?;
        let seq_no = self.seq_no.load(std::sync::atomic::Ordering::SeqCst);
        let record = LogRecord {
            key: SEQ_NO_KEY.as_bytes().to_vec(),
            value: seq_no.to_string().into_bytes(),
            rec_type: LogRecordType::Normal,
        };
        seq_no_file.write(&record.encode())?;
        seq_no_file.sync()?;

        let read_guard = self.active_file.read();
        read_guard.sync()?;

        // 释放文件锁
        self.lock_file.unlock().unwrap();

        Ok(())
    }

    /// 持久化当前活跃文件
    pub fn sync(&self) -> Result<()> {
        let read_guard = self.active_file.read();
        read_guard.sync()
    }

    /// 获取数据库统计信息
    pub fn stat(&self) -> Result<Stat> {
        let keys = self.list_keys()?;
        let older_files = self.older_files.read();
        Ok(Stat {
            key_num: keys.len(),
            data_file_num: older_files.len() + 1,
            reclaim_size: self.reclaim_size.load(std::sync::atomic::Ordering::SeqCst),
            disk_size: crate::util::file::dir_disk_size(self.options.dir_path.clone()),
        })
    }

    /// 存储 key/value 数据，key 不能为空
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        // 判断 key 的有效性
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        // 构造 LogRecord
        let mut record = LogRecord {
            key: log_record_key_with_seq(key.to_vec(), NON_TRANSCATION_SEQ_NO),
            value: value.to_vec(),
            rec_type: LogRecordType::Normal,
        };

        // 追加写活跃文件到数据文件中
        let log_record_pos = self.append_log_record(&mut record)?;

        // 更新内存索引
        if let Some(old_pos) = self.index.put(key.to_vec(), log_record_pos) {
            self.reclaim_size
                .fetch_add(old_pos.size as usize, std::sync::atomic::Ordering::SeqCst);
        }

        Ok(())
    }

    /// 根据 key 删除对应的数据
    pub fn delete(&self, key: Bytes) -> Result<()> {
        // 判断 key 的有效性
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        // 从内存索引当中取出对应的数据，不存在的话直接返回
        let pos = self.index.get(key.to_vec());

        if pos.is_none() {
            return Ok(());
        }

        // 构建 LogRecord，标志其是被删除的
        let mut record = LogRecord {
            key: log_record_key_with_seq(key.to_vec(), NON_TRANSCATION_SEQ_NO),
            value: Default::default(),
            rec_type: LogRecordType::Deleted,
        };

        // 写入到数据文件当中
        let pos = self.append_log_record(&mut record)?;
        self.reclaim_size
            .fetch_add(pos.size as usize, std::sync::atomic::Ordering::SeqCst);

        // 删除内存索引中对应的 key
        if let Some(old_pos) = self.index.delete(key.to_vec()) {
            self.reclaim_size
                .fetch_add(old_pos.size as usize, std::sync::atomic::Ordering::SeqCst);
        }

        Ok(())
    }
    /// 根据 key 获取对应的数据
    pub fn get(&self, key: Bytes) -> Result<Bytes> {
        // 判断 key 的有效性
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        // 从内存索引中获取 key 对应的数据信息
        let pos = self.index.get(key.to_vec());

        self.get_value_by_position(pos.as_ref())
    }

    /// 根据索引信息获取 value
    pub(crate) fn get_value_by_position(&self, pos: Option<&LogRecordPos>) -> Result<Bytes> {
        // 从对应的数据文件中获取对应的 LogRecord
        if let Some(log_record_pos) = pos {
            let active_file = self.active_file.read();
            let older_files = self.older_files.read();

            // 查看活跃文件中是否是对应的数据文件
            let log_record = match active_file.get_file_id() == log_record_pos.file_id {
                true => active_file.read_log_record(log_record_pos.offset)?.record,

                // 如果找不到，就去旧的活跃文件中去找
                false => {
                    let data_file = older_files.get(&log_record_pos.file_id);
                    if let Some(data_file) = data_file {
                        data_file.read_log_record(log_record_pos.offset)?.record
                    } else {
                        // 找不到对应的数据文件，返回错误
                        return Err(Errors::DataFileNotFound);
                    }
                }
            };

            // 判断 LogRecord 的类型
            if log_record.rec_type == LogRecordType::Deleted {
                return Err(Errors::KeyNotFound);
            }

            // 返回对应的 value 信息
            Ok(log_record.value.into())
        } else {
            // 如果 key 不存在则直接返回
            Err(Errors::KeyNotFound)
        }
    }
    /// 追加写数据到当前活跃文件中
    pub(crate) fn append_log_record(&self, log_record: &mut LogRecord) -> Result<LogRecordPos> {
        let dir_path = self.options.dir_path.clone();
        // 输入数据进行编码
        let enc_record = log_record.encode();
        let record_len: u64 = enc_record.len() as u64;

        // 获取当前活跃文件
        let mut active_file = self.active_file.write();

        // 判断当前活跃文件是否达到了阈值
        if active_file.get_write_off() + record_len > self.options.data_file_size {
            // 将当前活跃文件进行持久化
            active_file.sync()?;

            let current_fid = active_file.get_file_id();

            // 旧的数据文件存储到 map 中
            let mut older_files = self.older_files.write();
            let old_file = DataFile::new(
                dir_path.clone(),
                current_fid,
                crate::options::IOType::StandardFIO,
            )?;
            older_files.insert(current_fid, old_file);

            // 打开新的数据文件
            let new_file = DataFile::new(
                dir_path.clone(),
                current_fid + 1,
                crate::options::IOType::StandardFIO,
            )?;
            *active_file = new_file;
        }

        // 追加写数据到当前活跃文件中
        let write_off = active_file.get_write_off();
        active_file.write(&enc_record)?;

        let previous = self
            .bytes_write
            .fetch_add(enc_record.len(), std::sync::atomic::Ordering::SeqCst);
        // 根据配置项决定是否持久化
        let mut need_sync = self.options.sync_writes;
        if !need_sync
            && self.options.bytes_per_sync > 0
            && previous + enc_record.len() >= self.options.bytes_per_sync
        {
            need_sync = true;
        }

        if need_sync {
            active_file.sync()?;
            // 清空累计值
            self.bytes_write
                .store(0, std::sync::atomic::Ordering::SeqCst);
        }

        // 构造数据索引信息
        Ok(LogRecordPos {
            file_id: active_file.get_file_id(),
            offset: write_off,
            size: enc_record.len() as u32,
        })
    }

    /// 从数据文件中加载内存索引
    /// 遍历数据文件中的内容，并依次处理其中的记录
    fn load_index_from_data_files(&mut self) -> Result<usize> {
        let mut current_seq_no = NON_TRANSCATION_SEQ_NO;
        // 数据文件为空，直接返回
        if self.file_ids.is_empty() {
            return Ok(current_seq_no);
        }

        // 拿到最近未参与 merge 的文件 id
        let mut has_merge = false;
        let mut non_merge_fid = 0;
        let merge_fin_file = self.options.dir_path.join(MERGE_FINISHED_FILE_NAME);
        if merge_fin_file.is_file() {
            let merge_fin_file = DataFile::new_merge_fin_file(self.options.dir_path.clone())?;
            let merge_fin_record = merge_fin_file.read_log_record(0)?;
            let v = String::from_utf8(merge_fin_record.record.value).unwrap();

            non_merge_fid = v.parse::<u32>().unwrap();
            has_merge = true;
        }

        // 暂存事务相关的数据
        let mut transaction_records = HashMap::new();

        let active_file = self.active_file.read();
        let older_files = self.older_files.read();

        // 遍历每个文件 id，取出对应的数据文件，并加载其中的数据
        for (i, file_id) in self.file_ids.iter().enumerate() {
            // 如果比最近未参与 merge 的文件 id 更小，则已经从 hint 文件中加载索引了
            if has_merge && *file_id < non_merge_fid {
                continue;
            }
            let mut offset = 0;
            loop {
                let log_record_res = match *file_id == active_file.get_file_id() {
                    true => active_file.read_log_record(offset),
                    false => {
                        let datafile = older_files.get(file_id).unwrap();
                        datafile.read_log_record(offset)
                    }
                };

                let (mut log_record, size) = match log_record_res {
                    Ok(result) => (result.record, result.size),
                    Err(e) => {
                        if e == Errors::ReadDataFileEOF {
                            break;
                        } else {
                            return Err(e);
                        }
                    }
                };

                // 构建内存索引
                let log_record_pos = LogRecordPos {
                    file_id: *file_id,
                    offset,
                    size: size as u32,
                };

                // 解析 key，拿到实际的 key 和 seq no
                let (real_key, seq_no) = parse_log_record_key(log_record.key);

                // 非事务提交的情况，直接更新内存索引
                if seq_no == NON_TRANSCATION_SEQ_NO {
                    self.update_index(real_key, log_record.rec_type, log_record_pos);
                }
                // 事务有提交的标识，更新内存索引
                else if log_record.rec_type == LogRecordType::Txnfinished {
                    let records: &Vec<TransactionRecord> =
                        transaction_records.get(&seq_no).unwrap();
                    for txn_record in records.iter() {
                        self.update_index(
                            txn_record.record.key.clone(),
                            txn_record.record.rec_type,
                            txn_record.pos,
                        );
                    }
                    transaction_records.remove(&seq_no);
                } else {
                    log_record.key = real_key;
                    transaction_records
                        .entry(seq_no)
                        .or_insert(Vec::new())
                        .push(TransactionRecord {
                            record: log_record,
                            pos: log_record_pos,
                        });
                }

                // 更新当前事务序列号
                if seq_no > current_seq_no {
                    current_seq_no = seq_no;
                }

                // 递增活跃文件的 offset
                offset += size;
            }

            // 设置活跃文件的 offset
            if i == self.file_ids.len() - 1 {
                active_file.set_write_off(offset);
            }
        }
        Ok(current_seq_no)
    }

    /// 加载索引时更新数据
    fn update_index(&self, key: Vec<u8>, rec_type: LogRecordType, pos: LogRecordPos) {
        if rec_type == LogRecordType::Normal {
            if let Some(old_pos) = self.index.put(key.clone(), pos) {
                self.reclaim_size
                    .fetch_add(old_pos.size as usize, std::sync::atomic::Ordering::SeqCst);
            }
        }
        if rec_type == LogRecordType::Deleted {
            let mut size = pos.size;
            if let Some(old_pos) = self.index.delete(key) {
                size += old_pos.size;
            }
            self.reclaim_size
                .fetch_add(size as usize, std::sync::atomic::Ordering::SeqCst);
        }
    }

    fn load_seq_no(&self) -> (bool, usize) {
        let file_name = self.options.dir_path.join(SEQ_FILE_NAME);
        if !file_name.is_file() {
            return (false, 0);
        }

        let seq_no_file = DataFile::new_seq_no_file(self.options.dir_path.clone()).unwrap();
        let record = match seq_no_file.read_log_record(0) {
            Ok(res) => res.record,
            Err(e) => panic!("failed to read seq no: {}", e),
        };

        let v = String::from_utf8(record.value).unwrap();
        let seq_no = v.parse::<usize>().unwrap();

        // 加载后直接删除掉，避免追加写入
        fs::remove_file(file_name).unwrap();

        (true, seq_no)
    }

    fn reset_io_type(&self) -> Result<()> {
        let mut active_file = self.active_file.write();
        active_file.set_io_iomanager(self.options.dir_path.clone(), IOType::StandardFIO)?;
        let mut older_files = self.older_files.write();
        for (_, file) in older_files.iter_mut() {
            file.set_io_iomanager(self.options.dir_path.clone(), IOType::StandardFIO)?;
        }
        Ok(())
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        if let Err(e) = self.close() {
            log::error!("error while close engine: {}", e);
        }
    }
}

/// 从数据目录中加载数据文件
fn load_data_files(dir_path: PathBuf, use_mmap: bool) -> Result<Vec<DataFile>> {
    // 读取数据目录
    let dir = fs::read_dir(dir_path.clone());

    if let Ok(dir) = dir {
        let mut file_ids: Vec<u32> = Vec::new();
        let mut data_files: Vec<DataFile> = Vec::new();
        for file in dir {
            if let Ok(entry) = file {
                // 拿到文件名
                let file_os_str = entry.file_name();

                if let Some(file_name) = file_os_str.to_str() {
                    // 判断文件名是否是以 .data 结尾
                    if file_name.ends_with(DATA_FILE_NAME_SUFFIX) {
                        let split_names: Vec<&str> = file_name.split(".").collect();
                        let file_id = match split_names[0].parse::<u32>() {
                            Ok(fid) => fid,
                            Err(_) => return Err(Errors::DataDirtoryCorrupted),
                        };
                        file_ids.push(file_id);
                    }
                } else {
                    return Err(Errors::OsStringInvalidUTF8);
                }
            } else {
                return Err(Errors::DirEntryError);
            }
        }

        // 如果没有数据文件，则直接撤回
        if file_ids.is_empty() {
            return Ok(data_files);
        }

        // 对文件 id 进行排序，从小到大进行加载
        file_ids.sort();

        // 遍历所有文件 id，依次打开对应的数据文件
        for file_id in file_ids {
            let mut io_type = IOType::StandardFIO;
            if use_mmap {
                io_type = IOType::MemoryMap;
            }
            let data_file = DataFile::new(dir_path.clone(), file_id, io_type)?;
            data_files.push(data_file);
        }

        return Ok(data_files);
    }

    Err(Errors::FailedReadDatabaseDir)
}

/// 校验用户传递过来的配置项
fn check_options(opts: &Options) -> Result<()> {
    let dir_path = opts.dir_path.to_str();
    match dir_path {
        None => return Err(Errors::DirPathIsEmpty),
        Some(dir_path) => {
            if dir_path.is_empty() {
                return Err(Errors::DirPathIsEmpty);
            }
        }
    }

    if opts.data_file_size == 0 {
        return Err(Errors::DataFileSizeTooSmall);
    }

    if opts.data_file_merge_ratio < 0 as f32 || opts.data_file_merge_ratio > 1.0 {
        return Err(Errors::InvalidMergeRatio);
    }

    Ok(())
}
