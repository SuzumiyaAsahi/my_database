use crate::error::{Errors, Result};
use log::error;
use parking_lot::RwLock;
use std::{
    fs::{File, Metadata, OpenOptions},
    io::Write,
    os::unix::prelude::FileExt,
    path::PathBuf,
    sync::Arc,
};

use super::IOManager;

/// FileIO 标准系统文件 IO
pub struct FileIO {
    fd: Arc<RwLock<File>>,
}

impl FileIO {
    pub fn new(file_name: PathBuf) -> Result<Self> {
        match OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(file_name)
        {
            Ok(file) => Ok(Self {
                fd: Arc::new(RwLock::new(file)),
            }),
            Err(e) => {
                error!("failed to open data file in FileIO new{}", e);
                Err(Errors::FailedOpenDataFile)
            }
        }
    }
}

impl IOManager for FileIO {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let read_guard = self.fd.read();
        match read_guard.read_at(buf, offset) {
            Ok(n) => Ok(n),
            Err(e) => {
                error!("read from data file err: {}", e);
                Err(Errors::FailedReadFromDataFile)
            }
        }
    }
    fn write(&self, buf: &[u8]) -> Result<usize> {
        let mut write_guard = self.fd.write();
        match write_guard.write(buf) {
            Ok(n) => Ok(n),
            Err(e) => {
                error!("write to data file err: {}", e);
                Err(Errors::FailedWriteFromDataFile)
            }
        }
    }
    fn sync(&self) -> Result<()> {
        let read_guard = self.fd.read();
        if let Err(e) = read_guard.sync_all() {
            error!("failed to sync data file {}", e);
            return Err(Errors::FailedSyncDataFile);
        }
        Ok(())
    }

    fn size(&self) -> u64 {
        let read_guard = self.fd.read();
        let metadata: Metadata = read_guard.metadata().unwrap();
        metadata.len()
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    #[test]
    fn test_file_io_write() {
        let path = PathBuf::from("/tmp/a.data");
        let fio_res = FileIO::new(path.clone());
        assert!(fio_res.is_ok());
        let fio = fio_res.ok().unwrap();

        let res1 = fio.write("key-a".as_bytes());
        assert!(res1.is_ok());
        assert_eq!("key-a".len(), res1.ok().unwrap());

        let res2 = fio.write("key-b".as_bytes());
        assert!(res2.is_ok());
        assert_eq!("key-5".len(), res2.ok().unwrap());

        let res3 = fs::remove_file(path.clone());
        assert!(res3.is_ok());
    }

    #[test]
    fn test_file_io_read() {
        let path = PathBuf::from("/tmp/b.data");
        let fio_res = FileIO::new(path.clone());
        assert!(fio_res.is_ok());
        let fio = fio_res.ok().unwrap();

        let res1 = fio.write("key-a".as_bytes());
        assert!(res1.is_ok());
        assert_eq!("key-a".len(), res1.ok().unwrap());

        let res2 = fio.write("key-b".as_bytes());
        assert!(res2.is_ok());
        assert_eq!("key-5".len(), res2.ok().unwrap());

        let mut buf1 = [0u8; "key-a".len()];
        let read_res1 = fio.read(&mut buf1, 0);

        assert!(read_res1.is_ok());
        assert_eq!("key-a".len(), read_res1.ok().unwrap());

        let mut buf2 = [0u8; 5];
        let read_res2 = fio.read(&mut buf2, "key-a".len() as u64);
        assert!(read_res2.is_ok());
        assert_eq!("key-b".len(), read_res2.ok().unwrap());

        let res3 = fs::remove_file(path.clone());
        assert!(res3.is_ok());
    }

    #[test]
    fn test_io_sync() {
        let path = PathBuf::from("/tmp/c.data");
        let fio_res = FileIO::new(path.clone());
        assert!(fio_res.is_ok());
        let fio = fio_res.ok().unwrap();

        let res1 = fio.write("key-a".as_bytes());
        assert!(res1.is_ok());
        assert_eq!("key-a".len(), res1.ok().unwrap());

        let res2 = fio.write("key-b".as_bytes());
        assert!(res2.is_ok());
        assert_eq!("key-5".len(), res2.ok().unwrap());

        let sync_res = fio.sync();
        assert!(sync_res.is_ok());

        let res3 = fs::remove_file(path.clone());
        assert!(res3.is_ok());
    }
}
