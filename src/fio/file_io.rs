use crate::error::{Errors, Result};
use log::error;
use parking_lot::RwLock;
use std::{fs::File, io::Write, os::unix::prelude::FileExt, sync::Arc};

use super::IOManager;

/// FileIO 标准系统文件 IO
pub struct FileIO {
    fd: Arc<RwLock<File>>,
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
}
