use std::result;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Errors {
    #[error("failed to read from data file")]
    FailedReadFromDataFile,

    #[error("failed to read from data file")]
    FailedWriteFromDataFile,

    #[error("failed to sync data file")]
    FailedSyncDataFile,

    #[error("failed to open data file")]
    FailedOpenDataFile,

    #[error("the key is empty")]
    KeyIsEmpty,

    #[error("memory index failed to update")]
    IndexUpdateFailed,

    #[error("memory index failed to delete")]
    IndexDeleteFailed,

    #[error("key is not found in database")]
    KeyNotFound,
}

pub type Result<T> = result::Result<T, Errors>;
