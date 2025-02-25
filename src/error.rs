use std::result;
use thiserror::Error;

#[derive(Clone, Error, Debug, PartialEq)]
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

    #[error("data file is not found in databases")]
    DataFileNotFound,

    #[error("database dir path is empty")]
    DirPathIsEmpty,

    #[error("database data file size must be greater than 0")]
    DataFileSizeTooSmall,

    #[error("failed to create the database diretory")]
    FailedCreateDatabaseDir,

    #[error("failed to read the database diretory")]
    FailedReadDatabaseDir,

    #[error("the database directory maybe corrupted")]
    DataDirtoryCorrupted,

    #[error("the OsString may have invalid UTF-8 character")]
    OsStringInvalidUTF8,

    // need to remake
    #[error("there is something wrong in DirEntry")]
    DirEntryError,

    #[error("read data file eroor")]
    ReadDataFileEOF,

    #[error("invalid crc value, log record maybe corrupted")]
    InvalidLogRecordCrc,

    #[error("exceed the max batch num")]
    ExceedMaxBatchNum,

    #[error("update index error")]
    UpdateIndexError,

    #[error("merge is in progress, try again later")]
    MergeInProgress,

    #[error("cannot use write batch, seq file not exists")]
    UnableToUserWriteBatch,

    #[error("the database directory is used by another process")]
    DatabaseIsUsing,

    #[error("invalid merge ratio, must between 0 and 1")]
    InvalidMergeRatio,

    #[error("do not reach the merge ratio")]
    MergeRatioUnreached,

    #[error("disk space is not enough for merge")]
    MeregeNoEnoughSpace,

    #[error("failed to copy the database directory")]
    FailedToCopyDirectory,
}

pub type Result<T> = result::Result<T, Errors>;
