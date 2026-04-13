mod commit_log;
mod index;
mod segment;

pub use commit_log::{Log, LogConfig};
pub use segment::Segment;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum LogError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("crc mismatch: expected {expected:#010x}, got {actual:#010x}")]
    CrcMismatch { expected: u32, actual: u32 },

    #[error("offset {0} is out of range for this log")]
    OffsetOutOfRange(u64),

    #[error("log is empty")]
    Empty,

    #[error("corrupt record: {0}")]
    CorruptRecord(String),
}

pub type Result<T> = std::result::Result<T, LogError>;
