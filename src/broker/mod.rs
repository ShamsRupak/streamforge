pub mod consumer_group;
pub mod protocol;
pub mod server;
pub mod topic;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum BrokerError {
    #[error("log error: {0}")]
    Log(#[from] crate::log::LogError),
    #[error("topic '{0}' not found")]
    TopicNotFound(String),
    #[error("partition {0} out of range for topic '{1}' ({2} partitions)")]
    PartitionOutOfRange(u32, String, usize),
    #[error("topic '{0}' already exists with {1} partitions")]
    TopicAlreadyExists(String, u32),
}
