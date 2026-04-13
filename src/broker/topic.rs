use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use crate::log::{Log, LogConfig};

use super::BrokerError;

// ── Partition ─────────────────────────────────────────────────────────────────

pub struct Partition {
    pub log: Log,
}

// ── Topic ─────────────────────────────────────────────────────────────────────

pub struct Topic {
    pub name: String,
    pub partitions: Vec<Partition>,
}

impl Topic {
    fn new(name: &str, base_dir: &Path, num_partitions: u32, config: &LogConfig) -> Result<Self, BrokerError> {
        let mut partitions = Vec::with_capacity(num_partitions as usize);
        for i in 0..num_partitions {
            let dir = base_dir.join(name).join(format!("partition-{}", i));
            let log = Log::open(&dir, config.clone())?;
            partitions.push(Partition { log });
        }
        Ok(Self {
            name: name.to_string(),
            partitions,
        })
    }

    pub fn num_partitions(&self) -> u32 {
        self.partitions.len() as u32
    }
}

// ── TopicManager ─────────────────────────────────────────────────────────────

pub struct TopicManager {
    base_dir: PathBuf,
    log_config: LogConfig,
    topics: HashMap<String, Topic>,
}

impl TopicManager {
    pub fn new(base_dir: &Path, config: LogConfig) -> Self {
        Self {
            base_dir: base_dir.to_path_buf(),
            log_config: config,
            topics: HashMap::new(),
        }
    }

    /// Explicitly create a topic with `num_partitions` partitions.
    /// Returns `TopicAlreadyExists` if the topic is already present.
    pub fn create_topic(&mut self, name: &str, num_partitions: u32) -> Result<(), BrokerError> {
        if let Some(existing) = self.topics.get(name) {
            return Err(BrokerError::TopicAlreadyExists(
                name.to_string(),
                existing.num_partitions(),
            ));
        }
        let topic = Topic::new(name, &self.base_dir, num_partitions, &self.log_config)?;
        self.topics.insert(name.to_string(), topic);
        Ok(())
    }

    /// Get a mutable reference to the log at the given partition.
    /// Returns `None` if the topic doesn't exist.
    pub fn get_partition_mut(&mut self, topic: &str, partition: u32) -> Result<&mut Log, BrokerError> {
        let t = self
            .topics
            .get_mut(topic)
            .ok_or_else(|| BrokerError::TopicNotFound(topic.to_string()))?;

        let n = t.partitions.len();
        t.partitions
            .get_mut(partition as usize)
            .map(|p| &mut p.log)
            .ok_or(BrokerError::PartitionOutOfRange(partition, topic.to_string(), n))
    }

    /// Get or auto-create a topic, then return the log for `partition`.
    /// If the topic doesn't exist it is created with `partition + 1` partitions.
    pub fn get_or_create_partition_mut(
        &mut self,
        topic: &str,
        partition: u32,
    ) -> Result<&mut Log, BrokerError> {
        if !self.topics.contains_key(topic) {
            let n = partition + 1;
            let t = Topic::new(topic, &self.base_dir, n, &self.log_config)?;
            self.topics.insert(topic.to_string(), t);
        }
        self.get_partition_mut(topic, partition)
    }

    pub fn topic_names(&self) -> Vec<String> {
        self.topics.keys().cloned().collect()
    }

    pub fn num_partitions(&self, topic: &str) -> Option<u32> {
        self.topics.get(topic).map(|t| t.num_partitions())
    }
}
