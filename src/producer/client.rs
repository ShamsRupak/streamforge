use std::collections::HashMap;

use bytes::Bytes;
use tokio::net::TcpStream;

use crate::broker::protocol::{read_frame, write_frame, Frame};

// ── ProducerClient ────────────────────────────────────────────────────────────

pub struct ProducerClient {
    stream: TcpStream,
    /// Tracks the next partition to use per topic for round-robin assignment.
    round_robin: HashMap<String, u32>,
    /// Known partition counts per topic (populated after create_topic).
    partition_counts: HashMap<String, u32>,
    /// Buffered messages for batch mode: (topic, partition, payload).
    pending: Vec<(String, u32, Bytes)>,
    /// Maximum number of buffered messages before auto-flush is triggered.
    pub max_batch_size: usize,
}

impl ProducerClient {
    pub async fn connect(addr: &str) -> std::io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self {
            stream,
            round_robin: HashMap::new(),
            partition_counts: HashMap::new(),
            pending: Vec::new(),
            max_batch_size: 100,
        })
    }

    // ── Topic management ─────────────────────────────────────────────────────

    /// Ask the broker to create a topic with `num_partitions` partitions.
    /// Records the partition count locally for round-robin assignment.
    pub async fn create_topic(
        &mut self,
        name: &str,
        num_partitions: u32,
    ) -> std::io::Result<()> {
        write_frame(
            &mut self.stream,
            &Frame::CreateTopic {
                name: name.to_string(),
                num_partitions,
            },
        )
        .await?;

        let raw = read_frame(&mut self.stream).await?;
        match Frame::decode(raw) {
            Ok(Frame::Ack) => {
                self.partition_counts
                    .insert(name.to_string(), num_partitions);
                Ok(())
            }
            Ok(Frame::Error { code, message }) => Err(std::io::Error::other(format!(
                "broker error {:#04x}: {}",
                code, message
            ))),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unexpected response to CreateTopic",
            )),
        }
    }

    // ── Single-message send ───────────────────────────────────────────────────

    /// Send `payload` to `topic`, choosing a partition via round-robin.
    /// Returns the offset assigned by the broker.
    pub async fn send(&mut self, topic: &str, payload: Bytes) -> std::io::Result<u64> {
        let partition = self.next_partition(topic);
        self.send_to_partition(topic, partition, payload).await
    }

    /// Send `payload` to an explicit `partition` of `topic`.
    /// Returns the offset assigned by the broker.
    pub async fn send_to_partition(
        &mut self,
        topic: &str,
        partition: u32,
        payload: Bytes,
    ) -> std::io::Result<u64> {
        write_frame(
            &mut self.stream,
            &Frame::Produce {
                topic: topic.to_string(),
                partition,
                payload,
            },
        )
        .await?;

        let raw = read_frame(&mut self.stream).await?;
        match Frame::decode(raw) {
            Ok(Frame::ProduceAck { offset }) => Ok(offset),
            Ok(Frame::Error { code, message }) => Err(std::io::Error::other(format!(
                "broker error {:#04x}: {}",
                code, message
            ))),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unexpected response to Produce",
            )),
        }
    }

    // ── Batch mode ────────────────────────────────────────────────────────────

    /// Buffer a message for the next `flush_batch()` call.
    /// Selects a partition via round-robin.
    /// If the buffer reaches `max_batch_size`, the caller should call
    /// `flush_batch()` — this method does NOT auto-flush to keep it sync.
    pub fn buffer_message(&mut self, topic: &str, payload: Bytes) {
        let partition = self.next_partition(topic);
        self.pending.push((topic.to_string(), partition, payload));
    }

    /// Flush all buffered messages to the broker in order.
    /// Returns the offset list in the same order as the messages were buffered.
    pub async fn flush_batch(&mut self) -> std::io::Result<Vec<u64>> {
        let messages = std::mem::take(&mut self.pending);
        let mut offsets = Vec::with_capacity(messages.len());
        for (topic, partition, payload) in messages {
            let offset = self.send_to_partition(&topic, partition, payload).await?;
            offsets.push(offset);
        }
        Ok(offsets)
    }

    /// Number of messages currently buffered.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    fn next_partition(&mut self, topic: &str) -> u32 {
        let num_parts = self.partition_counts.get(topic).copied().unwrap_or(1);
        let counter = self.round_robin.entry(topic.to_string()).or_insert(0);
        let p = *counter % num_parts;
        *counter = counter.wrapping_add(1);
        p
    }
}
