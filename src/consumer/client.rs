use bytes::Bytes;
use tokio::net::TcpStream;

use crate::broker::protocol::{read_frame, write_frame, Frame};

// ── Record ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    pub offset: u64,
    pub payload: Bytes,
}

// ── ConsumerClient ────────────────────────────────────────────────────────────

pub struct ConsumerClient {
    stream: TcpStream,
    group: String,
    topic: String,
    partition: u32,
    current_offset: u64,
}

impl ConsumerClient {
    /// Connect to the broker. `group` is the consumer-group identifier used
    /// when committing offsets.
    pub async fn connect(addr: &str, group: &str) -> std::io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self {
            stream,
            group: group.to_string(),
            topic: String::new(),
            partition: 0,
            current_offset: 0,
        })
    }

    // ── Subscription ─────────────────────────────────────────────────────────

    /// Set which topic/partition to consume and the starting offset.
    pub fn subscribe(&mut self, topic: &str, partition: u32, start_offset: u64) {
        self.topic = topic.to_string();
        self.partition = partition;
        self.current_offset = start_offset;
    }

    // ── Polling ───────────────────────────────────────────────────────────────

    /// Fetch up to `max` records starting from `current_offset`.
    /// Stops early when the broker returns an error (e.g. end-of-log).
    /// Advances `current_offset` past each successfully fetched record.
    pub async fn poll_n(&mut self, max: usize) -> std::io::Result<Vec<Record>> {
        let mut records = Vec::new();

        for _ in 0..max {
            write_frame(
                &mut self.stream,
                &Frame::Fetch {
                    topic: self.topic.clone(),
                    partition: self.partition,
                    offset: self.current_offset,
                },
            )
            .await?;

            let raw = read_frame(&mut self.stream).await?;
            match Frame::decode(raw) {
                Ok(Frame::FetchData { offset, payload }) => {
                    self.current_offset = offset + 1;
                    records.push(Record { offset, payload });
                }
                // Any broker error means end-of-log or real error — stop polling.
                Ok(Frame::Error { .. }) => break,
                Ok(other) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("unexpected response to Fetch: {:?}", other),
                    ))
                }
                Err(e) => return Err(std::io::Error::other(e.to_string())),
            }
        }

        Ok(records)
    }

    /// Fetch up to 10 records (default batch size).
    pub async fn poll(&mut self) -> std::io::Result<Vec<Record>> {
        self.poll_n(10).await
    }

    // ── Offset management ─────────────────────────────────────────────────────

    /// Send `current_offset` to the broker's consumer-group coordinator.
    pub async fn commit_offset(&mut self) -> std::io::Result<()> {
        write_frame(
            &mut self.stream,
            &Frame::CommitOffset {
                group: self.group.clone(),
                topic: self.topic.clone(),
                partition: self.partition,
                offset: self.current_offset,
            },
        )
        .await?;

        let raw = read_frame(&mut self.stream).await?;
        match Frame::decode(raw) {
            Ok(Frame::Ack) => Ok(()),
            Ok(Frame::Error { message, .. }) => Err(std::io::Error::other(message)),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unexpected response to CommitOffset",
            )),
        }
    }

    /// The offset that will be requested on the next `poll_n` call.
    pub fn current_offset(&self) -> u64 {
        self.current_offset
    }
}
