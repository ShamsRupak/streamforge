use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info, warn};

use super::{
    consumer_group::ConsumerGroupCoordinator,
    protocol::{read_frame, write_frame, Frame},
    topic::TopicManager,
};
use crate::log::LogConfig;

// ── Server ────────────────────────────────────────────────────────────────────

pub struct Server {
    manager: Arc<Mutex<TopicManager>>,
    coordinator: Arc<Mutex<ConsumerGroupCoordinator>>,
}

impl Server {
    pub fn new(base_dir: &Path, config: LogConfig) -> Self {
        Self {
            manager: Arc::new(Mutex::new(TopicManager::new(base_dir, config))),
            coordinator: Arc::new(Mutex::new(ConsumerGroupCoordinator::new())),
        }
    }

    /// Bind to `addr` and run the accept loop indefinitely.
    pub async fn run(self, addr: &str) -> std::io::Result<()> {
        let listener = TcpListener::bind(addr).await?;
        info!("StreamForge listening on {}", listener.local_addr()?);
        self.serve(listener).await
    }

    /// Run the accept loop on an already-bound `listener`.
    /// Consumes `self` so it can be moved into the spawned task arc-free.
    pub async fn serve(self, listener: TcpListener) -> std::io::Result<()> {
        let manager = self.manager;
        let coordinator = self.coordinator;

        loop {
            let (stream, peer) = listener.accept().await?;
            let mgr = Arc::clone(&manager);
            let coord = Arc::clone(&coordinator);
            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, mgr, coord).await {
                    // ConnectionReset / UnexpectedEof are normal disconnects.
                    if e.kind() != std::io::ErrorKind::UnexpectedEof
                        && e.kind() != std::io::ErrorKind::ConnectionReset
                    {
                        error!("connection error from {}: {}", peer, e);
                    }
                }
            });
        }
    }
}

// ── Connection handler ────────────────────────────────────────────────────────

async fn handle_connection(
    mut stream: TcpStream,
    manager: Arc<Mutex<TopicManager>>,
    coordinator: Arc<Mutex<ConsumerGroupCoordinator>>,
) -> std::io::Result<()> {
    loop {
        let raw = match read_frame(&mut stream).await {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        };

        let frame = match Frame::decode(raw) {
            Ok(f) => f,
            Err(e) => {
                warn!("protocol decode error: {}", e);
                write_frame(
                    &mut stream,
                    &Frame::Error {
                        code: 0xFF,
                        message: e.to_string(),
                    },
                )
                .await?;
                break;
            }
        };

        let response = process_frame(frame, &manager, &coordinator);
        write_frame(&mut stream, &response).await?;
    }
    Ok(())
}

// ── Request dispatcher ────────────────────────────────────────────────────────

fn process_frame(
    frame: Frame,
    manager: &Mutex<TopicManager>,
    coordinator: &Mutex<ConsumerGroupCoordinator>,
) -> Frame {
    match frame {
        Frame::Produce {
            topic,
            partition,
            payload,
        } => {
            let mut mgr = manager.lock().unwrap();
            match mgr.get_or_create_partition_mut(&topic, partition) {
                Ok(log) => match log.append(&payload) {
                    Ok(offset) => Frame::ProduceAck { offset },
                    Err(e) => Frame::Error {
                        code: 0x01,
                        message: e.to_string(),
                    },
                },
                Err(e) => Frame::Error {
                    code: 0x02,
                    message: e.to_string(),
                },
            }
        }

        Frame::Fetch {
            topic,
            partition,
            offset,
        } => {
            let mut mgr = manager.lock().unwrap();
            match mgr.get_partition_mut(&topic, partition) {
                Ok(log) => match log.read(offset) {
                    Ok(payload) => Frame::FetchData {
                        offset,
                        payload: Bytes::from(payload),
                    },
                    Err(e) => Frame::Error {
                        code: 0x03,
                        message: e.to_string(),
                    },
                },
                Err(e) => Frame::Error {
                    code: 0x04,
                    message: e.to_string(),
                },
            }
        }

        Frame::CreateTopic {
            name,
            num_partitions,
        } => {
            let mut mgr = manager.lock().unwrap();
            match mgr.create_topic(&name, num_partitions) {
                Ok(()) => Frame::Ack,
                Err(e) => Frame::Error {
                    code: 0x05,
                    message: e.to_string(),
                },
            }
        }

        Frame::CommitOffset {
            group,
            topic,
            partition,
            offset,
        } => {
            let mut coord = coordinator.lock().unwrap();
            coord.commit(&group, &topic, partition, offset);
            Frame::Ack
        }

        other => Frame::Error {
            code: 0xFE,
            message: format!("unexpected request frame: {:?}", other),
        },
    }
}
