use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// ── Wire format ───────────────────────────────────────────────────────────────
//
//   [4 bytes: body_length (u32 LE)]   ← does NOT include these 4 bytes
//   [1 byte:  opcode                ]
//   [N bytes: body                  ]
//
// Requests
//   PRODUCE       0x01  [2B topic_len][topic][4B partition][4B payload_len][payload]
//   FETCH         0x02  [2B topic_len][topic][4B partition][8B offset]
//   CREATE_TOPIC  0x03  [2B name_len][name][4B num_partitions]
//   COMMIT_OFFSET 0x04  [2B group_len][group][2B topic_len][topic][4B partition][8B offset]
//
// Responses (all use RESPONSE opcode 0x80 + 1-byte kind, or ERROR opcode 0x81)
//   RESPONSE/OK      0x80 0x00  (no body)
//   RESPONSE/OFFSET  0x80 0x01  [8B offset]
//   RESPONSE/PAYLOAD 0x80 0x02  [8B offset][4B payload_len][payload]
//   ERROR            0x81       [1B code][2B msg_len][msg]

pub mod opcode {
    pub const PRODUCE: u8 = 0x01;
    pub const FETCH: u8 = 0x02;
    pub const CREATE_TOPIC: u8 = 0x03;
    pub const COMMIT_OFFSET: u8 = 0x04;
    pub const RESPONSE: u8 = 0x80;
    pub const ERROR: u8 = 0x81;
}

mod resp_kind {
    pub const OK: u8 = 0x00;
    pub const OFFSET: u8 = 0x01;
    pub const PAYLOAD: u8 = 0x02;
}

pub const MAX_FRAME_BYTES: u32 = 64 * 1024 * 1024; // 64 MiB

// ── Error ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("unknown opcode: {0:#04x}")]
    UnknownOpcode(u8),
    #[error("unknown response kind: {0:#04x}")]
    UnknownResponseKind(u8),
    #[error("frame too large: {0} bytes")]
    FrameTooLarge(u32),
    #[error("frame too short")]
    TooShort,
}

// ── Frame enum ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    // Requests -----------------------------------------------------------
    Produce {
        topic: String,
        partition: u32,
        payload: Bytes,
    },
    Fetch {
        topic: String,
        partition: u32,
        offset: u64,
    },
    CreateTopic {
        name: String,
        num_partitions: u32,
    },
    CommitOffset {
        group: String,
        topic: String,
        partition: u32,
        offset: u64,
    },
    // Responses ----------------------------------------------------------
    /// Generic success with no data (CreateTopic, CommitOffset).
    Ack,
    /// Offset returned after a successful Produce.
    ProduceAck { offset: u64 },
    /// Record data returned by a successful Fetch.
    FetchData { offset: u64, payload: Bytes },
    /// Error response.
    Error { code: u8, message: String },
}

impl Frame {
    // ── Encode ──────────────────────────────────────────────────────────────

    pub fn encode(&self) -> Bytes {
        let mut body = BytesMut::new();

        match self {
            Frame::Produce {
                topic,
                partition,
                payload,
            } => {
                let tb = topic.as_bytes();
                body.put_u8(opcode::PRODUCE);
                body.put_u16_le(tb.len() as u16);
                body.put_slice(tb);
                body.put_u32_le(*partition);
                body.put_u32_le(payload.len() as u32);
                body.put_slice(payload);
            }

            Frame::Fetch {
                topic,
                partition,
                offset,
            } => {
                let tb = topic.as_bytes();
                body.put_u8(opcode::FETCH);
                body.put_u16_le(tb.len() as u16);
                body.put_slice(tb);
                body.put_u32_le(*partition);
                body.put_u64_le(*offset);
            }

            Frame::CreateTopic {
                name,
                num_partitions,
            } => {
                let nb = name.as_bytes();
                body.put_u8(opcode::CREATE_TOPIC);
                body.put_u16_le(nb.len() as u16);
                body.put_slice(nb);
                body.put_u32_le(*num_partitions);
            }

            Frame::CommitOffset {
                group,
                topic,
                partition,
                offset,
            } => {
                let gb = group.as_bytes();
                let tb = topic.as_bytes();
                body.put_u8(opcode::COMMIT_OFFSET);
                body.put_u16_le(gb.len() as u16);
                body.put_slice(gb);
                body.put_u16_le(tb.len() as u16);
                body.put_slice(tb);
                body.put_u32_le(*partition);
                body.put_u64_le(*offset);
            }

            Frame::Ack => {
                body.put_u8(opcode::RESPONSE);
                body.put_u8(resp_kind::OK);
            }

            Frame::ProduceAck { offset } => {
                body.put_u8(opcode::RESPONSE);
                body.put_u8(resp_kind::OFFSET);
                body.put_u64_le(*offset);
            }

            Frame::FetchData { offset, payload } => {
                body.put_u8(opcode::RESPONSE);
                body.put_u8(resp_kind::PAYLOAD);
                body.put_u64_le(*offset);
                body.put_u32_le(payload.len() as u32);
                body.put_slice(payload);
            }

            Frame::Error { code, message } => {
                let mb = message.as_bytes();
                body.put_u8(opcode::ERROR);
                body.put_u8(*code);
                body.put_u16_le(mb.len() as u16);
                body.put_slice(mb);
            }
        }

        let mut out = BytesMut::with_capacity(4 + body.len());
        out.put_u32_le(body.len() as u32);
        out.put_slice(&body);
        out.freeze()
    }

    // ── Decode ──────────────────────────────────────────────────────────────

    /// Decode a frame from a buffer that starts with the 4-byte length prefix.
    pub fn decode(mut buf: Bytes) -> Result<Self, ProtocolError> {
        if buf.len() < 5 {
            return Err(ProtocolError::TooShort);
        }
        let frame_len = buf.get_u32_le();
        if frame_len > MAX_FRAME_BYTES {
            return Err(ProtocolError::FrameTooLarge(frame_len));
        }
        let op = buf.get_u8();

        match op {
            opcode::PRODUCE => {
                let tlen = buf.get_u16_le() as usize;
                let topic = get_str(&mut buf, tlen);
                let partition = buf.get_u32_le();
                let plen = buf.get_u32_le() as usize;
                let payload = buf.copy_to_bytes(plen);
                Ok(Frame::Produce {
                    topic,
                    partition,
                    payload,
                })
            }

            opcode::FETCH => {
                let tlen = buf.get_u16_le() as usize;
                let topic = get_str(&mut buf, tlen);
                let partition = buf.get_u32_le();
                let offset = buf.get_u64_le();
                Ok(Frame::Fetch {
                    topic,
                    partition,
                    offset,
                })
            }

            opcode::CREATE_TOPIC => {
                let nlen = buf.get_u16_le() as usize;
                let name = get_str(&mut buf, nlen);
                let num_partitions = buf.get_u32_le();
                Ok(Frame::CreateTopic {
                    name,
                    num_partitions,
                })
            }

            opcode::COMMIT_OFFSET => {
                let glen = buf.get_u16_le() as usize;
                let group = get_str(&mut buf, glen);
                let tlen = buf.get_u16_le() as usize;
                let topic = get_str(&mut buf, tlen);
                let partition = buf.get_u32_le();
                let offset = buf.get_u64_le();
                Ok(Frame::CommitOffset {
                    group,
                    topic,
                    partition,
                    offset,
                })
            }

            opcode::RESPONSE => {
                let kind = buf.get_u8();
                match kind {
                    resp_kind::OK => Ok(Frame::Ack),
                    resp_kind::OFFSET => {
                        let offset = buf.get_u64_le();
                        Ok(Frame::ProduceAck { offset })
                    }
                    resp_kind::PAYLOAD => {
                        let offset = buf.get_u64_le();
                        let plen = buf.get_u32_le() as usize;
                        let payload = buf.copy_to_bytes(plen);
                        Ok(Frame::FetchData { offset, payload })
                    }
                    other => Err(ProtocolError::UnknownResponseKind(other)),
                }
            }

            opcode::ERROR => {
                let code = buf.get_u8();
                let mlen = buf.get_u16_le() as usize;
                let message = get_str(&mut buf, mlen);
                Ok(Frame::Error { code, message })
            }

            other => Err(ProtocolError::UnknownOpcode(other)),
        }
    }
}

// ── Async I/O helpers (used by server, producer, consumer) ───────────────────

/// Read one length-prefixed frame from `r`. Returns the raw bytes including
/// the 4-byte length prefix so they can be passed directly to `Frame::decode`.
pub async fn read_frame<R>(r: &mut R) -> std::io::Result<Bytes>
where
    R: AsyncReadExt + Unpin,
{
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf).await?;

    let frame_len = u32::from_le_bytes(len_buf) as usize;
    if frame_len as u32 > MAX_FRAME_BYTES {
        return Err(std::io::Error::other(format!(
            "frame too large: {} bytes",
            frame_len
        )));
    }

    let mut body = vec![0u8; frame_len];
    r.read_exact(&mut body).await?;

    // Re-prepend the 4-byte length so Frame::decode sees the full encoding.
    let mut full = Vec::with_capacity(4 + frame_len);
    full.extend_from_slice(&len_buf);
    full.extend_from_slice(&body);
    Ok(Bytes::from(full))
}

/// Encode and write `frame` to `w`.
pub async fn write_frame<W>(w: &mut W, frame: &Frame) -> std::io::Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    w.write_all(&frame.encode()).await
}

// ── Private helpers ───────────────────────────────────────────────────────────

fn get_str(buf: &mut Bytes, len: usize) -> String {
    let bytes = buf.copy_to_bytes(len);
    String::from_utf8_lossy(&bytes).into_owned()
}
