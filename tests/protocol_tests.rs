use bytes::Bytes;
use streamforge::broker::protocol::{Frame, ProtocolError};

// ── Round-trip helpers ────────────────────────────────────────────────────────

fn rt(frame: Frame) -> Frame {
    Frame::decode(frame.encode()).expect("decode failed")
}

fn assert_rt(frame: Frame) {
    assert_eq!(rt(frame.clone()), frame);
}

// ── Request frames ────────────────────────────────────────────────────────────

#[test]
fn test_produce_roundtrip() {
    assert_rt(Frame::Produce {
        topic: "events".into(),
        partition: 0,
        payload: Bytes::from_static(b"hello world"),
    });
}

#[test]
fn test_produce_explicit_partition_roundtrip() {
    assert_rt(Frame::Produce {
        topic: "logs".into(),
        partition: 3,
        payload: Bytes::from_static(b"data"),
    });
}

#[test]
fn test_fetch_roundtrip() {
    assert_rt(Frame::Fetch {
        topic: "orders".into(),
        partition: 2,
        offset: 9_999_999,
    });
}

#[test]
fn test_fetch_offset_zero_roundtrip() {
    assert_rt(Frame::Fetch {
        topic: "t".into(),
        partition: 0,
        offset: 0,
    });
}

#[test]
fn test_create_topic_roundtrip() {
    assert_rt(Frame::CreateTopic {
        name: "my-topic".into(),
        num_partitions: 8,
    });
}

#[test]
fn test_create_topic_single_partition() {
    assert_rt(Frame::CreateTopic {
        name: "solo".into(),
        num_partitions: 1,
    });
}

#[test]
fn test_commit_offset_roundtrip() {
    assert_rt(Frame::CommitOffset {
        group: "analytics-group".into(),
        topic: "events".into(),
        partition: 1,
        offset: 42,
    });
}

// ── Response frames ───────────────────────────────────────────────────────────

#[test]
fn test_ack_roundtrip() {
    assert_rt(Frame::Ack);
}

#[test]
fn test_produce_ack_roundtrip() {
    assert_rt(Frame::ProduceAck { offset: 12345 });
}

#[test]
fn test_fetch_data_roundtrip() {
    assert_rt(Frame::FetchData {
        offset: 7,
        payload: Bytes::from(vec![0xDE, 0xAD, 0xBE, 0xEF]),
    });
}

#[test]
fn test_error_roundtrip() {
    assert_rt(Frame::Error {
        code: 0x03,
        message: "offset out of range".into(),
    });
}

#[test]
fn test_error_empty_message() {
    assert_rt(Frame::Error {
        code: 0xFF,
        message: String::new(),
    });
}

// ── Edge cases ────────────────────────────────────────────────────────────────

#[test]
fn test_empty_topic_name_roundtrip() {
    assert_rt(Frame::Produce {
        topic: String::new(),
        partition: 0,
        payload: Bytes::from_static(b"payload"),
    });
}

#[test]
fn test_large_payload_roundtrip() {
    let payload = Bytes::from(vec![0xABu8; 512_000]);
    assert_rt(Frame::FetchData {
        offset: 0,
        payload,
    });
}

#[test]
fn test_decode_too_short_returns_error() {
    let short = Bytes::from_static(b"\x01\x02");
    assert!(matches!(
        Frame::decode(short),
        Err(ProtocolError::TooShort)
    ));
}

#[test]
fn test_decode_unknown_opcode_returns_error() {
    // Craft a frame with opcode 0xAA (unknown)
    let mut buf = vec![0u8; 5];
    buf[0..4].copy_from_slice(&1u32.to_le_bytes()); // body_len = 1
    buf[4] = 0xAA; // unknown opcode
    assert!(matches!(
        Frame::decode(Bytes::from(buf)),
        Err(ProtocolError::UnknownOpcode(0xAA))
    ));
}

#[test]
fn test_encode_decode_preserves_u64_max_offset() {
    assert_rt(Frame::ProduceAck { offset: u64::MAX });
}
