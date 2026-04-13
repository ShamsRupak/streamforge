use proptest::prelude::*;
use streamforge::log::{Log, LogConfig, LogError};
use tempfile::TempDir;

fn small_config() -> LogConfig {
    LogConfig {
        max_segment_bytes: 512, // tiny segments so rotation tests are fast
        index_interval: 4,
    }
}

fn default_config() -> LogConfig {
    LogConfig {
        max_segment_bytes: 64 * 1024 * 1024,
        index_interval: 4,
    }
}

// ─── Basic append / read ─────────────────────────────────────────────────────

#[test]
fn test_append_single_record_and_read_back() {
    let dir = TempDir::new().unwrap();
    let mut log = Log::open(dir.path(), default_config()).unwrap();

    let offset = log.append(b"hello").unwrap();
    assert_eq!(offset, 0);

    let payload = log.read(0).unwrap();
    assert_eq!(payload, b"hello");
}

#[test]
fn test_append_multiple_records_read_in_order() {
    let dir = TempDir::new().unwrap();
    let mut log = Log::open(dir.path(), default_config()).unwrap();

    let messages: Vec<&[u8]> = vec![b"alpha", b"beta", b"gamma", b"delta"];
    let mut offsets = Vec::new();
    for msg in &messages {
        offsets.push(log.append(msg).unwrap());
    }

    assert_eq!(offsets, vec![0, 1, 2, 3]);
    for (i, msg) in messages.iter().enumerate() {
        let payload = log.read(i as u64).unwrap();
        assert_eq!(&payload, msg);
    }
}

#[test]
fn test_sequential_offsets_are_monotonically_increasing() {
    let dir = TempDir::new().unwrap();
    let mut log = Log::open(dir.path(), default_config()).unwrap();

    let mut prev = u64::MAX;
    for i in 0u32..20 {
        let offset = log.append(format!("record-{}", i).as_bytes()).unwrap();
        if prev != u64::MAX {
            assert_eq!(offset, prev + 1, "gap in offsets at record {}", i);
        }
        prev = offset;
    }
}

// ─── Empty log ────────────────────────────────────────────────────────────────

#[test]
fn test_empty_log_returns_error_on_read() {
    let dir = TempDir::new().unwrap();
    let mut log = Log::open(dir.path(), default_config()).unwrap();

    let result = log.read(0);
    assert!(
        matches!(result, Err(LogError::OffsetOutOfRange(0))),
        "expected OffsetOutOfRange, got {:?}",
        result
    );
}

#[test]
fn test_read_out_of_range_after_append() {
    let dir = TempDir::new().unwrap();
    let mut log = Log::open(dir.path(), default_config()).unwrap();

    log.append(b"only record").unwrap();
    let result = log.read(1); // offset 1 does not exist yet
    assert!(matches!(result, Err(LogError::OffsetOutOfRange(1))));
}

// ─── Segment rotation ─────────────────────────────────────────────────────────

#[test]
fn test_segment_rotation_when_max_bytes_exceeded() {
    let dir = TempDir::new().unwrap();
    let mut log = Log::open(dir.path(), small_config()).unwrap();

    // Each record is 8 (header) + payload bytes. With max_segment_bytes=512
    // and 50-byte payloads (58 bytes/record), we will rotate after ~8 records.
    let payload = vec![b'x'; 50];
    let mut offsets = Vec::new();
    for _ in 0..20 {
        offsets.push(log.append(&payload).unwrap());
    }

    // All records must be readable regardless of which segment they landed in.
    for (i, &offset) in offsets.iter().enumerate() {
        let data = log.read(offset).unwrap();
        assert_eq!(data, payload, "mismatch at index {}", i);
    }

    // There must be more than one .log file in the directory.
    let seg_files: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map(|x| x == "log").unwrap_or(false))
        .collect();
    assert!(
        seg_files.len() > 1,
        "expected multiple segments, found {}",
        seg_files.len()
    );
}

#[test]
fn test_offsets_span_segment_boundary() {
    let dir = TempDir::new().unwrap();
    let mut log = Log::open(dir.path(), small_config()).unwrap();

    let payload = vec![b'Z'; 60]; // forces rotation quickly
    for _ in 0..15 {
        log.append(&payload).unwrap();
    }
    // Read across segment boundary
    for i in 0..15u64 {
        let data = log.read(i).unwrap();
        assert_eq!(data, payload);
    }
}

// ─── CRC validation ───────────────────────────────────────────────────────────

#[test]
fn test_crc_mismatch_detected_on_corruption() {
    let dir = TempDir::new().unwrap();
    let mut log = Log::open(dir.path(), default_config()).unwrap();

    log.append(b"important data").unwrap();

    // Drop the log so the file handle is closed.
    drop(log);

    // Find the .log file and corrupt one byte in the payload area.
    let log_file = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .find(|e| e.path().extension().map(|x| x == "log").unwrap_or(false))
        .unwrap()
        .path();

    let mut data = std::fs::read(&log_file).unwrap();
    // Payload starts at byte 8; flip the first byte.
    if data.len() > 8 {
        data[8] ^= 0xFF;
    }
    std::fs::write(&log_file, &data).unwrap();

    let mut log2 = Log::open(dir.path(), default_config()).unwrap();
    // Recovery re-scans but doesn't validate CRC, so read must catch it.
    let result = log2.read(0);
    assert!(
        matches!(result, Err(LogError::CrcMismatch { .. })),
        "expected CrcMismatch, got {:?}",
        result
    );
}

// ─── Large payloads ───────────────────────────────────────────────────────────

#[test]
fn test_large_payload_roundtrip() {
    let dir = TempDir::new().unwrap();
    let mut log = Log::open(dir.path(), default_config()).unwrap();

    let payload: Vec<u8> = (0..1_000_000).map(|i| (i % 251) as u8).collect();
    let offset = log.append(&payload).unwrap();
    let back = log.read(offset).unwrap();
    assert_eq!(back, payload);
}

#[test]
fn test_empty_payload_roundtrip() {
    let dir = TempDir::new().unwrap();
    let mut log = Log::open(dir.path(), default_config()).unwrap();

    let offset = log.append(b"").unwrap();
    let back = log.read(offset).unwrap();
    assert_eq!(back, b"");
}

// ─── Index / sparse lookup ────────────────────────────────────────────────────

#[test]
fn test_index_sparse_lookup_finds_correct_record() {
    let dir = TempDir::new().unwrap();
    // index_interval=1 means every record is indexed → tests direct lookup
    let config = LogConfig {
        max_segment_bytes: 64 * 1024 * 1024,
        index_interval: 1,
    };
    let mut log = Log::open(dir.path(), config).unwrap();

    for i in 0u32..32 {
        log.append(format!("record-{:04}", i).as_bytes()).unwrap();
    }
    // Read any record by offset using the dense index.
    for i in 0u64..32 {
        let data = log.read(i).unwrap();
        let expected = format!("record-{:04}", i);
        assert_eq!(String::from_utf8(data).unwrap(), expected);
    }
}

#[test]
fn test_index_sparse_lookup_with_large_interval() {
    let dir = TempDir::new().unwrap();
    let config = LogConfig {
        max_segment_bytes: 64 * 1024 * 1024,
        index_interval: 16, // only every 16th record is indexed
    };
    let mut log = Log::open(dir.path(), config).unwrap();

    for i in 0u32..64 {
        log.append(format!("msg-{:04}", i).as_bytes()).unwrap();
    }
    // Read a record that falls between index entries (requires linear scan).
    for i in [7u64, 15, 23, 31, 47, 55, 63] {
        let data = log.read(i).unwrap();
        let expected = format!("msg-{:04}", i);
        assert_eq!(String::from_utf8(data).unwrap(), expected);
    }
}

// ─── Persistence / reopen ─────────────────────────────────────────────────────

#[test]
fn test_log_survives_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let mut log = Log::open(dir.path(), default_config()).unwrap();
        for i in 0u32..10 {
            log.append(format!("persist-{}", i).as_bytes()).unwrap();
        }
    }
    // Re-open the same directory.
    let mut log2 = Log::open(dir.path(), default_config()).unwrap();
    for i in 0u64..10 {
        let data = log2.read(i).unwrap();
        let expected = format!("persist-{}", i);
        assert_eq!(String::from_utf8(data).unwrap(), expected);
    }
}

#[test]
fn test_next_offset_after_reopen_is_correct() {
    let dir = TempDir::new().unwrap();
    let expected_next = {
        let mut log = Log::open(dir.path(), default_config()).unwrap();
        for _ in 0..5 {
            log.append(b"x").unwrap();
        }
        log.next_offset()
    };
    let log2 = Log::open(dir.path(), default_config()).unwrap();
    assert_eq!(log2.next_offset(), expected_next);
}

// ─── proptest property tests ──────────────────────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Any sequence of appended payloads can be read back identically.
    #[test]
    fn prop_append_then_read_is_identity(
        payloads in proptest::collection::vec(
            proptest::collection::vec(0u8..=255, 0..512),
            1..32
        )
    ) {
        let dir = TempDir::new().unwrap();
        let config = LogConfig {
            max_segment_bytes: 1024,
            index_interval: 4,
        };
        let mut log = Log::open(dir.path(), config).unwrap();
        let mut offsets = Vec::new();
        for p in &payloads {
            offsets.push(log.append(p).unwrap());
        }
        for (i, p) in payloads.iter().enumerate() {
            let back = log.read(offsets[i]).unwrap();
            prop_assert_eq!(&back, p);
        }
    }

    /// Offsets are always sequential with no gaps.
    #[test]
    fn prop_offsets_are_sequential_with_no_gaps(
        payloads in proptest::collection::vec(
            proptest::collection::vec(0u8..=255, 1..128),
            1..40
        )
    ) {
        let dir = TempDir::new().unwrap();
        let config = LogConfig {
            max_segment_bytes: 512,
            index_interval: 8,
        };
        let mut log = Log::open(dir.path(), config).unwrap();
        let mut offsets = Vec::new();
        for p in &payloads {
            offsets.push(log.append(p).unwrap());
        }
        for (i, &off) in offsets.iter().enumerate() {
            prop_assert_eq!(off, i as u64, "gap at index {}", i);
        }
    }
}
