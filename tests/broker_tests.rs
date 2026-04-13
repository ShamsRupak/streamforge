use streamforge::{
    broker::{
        consumer_group::ConsumerGroupCoordinator,
        topic::TopicManager,
        BrokerError,
    },
    compression::{compress, compress_batch, decompress, decompress_batch},
    log::LogConfig,
};
use tempfile::TempDir;

fn test_config() -> LogConfig {
    LogConfig {
        max_segment_bytes: 4 * 1024 * 1024,
        index_interval: 4,
    }
}

// ── TopicManager tests ────────────────────────────────────────────────────────

#[test]
fn test_topic_manager_create_and_append() {
    let dir = TempDir::new().unwrap();
    let mut mgr = TopicManager::new(dir.path(), test_config());

    mgr.create_topic("events", 2).unwrap();
    let log = mgr.get_partition_mut("events", 0).unwrap();
    let offset = log.append(b"hello").unwrap();
    assert_eq!(offset, 0);
}

#[test]
fn test_topic_manager_multiple_partitions_independent() {
    let dir = TempDir::new().unwrap();
    let mut mgr = TopicManager::new(dir.path(), test_config());

    mgr.create_topic("multi", 3).unwrap();

    // Append to partition 0 and 2 separately.
    mgr.get_partition_mut("multi", 0).unwrap().append(b"p0-msg0").unwrap();
    mgr.get_partition_mut("multi", 2).unwrap().append(b"p2-msg0").unwrap();
    mgr.get_partition_mut("multi", 0).unwrap().append(b"p0-msg1").unwrap();

    assert_eq!(mgr.get_partition_mut("multi", 0).unwrap().next_offset(), 2);
    assert_eq!(mgr.get_partition_mut("multi", 1).unwrap().next_offset(), 0);
    assert_eq!(mgr.get_partition_mut("multi", 2).unwrap().next_offset(), 1);
}

#[test]
fn test_topic_manager_get_or_create_auto_creates_topic() {
    let dir = TempDir::new().unwrap();
    let mut mgr = TopicManager::new(dir.path(), test_config());

    // Topic "auto" does not exist yet.
    let log = mgr.get_or_create_partition_mut("auto", 0).unwrap();
    log.append(b"auto-created").unwrap();
    assert_eq!(mgr.num_partitions("auto"), Some(1));
}

#[test]
fn test_topic_manager_partition_out_of_range() {
    let dir = TempDir::new().unwrap();
    let mut mgr = TopicManager::new(dir.path(), test_config());

    mgr.create_topic("small", 1).unwrap();

    // Only partition 0 exists.
    assert!(matches!(
        mgr.get_partition_mut("small", 1),
        Err(BrokerError::PartitionOutOfRange(1, _, 1))
    ));
}

#[test]
fn test_topic_manager_unknown_topic_returns_not_found() {
    let dir = TempDir::new().unwrap();
    let mut mgr = TopicManager::new(dir.path(), test_config());

    assert!(matches!(
        mgr.get_partition_mut("ghost", 0),
        Err(BrokerError::TopicNotFound(_))
    ));
}

#[test]
fn test_topic_manager_duplicate_create_returns_error() {
    let dir = TempDir::new().unwrap();
    let mut mgr = TopicManager::new(dir.path(), test_config());

    mgr.create_topic("dup", 2).unwrap();
    assert!(matches!(
        mgr.create_topic("dup", 4),
        Err(BrokerError::TopicAlreadyExists(_, 2))
    ));
}

#[test]
fn test_topic_manager_topic_names() {
    let dir = TempDir::new().unwrap();
    let mut mgr = TopicManager::new(dir.path(), test_config());

    mgr.create_topic("alpha", 1).unwrap();
    mgr.create_topic("beta", 2).unwrap();

    let mut names = mgr.topic_names();
    names.sort();
    assert_eq!(names, vec!["alpha", "beta"]);
}

// ── ConsumerGroupCoordinator tests ────────────────────────────────────────────

#[test]
fn test_coordinator_commit_and_fetch() {
    let mut coord = ConsumerGroupCoordinator::new();
    coord.commit("g1", "events", 0, 42);
    assert_eq!(coord.fetch_offset("g1", "events", 0), Some(42));
}

#[test]
fn test_coordinator_fetch_non_existent_returns_none() {
    let coord = ConsumerGroupCoordinator::new();
    assert_eq!(coord.fetch_offset("nobody", "nothing", 0), None);
}

#[test]
fn test_coordinator_groups_are_independent() {
    let mut coord = ConsumerGroupCoordinator::new();
    coord.commit("g1", "t", 0, 10);
    coord.commit("g2", "t", 0, 99);

    assert_eq!(coord.fetch_offset("g1", "t", 0), Some(10));
    assert_eq!(coord.fetch_offset("g2", "t", 0), Some(99));
}

#[test]
fn test_coordinator_partitions_tracked_independently() {
    let mut coord = ConsumerGroupCoordinator::new();
    coord.commit("g1", "t", 0, 5);
    coord.commit("g1", "t", 1, 7);
    coord.commit("g1", "t", 2, 3);

    assert_eq!(coord.fetch_offset("g1", "t", 0), Some(5));
    assert_eq!(coord.fetch_offset("g1", "t", 1), Some(7));
    assert_eq!(coord.fetch_offset("g1", "t", 2), Some(3));
}

#[test]
fn test_coordinator_overwrite_advances_offset() {
    let mut coord = ConsumerGroupCoordinator::new();
    coord.commit("g", "t", 0, 1);
    coord.commit("g", "t", 0, 50);
    assert_eq!(coord.fetch_offset("g", "t", 0), Some(50));
}

// ── Compression tests ─────────────────────────────────────────────────────────

#[test]
fn test_compress_decompress_roundtrip() {
    let data = b"the quick brown fox jumps over the lazy dog";
    let compressed = compress(data);
    let decompressed = decompress(&compressed).unwrap();
    assert_eq!(decompressed, data);
}

#[test]
fn test_compress_empty_payload() {
    let compressed = compress(b"");
    let decompressed = decompress(&compressed).unwrap();
    assert_eq!(decompressed, b"");
}

#[test]
fn test_compress_large_payload() {
    let data: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();
    let compressed = compress(&data);
    assert!(
        compressed.len() < data.len(),
        "compressed should be smaller than original"
    );
    let decompressed = decompress(&compressed).unwrap();
    assert_eq!(decompressed, data);
}

#[test]
fn test_decompress_bad_data_returns_error() {
    let garbage = b"\xDE\xAD\xBE\xEF\x00\x00\x00\x00";
    assert!(decompress(garbage).is_err());
}

#[test]
fn test_compress_batch_single_message() {
    let messages: &[&[u8]] = &[b"only message"];
    let compressed = compress_batch(messages);
    let decoded = decompress_batch(&compressed).unwrap();
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0], b"only message");
}

#[test]
fn test_compress_batch_multiple_messages() {
    let messages: &[&[u8]] = &[b"alpha", b"beta", b"gamma delta epsilon"];
    let compressed = compress_batch(messages);
    let decoded = decompress_batch(&compressed).unwrap();
    assert_eq!(decoded.len(), 3);
    assert_eq!(decoded[0], b"alpha");
    assert_eq!(decoded[1], b"beta");
    assert_eq!(decoded[2], b"gamma delta epsilon");
}

#[test]
fn test_compress_batch_empty_messages_in_batch() {
    let messages: &[&[u8]] = &[b"first", b"", b"last"];
    let compressed = compress_batch(messages);
    let decoded = decompress_batch(&compressed).unwrap();
    assert_eq!(decoded.len(), 3);
    assert_eq!(decoded[1], b"");
}

#[test]
fn test_decompress_batch_bad_data_returns_error() {
    assert!(decompress_batch(b"\xFF\xFF\xFF\xFF").is_err());
}
