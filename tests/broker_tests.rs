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

// ── ConsumerGroupCoordinator – membership & assignment tests ──────────────────

#[test]
fn test_register_consumer_appears_in_members() {
    let mut coord = ConsumerGroupCoordinator::new();
    coord.register_consumer("g", "consumer-0");
    coord.register_consumer("g", "consumer-1");
    let mut members = coord.group_members("g");
    members.sort();
    assert_eq!(members, vec!["consumer-0", "consumer-1"]);
}

#[test]
fn test_register_consumer_is_idempotent() {
    let mut coord = ConsumerGroupCoordinator::new();
    coord.register_consumer("g", "c0");
    coord.register_consumer("g", "c0"); // duplicate
    assert_eq!(coord.group_members("g").len(), 1);
}

#[test]
fn test_deregister_consumer_removes_member() {
    let mut coord = ConsumerGroupCoordinator::new();
    coord.register_consumer("g", "c0");
    coord.register_consumer("g", "c1");
    coord.deregister_consumer("g", "c0");
    assert_eq!(coord.group_members("g"), vec!["c1"]);
}

#[test]
fn test_assign_partitions_range_even_split() {
    // 4 partitions, 2 consumers → [0,1] and [2,3]
    let mut coord = ConsumerGroupCoordinator::new();
    coord.register_consumer("g", "c0");
    coord.register_consumer("g", "c1");
    let assignment = coord.assign_partitions("g", 4);
    let mut c0 = assignment["c0"].clone();
    let mut c1 = assignment["c1"].clone();
    c0.sort();
    c1.sort();
    assert_eq!(c0, vec![0, 1]);
    assert_eq!(c1, vec![2, 3]);
}

#[test]
fn test_assign_partitions_range_uneven_split() {
    // 7 partitions, 3 consumers → [0,1,2], [3,4], [5,6]
    let mut coord = ConsumerGroupCoordinator::new();
    coord.register_consumer("g", "c0");
    coord.register_consumer("g", "c1");
    coord.register_consumer("g", "c2");
    let assignment = coord.assign_partitions("g", 7);

    let mut c0 = assignment["c0"].clone();
    let mut c1 = assignment["c1"].clone();
    let mut c2 = assignment["c2"].clone();
    c0.sort();
    c1.sort();
    c2.sort();

    assert_eq!(c0, vec![0, 1, 2]);
    assert_eq!(c1, vec![3, 4]);
    assert_eq!(c2, vec![5, 6]);
}

#[test]
fn test_assign_partitions_single_consumer_gets_all() {
    let mut coord = ConsumerGroupCoordinator::new();
    coord.register_consumer("g", "solo");
    let assignment = coord.assign_partitions("g", 6);
    let mut partitions = assignment["solo"].clone();
    partitions.sort();
    assert_eq!(partitions, vec![0, 1, 2, 3, 4, 5]);
}

#[test]
fn test_assign_partitions_more_consumers_than_partitions() {
    // 2 partitions, 4 consumers → some consumers get 0 partitions
    let mut coord = ConsumerGroupCoordinator::new();
    for i in 0..4 {
        coord.register_consumer("g", &format!("c{}", i));
    }
    let assignment = coord.assign_partitions("g", 2);
    let total: usize = assignment.values().map(|v| v.len()).sum();
    assert_eq!(total, 2, "all partitions must be assigned");
    // The two that have partitions each have exactly 1.
    let non_empty: Vec<_> = assignment.values().filter(|v| !v.is_empty()).collect();
    assert_eq!(non_empty.len(), 2);
    for v in non_empty {
        assert_eq!(v.len(), 1);
    }
}

#[test]
fn test_assign_partitions_no_members_returns_empty() {
    let coord = ConsumerGroupCoordinator::new();
    let assignment = coord.assign_partitions("empty-group", 8);
    assert!(assignment.is_empty());
}

#[test]
fn test_assign_partitions_all_partitions_covered() {
    // Every partition 0..N must appear exactly once across all assignments.
    let mut coord = ConsumerGroupCoordinator::new();
    coord.register_consumer("g", "c0");
    coord.register_consumer("g", "c1");
    coord.register_consumer("g", "c2");

    for num_parts in [1u32, 3, 6, 7, 12] {
        let assignment = coord.assign_partitions("g", num_parts);
        let mut all: Vec<u32> = assignment.values().flatten().copied().collect();
        all.sort();
        let expected: Vec<u32> = (0..num_parts).collect();
        assert_eq!(all, expected, "failed for num_partitions={}", num_parts);
    }
}
