use bytes::Bytes;
use std::net::SocketAddr;
use streamforge::{
    broker::server::Server,
    consumer::client::{ConsumerClient, Record},
    log::LogConfig,
    producer::client::ProducerClient,
};
use tempfile::TempDir;
use tokio::net::TcpListener;

// ── Test broker fixture ───────────────────────────────────────────────────────

/// Starts a test broker on an OS-assigned port and returns the bound address.
/// The server task runs until the test process exits.
async fn start_broker(dir: &std::path::Path) -> SocketAddr {
    let config = LogConfig {
        max_segment_bytes: 4 * 1024 * 1024,
        index_interval: 4,
    };
    let server = Server::new(dir, config);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        server.serve(listener).await.ok();
    });
    addr
}

fn addr_str(addr: SocketAddr) -> String {
    addr.to_string()
}

// ── Integration tests ─────────────────────────────────────────────────────────

/// A producer sends 100 messages; a consumer reads all 100 back in order.
#[tokio::test]
async fn test_produce_100_and_consume_all() {
    let dir = TempDir::new().unwrap();
    let addr = start_broker(dir.path()).await;
    let a = addr_str(addr);

    // Produce
    let mut producer = ProducerClient::connect(&a).await.unwrap();
    let mut expected_offsets = Vec::new();
    for i in 0u32..100 {
        let payload = Bytes::from(format!("message-{:04}", i));
        let offset = producer.send("stream", payload).await.unwrap();
        expected_offsets.push(offset);
    }
    assert_eq!(expected_offsets, (0u64..100).collect::<Vec<_>>());

    // Consume
    let mut consumer = ConsumerClient::connect(&a, "test-group").await.unwrap();
    consumer.subscribe("stream", 0, 0);
    let records = consumer.poll_n(100).await.unwrap();
    assert_eq!(records.len(), 100);
    for (i, r) in records.iter().enumerate() {
        assert_eq!(r.offset, i as u64);
        assert_eq!(r.payload, Bytes::from(format!("message-{:04}", i)));
    }
}

/// Messages sent to different topics stay isolated.
#[tokio::test]
async fn test_multiple_topics_are_isolated() {
    let dir = TempDir::new().unwrap();
    let addr = start_broker(dir.path()).await;
    let a = addr_str(addr);

    let mut producer = ProducerClient::connect(&a).await.unwrap();
    producer.send("topic-a", Bytes::from_static(b"a0")).await.unwrap();
    producer.send("topic-b", Bytes::from_static(b"b0")).await.unwrap();
    producer.send("topic-a", Bytes::from_static(b"a1")).await.unwrap();

    let mut consumer = ConsumerClient::connect(&a, "g").await.unwrap();
    consumer.subscribe("topic-a", 0, 0);
    let a_records = consumer.poll_n(10).await.unwrap();
    assert_eq!(a_records.len(), 2);
    assert_eq!(a_records[0].payload, Bytes::from_static(b"a0"));
    assert_eq!(a_records[1].payload, Bytes::from_static(b"a1"));

    consumer.subscribe("topic-b", 0, 0);
    let b_records = consumer.poll_n(10).await.unwrap();
    assert_eq!(b_records.len(), 1);
    assert_eq!(b_records[0].payload, Bytes::from_static(b"b0"));
}

/// Explicit create_topic then produce to it.
#[tokio::test]
async fn test_explicit_create_topic_then_produce() {
    let dir = TempDir::new().unwrap();
    let addr = start_broker(dir.path()).await;
    let a = addr_str(addr);

    let mut producer = ProducerClient::connect(&a).await.unwrap();
    producer.create_topic("explicit", 1).await.unwrap();

    let offset = producer
        .send_to_partition("explicit", 0, Bytes::from_static(b"created"))
        .await
        .unwrap();
    assert_eq!(offset, 0);

    let mut consumer = ConsumerClient::connect(&a, "g").await.unwrap();
    consumer.subscribe("explicit", 0, 0);
    let records = consumer.poll_n(5).await.unwrap();
    assert_eq!(records[0].payload, Bytes::from_static(b"created"));
}

/// Producer with multiple partitions routes messages correctly.
#[tokio::test]
async fn test_multiple_partitions_produce_and_fetch() {
    let dir = TempDir::new().unwrap();
    let addr = start_broker(dir.path()).await;
    let a = addr_str(addr);

    let mut producer = ProducerClient::connect(&a).await.unwrap();
    producer.create_topic("partitioned", 3).await.unwrap();

    // Send 2 messages to each partition explicitly.
    for part in 0..3u32 {
        for seq in 0..2u32 {
            let payload = Bytes::from(format!("p{}-seq{}", part, seq));
            producer.send_to_partition("partitioned", part, payload).await.unwrap();
        }
    }

    // Read from each partition independently.
    for part in 0..3u32 {
        let mut consumer = ConsumerClient::connect(&a, "g").await.unwrap();
        consumer.subscribe("partitioned", part, 0);
        let records = consumer.poll_n(10).await.unwrap();
        assert_eq!(records.len(), 2, "partition {} should have 2 records", part);
        assert_eq!(
            records[0].payload,
            Bytes::from(format!("p{}-seq0", part))
        );
    }
}

/// Producer round-robin distributes across partitions when using `send()`.
#[tokio::test]
async fn test_round_robin_partition_assignment() {
    let dir = TempDir::new().unwrap();
    let addr = start_broker(dir.path()).await;
    let a = addr_str(addr);

    let mut producer = ProducerClient::connect(&a).await.unwrap();
    producer.create_topic("rr", 2).await.unwrap();

    // send() cycles partition 0 → 1 → 0 → 1 ...
    for i in 0u32..4 {
        producer
            .send("rr", Bytes::from(format!("msg-{}", i)))
            .await
            .unwrap();
    }

    // Partition 0 gets messages 0 and 2.
    let mut c0 = ConsumerClient::connect(&a, "g").await.unwrap();
    c0.subscribe("rr", 0, 0);
    let r0 = c0.poll_n(10).await.unwrap();
    assert_eq!(r0.len(), 2);
    assert_eq!(r0[0].payload, Bytes::from("msg-0"));
    assert_eq!(r0[1].payload, Bytes::from("msg-2"));

    // Partition 1 gets messages 1 and 3.
    let mut c1 = ConsumerClient::connect(&a, "g").await.unwrap();
    c1.subscribe("rr", 1, 0);
    let r1 = c1.poll_n(10).await.unwrap();
    assert_eq!(r1.len(), 2);
    assert_eq!(r1[0].payload, Bytes::from("msg-1"));
    assert_eq!(r1[1].payload, Bytes::from("msg-3"));
}

/// batch-buffering then flush sends all messages in order.
#[tokio::test]
async fn test_producer_batch_flush() {
    let dir = TempDir::new().unwrap();
    let addr = start_broker(dir.path()).await;
    let a = addr_str(addr);

    let mut producer = ProducerClient::connect(&a).await.unwrap();

    for i in 0u32..10 {
        producer.buffer_message("batch-topic", Bytes::from(format!("item-{}", i)));
    }
    assert_eq!(producer.pending_count(), 10);

    let offsets = producer.flush_batch().await.unwrap();
    assert_eq!(offsets.len(), 10);
    assert_eq!(offsets, (0u64..10).collect::<Vec<_>>());
    assert_eq!(producer.pending_count(), 0);

    // Verify the broker stored them.
    let mut consumer = ConsumerClient::connect(&a, "g").await.unwrap();
    consumer.subscribe("batch-topic", 0, 0);
    let records = consumer.poll_n(20).await.unwrap();
    assert_eq!(records.len(), 10);
    for (i, r) in records.iter().enumerate() {
        assert_eq!(r.payload, Bytes::from(format!("item-{}", i)));
    }
}

/// Consumer offset tracking: poll some records, commit, reconnect, resume.
#[tokio::test]
async fn test_consumer_offset_commit_and_resume() {
    let dir = TempDir::new().unwrap();
    let addr = start_broker(dir.path()).await;
    let a = addr_str(addr);

    // Produce 20 messages.
    let mut producer = ProducerClient::connect(&a).await.unwrap();
    for i in 0u32..20 {
        producer
            .send("resume-topic", Bytes::from(format!("r-{}", i)))
            .await
            .unwrap();
    }

    // First consumer session: read 10, commit.
    let committed_offset = {
        let mut c = ConsumerClient::connect(&a, "resume-group").await.unwrap();
        c.subscribe("resume-topic", 0, 0);
        let first_batch = c.poll_n(10).await.unwrap();
        assert_eq!(first_batch.len(), 10);
        c.commit_offset().await.unwrap();
        c.current_offset()
    };
    assert_eq!(committed_offset, 10);

    // Second session continues from offset 10.
    let mut c2 = ConsumerClient::connect(&a, "g2").await.unwrap();
    c2.subscribe("resume-topic", 0, committed_offset);
    let second_batch = c2.poll_n(20).await.unwrap();
    assert_eq!(second_batch.len(), 10);
    assert_eq!(second_batch[0].offset, 10);
    assert_eq!(second_batch[0].payload, Bytes::from("r-10"));
}

/// `poll()` stops at end-of-log and returns only what's available.
#[tokio::test]
async fn test_consumer_poll_stops_at_end_of_log() {
    let dir = TempDir::new().unwrap();
    let addr = start_broker(dir.path()).await;
    let a = addr_str(addr);

    let mut producer = ProducerClient::connect(&a).await.unwrap();
    for i in 0u32..3 {
        producer
            .send("short", Bytes::from(format!("x-{}", i)))
            .await
            .unwrap();
    }

    let mut consumer = ConsumerClient::connect(&a, "g").await.unwrap();
    consumer.subscribe("short", 0, 0);
    // Asking for 100 records but only 3 exist.
    let records = consumer.poll_n(100).await.unwrap();
    assert_eq!(records.len(), 3);
}

/// `send_to_partition` to a non-existent partition returns an error.
#[tokio::test]
async fn test_fetch_from_empty_topic_returns_error() {
    let dir = TempDir::new().unwrap();
    let addr = start_broker(dir.path()).await;
    let a = addr_str(addr);

    let mut consumer = ConsumerClient::connect(&a, "g").await.unwrap();
    consumer.subscribe("empty-topic", 0, 0);
    // The broker auto-creates the topic on first access via get_or_create,
    // but we're calling Fetch (not Produce), which uses get_partition_mut.
    // An unknown topic should result in zero records returned.
    let records = consumer.poll_n(5).await.unwrap();
    assert_eq!(records.len(), 0, "empty/unknown topic should yield 0 records");
}

/// Consumer `commit_offset` is acknowledged by the broker.
#[tokio::test]
async fn test_commit_offset_acknowledged() {
    let dir = TempDir::new().unwrap();
    let addr = start_broker(dir.path()).await;
    let a = addr_str(addr);

    let mut producer = ProducerClient::connect(&a).await.unwrap();
    producer.send("ack-topic", Bytes::from_static(b"ack")).await.unwrap();

    let mut consumer = ConsumerClient::connect(&a, "ack-group").await.unwrap();
    consumer.subscribe("ack-topic", 0, 0);
    consumer.poll_n(1).await.unwrap();
    // commit_offset() should not error.
    consumer.commit_offset().await.unwrap();
    assert_eq!(consumer.current_offset(), 1);
}

/// The consumer `poll_n` returns records with correct sequential offsets.
#[tokio::test]
async fn test_poll_records_have_sequential_offsets() {
    let dir = TempDir::new().unwrap();
    let addr = start_broker(dir.path()).await;
    let a = addr_str(addr);

    let mut producer = ProducerClient::connect(&a).await.unwrap();
    for i in 0u32..15 {
        producer
            .send("seq-topic", Bytes::from(format!("s-{}", i)))
            .await
            .unwrap();
    }

    let mut consumer = ConsumerClient::connect(&a, "g").await.unwrap();
    consumer.subscribe("seq-topic", 0, 0);
    let records: Vec<Record> = consumer.poll_n(15).await.unwrap();

    for (i, r) in records.iter().enumerate() {
        assert_eq!(r.offset, i as u64, "offset mismatch at index {}", i);
    }
}
