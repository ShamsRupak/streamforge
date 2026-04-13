use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use streamforge::{
    compression,
    log::{Log, LogConfig},
};
use tempfile::TempDir;

// ── Shared constants ──────────────────────────────────────────────────────────

const RECORD_SIZE: usize = 256;
const RECORD: [u8; RECORD_SIZE] = [0xAB; RECORD_SIZE];

fn bench_config() -> LogConfig {
    LogConfig {
        // Large enough that segments never rotate during a benchmark run.
        max_segment_bytes: 2 * 1024 * 1024 * 1024,
        index_interval: 64,
    }
}

// ── append_single ─────────────────────────────────────────────────────────────
//
// Measures the cost of a single 256-byte append (write + flush + CRC).

fn bench_append_single(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    let mut log = Log::open(dir.path(), bench_config()).unwrap();

    let mut group = c.benchmark_group("append_single");
    group.throughput(Throughput::Elements(1));

    group.bench_function("256b", |b| {
        b.iter(|| log.append(black_box(&RECORD)).unwrap())
    });

    group.finish();
}

// ── append_batch ──────────────────────────────────────────────────────────────
//
// Each Criterion iteration appends N records sequentially.
// Throughput is reported in records/s so you can divide by 10^6 for Mrec/s.

fn bench_append_batch_100(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    let mut log = Log::open(dir.path(), bench_config()).unwrap();

    let mut group = c.benchmark_group("append_batch_100");
    group.throughput(Throughput::Elements(100));

    group.bench_function("100x256b", |b| {
        b.iter(|| {
            for _ in 0..100 {
                log.append(black_box(&RECORD)).unwrap();
            }
        })
    });

    group.finish();
}

fn bench_append_batch_1000(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    let mut log = Log::open(dir.path(), bench_config()).unwrap();

    let mut group = c.benchmark_group("append_batch_1000");
    group.throughput(Throughput::Elements(1000));

    group.bench_function("1000x256b", |b| {
        b.iter(|| {
            for _ in 0..1_000 {
                log.append(black_box(&RECORD)).unwrap();
            }
        })
    });

    group.finish();
}

// ── read_sequential ───────────────────────────────────────────────────────────
//
// Pre-fill 1 000 records then measure sequential read of all of them.

fn bench_read_sequential(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    let mut log = Log::open(dir.path(), bench_config()).unwrap();
    for _ in 0..1_000 {
        log.append(&RECORD).unwrap();
    }

    let mut group = c.benchmark_group("read_sequential");
    group.throughput(Throughput::Elements(1_000));

    group.bench_function("1000_records", |b| {
        b.iter(|| {
            for i in 0u64..1_000 {
                let _ = black_box(log.read(i).unwrap());
            }
        })
    });

    group.finish();
}

// ── read_random ───────────────────────────────────────────────────────────────
//
// Pre-fill 10 000 records then measure 100 random-offset reads per iteration.
// Uses an inline XorShift64 so we don't need the `rand` crate.

fn bench_read_random(c: &mut Criterion) {
    const TOTAL: u64 = 10_000;
    const READS: usize = 100;

    let dir = TempDir::new().unwrap();
    let mut log = Log::open(dir.path(), bench_config()).unwrap();
    for _ in 0..TOTAL {
        log.append(&RECORD).unwrap();
    }

    // Pre-generate a deterministic shuffle of offsets — same every iteration.
    let offsets: Vec<u64> = {
        let mut s = 0xDEAD_BEEF_CAFE_BABEu64;
        (0..READS)
            .map(|_| {
                s = xorshift64(s);
                s % TOTAL
            })
            .collect()
    };

    let mut group = c.benchmark_group("read_random");
    group.throughput(Throughput::Elements(READS as u64));

    group.bench_function("100_random_from_10k", |b| {
        b.iter(|| {
            for &off in black_box(&offsets) {
                let _ = black_box(log.read(off).unwrap());
            }
        })
    });

    group.finish();
}

// ── compress_decompress ───────────────────────────────────────────────────────
//
// LZ4 roundtrip on a batch of 100 × 256-byte messages.

fn bench_compress_decompress(c: &mut Criterion) {
    const BATCH: usize = 100;
    const BATCH_BYTES: u64 = (BATCH * RECORD_SIZE) as u64;

    let messages: Vec<[u8; RECORD_SIZE]> = vec![RECORD; BATCH];
    let refs: Vec<&[u8]> = messages.iter().map(|m| m.as_slice()).collect();

    // Pre-compress once so decompress-only bench has something to work with.
    let compressed = compression::compress_batch(&refs);

    let mut group = c.benchmark_group("compress_decompress");

    // Full roundtrip: compress + decompress
    group.throughput(Throughput::Bytes(BATCH_BYTES));
    group.bench_function("lz4_roundtrip_100x256b", |b| {
        b.iter(|| {
            let c = compression::compress_batch(black_box(&refs));
            let _ = black_box(compression::decompress_batch(&c).unwrap());
        })
    });

    // Decompress only (lower bound on read path)
    group.bench_with_input(
        BenchmarkId::new("lz4_decompress_only", "100x256b"),
        &compressed,
        |b, blob| {
            b.iter(|| {
                let _ = black_box(compression::decompress_batch(black_box(blob)).unwrap());
            })
        },
    );

    group.finish();
}

// ── XorShift64 PRNG (no external dep) ────────────────────────────────────────

fn xorshift64(mut x: u64) -> u64 {
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    x
}

// ── Criterion wiring ──────────────────────────────────────────────────────────

criterion_group!(
    benches,
    bench_append_single,
    bench_append_batch_100,
    bench_append_batch_1000,
    bench_read_sequential,
    bench_read_random,
    bench_compress_decompress,
);
criterion_main!(benches);
