use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use streamforge::log::{Log, LogConfig};
use tempfile::TempDir;

fn bench_append_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("log_append");

    for payload_size in [64usize, 512, 4096, 65536] {
        let payload = vec![0u8; payload_size];

        group.throughput(Throughput::Bytes(payload_size as u64));
        group.bench_with_input(
            BenchmarkId::new("payload_bytes", payload_size),
            &payload,
            |b, p| {
                let dir = TempDir::new().unwrap();
                let config = LogConfig {
                    max_segment_bytes: 512 * 1024 * 1024,
                    index_interval: 64,
                };
                let mut log = Log::open(dir.path(), config).unwrap();
                b.iter(|| {
                    log.append(p).unwrap();
                });
            },
        );
    }
    group.finish();
}

fn bench_read_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("log_read");

    for payload_size in [64usize, 512, 4096] {
        let payload = vec![0xAAu8; payload_size];

        group.throughput(Throughput::Bytes(payload_size as u64));
        group.bench_with_input(
            BenchmarkId::new("payload_bytes", payload_size),
            &payload,
            |b, p| {
                let dir = TempDir::new().unwrap();
                let config = LogConfig {
                    max_segment_bytes: 512 * 1024 * 1024,
                    index_interval: 64,
                };
                let mut log = Log::open(dir.path(), config).unwrap();

                // Pre-fill 1 000 records.
                for _ in 0..1_000 {
                    log.append(p).unwrap();
                }

                let mut offset = 0u64;
                b.iter(|| {
                    let _ = log.read(offset % 1_000).unwrap();
                    offset += 1;
                });
            },
        );
    }
    group.finish();
}

fn bench_sequential_scan(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    let config = LogConfig {
        max_segment_bytes: 512 * 1024 * 1024,
        index_interval: 128,
    };
    let mut log = Log::open(dir.path(), config).unwrap();

    let payload = vec![0u8; 256];
    for _ in 0..10_000 {
        log.append(&payload).unwrap();
    }

    c.bench_function("sequential_scan_10k", |b| {
        b.iter(|| {
            for i in 0u64..10_000 {
                let _ = log.read(i).unwrap();
            }
        });
    });
}

criterion_group!(
    benches,
    bench_append_throughput,
    bench_read_throughput,
    bench_sequential_scan
);
criterion_main!(benches);
