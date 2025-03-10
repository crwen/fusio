use common::write_parquet;
use criterion::{criterion_group, criterion_main};
use monoio::RuntimeBuilder;
use tempfile::tempdir;

mod common;

fn multi_write() {
    let tmp_dir = tempdir().unwrap();
    let path = fusio::path::Path::from_filesystem_path(tmp_dir.path())
        .unwrap()
        .child("monoio");

    c.bench_with_input(
        BenchmarkId::new("parquet_tokio", path.clone()),
        &path,
        |b, path| {
            b.iter(|| {
                RuntimeBuilder::<monoio::IoUringDriver>::new()
                    .with_entries(32768)
                    .build()
                    .unwrap()
                    .block_on(write_parquet(path.clone()))
            });
        },
    );
}

fn multi_write(c: &mut Criterion) {
    let tmp_dir = tempdir().unwrap();
}

criterion_group!(benches, multi_write);
criterion_main!(benches);
