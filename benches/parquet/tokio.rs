use common::write_parquet;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use tempfile::tempdir;
use tokio::runtime::Builder;

mod common;

fn multi_write(c: &mut Criterion) {
    let tmp_dir = tempdir().unwrap();

    let path = fusio::path::Path::from_filesystem_path(tmp_dir.path())
        .unwrap()
        .child("tokio");

    c.bench_with_input(
        BenchmarkId::new("parquet_tokio", path.clone()),
        &path,
        |b, path| {
            b.iter(|| {
                Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(write_parquet(path.clone()))
            });
        },
    );
}

criterion_group!(benches, multi_write);
criterion_main!(benches);
