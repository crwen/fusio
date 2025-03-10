use std::sync::Arc;

use common::write_parquet;
use fusio::disk::MonoIoFs;
use monoio::RuntimeBuilder;
use tempfile::tempdir;

mod common;

fn main() {
    let tmp_dir = tempdir().unwrap();
    // let _ = std::fs::remove_dir_all("/tmp/tonbo/parquet");
    // let _ = std::fs::create_dir_all("/tmp/tonbo/parquet");
    RuntimeBuilder::<monoio::IoUringDriver>::new()
        .with_entries(32768)
        .build()
        .unwrap()
        .block_on(write_parquet(
            Arc::new(MonoIoFs),
            fusio::path::Path::from_filesystem_path(tmp_dir.path()).unwrap(),
        ))
}
