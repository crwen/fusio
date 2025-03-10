use std::sync::Arc;

use common::write_parquet;
use fusio::disk::TokioFs;
use tempfile::tempdir;
use tokio::runtime::Builder;

mod common;

fn main() {
    let tmp_dir = tempdir().unwrap();
    // let _ = std::fs::remove_dir_all("/tmp/tonbo/parquet");
    // let _ = std::fs::create_dir_all("/tmp/tonbo/parquet");
    Builder::new_multi_thread()
        .enable_all()
        .worker_threads(8)
        .build()
        .unwrap()
        .block_on(write_parquet(
            Arc::new(TokioFs),
            fusio::path::Path::from_filesystem_path(tmp_dir.path())
                .unwrap()
                .child("tokio"),
        ))
}
