use std::sync::Arc;

use arrow::{
    array::{ArrayRef, RecordBatch, StringArray, UInt64Array, UInt8Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use fusio::{fs::OpenOptions, path::Path, DynFs};
use fusio_parquet::writer::AsyncWriter;
use parquet::arrow::AsyncArrowWriter;
use rand::{distributions::Alphanumeric, thread_rng, Rng};

const RECORD_PER_BATCH: usize = 1000;
const ITERATION_TIMES: usize = 500_000;

pub(crate) async fn write_parquet(fs: Arc<dyn DynFs>, path: Path) {
    let options = OpenOptions::default().create(true).write(true);

    let writer = AsyncWriter::new(Box::new(fs.open_options(&path, options).await.unwrap()));

    let mut writer = AsyncArrowWriter::try_new(writer, schema(), None).unwrap();
    for _ in 0..ITERATION_TIMES {
        writer.write(&generate_record_batch()).await.unwrap();
    }
    writer.close().await.unwrap();
}

fn schema() -> SchemaRef {
    SchemaRef::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::UInt8, false),
    ]))
}

fn generate_record_batch() -> RecordBatch {
    let mut rng = thread_rng();
    let mut ids = Vec::with_capacity(RECORD_PER_BATCH);
    let mut names = Vec::with_capacity(RECORD_PER_BATCH);
    let mut ages = Vec::with_capacity(RECORD_PER_BATCH);
    for _ in 0..RECORD_PER_BATCH {
        ids.push(rng.gen::<u64>());
        ages.push(rng.gen::<u8>());
        let len: usize = rng.gen_range(0..=100);
        names.push(
            thread_rng()
                .sample_iter(&Alphanumeric)
                .take(len)
                .map(char::from)
                .collect::<String>(),
        );
    }

    RecordBatch::try_from_iter(vec![
        ("id", Arc::new(UInt64Array::from(ids)) as ArrayRef),
        ("name", Arc::new(StringArray::from(names)) as ArrayRef),
        ("age", Arc::new(UInt8Array::from(ages)) as ArrayRef),
    ])
    .unwrap()
}
