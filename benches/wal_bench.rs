use bincode::{Decode, Encode};
use criterion::{Criterion, criterion_group, criterion_main};
use pwal::wal::WalLocalFile;
use pwal::{FramedWalRecord, WalError, WalPartitionId, WalPosition, WalReader, WalWriter};
use serde::{Deserialize, Serialize};
use tempfile::tempdir;

#[derive(Serialize, Deserialize, Encode, Decode, Clone, Debug, PartialEq)]
pub enum BenchRecord {
    Put { key: Vec<u8>, value: Vec<u8> },
}

fn bench_append(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let dir = tempdir().unwrap();
    let wal = rt
        .block_on(WalLocalFile::new(
            dir.path().to_path_buf(),
            true,
            100 * 1024 * 1024,
        ))
        .unwrap();
    let partition = WalPartitionId(0);

    let record = BenchRecord::Put {
        key: b"key".to_vec(),
        value: b"value".to_vec(),
    };

    c.bench_function("append_record", |b| {
        b.to_async(&rt).iter(|| async {
            wal.append(partition, record.clone()).await.unwrap();
        });
    });
}

fn bench_stream(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let dir = tempdir().unwrap();
    let wal = rt
        .block_on(WalLocalFile::new(
            dir.path().to_path_buf(),
            true,
            100 * 1024 * 1024,
        ))
        .unwrap();
    let partition = WalPartitionId(0);

    let value = r#"{"user": "123456", "name": "bob", "number": "123456789"}"#.to_string();

    let record = BenchRecord::Put {
        key: b"123456789".to_vec(),
        value: value.into_bytes(),
    };

    // Pre-fill WAL with 1000 records
    for _i in 0..1000 {
        let rec = record.clone();
        rt.block_on(wal.append(partition, rec)).unwrap();
    }

    #[allow(clippy::type_complexity)]
    c.bench_function("stream_1000_linear", |b| {
        b.to_async(&rt).iter(|| async {
            let iterator: Box<
                dyn Iterator<Item = Result<(FramedWalRecord<BenchRecord>, WalPosition), WalError>>,
            > = wal.stream_from_lsn(0, partition, false).await;
            let mut count = 0;
            for res in iterator {
                let _ = res.unwrap();
                count += 1;
            }
            assert_eq!(count, 1000);
        });
    });
}

fn bench_append_1000(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let dir = tempdir().unwrap();
    let wal = rt
        .block_on(WalLocalFile::new(
            dir.path().to_path_buf(),
            true,
            100 * 1024 * 1024,
        ))
        .unwrap();
    let partition = WalPartitionId(0);

    let value = r#"{"user": "123456", "name": "bob", "number": "123456789"}"#.to_string();
    let record = BenchRecord::Put {
        key: b"123456789".to_vec(),
        value: value.into_bytes(),
    };

    c.bench_function("append_1000_records", |b| {
        b.to_async(&rt).iter(|| async {
            for _i in 0..1000 {
                let rec = record.clone();
                wal.append(partition, rec).await.unwrap();
            }
        });
    });
}

criterion_group!(benches, bench_append, bench_append_1000, bench_stream);
criterion_main!(benches);
