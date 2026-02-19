# gossipgrid-wal

Write-Ahead Log (WAL) implementation for GossipGrid.

This crate provides a structured WAL for recording state changes (Puts and Deletes) in a distributed database. It supports multiple partitions, LSN (Log Sequence Number) tracking, and asynchronous IO with `tokio`.

## Features

- **Partitioned Logs**: Each partition has its own WAL file for better concurrency and recovery.
- **Asynchronous IO**: Built on `tokio` for high-performance non-blocking disk operations.
- **LSN Tracking**: Every record is assigned an LSN for synchronization.
- **Watchers**: Subscribe to LSN updates for real-time replication.

## Usage

```rust
use pwal::wal::WalLocalFile;
use pwal::{WalWriter, WalPartitionId, WalCommon};
use serde::{Serialize, Deserialize};
use bincode::{Encode, Decode};
use std::path::PathBuf;

#[derive(Serialize, Deserialize, Encode, Decode, Clone, Debug, PartialEq)]
pub enum MyRecord {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize WAL with base directory, truncate=false, and max segment size (e.g., 1MB)
    let wal = WalLocalFile::new(PathBuf::from("./wal_data"), false, 1024 * 1024).await?;
    
    let record = MyRecord::Put {
        key: b"my-key".to_vec(),
        value: b"my-value".to_vec(),
    };
    
    let partition = WalPartitionId(1);
    let (lsn, pos) = wal.append(partition, record).await?;
    println!("Appended record with LSN {} at segment {} offset {}", 
        lsn, pos.segment_index, pos.local_offset);
    wal.io_sync().await?;
    Ok(())
}
```

## Benchmarks

The following benchmarks were performed on a **MacBook Pro** with an **Apple M4** CPU.

### System Specifications
- **CPU**: Apple M4 (10 cores)
- **RAM**: 16 GB
- **OS**: macOS

### Results
| Benchmark | Average Time |
|-----------|--------------|
| `append_record` | 6.8 µs |
| `append_1000_records` | 6.79 ms |
| `stream_1000_linear` | 75.75 µs |

Run benchmarks yourself with:
```bash
cargo bench --bench wal_bench
```

## License

MIT OR Apache-2.0
