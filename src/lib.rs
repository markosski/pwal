//! A file-backed, segmented Write-Ahead Log (WAL).
//!
//! `WalLocalFile` stores records on disk as a sequence of numbered segment files,
//! one set per partition. Each record is assigned a monotonically increasing
//! **LSN** (Log Sequence Number) and can be read back via streaming iterators.
//!
//! # Features
//!
//! - **Partitioned** – every [`WalPartitionId`] gets its own independent log stream.
//! - **Segmented** – when a segment exceeds `max_segment_size` bytes a new file
//!   is created automatically (rotation).
//! - **Recoverable** – on restart the WAL discovers existing segments and resumes
//!   from the last persisted LSN.
//! - **LSN watchers** – callers can subscribe to real-time LSN updates via
//!   [`tokio::sync::watch`] channels (useful for replication).
//!
//! # Quick start
//!
//! ```rust,no_run
//! use pwal::wal::WalLocalFile;
//! use pwal::{WalWriter, WalReader, WalCommon, WalPartitionId};
//! use bincode::{Encode, Decode};
//! use serde::{Serialize, Deserialize};
//! use std::path::PathBuf;
//!
//! #[derive(Serialize, Deserialize, Encode, Decode, Clone, Debug)]
//! enum MyRecord {
//!     Put { key: Vec<u8>, value: Vec<u8> },
//!     Delete { key: Vec<u8> },
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // 1. Create (or reopen) the WAL.
//!     //    • `truncate = true`  → wipe any previous data (useful for tests).
//!     //    • `max_segment_size` → bytes before rotating to a new segment file.
//!     let wal = WalLocalFile::new(
//!         PathBuf::from("./wal_data"),
//!         false,          // truncate
//!         1024 * 1024,    // 1 MB segments
//!     ).await?;
//!
//!     let partition = WalPartitionId(0);
//!
//!     // 2. Append a record – returns the assigned LSN and its position.
//!     let record = MyRecord::Put {
//!         key: b"hello".to_vec(),
//!         value: b"world".to_vec(),
//!     };
//!     let (lsn, position) = wal.append(partition, record).await?;
//!
//!     // 3. Flush to disk - optional
//!     wal.io_sync().await?;
//!
//!     // 4. Stream records starting from LSN 0 (inclusive).
//!     let records: Vec<_> =
//!         WalReader::<MyRecord>::stream_from_lsn(&wal, 0, partition, false)
//!             .await
//!             .collect();
//!
//!     // 5. Watch for new LSNs if needed (e.g. for replication purposes).
//!     let mut watcher = wal.get_lsn_watcher(partition).await;
//!     // watcher.changed().await will resolve when the next record is appended.
//!
//!     // 6. Purge old segments whose max LSN < a threshold.
//!     let deleted = wal.purge_before(partition, lsn).await?;
//!     println!("purged {deleted} old segment(s)");
//!
//!     Ok(())
//! }
//! ```
//!
//! # Recovery
//!
//! To resume after a restart, simply call [`WalLocalFile::new`] with `truncate = false`
//! on the same directory. The WAL will scan existing segment files, recover the
//! last LSN, and continue appending from there.

#[cfg(test)]
mod tests;

mod reader;
mod segment;
mod types;
pub mod wal;
mod writer;

pub use types::*;
