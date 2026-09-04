//! The database catalog: the small durable file that says how to open
//! the physical storage.
//!
//! Without it, opening the database means discovering its shape by
//! reading it — which is exactly the full-scan startup this engine is
//! built to stop doing. The catalog answers the questions an open needs
//! answered *before* any data is touched: what format is this, how big
//! is a page, which heap segments exist, how long is each one, which one
//! is being appended to, and how much of each is dead weight compaction
//! should reclaim.
//!
//! It is deliberately tiny and deliberately not application data. It
//! holds no nodes, no edges, no keys — nothing whose size grows with the
//! database. That is what lets it be rewritten whole, atomically, on
//! every change: write a temp file, fsync it, rename it over the real
//! one, fsync the directory. A reader sees the old catalog or the new
//! one, never a half-written one, and the rename cannot be undone by a
//! crash.
//!
//! # Ordering rule
//!
//! The catalog must be durable *before* any index entry points into
//! storage the catalog describes. A committed index that names segment 7
//! while the catalog has never heard of segment 7 is an index pointing
//! at nothing. `RecordStore::sync` and `StorageEngine::checkpoint` hold
//! that order: heap pages, then catalog, then index metas, then the WAL
//! checkpoint.

use std::fs;
use std::io::{Error, ErrorKind, Result, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::config;
use crate::crypto;
use crate::storage::binary;
use crate::storage::page::PAGE_SIZE;

/// Physical format generation of the whole database.
///
/// Bumped when the on-disk shape of the heap, the indexes or this
/// catalog changes incompatibly. An older or newer generation is
/// refused at open rather than interpreted on a guess — the failure mode
/// of guessing is a database that reads as plausible nonsense.
pub const DATABASE_FORMAT_VERSION: u32 = 1;

/// One heap segment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentMeta {
    pub id: u32,
    /// Pages the segment durably contains. The file may be longer after
    /// a crash — those pages hold records no committed index references,
    /// and the next append overwrites them.
    pub pages: u32,
    /// Bytes belonging to records that have since been superseded or
    /// deleted. A compaction heuristic, so an approximate value is
    /// fine — it is never used to decide whether a record is live.
    pub obsolete_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogData {
    pub format_version: u32,
    pub page_size: u32,
    /// Next segment id to hand out. Monotonic: a retired segment's id is
    /// never reused, so a stale `RecordLocation` can only ever fail to
    /// resolve, never resolve to the wrong record.
    pub next_segment: u32,
    /// Segment currently being appended to.
    pub active_segment: u32,
    pub segments: Vec<SegmentMeta>,
}

pub struct Catalog {
    path: PathBuf,
    data: Mutex<CatalogData>,
}

impl Catalog {
    /// Open the catalog, creating a fresh one for a new database.
    pub fn open() -> Result<Catalog> {
        let path = config::data_file("facetql.catalog");

        let data = match fs::read(&path) {
            Ok(bytes) => {
                let data = decode(&bytes)?;

                if data.format_version != DATABASE_FORMAT_VERSION {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!(
                            "database format version {} is not supported by \
                             this build (expected {DATABASE_FORMAT_VERSION}). \
                             The physical layout changed; recreate the data \
                             directory.",
                            data.format_version
                        ),
                    ));
                }

                if data.page_size != PAGE_SIZE as u32 {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!(
                            "database was created with a {}-byte page size; \
                             this build uses {PAGE_SIZE}",
                            data.page_size
                        ),
                    ));
                }

                data
            }

            Err(e) if e.kind() == ErrorKind::NotFound => CatalogData {
                format_version: DATABASE_FORMAT_VERSION,
                page_size: PAGE_SIZE as u32,
                next_segment: 1,
                active_segment: 0,
                segments: vec![SegmentMeta {
                    id: 0,
                    pages: 0,
                    obsolete_bytes: 0,
                }],
            },

            Err(e) => return Err(e),
        };

        Ok(Catalog { path, data: Mutex::new(data) })
    }

    /// Read the catalog under its lock.
    pub fn with<R>(&self, read: impl FnOnce(&CatalogData) -> R) -> R {
        read(&self.lock())
    }

    /// Modify the catalog in memory. Nothing is durable until
    /// [`Catalog::save`].
    pub fn update<R>(&self, change: impl FnOnce(&mut CatalogData) -> R) -> R {
        change(&mut self.lock())
    }

    /// Write the catalog atomically.
    pub fn save(&self) -> Result<()> {
        static NEXT_TMP_ID: AtomicU64 = AtomicU64::new(0);

        let bytes = encode(&self.lock())?;

        let tmp = config::data_file(&format!(
            "facetql.catalog.{}.{}.tmp",
            std::process::id(),
            NEXT_TMP_ID.fetch_add(1, Ordering::Relaxed),
        ));

        {
            let mut file = fs::File::create(&tmp)?;
            file.write_all(&bytes)?;
            // sync_all, not sync_data: the rename below publishes this
            // inode, and an inode whose length has not reached the disk
            // is published as an empty catalog.
            file.sync_all()?;
        }

        fs::rename(&tmp, &self.path)?;

        sync_parent_dir(&self.path)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, CatalogData> {
        self.data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// The catalog is framed and encrypted exactly like a record: the same
/// magic, version byte, length prefix and CRC, so a truncated or
/// corrupted catalog is reported as such instead of bincode-decoding
/// into a plausible set of segments that do not exist.
fn encode(data: &CatalogData) -> Result<Vec<u8>> {
    let plain = bincode::serialize(data).map_err(|e| {
        Error::new(ErrorKind::InvalidData, format!("failed to encode catalog: {e}"))
    })?;

    binary::encode_frame(&crypto::encrypt(&plain))
}

fn decode(bytes: &[u8]) -> Result<CatalogData> {
    let payload = binary::decode_frame(bytes).map_err(|e| {
        Error::new(e.kind(), format!("catalog is unreadable: {e}"))
    })?;

    let plain = crypto::decrypt(payload).map_err(|e| {
        Error::new(
            ErrorKind::InvalidData,
            format!(
                "failed to decrypt the catalog: {e} — wrong \
                 ENOCHIAN_MASTER_KEY, or the file is damaged"
            ),
        )
    })?;

    bincode::deserialize(&plain).map_err(|e| {
        Error::new(ErrorKind::InvalidData, format!("failed to decode catalog: {e}"))
    })
}

#[cfg(unix)]
fn sync_parent_dir(path: &std::path::Path) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };

    fs::File::open(parent)?.sync_all()
}

#[cfg(not(unix))]
fn sync_parent_dir(_path: &std::path::Path) -> Result<()> {
    Ok(())
}
