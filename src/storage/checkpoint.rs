use std::fs;
use std::io::{self, Write};

use crate::config;

/// Tracks the highest WAL sequence number that is already durably
/// reflected in the physical storage files (facetql.data,
/// facetql.edges, facetql.users, facetql.history, facetql.tombstones).
///
/// Why this exists:
///
/// Every mutation writes its WAL record first, then writes the matching
/// physical record. `StorageEngine::load()` reconstructs state directly
/// from the physical files, so by the time it runs, every WAL record at
/// or below the checkpoint has already been applied through the normal
/// physical-storage path.
///
/// Without this checkpoint, `recovery::recover()` would replay the
/// *entire* WAL on every startup — including operations already present
/// in physical storage. `Insert`/`Delete`/user operations happen to be
/// idempotent (replaying them just overwrites/removes the same key), but
/// `Archive` and `InsertEdge` are not: replaying them again duplicates
/// history entries and edge adjacency lists on every single restart.
///
/// The checkpoint is advanced only after a physical write has completed,
/// so it always trails (or matches) what's actually durable on disk. If
/// the process crashes between a WAL append and the matching physical
/// write, the checkpoint stays behind, and recovery correctly replays
/// that operation from the WAL.
pub fn read() -> io::Result<u64> {
    let path = config::data_file("facetql.checkpoint");

    if !path.exists() {
        return Ok(0);
    }

    let raw = fs::read_to_string(&path)?;

    let trimmed = raw.trim();

    if trimmed.is_empty() {
        return Ok(0);
    }

    trimmed.parse::<u64>().map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("corrupt checkpoint file: {e}"),
        )
    })
}

/// Advance the checkpoint to `sequence`, if it's newer than what's
/// already recorded.
///
/// Writes are whole-file overwrites (the value is a single small
/// integer) followed by an explicit `sync_data()`, so a checkpoint
/// update is itself crash-safe: readers either see the old value or the
/// new one, never a torn write.
pub fn advance(sequence: u64) -> io::Result<()> {
    let current = read()?;

    if sequence <= current {
        return Ok(());
    }

    let path = config::data_file("facetql.checkpoint");
    let tmp_path = config::data_file("facetql.checkpoint.tmp");

    {
        let mut file = fs::File::create(&tmp_path)?;
        file.write_all(sequence.to_string().as_bytes())?;
        file.sync_data()?;
    }

    fs::rename(&tmp_path, &path)?;

    Ok(())
}
