use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};
use crate::config;
use crate::crypto;

const TOMBSTONES_PATH: &str = "facetql.tombstones";

/// Deletes in an append-only log can't remove bytes that are already
/// written — the v0.1 checkpoint had no delete route at all for this
/// reason (`can_write` existed but nothing called it). This adds the
/// standard append-only-storage answer: record the address as deleted
/// in its own log, and have `load()` filter tombstoned addresses out of
/// the rebuilt in-memory view. `facetql.data` itself is untouched, so
/// a deleted node's history is still recoverable by an operator who
/// needs to investigate — it's just no longer live.
///
/// Encrypted the same way as the WAL (see wal.rs) — a tombstoned
/// address is still a real address, worth protecting the same as
/// everything else at rest.
/// Durability boundary: like `binary::append_record`, this does not
/// return until the tombstone has been `sync_all`'d. The delete path
/// advances the WAL checkpoint immediately after this call, and the
/// checkpoint's whole meaning is "everything at or below this sequence
/// is already in physical storage". Without the fsync, a crash could
/// lose a buffered tombstone while the checkpoint claimed the delete was
/// durable — recovery would then filter that `Delete` record out of the
/// WAL replay and the deleted node would come back to life.
pub fn append_tombstone(address: &str) -> std::io::Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(config::data_file(TOMBSTONES_PATH))?;
    let encrypted = crypto::encrypt(address.as_bytes());
    writeln!(file, "{}", crypto::encode_hex(&encrypted))?;
    file.sync_all()
}

/// Reads every tombstoned address. Order doesn't matter here — a
/// tombstone is permanent in v0.1 (no "un-delete"), so a set is enough.
pub fn read_tombstones() -> std::io::Result<HashSet<String>> {
    let file = match std::fs::File::open(config::data_file(TOMBSTONES_PATH)) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(HashSet::new()),
        Err(e) => return Err(e),
    };

    let mut addresses = HashSet::new();
    for line in BufReader::new(file).lines().map_while(Result::ok) {
        if line.is_empty() {
            continue;
        }
        match crypto::decode_hex(&line).and_then(|bytes| crypto::decrypt(&bytes)) {
            Ok(plaintext) => {
                addresses.insert(String::from_utf8_lossy(&plaintext).to_string());
            }
            Err(e) => {
                eprintln!("warning: could not decrypt a tombstone line ({e}) — skipping.");
            }
        }
    }
    Ok(addresses)
}
