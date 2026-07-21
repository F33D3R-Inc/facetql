use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use serde::Serialize;
use serde::de::DeserializeOwned;
use crate::core::node::Node;
use crate::config;
use crate::crypto;

/// Appends any serializable record to `path`, encrypted at rest
/// (AES-256-GCM, see crypto.rs), as a length-prefixed blob, and returns
/// the byte offset it was written at. The length prefix covers the
/// encrypted blob (nonce + ciphertext + auth tag), not the plaintext —
/// callers on the read side don't need to know or care, they just get
/// the same bytes back through `crypto::decrypt`.
///
/// This is a breaking format change from every prior checkpoint's
/// `facetql.data`/`facetql.edges`/`facetql.users` — those files were
/// plaintext bincode; this reads and writes encrypted blobs. A
/// pre-encryption data file will fail to decrypt (loudly, not
/// silently) if loaded under this version.
pub fn append_record<T: Serialize>(path: &std::path::Path, record: &T) -> std::io::Result<u64> {
    let bytes = bincode::serialize(record).expect("failed to serialize record");
    let encrypted = crypto::encrypt(&bytes);
    let len = encrypted.len() as u32;

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;

    let offset = file.metadata()?.len();

    file.write_all(&len.to_le_bytes())?;
    file.write_all(&encrypted)?;

    Ok(offset)
}

/// Reads a single record back from a known offset in `path`.
/// Kept for future point-reads once a dataset outgrows an in-memory
/// index — reads today go through load()/read_all_records() at startup.
#[allow(dead_code)]
pub fn read_record_at<T: DeserializeOwned>(path: &std::path::Path, offset: u64) -> std::io::Result<T> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(offset))?;

    let mut len_buf = [0u8; 4];
    file.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;

    let mut data_buf = vec![0u8; len];
    file.read_exact(&mut data_buf)?;

    let decrypted = crypto::decrypt(&data_buf)
        .unwrap_or_else(|e| panic!("failed to decrypt record at offset {offset} in {}: {e}", path.display()));
    let record: T = bincode::deserialize(&decrypted)
        .expect("failed to deserialize decrypted record — file may be corrupted");
    Ok(record)
}

/// Sequentially replays `path` from the start, returning every
/// (offset, record) in file order. Because writes are append-only, a
/// later record for the same key is the newer value — callers rebuild
/// state by letting later entries overwrite earlier ones. Returns an
/// empty list if the file doesn't exist yet (first run).
pub fn read_all_records<T: DeserializeOwned>(path: &std::path::Path) -> std::io::Result<Vec<(u64, T)>> {
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };

    let file_len = file.metadata()?.len();
    let mut records = Vec::new();
    let mut offset: u64 = 0;

    while offset < file_len {
        file.seek(SeekFrom::Start(offset))?;

        let mut len_buf = [0u8; 4];
        file.read_exact(&mut len_buf)?;
        let record_len = u32::from_le_bytes(len_buf) as usize;

        let mut data_buf = vec![0u8; record_len];
        file.read_exact(&mut data_buf)?;

        let decrypted = crypto::decrypt(&data_buf).unwrap_or_else(|e| {
            panic!(
                "failed to decrypt record at offset {offset} in {}: {e} — wrong \
                 ENOCHIAN_MASTER_KEY, or this file predates encryption at rest",
                path.display()
            )
        });
        let record: T = bincode::deserialize(&decrypted)
            .expect("failed to deserialize decrypted record during recovery — file may be corrupted");

        records.push((offset, record));
        offset += 4 + record_len as u64;
    }

    Ok(records)
}

/// Node-specific convenience wrappers over the generic functions above,
/// kept so call sites that only deal with nodes (the common case) don't
/// need to name the storage path or turbofish the type at every call.
pub fn append_node(node: &Node) -> std::io::Result<u64> {
    append_record(&nodes_path(), node)
}

#[allow(dead_code)]
pub fn read_node_at(offset: u64) -> std::io::Result<Node> {
    read_record_at(&nodes_path(), offset)
}

pub fn read_all() -> std::io::Result<Vec<(u64, Node)>> {
    read_all_records(&nodes_path())
}

/// Full paths under the configured data directory (see `config.rs`) —
/// these replace what used to be hardcoded "facetql.data" /
/// "facetql.edges" literals in the repo root.
pub fn nodes_path() -> std::path::PathBuf {
    config::data_file("facetql.data")
}

pub fn edges_path() -> std::path::PathBuf {
    config::data_file("facetql.edges")
}

/// Persistent, admin-manageable user records — see core/user.rs.
/// Same append-only, offset-indexed format as nodes and edges;
/// StorageEngine::load() replays it the same way.
pub fn users_path() -> std::path::PathBuf {
    config::data_file("facetql.users")
}

/// Archived previous node states — see core/history.rs.
pub fn history_path() -> std::path::PathBuf {
    config::data_file("facetql.history")
}
