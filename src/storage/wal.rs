use std::fs::OpenOptions;
use std::io::Write;
use crate::config;
use crate::crypto;

/// Appends one operation to the write-ahead log before it's applied to
/// storage. Each line is encrypted (AES-256-GCM, see crypto.rs) and
/// hex-encoded so it's still a normal newline-delimited text file on
/// disk, just unreadable without the key — a WAL line like
/// "INSERT TxPerson" reveals a real address even without the node's
/// `data` payload, so this closes that leak the same way binary.rs
/// closes it for the main data files.
pub fn log(operation: String) {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(config::data_file("facetql.wal"))
        .expect("failed to open WAL file");

    let encrypted = crypto::encrypt(operation.as_bytes());
    writeln!(file, "{}", crypto::encode_hex(&encrypted)).expect("failed to write WAL entry");
}
