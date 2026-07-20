use std::fs;
use crate::config;
use crate::crypto;

/// Boot-time status readout of the WAL — decrypts each hex-encoded
/// encrypted line (see wal.rs) before printing. This is informational
/// only, not real crash recovery: actual durability still comes from
/// every insert completing its disk write before the API call returns
/// (see StorageEngine::insert), same as every prior checkpoint.
pub fn recover() {
    let raw = fs::read_to_string(config::data_file("facetql.wal")).unwrap_or_default();
    for line in raw.lines() {
        match crypto::decode_hex(line).and_then(|bytes| crypto::decrypt(&bytes)) {
            Ok(plaintext) => {
                println!("Recovering {}", String::from_utf8_lossy(&plaintext));
            }
            Err(e) => {
                eprintln!("warning: could not decrypt a WAL line ({e}) — skipping. \
                           This is expected if this WAL predates encryption at rest.");
            }
        }
    }
}
