use std::fs;
use crate::config;
use crate::crypto;
use crate::storage::engine::StorageEngine;
use crate::storage::wal::WalEntry;

pub fn recover(engine: &mut StorageEngine) {
    let raw = fs::read_to_string(config::data_file("facetql.wal")).unwrap_or_default();
    let mut replayed = 0usize;

    for line in raw.lines() {
        let entry: WalEntry = match crypto::decode_hex(line).and_then(|bytes| crypto::decrypt(&bytes)) {
            Ok(plaintext) => match bincode::deserialize(&plaintext) {
                Ok(entry) => entry,
                Err(e) => {
                    eprintln!("warning: could not decode a WAL entry ({e}) — skipping.");
                    continue;
                }
            },
            Err(e) => {
                eprintln!("warning: could not decrypt a WAL line ({e}) — skipping.");
                continue;
            }
        };

        let already_applied = match &entry {
            WalEntry::Archive(_) => true,
            WalEntry::Insert(node) => engine.get(&node.address).is_some(),
            WalEntry::Delete(address) => engine.get(address).is_none(),
            WalEntry::InsertEdge(edge) => engine
                .edges_from(&edge.from)
                .iter()
                .any(|existing| existing.to == edge.to && existing.kind == edge.kind),
            WalEntry::InsertUser(record) => engine.find_user_by_hash(&record.token_hash).is_some(),
            WalEntry::RevokeUser(hash) => engine.find_user_by_hash(hash).is_none(),
        };

        if already_applied {
            continue;
        }

        replayed += 1;
        let result = match entry {
            WalEntry::Archive(_) => Ok(()),
            WalEntry::Insert(node) => engine.insert(node),
            WalEntry::Delete(address) => engine.delete(&address),
            WalEntry::InsertEdge(edge) => engine.insert_edge(edge),
            WalEntry::InsertUser(record) => engine.insert_user(record),
            WalEntry::RevokeUser(hash) => engine.revoke_user(&hash),
        };

        if let Err(e) = result {
            eprintln!("warning: failed to replay a WAL entry during recovery: {e}");
        }
    }

    if replayed > 0 {
        let plural = if replayed == 1 { "y" } else { "ies" };
        println!("Recovery: replayed {replayed} WAL entr{plural} not found in durable storage.");
    }
}