use serde::{Serialize, Deserialize};
use std::fs::OpenOptions;
use std::io::Write;
use crate::config;
use crate::crypto;
use crate::core::node::Node;
use crate::core::edge::Edge;
use crate::core::user::UserRecord;
use crate::core::history::HistoryEntry;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalEntry {
    Archive(HistoryEntry),
    Insert(Node),
    Delete(String),
    InsertEdge(Edge),
    InsertUser(UserRecord),
    RevokeUser(String),
}

pub fn log(entry: &WalEntry) {
    let bytes = bincode::serialize(entry).expect("failed to serialize WAL entry");

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(config::data_file("facetql.wal"))
        .expect("failed to open WAL file");

    let encrypted = crypto::encrypt(&bytes);
    writeln!(file, "{}", crypto::encode_hex(&encrypted)).expect("failed to write WAL entry");
}