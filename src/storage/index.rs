//! The engine's durable access paths.
//!
//! Every index here is a real on-disk B+tree (`storage::btree`), not a
//! map that happens to be written out. That distinction is the whole
//! point of this module: an index that lives in RAM makes the size of
//! the database the size of the process, and an index that is rebuilt by
//! scanning every record at startup makes opening the database cost as
//! much as reading it.
//!
//! ```text
//!             logical question                    index
//!   ---------------------------------------   -------------
//!   where is the node at this address?        primary
//!   which nodes have this kind?               kind
//!   which nodes does this owner own?          owner
//!   what does this node point at?             edge_out
//!   what points at this node?                 edge_in
//!   what did this node used to be?            history
//! ```
//!
//! # Key encoding
//!
//! Composite keys are built from length-prefixed components
//! ([`component`]) rather than by joining with a separator byte. A
//! separator has to assume the values it separates cannot contain it,
//! and `kind`, `owner` and `address` are caller-supplied strings that
//! can contain anything at all — an address containing the separator
//! would land in another key's range and silently corrupt a scan. A
//! length prefix cannot be spoofed by content.
//!
//! Every composite key starts with the whole component that a scan
//! filters on, so "every node of this kind" is a prefix range and costs
//! only the entries it returns.
//!
//! # The discipline this depends on
//!
//! An index is a derived structure, so it is only as correct as the
//! paths that maintain it. These are authoritative — a query answers
//! from them without consulting the heap first — which means a mutation
//! that updates the record and forgets an index does not produce a slow
//! query, it produces a wrong answer. That is why every mutation goes
//! through `StorageEngine::apply_committed` and nothing else writes
//! records or index entries.

use std::io::Result;
use std::path::PathBuf;

use crate::config;
use crate::storage::btree::BTree;

/// The six durable access paths, opened together.
pub struct Indexes {
    /// `address → RecordLocation` of the node's current record. The one
    /// index a point read cannot be served without.
    pub primary: BTree,

    /// `kind + address → ()`. Membership only: the location comes from
    /// the primary index, so a node that moves (every update moves it)
    /// costs one write here instead of one per secondary index.
    pub kind: BTree,

    /// `owner + address → ()`.
    pub owner: BTree,

    /// `from + kind + to → RecordLocation` of the edge record.
    pub edge_out: BTree,

    /// `to + kind + from → RecordLocation` of the same edge, indexed for
    /// the reverse traversal.
    pub edge_in: BTree,

    /// `address + version → RecordLocation` of one archived state.
    /// Ordered by version, so "this node's history, oldest first" is a
    /// prefix scan and reading one node's history never touches
    /// another's.
    pub history: BTree,
}

impl Indexes {
    pub fn open() -> Result<Indexes> {
        Ok(Indexes {
            primary: BTree::open(&index_path("primary"))?,
            kind: BTree::open(&index_path("kind"))?,
            owner: BTree::open(&index_path("owner"))?,
            edge_out: BTree::open(&index_path("edge_out"))?,
            edge_in: BTree::open(&index_path("edge_in"))?,
            history: BTree::open(&index_path("history"))?,
        })
    }

    /// Publish every index's pending generation.
    ///
    /// Called from the engine's checkpoint, after the heap and catalog
    /// are durable and before the WAL checkpoint advances. A crash
    /// partway through leaves some indexes at the new generation and
    /// some at the old one — which is safe, and is why the WAL
    /// checkpoint moves last: recovery replays every operation above it
    /// against all six, and each apply is idempotent.
    pub fn commit(&self) -> Result<()> {
        self.primary.commit()?;
        self.kind.commit()?;
        self.owner.commit()?;
        self.edge_out.commit()?;
        self.edge_in.commit()?;
        self.history.commit()
    }
}

fn index_path(name: &str) -> PathBuf {
    config::data_file(&format!("facetql.idx.{name}"))
}

// ---------------------------------------------------------------------
// Key encoding
// ---------------------------------------------------------------------

/// One length-prefixed key component: `len(u16, big-endian) || bytes`.
///
/// Big-endian so the length itself sorts numerically, which keeps the
/// key space tidy; the property that actually matters is that a
/// component's bytes can never be confused with the start of the next
/// one.
pub fn component(value: &str) -> Vec<u8> {
    let bytes = value.as_bytes();

    let mut out = Vec::with_capacity(2 + bytes.len());
    out.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
    out.extend_from_slice(bytes);

    out
}

/// Split a key into its leading component and the rest.
pub fn split_component(key: &[u8]) -> Option<(&[u8], &[u8])> {
    if key.len() < 2 {
        return None;
    }

    let len = u16::from_be_bytes([key[0], key[1]]) as usize;

    if key.len() < 2 + len {
        return None;
    }

    Some((&key[2..2 + len], &key[2 + len..]))
}

/// `kind` index prefix: every node of that kind.
pub fn kind_prefix(kind: &str) -> Vec<u8> {
    component(kind)
}

pub fn kind_key(kind: &str, address: &str) -> Vec<u8> {
    let mut key = component(kind);
    key.extend_from_slice(address.as_bytes());
    key
}

/// `owner` index prefix: every node that owner owns.
pub fn owner_prefix(owner: &str) -> Vec<u8> {
    component(owner)
}

pub fn owner_key(owner: &str, address: &str) -> Vec<u8> {
    let mut key = component(owner);
    key.extend_from_slice(address.as_bytes());
    key
}

/// Outgoing-edge prefix: every edge leaving `from`.
pub fn edge_out_prefix(from: &str) -> Vec<u8> {
    component(from)
}

pub fn edge_out_key(from: &str, kind: &str, to: &str) -> Vec<u8> {
    let mut key = component(from);
    key.extend_from_slice(&component(kind));
    key.extend_from_slice(to.as_bytes());
    key
}

/// Incoming-edge prefix: every edge arriving at `to`.
pub fn edge_in_prefix(to: &str) -> Vec<u8> {
    component(to)
}

pub fn edge_in_key(to: &str, kind: &str, from: &str) -> Vec<u8> {
    let mut key = component(to);
    key.extend_from_slice(&component(kind));
    key.extend_from_slice(from.as_bytes());
    key
}

/// History prefix: every archived state of one node.
pub fn history_prefix(address: &str) -> Vec<u8> {
    component(address)
}

/// One archived state, keyed by the version that produced it.
///
/// The version is big-endian so byte order is numeric order, which makes
/// a prefix scan return a node's history oldest-first without sorting
/// anything.
pub fn history_key(address: &str, version: u64) -> Vec<u8> {
    let mut key = component(address);
    key.extend_from_slice(&version.to_be_bytes());
    key
}
