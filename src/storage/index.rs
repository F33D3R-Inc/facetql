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
//!   whose text contains this substring?       text
//! ```
//!
//! The last of those is not a sorted question, so it is not a sorted
//! structure — see [`crate::storage::text`]. It is opened, maintained,
//! checkpointed and recovered here beside the others because it is one
//! of the database's access paths, and an access path that lives
//! somewhere else is one a mutation can forget to maintain.
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

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::io::Result;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config;
use crate::storage::btree::BTree;
use crate::storage::text::{TextIndex, TextIndexDef};

/// The six built-in access paths plus every operator-declared one,
/// opened together.
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

    /// Operator-declared indexes over a `data` field, keyed by name.
    ///
    /// The six above are fixed because the questions they answer are
    /// fixed — an address, a kind, an owner, an edge, a version. A
    /// `data` field is not: `data` is opaque JSON whose shape the
    /// application chooses, so the only honest way to have an access
    /// path over it is to let the application declare which field
    /// deserves one. See [`DataIndex`].
    ///
    /// Definition and tree live in the same value on purpose: they are
    /// one thing. Keeping the catalog in a second map beside the trees
    /// is how an index ends up open with no definition (invisible, still
    /// maintained) or defined with no tree (planned for, absent at read
    /// time) — the exact drift that makes a derived structure lie.
    /// Behind a lock, and holding `Arc`s rather than values, because
    /// declaring or dropping an index is the only structural change a
    /// live database makes to its own set of access paths — and it now
    /// has to be possible while readers are walking those paths, without
    /// an exclusive borrow of the whole engine. Handing out an `Arc`
    /// rather than a reference is what keeps a guard from escaping into
    /// every caller's signature.
    data: RwLock<HashMap<String, Arc<DataIndex>>>,

    /// Operator-declared inverted indexes over a `data` field, keyed by
    /// name.
    ///
    /// A second map rather than a second flavour of value in `data`,
    /// because the two answer different questions with different key
    /// encodings and different maintenance: an ordered index writes one
    /// key per node, an inverted index writes one per trigram of the
    /// node's text. Merging them would mean every consumer of `data`
    /// asking "but which sort is this one" before it could use the tree.
    ///
    /// Names are still unique across both — `DELETE /admin/indexes/:name`
    /// names exactly one index — which [`crate::storage::engine`]
    /// enforces at declaration time.
    text: RwLock<HashMap<String, Arc<TextIndex>>>,
}

/// One operator-declared access path over a `data` field.
pub struct DataIndex {
    pub def: IndexDef,
    pub tree: BTree,
}

/// The declaration of a `data`-field index.
///
/// Small, bounded by the number of indexes rather than the amount of
/// data, and consulted on every write — so, like users, it is fully
/// resident and lives in its own append-only log rather than in the
/// heap.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexDef {
    /// Operator-chosen identity. Also the index's filename suffix,
    /// which is why [`IndexDef::validate`] restricts its alphabet.
    pub name: String,

    /// The node `kind` this index covers. An index is per-kind because
    /// `data` has no schema across kinds: `created_at` on a `Post` and
    /// `created_at` on a `Session` are unrelated fields that happen to
    /// share a name.
    pub kind: String,

    /// The top-level `data` field it orders by. Top-level to match
    /// `order` on `POST /nodes/query`, which is the read this exists to
    /// serve — an index on a path the query language cannot name would
    /// be an index no query could use.
    pub field: String,

    /// Refuse a write that would give two nodes of this kind the same
    /// value for this field.
    ///
    /// Declared on the index rather than as a separate object because it
    /// *is* the index: the check is a prefix scan of the entries already
    /// there, so a uniqueness rule with no access path behind it would
    /// be a full scan on every write. Postgres makes the same
    /// identification — a unique constraint is a unique index.
    ///
    /// `#[serde(default)]` so an index declared before this field
    /// existed replays from the log as non-unique, which is what it was.
    #[serde(default)]
    pub unique: bool,
}

/// Every index declaration one mutation has to be admissible against.
///
/// Carried as one value rather than two arguments because the two kinds
/// of index are one question — "what access paths will this write have
/// to maintain, and can it?" — and a signature that takes them
/// separately is one a third kind of index would have to widen again.
///
/// Snapshotted before the write path runs, because validating a batch
/// needs to know which indexes a node's keys will land in while the
/// apply pass needs the engine.
pub struct Declared {
    pub data: Vec<IndexDef>,
    pub text: Vec<TextIndexDef>,
}

impl Declared {
    /// The ordered indexes covering this kind.
    pub fn data_for(&self, kind: &str) -> impl Iterator<Item = &IndexDef> {
        self.data.iter().filter(move |def| def.kind == kind)
    }

    /// The inverted indexes covering this kind.
    pub fn text_for(&self, kind: &str) -> impl Iterator<Item = &TextIndexDef> {
        self.text.iter().filter(move |def| def.kind == kind)
    }
}

/// One declared index as an operator sees it, whichever kind it is.
///
/// The listing endpoint answers for both kinds, and an operator reading
/// it is asking one question — "is the field I care about covered, and
/// covered how?" — so they arrive as one table with a `mode` column
/// rather than two lists the reader has to merge. Serialize-only: the
/// durable shapes are [`IndexDef`] and [`TextIndexDef`], and this is the
/// view over them.
#[derive(Debug, Clone, Serialize)]
pub struct IndexInfo {
    pub name: String,
    pub kind: String,
    pub field: String,

    /// Always `false` for an inverted index — it stores windows of a
    /// value, so it has no whole value to be unique about.
    pub unique: bool,

    /// `"ordered"` or `"text"`. Additive on the wire: a client that
    /// predates it sees the three fields it always saw.
    pub mode: &'static str,
}

impl IndexInfo {
    pub fn ordered(def: &IndexDef) -> IndexInfo {
        IndexInfo {
            name: def.name.clone(),
            kind: def.kind.clone(),
            field: def.field.clone(),
            unique: def.unique,
            mode: "ordered",
        }
    }

    pub fn text(def: &TextIndexDef) -> IndexInfo {
        IndexInfo {
            name: def.name.clone(),
            kind: def.kind.clone(),
            field: def.field.clone(),
            unique: false,
            mode: "text",
        }
    }
}

/// One operation in `facetql.indexes`, the index-definition log.
///
/// Same shape and the same last-write-wins replay as
/// [`crate::storage::binary::UserOpRecord`], for the same reason: file
/// order in one log is the total order for the key, so an index can be
/// created, dropped, and created again with nothing to reconcile.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexOpRecord {
    Put(IndexDef),
    Drop(String),
}

/// Longest an index name may be.
///
/// The name becomes a filename (`facetql.idx.data.<name>`), so this is a
/// path-length bound as much as an identity one.
pub const MAX_INDEX_NAME_LEN: usize = 64;

/// Longest encoded `data` value an index entry may carry.
///
/// A `data` field is caller-supplied JSON of any size, and the index key
/// is that value followed by the address. `BTree::put` refuses a key
/// over [`MAX_KEY_LEN`], and the write path logs intent *before* it
/// applies — so an unbounded value would be a committed mutation that
/// cannot be applied, which recovery re-attempts and fails on forever.
/// Bounding the value here, ahead of the WAL, is what turns "this row
/// bricks the database" into "this row is rejected".
///
/// The bound is deliberately checked rather than worked around by
/// truncating the key to a prefix. A truncated key makes the index
/// non-covering: two values sharing a prefix become indistinguishable,
/// so an ordered read through it returns rows in an order the index
/// cannot actually justify. A refusal is recoverable — index a shorter
/// field, or don't index this one. A wrong order is not.
pub const MAX_INDEX_VALUE_LEN: usize = 512;

impl IndexDef {
    /// Reject a definition the storage layer could not honour, at the
    /// point where rejecting it is still just a failed request.
    pub fn validate(&self) -> std::result::Result<(), String> {
        if self.name.is_empty() || self.name.len() > MAX_INDEX_NAME_LEN {
            return Err(format!(
                "index name must be 1..={MAX_INDEX_NAME_LEN} bytes"
            ));
        }

        // The name is interpolated into a filename. Anything outside
        // this alphabet — a slash, a dot, a NUL — is a path, not a
        // name.
        if !self
            .name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            return Err(
                "index name may contain only letters, digits, '_' and '-'"
                    .to_string(),
            );
        }

        if self.kind.is_empty() {
            return Err("index kind must not be empty".to_string());
        }

        if self.field.is_empty() {
            return Err("index field must not be empty".to_string());
        }

        check_component("index kind", &self.kind)?;
        check_component("index field", &self.field)?;

        Ok(())
    }
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
            data: RwLock::new(HashMap::new()),
            text: RwLock::new(HashMap::new()),
        })
    }

    /// Open (or create) the tree backing one declared index.
    ///
    /// Idempotent by name: re-opening a definition that is already
    /// present replaces the definition and keeps the tree, which is what
    /// makes replaying a `CreateIndex` record harmless.
    pub fn open_data(&self, def: IndexDef) -> Result<()> {
        let mut data = self.data();

        let existing = data.remove(&def.name);

        // Re-opening an unchanged definition keeps the tree it already
        // has, so a replayed `CreateIndex` costs nothing and — more
        // importantly — does not swap the tree out from under a reader
        // holding an `Arc` to it.
        let index = match existing {
            Some(existing) if existing.def == def => existing,
            Some(_) | None => Arc::new(DataIndex {
                tree: BTree::open(&data_index_path(&def.name))?,
                def: def.clone(),
            }),
        };

        data.insert(def.name.clone(), index);

        Ok(())
    }

    /// Forget one declared index and remove its file.
    ///
    /// Dropping an index it does not have is not an error: recovery
    /// replays a drop against a database that already applied it.
    pub fn drop_data(&self, name: &str) -> Result<()> {
        // A reader mid-scan may still hold an `Arc` to this index. It
        // keeps working against the tree it already has — the file is
        // unlinked, not truncated, so the open descriptor stays valid
        // until the last holder drops it. The index is gone from the
        // catalog immediately, which is what "dropped" means.
        self.data().remove(name);

        match std::fs::remove_file(data_index_path(name)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Open (or create) the tree backing one declared inverted index.
    ///
    /// Idempotent by name, exactly like [`Self::open_data`] and for the
    /// same reason: replaying a `CreateTextIndex` record must not swap
    /// the tree out from under a reader holding an `Arc` to it.
    pub fn open_text(&self, def: TextIndexDef) -> Result<()> {
        let mut text = self.text();

        let existing = text.remove(&def.name);

        let index = match existing {
            Some(existing) if existing.def == def => existing,
            Some(_) | None => Arc::new(TextIndex {
                tree: BTree::open(&crate::storage::text::index_path(&def.name))?,
                def: def.clone(),
            }),
        };

        text.insert(def.name.clone(), index);

        Ok(())
    }

    /// Forget one declared inverted index and remove its file.
    ///
    /// Dropping one it does not have is not an error: recovery replays a
    /// drop against a database that already applied it.
    pub fn drop_text(&self, name: &str) -> Result<()> {
        self.text().remove(name);

        match std::fs::remove_file(crate::storage::text::index_path(name)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn text(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<String, Arc<TextIndex>>> {
        self.text.write().unwrap_or_else(|e| e.into_inner())
    }

    fn text_read(&self) -> std::sync::RwLockReadGuard<'_, HashMap<String, Arc<TextIndex>>> {
        self.text.read().unwrap_or_else(|e| e.into_inner())
    }

    pub fn text_get(&self, name: &str) -> Option<Arc<TextIndex>> {
        self.text_read().get(name).cloned()
    }

    /// Every declared inverted index, in name order so listings and
    /// plans are stable rather than following hash iteration order.
    pub fn text_all(&self) -> Vec<Arc<TextIndex>> {
        let mut all: Vec<Arc<TextIndex>> = self.text_read().values().cloned().collect();
        all.sort_by(|a, b| a.def.name.cmp(&b.def.name));
        all
    }

    /// The inverted indexes a node of this kind must maintain.
    pub fn text_for_kind(&self, kind: &str) -> Vec<Arc<TextIndex>> {
        self.text_read()
            .values()
            .filter(|i| i.def.kind == kind)
            .cloned()
            .collect()
    }

    /// The inverted index over exactly this `(kind, field)`, if one was
    /// declared.
    pub fn text_find(&self, kind: &str, field: &str) -> Option<Arc<TextIndex>> {
        self.text_read()
            .values()
            .find(|i| i.def.kind == kind && i.def.field == field)
            .cloned()
    }

    fn data(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<String, Arc<DataIndex>>> {
        self.data.write().unwrap_or_else(|e| e.into_inner())
    }

    fn data_read(&self) -> std::sync::RwLockReadGuard<'_, HashMap<String, Arc<DataIndex>>> {
        self.data.read().unwrap_or_else(|e| e.into_inner())
    }

    pub fn data_get(&self, name: &str) -> Option<Arc<DataIndex>> {
        self.data_read().get(name).cloned()
    }

    /// Every declared index, in name order so listings are stable.
    pub fn data_all(&self) -> Vec<Arc<DataIndex>> {
        let mut all: Vec<Arc<DataIndex>> = self.data_read().values().cloned().collect();
        all.sort_by(|a, b| a.def.name.cmp(&b.def.name));
        all
    }

    /// The indexes a node of this kind must maintain.
    pub fn data_for_kind(&self, kind: &str) -> Vec<Arc<DataIndex>> {
        self.data_read()
            .values()
            .filter(|i| i.def.kind == kind)
            .cloned()
            .collect()
    }

    /// The access path serving `order by <field>` on this kind, if one
    /// was declared. The planner's whole question.
    pub fn data_find(&self, kind: &str, field: &str) -> Option<Arc<DataIndex>> {
        self.data_read()
            .values()
            .find(|i| i.def.kind == kind && i.def.field == field)
            .cloned()
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
        self.history.commit()?;

        for index in self.data_read().values() {
            index.tree.commit()?;
        }

        for index in self.text_read().values() {
            index.tree.commit()?;
        }

        Ok(())
    }
}

fn index_path(name: &str) -> PathBuf {
    config::data_file(&format!("facetql.idx.{name}"))
}

fn data_index_path(name: &str) -> PathBuf {
    index_path(&format!("data.{name}"))
}

/// The index-definition log — see [`IndexOpRecord`].
pub fn definitions_path() -> PathBuf {
    config::data_file("facetql.indexes")
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

// ---------------------------------------------------------------------
// Key admissibility
// ---------------------------------------------------------------------
//
// A key that the B+tree refuses is not a slow write, it is an
// *unrecoverable* one — and that is the whole reason these checks
// exist rather than being left to `BTree::put`.
//
// The durable write path is: WAL record fsync'd, then heap, then
// indexes. By the time `put` sees a 2 KiB address the intent is already
// durable in the WAL, so the failure it returns is not a rejected
// request: it is a committed operation that cannot be applied. Recovery
// replays that record on the next start, hits the same refusal, and
// startup fails — permanently, from one HTTP request.
//
// So admissibility is decided *before* anything is logged. The bounds
// below are not invented constants: they are the actual encoded keys
// this module builds, measured against the actual limit
// `btree::MAX_KEY_LEN` enforces. Change the key encoding and these
// follow it automatically.

use crate::storage::btree::MAX_KEY_LEN;

/// Longest a single key component may be, imposed by [`component`]'s
/// `u16` length prefix.
///
/// Checked ahead of the composite-key bound purely so the cast in
/// `component` can never silently wrap a longer string down into a
/// short one — a wrapped length would produce a key that parses as a
/// *different* key, which is corruption rather than a rejected write.
/// In practice `MAX_KEY_LEN` is far smaller and fires first.
pub const MAX_COMPONENT_LEN: usize = u16::MAX as usize;

pub(crate) fn check_component(name: &str, value: &str) -> std::result::Result<(), String> {
    if value.len() > MAX_COMPONENT_LEN {
        return Err(format!(
            "{name} is {} bytes; the maximum is {MAX_COMPONENT_LEN}",
            value.len()
        ));
    }

    Ok(())
}

/// One already-built key, measured against the tree's own limit.
pub fn check_key_admissible(index: &str, key: &[u8]) -> std::result::Result<(), String> {
    if key.is_empty() {
        return Err(format!("the {index} index key would be empty"));
    }

    if key.len() > MAX_KEY_LEN {
        return Err(format!(
            "the {index} index key would be {} bytes; the maximum is \
             {MAX_KEY_LEN}",
            key.len()
        ));
    }

    Ok(())
}

/// Every durable key a node's identity produces, checked as a set.
///
/// Checking the composites rather than each field separately is what
/// keeps the limit honest: `address` and `kind` share one key, so
/// neither has a bound of its own — what has a bound is the key they
/// build together.
pub fn check_node_keys(
    address: &str,
    kind: &str,
    owner: &str,
) -> std::result::Result<(), String> {
    check_component("node address", address)?;
    check_component("node kind", kind)?;
    check_component("node owner", owner)?;

    check_key_admissible("primary", address.as_bytes())?;
    check_key_admissible("kind", &kind_key(kind, address))?;
    check_key_admissible("owner", &owner_key(owner, address))?;

    // The version is a fixed-width suffix, so any version produces a key
    // of the same length; `u64::MAX` stands in for all of them.
    check_key_admissible("history", &history_key(address, u64::MAX))?;

    Ok(())
}

/// The keys an archived state produces. A history entry carries a whole
/// node, and that node is handed back on a history read, so it is held
/// to the same admissibility as a live one.
pub fn check_history_keys(
    address: &str,
    node_address: &str,
    node_kind: &str,
    node_owner: &str,
) -> std::result::Result<(), String> {
    check_component("history address", address)?;
    check_key_admissible("history", &history_key(address, u64::MAX))?;

    check_node_keys(node_address, node_kind, node_owner)
}

/// Both edge index keys for one edge identity.
pub fn check_edge_keys(from: &str, to: &str, kind: &str) -> std::result::Result<(), String> {
    check_component("edge 'from'", from)?;
    check_component("edge 'to'", to)?;
    check_component("edge 'kind'", kind)?;

    check_key_admissible("outgoing-edge", &edge_out_key(from, kind, to))?;
    check_key_admissible("incoming-edge", &edge_in_key(to, kind, from))?;

    Ok(())
}

// ---------------------------------------------------------------------
// `data`-field ordering: one definition of "in order"
// ---------------------------------------------------------------------
//
// A sorted read and an index scan have to agree about what "sorted"
// means, or paging through an index returns rows a re-sort would have
// put somewhere else. So the byte encoding a key is built from and the
// comparator the in-memory sort uses are defined here, together, from
// the same type ranking — rather than in the two places that consume
// them.

/// Type rank: the outer ordering a `data` value sorts by.
///
/// JSON is untyped, so a field can hold a number in one node and a
/// string in the next. That has to produce *an* order, and it has to be
/// a total one — a comparator that calls two values of different types
/// equal is not merely vague, it is non-transitive (`5 == "a"` and
/// `"a" == 3` while `5 > 3`), and a non-transitive comparator makes
/// `sort_by` return an arbitrary permutation rather than a wrong-but-
/// stable one.
///
/// Absent sorts last so `order by` puts rows that have the field ahead
/// of rows that do not, which is the useful default and matches what
/// this engine did for the single-type case before the ranking existed.
fn type_rank(value: Option<&Value>) -> u8 {
    match value {
        Some(Value::Null) => 0x10,
        Some(Value::Bool(_)) => 0x20,
        Some(Value::Number(_)) => 0x30,
        Some(Value::String(_)) => 0x40,
        Some(Value::Array(_)) | Some(Value::Object(_)) => 0x50,
        None => 0xF0,
    }
}

/// Total order over `data` values, absent included.
///
/// The in-memory counterpart of [`encode_order_value`]: comparing two
/// values here and comparing their encodings byte-for-byte must give the
/// same answer, which is why both are driven by [`type_rank`].
pub fn compare_order_values(a: Option<&Value>, b: Option<&Value>) -> Ordering {
    let rank = type_rank(a).cmp(&type_rank(b));

    if rank != Ordering::Equal {
        return rank;
    }

    match (a, b) {
        (Some(Value::Bool(x)), Some(Value::Bool(y))) => x.cmp(y),

        (Some(Value::Number(x)), Some(Value::Number(y))) => {
            let x = x.as_f64().unwrap_or(0.0);
            let y = y.as_f64().unwrap_or(0.0);
            x.partial_cmp(&y).unwrap_or(Ordering::Equal)
        }

        (Some(Value::String(x)), Some(Value::String(y))) => x.cmp(y),

        // Composites order by their serialized form. Not a meaningful
        // ordering of structure, but a deterministic one — which is all
        // a tiebreak in a sort has to be.
        (Some(x @ (Value::Array(_) | Value::Object(_))), Some(y)) => {
            x.to_string().cmp(&y.to_string())
        }

        // Equal ranks with no payload to compare: both null, or both
        // absent.
        _ => Ordering::Equal,
    }
}

/// A `data` value encoded so that byte order is [`compare_order_values`]
/// order.
///
/// Numbers use the standard order-preserving `f64` transform: flip every
/// bit of a negative, flip only the sign bit of a non-negative. That
/// maps the IEEE-754 bit pattern onto an unsigned integer whose natural
/// order is numeric order, so `-3.5 < -1 < 0 < 2 < 10` holds as raw
/// big-endian bytes.
fn encode_value_body(value: Option<&Value>, out: &mut Vec<u8>) {
    out.push(type_rank(value));

    match value {
        Some(Value::Bool(b)) => out.push(u8::from(*b)),

        Some(Value::Number(n)) => {
            let f = n.as_f64().unwrap_or(0.0);
            let bits = f.to_bits();

            let ordered = if f.is_sign_negative() {
                !bits
            } else {
                bits ^ (1 << 63)
            };

            out.extend_from_slice(&ordered.to_be_bytes());
        }

        Some(Value::String(s)) => out.extend_from_slice(s.as_bytes()),

        Some(v @ (Value::Array(_) | Value::Object(_))) => {
            out.extend_from_slice(v.to_string().as_bytes())
        }

        Some(Value::Null) | None => {}
    }
}

/// Append `bytes` in a form that can be followed by more key material
/// without losing order.
///
/// `0x00` is escaped to `0x00 0xFF` and the value is terminated by
/// `0x00 0x00`. A length prefix — which is how [`component`] separates
/// the *unordered* components of the other indexes — cannot be used
/// here: it would sort every short value ahead of every long one, so
/// `"b"` would sort before `"aa"`. With this encoding the terminator is
/// the lowest thing that can appear at any position, so a value that is
/// a prefix of another sorts before it, exactly as string order
/// requires.
fn append_escaped(out: &mut Vec<u8>, bytes: &[u8]) {
    for &b in bytes {
        if b == 0x00 {
            out.push(0x00);
            out.push(0xFF);
        } else {
            out.push(b);
        }
    }

    out.push(0x00);
    out.push(0x00);
}

/// The escaped, order-preserving encoding of one `data` value —
/// everything in a data-index key before the address.
///
/// Also the exact prefix of every entry holding that value, which is
/// what makes "the rows where this field equals X" a range scan.
/// The key prefix shared by every entry whose string value begins with
/// `prefix`.
///
/// The same encoding as [`encode_order_value`] but stopping before the
/// terminator: a value's encoded form is `rank · escaped bytes ·
/// 0x00 0x00`, so the escaped bytes of a prefix are a byte prefix of
/// every longer value's key. That is what turns `starts_with` into a
/// range scan instead of a scan of the kind — typeahead over a handle is
/// the query this exists for.
pub fn encode_string_prefix(prefix: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(prefix.len() + 2);

    out.push(type_rank(Some(&Value::String(String::new()))));

    for &b in prefix.as_bytes() {
        if b == 0x00 {
            out.push(0x00);
            out.push(0xFF);
        } else {
            out.push(b);
        }
    }

    out
}

pub fn encode_order_value(value: Option<&Value>) -> Vec<u8> {
    let mut body = Vec::new();
    encode_value_body(value, &mut body);

    let mut out = Vec::with_capacity(body.len() + 2);
    append_escaped(&mut out, &body);

    out
}

/// `encoded(value) + address → ()`.
///
/// Membership only, like the `kind` and `owner` indexes: the record's
/// location comes from the primary index, so a node moving in the heap —
/// which every update does — costs nothing here.
pub fn data_key(value: Option<&Value>, address: &str) -> Vec<u8> {
    let mut key = encode_order_value(value);
    key.extend_from_slice(address.as_bytes());
    key
}

/// Recover the address from a data-index key by skipping past the
/// encoded value's terminator.
pub fn address_from_data_key(key: &[u8]) -> Option<&[u8]> {
    let mut i = 0;

    while i + 1 < key.len() {
        if key[i] == 0x00 {
            if key[i + 1] == 0x00 {
                return Some(&key[i + 2..]);
            }

            // 0x00 0xFF — an escaped zero inside the value.
            i += 2;
            continue;
        }

        i += 1;
    }

    None
}

/// The keys a node produces in every index declared over its kind.
///
/// Checked before the mutation is logged, for the same reason
/// [`check_node_keys`] is: an index key the tree would refuse is not a
/// rejected write once the WAL has it, it is a permanently unapplicable
/// one.
pub fn check_data_keys<'a>(
    defs: impl Iterator<Item = &'a IndexDef>,
    address: &str,
    data: &str,
) -> std::result::Result<(), String> {
    let decoded: Option<Value> = serde_json::from_str(data).ok();

    for def in defs {
        let value = decoded.as_ref().and_then(|d| d.get(&def.field));

        let encoded = encode_order_value(value);

        if encoded.len() > MAX_INDEX_VALUE_LEN {
            return Err(format!(
                "field '{}' is {} bytes encoded, over the \
                 {MAX_INDEX_VALUE_LEN}-byte maximum for index '{}'",
                def.field,
                encoded.len(),
                def.name
            ));
        }

        let mut key = encoded;
        key.extend_from_slice(address.as_bytes());

        check_key_admissible(&format!("'{}'", def.name), &key)?;
    }

    Ok(())
}
