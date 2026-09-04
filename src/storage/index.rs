use std::collections::{BTreeMap, HashMap, HashSet};

use crate::core::node::Node;

/// The engine's in-memory indexes.
///
/// Two different jobs live here:
///
/// * `addresses` maps an address to the byte offset of that node's most
///   recently written record frame in `facetql.data`. Reads don't need it
///   yet — the engine keeps every node in memory — but it is what a
///   future out-of-core read path seeks with. The exact meaning of that
///   offset, and what is still missing before a read can be served from
///   it, is spelled out on [`Index::insert`] and [`Index::get`].
///
/// * `by_kind` / `by_owner` are the **secondary indexes** a filtered
///   query wants. Without them every `GET /nodes?kind=Post` and every
///   `POST /nodes/query` walks the entire node map, so the cost of
///   reading one user's posts grows with the size of the whole database
///   — the behaviour that stops a graph like this from scaling past a
///   toy dataset. With them, a `kind`- or `owner`-filtered read visits
///   only the rows that could match.
///
///   NOT YET WIRED UP: nothing calls [`Index::track`] /
///   [`Index::untrack`] today, so both maps are permanently empty and
///   the query path still scans `StorageEngine::nodes`. This matters to
///   whoever wires them up, because an empty index is not a slow index —
///   it is a *wrong* one: [`Index::by_kind`] returning `None` reads as
///   "no node has that kind", so a query served from it today would
///   answer "nothing found" for a database full of matches.
///
/// The index is a derived structure, so it is only as good as its
/// discipline: it must be updated by *every* path that makes a node live
/// or removes one. An index that can be bypassed is worse than no index,
/// because the query silently returns the wrong answer instead of being
/// slow. `StorageEngine::nodes` is currently `pub` and mutated directly
/// from several paths, which is precisely why the maps below cannot be
/// switched on where they stand: making them authoritative means first
/// funnelling every live-node mutation through a single pair of
/// insert/remove methods on the engine, so no path can update `nodes`
/// without updating these.
pub struct Index {
    pub addresses: HashMap<String, u64>,

    /// kind → the addresses of every live node of that kind.
    by_kind: HashMap<String, HashSet<String>>,

    /// owner → the addresses of every live node that owner owns.
    by_owner: HashMap<String, HashSet<String>>,
}

impl Index {
    pub fn new() -> Self {
        Self {
            addresses: HashMap::new(),
            by_kind: HashMap::new(),
            by_owner: HashMap::new(),
        }
    }

    /// Record where `address`'s current record lives in `facetql.data`.
    ///
    /// # What `position` means, exactly
    ///
    /// `position` is a **frame-start offset**: the byte offset of the
    /// first byte of the record frame's header (its `RECORD_MAGIC`), not
    /// of the payload inside it and not of the record after it. That is
    /// precisely the value `binary::append_record` returns — it captures
    /// the file length *before* writing the frame — and precisely what
    /// `binary::read_record_at` expects to seek to, since that function
    /// starts by parsing a 13-byte header at the offset it is given.
    /// Every call site that populates this map passes an offset straight
    /// through from `append_record`, except `StorageEngine::load()`,
    /// which passes the offsets `binary::read_all_records` reports —
    /// the same frame starts, observed on replay instead of on write. So
    /// the invariant holds by construction on every path, and it is the
    /// *only* offset convention
    /// that reads back: an offset pointing one byte into a frame is not a
    /// slightly-wrong seek, it is a magic-byte mismatch reported as
    /// corruption.
    ///
    /// # Overwrite semantics
    ///
    /// `facetql.data` is append-only, so updating a node writes a *new*
    /// frame and leaves the old one in place. Inserting here overwrites
    /// the previous offset, which is what makes this map mean "where the
    /// **current** value is" rather than "where this address was first
    /// seen". The superseded frames stay on disk — that is what makes a
    /// node's history recoverable by an operator — and are simply no
    /// longer reachable through this map.
    ///
    /// Ordering requirement: call this only *after* `append_record`
    /// returns, never before. `append_record` fsyncs before returning, so
    /// an offset recorded after it is an offset that is durable; an
    /// offset recorded before it could name bytes a crash then discards.
    pub fn insert(&mut self, address: String, position: u64) {
        self.addresses.insert(address, position);
    }

    /// The frame-start offset of `address`'s current record, if the
    /// engine has one.
    ///
    /// Not yet used: reads are served from `StorageEngine::nodes`, the
    /// in-memory map `load()` rebuilds from disk at boot. The reader this
    /// offset is meant for already exists — `binary::read_record_at`,
    /// which re-verifies the frame (magic, version, length, CRC) and
    /// decrypts before handing back a `Node`, so a point-read through
    /// this map is no less safe than a full replay.
    ///
    /// # What would have to be true to serve point-reads from here
    ///
    /// This is deliberately spelled out because the map *looks* ready and
    /// is not. Three things are missing, and none of them are in this
    /// module:
    ///
    /// 1. **Deletes must prune it.** The delete path removes the address
    ///    from `StorageEngine::nodes` and appends a tombstone, but leaves
    ///    the entry here — and `load()` likewise inserts an offset for
    ///    every node record it replays, then filters tombstoned addresses
    ///    out of `nodes` only. A point-read served from this map today
    ///    would therefore resurrect deleted nodes: the offset still names
    ///    a perfectly valid, checksum-clean frame on disk. Either this
    ///    map gets a `remove` that every delete path calls, or the read
    ///    path has to consult the tombstone set as well.
    ///
    /// 2. **It must survive the reads it would serve.** Entries are only
    ///    created by `insert` above; nothing rebuilds them lazily. That
    ///    is fine while `load()` walks the whole file at boot (it fills
    ///    the map as a side effect), but an out-of-core engine that
    ///    *doesn't* replay everything at startup needs this map itself to
    ///    be durable, which means a persistent index file with its own
    ///    crash semantics — not a `HashMap` rebuilt from a full scan.
    ///
    /// 3. **The offsets must be recoverable after a torn tail.** A crash
    ///    mid-append can leave a partial frame that `read_all_records`
    ///    drops; any offset recorded for it must never reach this map.
    ///    The ordering rule on [`Index::insert`] (record only after
    ///    `append_record` returns) is what guarantees that, and it has to
    ///    keep holding on every future write path.
    #[allow(dead_code)]
    pub fn get(&self, address: &str) -> Option<u64> {
        self.addresses.get(address).copied()
    }

    /// Record a node in the secondary indexes.
    pub fn track(&mut self, node: &Node) {
        self.by_kind
            .entry(node.kind.clone())
            .or_default()
            .insert(node.address.clone());

        self.by_owner
            .entry(node.owner.clone())
            .or_default()
            .insert(node.address.clone());
    }

    /// Remove a node from the secondary indexes.
    ///
    /// Emptied buckets are dropped rather than left behind: `owner` is
    /// unbounded (one per identity that ever wrote), so keeping empty
    /// sets would leak a slot per departed user forever.
    pub fn untrack(&mut self, node: &Node) {
        if let Some(addresses) = self.by_kind.get_mut(&node.kind) {
            addresses.remove(&node.address);
            if addresses.is_empty() {
                self.by_kind.remove(&node.kind);
            }
        }

        if let Some(addresses) = self.by_owner.get_mut(&node.owner) {
            addresses.remove(&node.address);
            if addresses.is_empty() {
                self.by_owner.remove(&node.owner);
            }
        }
    }

    /// Addresses of every live node of `kind`, or `None` when no node
    /// has that kind. `None` and an empty set mean the same thing to a
    /// caller; the distinction just avoids allocating one.
    pub fn by_kind(&self, kind: &str) -> Option<&HashSet<String>> {
        self.by_kind.get(kind)
    }

    /// Addresses of every live node owned by `owner`.
    pub fn by_owner(&self, owner: &str) -> Option<&HashSet<String>> {
        self.by_owner.get(owner)
    }

    /// Live node count per kind, sorted by kind.
    ///
    /// The index already groups nodes this way, so `GET /stats` reads it
    /// straight off rather than re-walking every node to count them.
    pub fn kind_counts(&self) -> BTreeMap<String, u64> {
        self.by_kind
            .iter()
            .map(|(kind, addresses)| (kind.clone(), addresses.len() as u64))
            .collect()
    }
}
