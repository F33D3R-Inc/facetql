use std::collections::{BTreeMap, HashMap, HashSet};

use crate::core::node::Node;

/// The engine's in-memory indexes.
///
/// Two different jobs live here:
///
/// * `addresses` maps an address to its byte offset in `facetql.data`.
///   Reads don't need it yet — the engine keeps every node in memory —
///   but it is what a future out-of-core read path seeks with.
///
/// * `by_kind` / `by_owner` are the **secondary indexes** the query path
///   actually uses. Without them every `GET /nodes?kind=Post` and every
///   `POST /nodes/query` walks the entire node map, so the cost of
///   reading one user's posts grows with the size of the whole database
///   — the behaviour that stops a graph like this from scaling past a
///   toy dataset. With them, a `kind`- or `owner`-filtered read visits
///   only the rows that could match.
///
/// The index is a derived structure, so it is only as good as its
/// discipline: it must be updated by *every* path that makes a node live
/// or removes one. That is why `StorageEngine::nodes` is private and all
/// of them funnel through `put_node`/`remove_node` — an index that can be
/// bypassed is worse than no index, because the query silently returns
/// the wrong answer instead of being slow.
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

    pub fn insert(&mut self, address: String, position: u64) {
        self.addresses.insert(address, position);
    }

    /// Not yet used — reads go through StorageEngine's in-memory
    /// HashMap, which load() rebuilds from disk at boot. This becomes
    /// useful once the dataset is too large to keep fully in memory
    /// and reads need to go straight to disk by offset.
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
