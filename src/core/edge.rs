use serde::{Serialize, Deserialize};

/// A directed, typed relationship between two nodes.
///
/// This is the piece the v0.1 checkpoint didn't have: nodes could exist
/// but nothing could connect them. A case-management style data model
/// (person -> goal -> step, org -> resource, resource -> review) is
/// fundamentally a graph of relationships between entities, not just a
/// bag of standalone nodes, so this is a foundational addition rather
/// than an optional one.
///
/// `kind` is a free-text relationship label (e.g. "HAS_GOAL",
/// "VERIFIED_BY", "REVIEWS") rather than a closed enum, since the set of
/// relationship types is a product decision that will keep growing —
/// the storage layer shouldn't need a code change every time a new
/// relationship type is introduced.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Edge {
    pub from: String,
    pub to: String,
    pub kind: String,
    pub owner: String,
}

impl Edge {
    pub fn new(from: String, to: String, kind: String, owner: String) -> Self {
        Self { from, to, kind, owner }
    }

    /// Same ownership model as Node::can_write for v0.1 — only the
    /// creator of the edge may remove it. Revisit once verification
    /// flows need a *different* party (e.g. the org that granted a
    /// capability) to be able to revoke it than the party who requested
    /// it — that's an ACL-list problem, same caveat as Node.
    ///
    /// Not yet called anywhere — there's no DELETE /edge route in this
    /// pass. Edge deletion needs its own tombstone identity scheme
    /// (edges have no single address the way nodes do — likely
    /// (from, to, kind)) and is deliberately left for the next slice
    /// rather than rushed in alongside node delete. Wire this in then;
    /// don't skip it.
    #[allow(dead_code)]
    pub fn can_write(&self, requester: &str) -> bool {
        self.owner == requester
    }
}
