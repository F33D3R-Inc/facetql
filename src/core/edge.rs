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

/// The address of an edge: the triple `(from, to, kind)`.
///
/// A node has an obvious identity — its `address` — and an edge did not,
/// which is exactly why there was no way to delete one. This is that
/// missing address, and it is what `DELETE /edge`, the `delete_edge`
/// transaction op and the engine's `find_edge`/`delete_edge` all
/// address an edge by.
///
/// # Why `owner` is not part of it
///
/// An edge is a *statement that a relationship of a given type exists
/// between two nodes*. "A follows B" is one fact about the graph, not
/// one fact per person who asserts it. If `owner` were part of the
/// identity, two owners could each hold their own "A follows B" and
/// both would be live at once: `edges_from("A")` would return the same
/// relationship twice, a traversal would double-count it, and
/// unfollowing would delete one copy and leave the other — the
/// relationship would still be there afterwards. Making identity
/// owner-free makes the second insert land on the same edge instead of
/// beside it, so the graph holds one copy of each fact.
///
/// Ownership is therefore an *authorization* attribute, not an identity
/// one: it records who may remove the edge (see [`Edge::can_write`]),
/// not what the edge is. That is the same split nodes have — a node's
/// `owner` decides who may write it, while its `address` decides which
/// node it is.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EdgeId {
    pub from: String,
    pub to: String,
    pub kind: String,
}

impl EdgeId {
    pub fn new(from: String, to: String, kind: String) -> Self {
        Self { from, to, kind }
    }
}

impl Edge {
    pub fn new(from: String, to: String, kind: String, owner: String) -> Self {
        Self { from, to, kind, owner }
    }

    /// The identity of this edge, with `owner` deliberately dropped —
    /// see [`EdgeId`] for why the owner is not part of what an edge is.
    pub fn id(&self) -> EdgeId {
        EdgeId::new(self.from.clone(), self.to.clone(), self.kind.clone())
    }

    /// Same ownership model as Node::can_write — only the creator of the
    /// edge may remove it. Called on the delete paths (`DELETE /edge`
    /// and the `delete_edge` transaction op), where an admin bypasses
    /// this the same way it bypasses a node's `can_write`.
    ///
    /// Revisit once verification flows need a *different* party (e.g.
    /// the org that granted a capability) to be able to revoke an edge
    /// than the party who requested it — that's an ACL-list problem,
    /// same caveat as Node. Note the asymmetry this leaves today:
    /// because [`EdgeId`] excludes the owner, whoever creates
    /// "A follows B" first owns the single copy of that fact, and only
    /// they (or an admin) can retract it.
    pub fn can_write(&self, requester: &str) -> bool {
        self.owner == requester
    }
}
