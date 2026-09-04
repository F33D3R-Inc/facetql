use serde::{Serialize, Deserialize};
use super::coordinate::Coordinate;

/// Who is allowed to read a node.
/// Private: only the owner. Public: anyone who is authenticated at all.
/// This is intentionally minimal for v0.1 — a real ACL list per-node
/// (multiple grantees, per-relationship permissions) is the next step,
/// not yet built.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum Visibility {
    Private,
    Public,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Node {
    pub address: String,
    pub coordinate: Coordinate,
    pub value: u64,
    /// Entity type, e.g. "Person", "Goal", "Resource", "Organization",
    /// "Review". This is what makes FacetQL usable like a real
    /// application database instead of just a key-value store — a
    /// client building Project Interstate needs "list every Goal
    /// belonging to this Person," and that requires nodes to declare
    /// what they are, not just hold an opaque `data` blob. Free-text
    /// rather than a closed enum for the same reason `Edge::kind` is:
    /// the DB shouldn't need a code change every time the application
    /// adds a new entity type.
    pub kind: String,
    pub data: String,
    pub owner: String,
    /// Set by `POST /node/:address/claim` — see storage/engine.rs `claim()`.
    /// Deliberately a real field, not something encoded inside `data` and
    /// checked with string matching: a job-queue "is this already claimed"
    /// check needs to be exact and type-safe, not dependent on `data`
    /// happening to contain valid JSON with a particular shape.
    /// NOTE: adding a field here is a breaking change to the on-disk
    /// format — bincode is a fixed binary layout, so
    /// `#[serde(default)]`-style "missing field is fine" behavior does
    /// NOT apply the way it would for JSON. Records written by a build
    /// with a different `Node` shape will fail to deserialize.
    pub claimed_by: Option<String>,
    pub visibility: Visibility,
}

impl Node {
    pub fn new(coordinate: Coordinate, address: String, kind: String, owner: String) -> Self {
        Self {
            address,
            coordinate,
            value: 0,
            kind,
            data: String::new(),
            owner,
            claimed_by: None,
            visibility: Visibility::Private,
        }
    }

    /// Address knowledge alone (e.g. "7Bm3") never grants access.
    /// Every read must pass through this check.
    pub fn can_read(&self, requester: &str) -> bool {
        self.visibility == Visibility::Public || self.owner == requester
    }

    /// Only the owner may mutate a node in this version.
    /// Shared/collaborative write access is a future ACL-list feature.
    /// Enforced by the update/delete routes in api/routes.rs.
    pub fn can_write(&self, requester: &str) -> bool {
        self.owner == requester
    }
}
