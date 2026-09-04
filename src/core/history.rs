use serde::{Deserialize, Serialize};
use crate::core::node::Node;

/// One archived previous state of a node, captured automatically the
/// instant it's about to be overwritten. This is the real feature that
/// replaces the "3D temporal axis" idea from earlier design discussion:
/// no coordinate morphing, no dissonance score — just "here's what this
/// node looked like right before this write," stored the same
/// straightforward way every other record in FacetQL is stored.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryEntry {
    pub address: String,
    /// Seconds since the Unix epoch. Not pulling in a datetime library
    /// for one timestamp field — this project already has enough
    /// dependency-version friction (see SECURITY_NOTES.md) to avoid
    /// adding one where a plain integer does the job.
    pub archived_at_unix: u64,
    /// The full previous node — every field, not just `data` — so a
    /// caller can see what the owner, visibility, or kind used to be
    /// too, not only the payload.
    pub node: Node,
    /// Monotonic version number, unique across the life of the database.
    ///
    /// This is what makes history *addressable* rather than merely
    /// appended. The history index is keyed `(address, version)`, so
    /// "this node's versions in order" is a range scan and "this exact
    /// archived state" is a point lookup — neither of which a timestamp
    /// could serve, because two overwrites in the same second share one
    /// `archived_at_unix` and would collide into a single key, silently
    /// losing one of them.
    ///
    /// It is assigned once, at the moment the entry is created, and then
    /// travels with the entry through the WAL. That is what makes replay
    /// idempotent: replaying an archive re-derives the *same* key, so it
    /// lands on the entry it already wrote instead of appending a
    /// duplicate — the failure mode that previously required recovery to
    /// scan history for a matching entry before every replay.
    pub version: u64,
}

impl HistoryEntry {
    /// Capture `node` as an archived state, stamped with the current
    /// time and the next version number.
    ///
    /// The version comes from the WAL's operation-id counter rather than
    /// from a counter of its own. That counter is already the one thing
    /// in the process guaranteed to be unique and increasing *across
    /// restarts* — recovery advances it past every identifier in the
    /// durable WAL before the first new write — which is exactly the
    /// property a version needs and the property a fresh counter would
    /// not have.
    pub fn now(node: Node) -> Self {
        let archived_at_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        Self {
            address: node.address.clone(),
            archived_at_unix,
            node,
            version: crate::storage::wal::next_operation_id(),
        }
    }
}
