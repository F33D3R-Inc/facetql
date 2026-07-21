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
}

impl HistoryEntry {
    pub fn now(node: Node) -> Self {
        let archived_at_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        Self { address: node.address.clone(), archived_at_unix, node }
    }
}
