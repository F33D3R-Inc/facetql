use std::sync::{Arc, RwLock};
use tokio::sync::broadcast;
use crate::storage::engine::StorageEngine;
use crate::core::generator;

pub struct Database {
    pub engine: RwLock<StorageEngine>,
    /// Live change feed. Every successful write publishes a message here;
    /// anyone connected to GET /events gets it immediately. 1024-message
    /// buffer means a slow subscriber can fall behind that far before it
    /// starts missing messages (tokio::sync::broadcast's designed
    /// behavior) — fine for "refresh the UI," not fine if a consumer
    /// needs a guaranteed-delivery log of every change; that's a
    /// different, not-yet-built feature (see SECURITY_NOTES.md).
    pub broadcaster: broadcast::Sender<String>,
}

impl Database {
    /// Loads existing state from facetql.data if it exists. On a
    /// genuinely fresh install (empty/missing data file), seeds the
    /// 12x13 genesis coordinate grid so there's something to query on
    /// first boot. Previously generate_genesis() was never called from
    /// anywhere — the grid existed in code but never actually ran.
    pub fn new() -> Arc<Self> {
        let mut engine = StorageEngine::load().unwrap_or_else(|e| {
            eprintln!("warning: failed to load facetql.data ({e}) — starting with empty storage");
            StorageEngine::new()
        });

        if engine.is_empty() {
            println!("No existing data found — seeding genesis coordinate grid (156 nodes)");
            for node in generator::generate_genesis() {
                if let Err(e) = engine.insert(node) {
                    eprintln!("warning: failed to seed genesis node: {e}");
                }
            }
        }

        let (broadcaster, _rx) = broadcast::channel(1024);

        Arc::new(Self {
            engine: RwLock::new(engine),
            broadcaster,
        })
    }

    /// Publishes one change-feed message. `_ =` on the send result is
    /// intentional and documented, not an oversight: broadcast::send
    /// only errors when there are zero subscribers, which is the normal
    /// case when nobody's listening on /events yet — that's not a
    /// failure, it just means the message had no one to deliver to.
    pub fn publish(&self, message: String) {
        let _ = self.broadcaster.send(message);
    }
}
