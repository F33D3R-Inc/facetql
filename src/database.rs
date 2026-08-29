use std::io;
use std::sync::{Arc, RwLock};

use tokio::sync::broadcast;

use crate::config;
use crate::storage::engine::StorageEngine;
use crate::storage::recovery;

#[derive(Clone)]
pub struct Database {
    pub engine: Arc<RwLock<StorageEngine>>,
    pub broadcaster: broadcast::Sender<String>,
}

impl Database {
    pub fn new() -> io::Result<Self> {
        config::ensure_data_dir()?;

        let mut engine = StorageEngine::load()?;

        /*
         * WAL recovery is part of opening the database.
         *
         * A recovery failure must prevent startup. Continuing after
         * an authentication, corruption, or format error could cause
         * the server to expose state that is not known to be durable
         * or valid.
         */
        recovery::recover(&mut engine)?;

        let (broadcaster, _) =
            broadcast::channel(1024);

        Ok(Self {
            engine: Arc::new(RwLock::new(engine)),
            broadcaster,
        })
    }

    /// Publish a database event to subscribers.
    ///
    /// The database mutation itself is responsible for durability.
    /// This channel is only the live notification mechanism and must
    /// never be treated as the source of truth.
    pub fn publish(&self, event: String) {
        /*
         * There may be no subscribers. broadcast::Sender::send()
         * returns an error in that case, but that does not mean the
         * database operation failed.
         *
         * Events are intentionally best-effort notifications.
         */
        let _ = self.broadcaster.send(event);
    }
}