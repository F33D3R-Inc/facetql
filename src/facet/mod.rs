use std::sync::Arc;

use crate::core::coordinate::Coordinate;
use crate::core::node::Node;
use crate::database::Database;

pub struct FacetStore {
    db: Arc<Database>,
}

impl FacetStore {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    /// Store a Facet entity directly in FacetQL.
    ///
    /// This is the native write boundary:
    ///
    /// Facet
    ///   ↓
    /// FacetStore
    ///   ↓
    /// StorageEngine
    ///   ↓
    /// WAL + heap + indexes
    pub fn put(
        &self,
        address: impl Into<String>,
        kind: impl Into<String>,
        x: u8,
        y: u8,
        z: u8,
        q: u8,
        data: impl Into<String>,
        owner: impl Into<String>,
    ) -> Result<(), String> {
        let node = Node::new(
            Coordinate::new(x, y, z, q),
            address.into(),
            kind.into(),
            owner.into(),
        );

        let mut node = node;
        node.data = data.into();

        let mut engine = self
            .db
            .engine
            .write()
            .map_err(|_| "storage engine lock poisoned".to_string())?;

        engine.insert(node)
    }

    /// Read one Facet entity directly from FacetQL.
    pub fn get(
        &self,
        address: &str,
    ) -> Result<Option<Node>, String> {
        let engine = self
            .db
            .engine
            .read()
            .map_err(|_| "storage engine lock poisoned".to_string())?;

        engine.get(address).map_err(|e| e.to_string())
    }

    /// Delete one Facet entity.
    pub fn delete(
        &self,
        address: &str,
    ) -> Result<(), String> {
        let mut engine = self
            .db
            .engine
            .write()
            .map_err(|_| "storage engine lock poisoned".to_string())?;

        engine.delete(address)
    }
}