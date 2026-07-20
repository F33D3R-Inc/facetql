use crate::core::node::Node;

/// Multi-op transaction scaffolding. StorageEngine::insert() currently
/// commits each write individually (WAL -> disk -> index -> memory) —
/// there's no grouping of multiple operations into one atomic unit yet.
/// This type is the shape that will carry, e.g., "create user node +
/// create profile node + link them" as one all-or-nothing commit.
#[allow(dead_code)]
pub enum Operation {
    Insert(Node),
    Update(Node),
    Delete(String),
}

#[allow(dead_code)]
pub struct Transaction {
    pub id: u64,
    pub operations: Vec<Operation>,
}

impl Transaction {
    #[allow(dead_code)]
    pub fn new(id: u64) -> Self {
        Self {
            id,
            operations: Vec::new(),
        }
    }
}
