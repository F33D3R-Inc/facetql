use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::core::edge::Edge;
use crate::core::history::HistoryEntry;
use crate::core::node::Node;
use crate::core::user::UserRecord;
use crate::storage::binary;
use crate::storage::index::Index;
use crate::storage::tombstone;
use crate::storage::wal;

static NEXT_WAL_SEQUENCE: AtomicU64 = AtomicU64::new(1);

fn next_wal_sequence() -> u64 {
    NEXT_WAL_SEQUENCE.fetch_add(1, Ordering::Relaxed)
}

/// Advance the in-process WAL sequence counter past `observed_max`.
///
/// `NEXT_WAL_SEQUENCE` starts at 1 every time the process starts, but an
/// existing `facetql.wal` file may already contain records with much
/// higher sequence numbers from a previous run. Without this call, the
/// first write after a restart would reuse a low sequence number,
/// appending an out-of-order record to a file whose sequence numbers
/// must be strictly increasing (`recovery::validate_sequence` enforces
/// this) — the *next* restart would then fail to recover at all.
///
/// `recovery::recover()` calls this with the highest sequence number it
/// finds in the WAL before doing anything else, so newly generated
/// sequence numbers always continue past whatever was already durable.
pub(crate) fn advance_wal_sequence(observed_max: u64) {
    let desired = observed_max.saturating_add(1);
    let mut current = NEXT_WAL_SEQUENCE.load(Ordering::Relaxed);

    while current < desired {
        match NEXT_WAL_SEQUENCE.compare_exchange(
            current,
            desired,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(actual) => current = actual,
        }
    }
}

#[derive(Debug)]
pub enum ClaimError {
    NotFound,

    /// Carries who already holds the claim, so the caller can report
    /// something more useful than a bare 409.
    AlreadyClaimed(String),

    StorageError(String),
}

impl std::fmt::Display for ClaimError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClaimError::NotFound => write!(f, "node not found"),
            ClaimError::AlreadyClaimed(by) => write!(f, "already claimed by {by}"),
            ClaimError::StorageError(e) => write!(f, "{e}"),
        }
    }
}

pub struct StorageEngine {
    pub nodes: HashMap<String, Node>,
    pub index: Index,

    /// Adjacency lists are kept in both directions so:
    ///
    /// "what does this node point to?"
    ///
    /// and:
    ///
    /// "what points to this node?"
    ///
    /// are both O(1) lookups instead of requiring a full graph scan.
    pub edges_out: HashMap<String, Vec<Edge>>,
    pub edges_in: HashMap<String, Vec<Edge>>,

    /// Persistent users, keyed by token_hash.
    pub users: HashMap<String, UserRecord>,

    /// Archived previous states, keyed by node address, oldest first.
    pub history: HashMap<String, Vec<HistoryEntry>>,
}

impl StorageEngine {
    // ---------------------------------------------------------------------
    // WAL
    // ---------------------------------------------------------------------

    /// Append a single operation to the WAL.
    ///
    /// transaction_id == 0 means this is a standalone operation and is
    /// immediately replayable.
    ///
    /// Real multi-operation transaction IDs are introduced by the
    /// transaction coordinator.
    ///
    /// Returns the WAL sequence number assigned to this record so the
    /// caller can advance the durability checkpoint once the matching
    /// physical write has also completed. See `storage::checkpoint`.
    fn append_wal(
        &self,
        transaction_id: u64,
        operation: wal::WalOperation,
    ) -> Result<u64, String> {
        let sequence = next_wal_sequence();

        let record = wal::WalRecord::new(
            sequence,
            transaction_id,
            sequence,
            operation,
        );

        wal::append(&record).map_err(|e| e.to_string())?;

        Ok(sequence)
    }

    /// Advance the durability checkpoint past `sequence`.
    ///
    /// Called only after the matching physical record has been written,
    /// so the checkpoint never claims durability recovery can't back up.
    /// Best-effort: a failure here doesn't fail the caller's mutation
    /// (the data is already safely on disk either way) but does mean
    /// the next startup may redundantly replay a bit more WAL than
    /// strictly necessary, which is safe, just slightly slower.
    fn advance_checkpoint(sequence: u64) {
        if let Err(e) = crate::storage::checkpoint::advance(sequence) {
            eprintln!(
                "warning: failed to advance WAL checkpoint to {sequence}: {e}"
            );
        }
    }

    // ---------------------------------------------------------------------
    // Recovery-only mutation path
    // ---------------------------------------------------------------------
    //
    // IMPORTANT:
    //
    // These methods MUST NOT call append_wal().
    //
    // Recovery reads the WAL and applies these operations directly to
    // memory. Calling normal mutation methods during recovery would append
    // another WAL record while replaying the original WAL.
    //
    // They also intentionally do not append to the binary storage files.
    // Recovery operates on the state being reconstructed from durable
    // storage/WAL rather than duplicating the durable records.
    // ---------------------------------------------------------------------

    pub(crate) fn replay_archive(
        &mut self,
        entry: HistoryEntry,
    ) -> Result<(), String> {
        self.history
            .entry(entry.address.clone())
            .or_default()
            .push(entry);

        Ok(())
    }

    pub(crate) fn replay_insert(
        &mut self,
        node: Node,
    ) -> Result<(), String> {
        self.nodes.insert(node.address.clone(), node);
        Ok(())
    }

    pub(crate) fn replay_delete(
        &mut self,
        address: &str,
    ) -> Result<(), String> {
        self.nodes.remove(address);
        Ok(())
    }

    pub(crate) fn replay_insert_edge(
        &mut self,
        edge: Edge,
    ) -> Result<(), String> {
        self.index_edge(edge);
        Ok(())
    }

    pub(crate) fn replay_insert_user(
        &mut self,
        record: UserRecord,
    ) -> Result<(), String> {
        self.users.insert(record.token_hash.clone(), record);
        Ok(())
    }

    pub(crate) fn replay_revoke_user(
        &mut self,
        token_hash: &str,
    ) -> Result<(), String> {
        self.users.remove(token_hash);
        Ok(())
    }

    // ---------------------------------------------------------------------
    // Construction / loading
    // ---------------------------------------------------------------------

    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            index: Index::new(),
            edges_out: HashMap::new(),
            edges_in: HashMap::new(),
            users: HashMap::new(),
            history: HashMap::new(),
        }
    }

    /// Rebuilds engine state from durable storage.
    ///
    /// facetql.data
    /// facetql.edges
    /// facetql.users
    /// facetql.history
    /// facetql.tombstones
    ///
    /// are loaded in append order. Later records for the same key replace
    /// the current in-memory value.
    ///
    /// WAL recovery is deliberately separate from this physical-storage
    /// reconstruction. The recovery layer can subsequently apply committed
    /// WAL operations through the replay_* methods without generating new
    /// WAL records.
    pub fn load() -> io::Result<Self> {
        let mut engine = Self::new();

        // -------------------------------------------------------------
        // Nodes
        // -------------------------------------------------------------

        for (offset, node) in binary::read_all()? {
            engine.index.insert(node.address.clone(), offset);
            engine.nodes.insert(node.address.clone(), node);
        }

        // -------------------------------------------------------------
        // Edges
        // -------------------------------------------------------------

        for (_offset, edge) in
            binary::read_all_records::<Edge>(&binary::edges_path())?
        {
            engine.index_edge(edge);
        }

        // -------------------------------------------------------------
        // Users
        // -------------------------------------------------------------

        for (_offset, user) in
            binary::read_all_records::<UserRecord>(&binary::users_path())?
        {
            engine.users.insert(user.token_hash.clone(), user);
        }

        // -------------------------------------------------------------
        // History
        // -------------------------------------------------------------

        for (_offset, entry) in
            binary::read_all_records::<HistoryEntry>(&binary::history_path())?
        {
            engine
                .history
                .entry(entry.address.clone())
                .or_default()
                .push(entry);
        }

        // -------------------------------------------------------------
        // Tombstones
        // -------------------------------------------------------------
        //
        // User tombstones are namespaced with "user:" so a user token hash
        // can never collide with a node address.
        // -------------------------------------------------------------

        for key in tombstone::read_tombstones()? {
            match key.strip_prefix("user:") {
                Some(token_hash) => {
                    engine.users.remove(token_hash);
                }

                None => {
                    engine.nodes.remove(&key);
                }
            }
        }

        Ok(engine)
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    // ---------------------------------------------------------------------
    // Nodes
    // ---------------------------------------------------------------------

    /// Insert or replace a node.
    ///
    /// If a node already exists at the address, its current state is
    /// archived before the new state is written.
    ///
    /// WAL ordering:
    ///
    /// archive intent
    ///     ↓
    /// history durable
    ///     ↓
    /// insert intent
    ///     ↓
    /// node durable
    ///     ↓
    /// memory update
    ///
    /// Standalone operations use transaction_id = 0.
    pub fn insert(&mut self, node: Node) -> Result<(), String> {
        // -------------------------------------------------------------
        // Archive the previous value.
        // -------------------------------------------------------------

        if let Some(previous) = self.nodes.get(&node.address) {
            let entry = HistoryEntry::now(previous.clone());

            let archive_sequence = self.append_wal(
                0,
                wal::WalOperation::Archive(entry.clone()),
            )?;

            binary::append_record(
                &binary::history_path(),
                &entry,
            )
                .map_err(|e| e.to_string())?;

            Self::advance_checkpoint(archive_sequence);

            self.history
                .entry(node.address.clone())
                .or_default()
                .push(entry);
        }

        // -------------------------------------------------------------
        // WAL the new value before making it visible in memory.
        // -------------------------------------------------------------

        let insert_sequence = self.append_wal(
            0,
            wal::WalOperation::Insert(node.clone()),
        )?;

        // -------------------------------------------------------------
        // Persist node.
        // -------------------------------------------------------------

        let offset = binary::append_node(&node)
            .map_err(|e| e.to_string())?;

        Self::advance_checkpoint(insert_sequence);

        // -------------------------------------------------------------
        // Update indexes and live state.
        // -------------------------------------------------------------

        self.index.insert(
            node.address.clone(),
            offset,
        );

        self.nodes.insert(
            node.address.clone(),
            node,
        );

        Ok(())
    }

    /// Every archived previous state for `address`, oldest first.
    ///
    /// Does not include the current live value.
    pub fn history_for(
        &self,
        address: &str,
    ) -> &[HistoryEntry] {
        self.history
            .get(address)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn get(
        &self,
        address: &str,
    ) -> Option<&Node> {
        self.nodes.get(address)
    }

    // ---------------------------------------------------------------------
    // Claims
    // ---------------------------------------------------------------------

    /// Atomically claims a node for `worker`.
    ///
    /// StorageEngine itself does not provide an independent concurrency
    /// primitive. The database layer currently serializes mutations through
    /// its write lock, so the check and update occur within one mutation
    /// operation.
    pub fn claim(
        &mut self,
        address: &str,
        worker: &str,
    ) -> Result<(), ClaimError> {
        let mut node = match self.nodes.get(address) {
            Some(n) => n.clone(),
            None => return Err(ClaimError::NotFound),
        };

        if let Some(existing) = &node.claimed_by {
            return Err(ClaimError::AlreadyClaimed(
                existing.clone(),
            ));
        }

        node.claimed_by = Some(worker.to_string());

        self.insert(node)
            .map_err(ClaimError::StorageError)?;

        Ok(())
    }

    // ---------------------------------------------------------------------
    // Delete
    // ---------------------------------------------------------------------

    /// Tombstones a node so it no longer appears live.
    ///
    /// Deletion is append-only. The original bytes are not removed from
    /// durable storage.
    pub fn delete(
        &mut self,
        address: &str,
    ) -> Result<(), String> {
        let sequence = self.append_wal(
            0,
            wal::WalOperation::Delete(
                address.to_string(),
            ),
        )?;

        tombstone::append_tombstone(address)
            .map_err(|e| e.to_string())?;

        Self::advance_checkpoint(sequence);

        self.nodes.remove(address);

        Ok(())
    }

    // ---------------------------------------------------------------------
    // Edges
    // ---------------------------------------------------------------------

    /// Creates a relationship between two existing nodes.
    ///
    /// Both endpoints must exist before the edge is written.
    pub fn insert_edge(
        &mut self,
        edge: Edge,
    ) -> Result<(), String> {
        if !self.nodes.contains_key(&edge.from) {
            return Err(format!(
                "edge 'from' address not found: {}",
                edge.from
            ));
        }

        if !self.nodes.contains_key(&edge.to) {
            return Err(format!(
                "edge 'to' address not found: {}",
                edge.to
            ));
        }

        let sequence = self.append_wal(
            0,
            wal::WalOperation::InsertEdge(
                edge.clone(),
            ),
        )?;

        binary::append_record(
            &binary::edges_path(),
            &edge,
        )
            .map_err(|e| e.to_string())?;

        Self::advance_checkpoint(sequence);

        self.index_edge(edge);

        Ok(())
    }

    fn index_edge(
        &mut self,
        edge: Edge,
    ) {
        self.edges_out
            .entry(edge.from.clone())
            .or_default()
            .push(edge.clone());

        self.edges_in
            .entry(edge.to.clone())
            .or_default()
            .push(edge);
    }

    pub fn edges_from(
        &self,
        address: &str,
    ) -> &[Edge] {
        self.edges_out
            .get(address)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn edges_to(
        &self,
        address: &str,
    ) -> &[Edge] {
        self.edges_in
            .get(address)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    // ---------------------------------------------------------------------
    // Queries
    // ---------------------------------------------------------------------

    /// Every live node owned by `owner`.
    ///
    /// This remains a linear scan until the secondary owner index is
    /// implemented.
    pub fn nodes_by_owner(
        &self,
        owner: &str,
    ) -> Vec<&Node> {
        self.nodes
            .values()
            .filter(|n| n.owner == owner)
            .collect()
    }

    /// General-purpose listing.
    ///
    /// Current implementation is a linear scan. Secondary indexes and
    /// cursor pagination are later work in the query-engine epic.
    pub fn query(
        &self,
        kind: Option<&str>,
        owner: Option<&str>,
        requester: &str,
        limit: usize,
        offset: usize,
    ) -> Vec<&Node> {
        self.nodes
            .values()
            .filter(|n| {
                kind.map_or(true, |k| n.kind == k)
            })
            .filter(|n| {
                owner.map_or(true, |o| n.owner == o)
            })
            .filter(|n| n.can_read(requester))
            .skip(offset)
            .take(limit)
            .collect()
    }

    // ---------------------------------------------------------------------
    // Insert with edges
    // ---------------------------------------------------------------------

    /// Creates a node and one or more outgoing edges.
    ///
    /// This remains best-effort until the full transaction coordinator is
    /// implemented.
    ///
    /// If an edge fails:
    ///
    /// * the newly created node is tombstoned;
    /// * already-created edges remain;
    /// * the caller receives the successfully-created edges.
    ///
    /// FQL-003/FQL-004 will replace this behavior with real atomic
    /// transactions.
    pub fn insert_with_edges(
        &mut self,
        node: Node,
        edge_targets: Vec<(String, String)>,
    ) -> Result<Vec<Edge>, (String, Vec<Edge>)> {
        let address = node.address.clone();
        let owner = node.owner.clone();

        self.insert(node)
            .map_err(|e| (e, Vec::new()))?;

        let mut created = Vec::new();

        for (to, kind) in edge_targets {
            let edge = Edge::new(
                address.clone(),
                to,
                kind,
                owner.clone(),
            );

            match self.insert_edge(edge.clone()) {
                Ok(()) => {
                    created.push(edge);
                }

                Err(e) => {
                    let _ = self.delete(&address);

                    return Err((
                        e,
                        created,
                    ));
                }
            }
        }

        Ok(created)
    }

    // ---------------------------------------------------------------------
    // Users
    // ---------------------------------------------------------------------

    /// Persists a new user record.
    ///
    /// Only the token hash is persisted. Plaintext tokens are never stored
    /// in the engine.
    pub fn insert_user(
        &mut self,
        record: UserRecord,
    ) -> Result<(), String> {
        let sequence = self.append_wal(
            0,
            wal::WalOperation::InsertUser(
                record.clone(),
            ),
        )?;

        binary::append_record(
            &binary::users_path(),
            &record,
        )
            .map_err(|e| e.to_string())?;

        Self::advance_checkpoint(sequence);

        self.users.insert(
            record.token_hash.clone(),
            record,
        );

        Ok(())
    }

    /// Revokes a user by token hash.
    ///
    /// User tombstones are namespaced with "user:".
    pub fn revoke_user(
        &mut self,
        token_hash: &str,
    ) -> Result<(), String> {
        let sequence = self.append_wal(
            0,
            wal::WalOperation::RevokeUser(
                token_hash.to_string(),
            ),
        )?;

        tombstone::append_tombstone(
            &format!("user:{token_hash}"),
        )
            .map_err(|e| e.to_string())?;

        Self::advance_checkpoint(sequence);

        self.users.remove(token_hash);

        Ok(())
    }

    pub fn find_user_by_hash(
        &self,
        token_hash: &str,
    ) -> Option<&UserRecord> {
        self.users.get(token_hash)
    }

    /// Returns every persistent user.
    ///
    /// Bootstrap identities that exist only in environment configuration
    /// are not represented here.
    pub fn list_users(&self) -> Vec<&UserRecord> {
        self.users.values().collect()
    }

    // ---------------------------------------------------------------------
    // Transactions
    // ---------------------------------------------------------------------

    /// Executes a batch of operations.
    ///
    /// IMPORTANT:
    ///
    /// This is still validation-atomic, not crash-atomic.
    ///
    /// Before the transaction coordinator is implemented, this function:
    ///
    /// 1. Builds a staged view.
    /// 2. Validates every operation.
    /// 3. Applies operations sequentially.
    ///
    /// If validation fails, nothing is written.
    ///
    /// If the process crashes during application, previously-applied
    /// operations may already exist on disk.
    ///
    /// FQL-003 and FQL-004 replace this implementation with:
    ///
    /// BEGIN
    /// OP
    /// OP
    /// COMMIT
    ///
    /// followed by deterministic recovery.
    pub fn execute_transaction(
        &mut self,
        ops: Vec<TxOperation>,
    ) -> Result<(), String> {
        // -------------------------------------------------------------
        // Pass 1:
        //
        // Determine which addresses exist after the complete logical
        // batch.
        // -------------------------------------------------------------

        let mut staged_existing: HashSet<&str> =
            self.nodes
                .keys()
                .map(String::as_str)
                .collect();

        for op in &ops {
            match op {
                TxOperation::InsertNode(node) => {
                    staged_existing.insert(
                        node.address.as_str(),
                    );
                }

                TxOperation::DeleteNode(address) => {
                    staged_existing.remove(
                        address.as_str(),
                    );
                }

                TxOperation::InsertEdge(_) => {}
            }
        }

        // -------------------------------------------------------------
        // Pass 2:
        //
        // Validate everything before touching WAL, disk, or memory.
        // -------------------------------------------------------------

        for op in &ops {
            match op {
                TxOperation::InsertNode(node) => {
                    // SECURITY:
                    //
                    // Do not silently allow a transaction to overwrite
                    // another owner's node.
                    //
                    // The current normal insert() behavior is replacement
                    // semantics, so this check intentionally preserves
                    // existing behavior for now unless an existing node
                    // belongs to a different owner.
                    if let Some(existing) =
                        self.nodes.get(&node.address)
                    {
                        if existing.owner != node.owner {
                            return Err(format!(
                                "transaction failed, nothing applied: \
                                 address {} is owned by {}",
                                node.address,
                                existing.owner
                            ));
                        }
                    }
                }

                TxOperation::DeleteNode(address) => {
                    if !self.nodes.contains_key(address) {
                        return Err(format!(
                            "transaction failed, nothing applied: \
                             delete target not found: {address}"
                        ));
                    }
                }

                TxOperation::InsertEdge(edge) => {
                    if !staged_existing
                        .contains(edge.from.as_str())
                    {
                        return Err(format!(
                            "transaction failed, nothing applied: \
                             edge 'from' address not found: {}",
                            edge.from
                        ));
                    }

                    if !staged_existing
                        .contains(edge.to.as_str())
                    {
                        return Err(format!(
                            "transaction failed, nothing applied: \
                             edge 'to' address not found: {}",
                            edge.to
                        ));
                    }
                }
            }
        }

        // -------------------------------------------------------------
        // Pass 3:
        //
        // Apply the validated operations.
        //
        // This remains non-crash-atomic until the transaction coordinator
        // is implemented.
        // -------------------------------------------------------------

        for op in ops {
            match op {
                TxOperation::InsertNode(node) => {
                    self.insert(node)?;
                }

                TxOperation::DeleteNode(address) => {
                    self.delete(&address)?;
                }

                TxOperation::InsertEdge(edge) => {
                    self.insert_edge(edge)?;
                }
            }
        }

        Ok(())
    }
}

/// One operation inside a transaction.
///
/// Deliberately small for this storage-foundation pass.
///
/// Update-in-place and additional operation types should be introduced
/// through the transaction protocol rather than by bypassing it.
#[derive(Debug, Clone)]
pub enum TxOperation {
    InsertNode(Node),
    DeleteNode(String),
    InsertEdge(Edge),
}