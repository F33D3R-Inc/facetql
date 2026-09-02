use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Serialize, Deserialize};

use crate::core::edge::Edge;
use crate::core::history::HistoryEntry;
use crate::core::node::Node;
use crate::core::predicate::{self, Expr};
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

    /// Live addresses a `ClearKind` would remove: every node whose
    /// `kind` matches and that the caller may write.
    ///
    /// An admin (`is_admin`) matches all of that kind; a non-admin
    /// matches only nodes it owns, via the same `can_write` check the
    /// delete route uses. This is a read-only computation — the caller
    /// tombstones each returned address through the normal `delete`
    /// path — so it can run during transaction staging without touching
    /// WAL, disk, or memory.
    fn clear_targets(
        &self,
        kind: &str,
        owner: &str,
        is_admin: bool,
    ) -> Vec<String> {
        self.nodes
            .values()
            .filter(|n| n.kind == kind)
            .filter(|n| is_admin || n.can_write(owner))
            .map(|n| n.address.clone())
            .collect()
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

    /// Predicate-pushdown query: the `kind`/`owner`/visibility filtering
    /// of `query()`, plus a pushable `Expr` predicate evaluated against
    /// each candidate node's decoded `data`, plus in-engine ordering.
    ///
    /// `item_var` is the loop-variable name the predicate's field
    /// accesses are written against (mirrors FCT's `Query.ItemVar`).
    ///
    /// Still a full scan of the `kind`/`owner`-filtered candidates —
    /// there's no secondary index on `data` fields yet, so this doesn't
    /// avoid the O(n) walk `exprSQL`'s indexed WHERE would with a real
    /// index. What it *does* do is move the filter (and now the sort)
    /// into the engine instead of pulling every candidate row back to
    /// the caller to filter client-side, which is the part that
    /// matters for correctness parity with a single evaluator instead
    /// of two (this one, and whatever a client would otherwise write).
    ///
    /// Ordering: `order` names a field in `data` to sort by, or `None`
    /// (or `"id"`) to sort by `address`. Malformed or missing values on
    /// a given node sort as if absent (after present values), so one
    /// odd row can't silently exclude the rest of the page.
    ///
    /// Pagination is an **opaque keyset cursor**, matching FCT's
    /// `Query.After` contract (runtime/sql.go): the ordering is the
    /// composite `(order_field, address)` — `address` is the stable
    /// tiebreak that makes the total order deterministic even when the
    /// `order_field` values collide — and a cursor encodes the last
    /// returned row's `(order_value, address)`. The next page selects
    /// rows *strictly past* that point in the requested direction, so
    /// paging stays stable under concurrent inserts/deletes the way a
    /// plain offset does not.
    ///
    /// `after` is the cursor from the previous page (`None`/empty for
    /// the first page). The returned [`QueryPage::next`] is the cursor
    /// to feed back in for the following page, or `""` when this was the
    /// last page. `offset` is retained as a fallback and only applies
    /// when no `after` cursor is supplied; a `next` cursor is always
    /// produced so a caller can switch to keyset paging after page one.
    pub fn query_where(
        &self,
        kind: Option<&str>,
        owner: Option<&str>,
        requester: &str,
        predicate: Option<&Expr>,
        item_var: &str,
        order: Option<&str>,
        desc: bool,
        after: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<QueryPage<'_>, String> {
        let mut candidates: Vec<&Node> = self
            .nodes
            .values()
            .filter(|n| kind.map_or(true, |k| n.kind == k))
            .filter(|n| owner.map_or(true, |o| n.owner == o))
            .filter(|n| n.can_read(requester))
            .collect();

        if let Some(expr) = predicate {
            let mut filtered = Vec::with_capacity(candidates.len());

            for node in candidates {
                let data: serde_json::Value =
                    serde_json::from_str(&node.data).unwrap_or(serde_json::Value::Null);

                let matched = predicate::eval(expr, item_var, &data)
                    .map(|v| matches!(v, serde_json::Value::Bool(true)))
                    .map_err(|e| format!("predicate evaluation failed: {e}"))?;

                if matched {
                    filtered.push(node);
                }
            }

            candidates = filtered;
        }

        // Normalize the order field: `None` or `"id"` both mean "order
        // by address alone" (the tiebreak becomes the whole key).
        let order_field = order.filter(|o| *o != "id");

        // Sort into the ascending base order `(order_key, address)`,
        // then flip for `desc`. The `address` tiebreak is what makes the
        // keyset cursor well-defined when `order_field` values repeat.
        candidates.sort_by(|a, b| {
            let base = match order_field {
                Some(field) => compare_order_keys(&order_key(a, field), &order_key(b, field)),
                None => std::cmp::Ordering::Equal,
            };
            base.then_with(|| a.address.cmp(&b.address))
        });
        if desc {
            candidates.reverse();
        }

        // Choose the page start: a keyset cursor (strictly past the
        // encoded row, in the requested direction) takes precedence;
        // otherwise fall back to plain offset.
        let start = match after.filter(|c| !c.is_empty()) {
            Some(cursor_str) => {
                let cur = Cursor::decode(cursor_str)?;
                candidates
                    .iter()
                    .position(|n| {
                        let c = cmp_node_to_cursor(n, order_field, &cur);
                        if desc {
                            c == std::cmp::Ordering::Less
                        } else {
                            c == std::cmp::Ordering::Greater
                        }
                    })
                    .unwrap_or(candidates.len())
            }
            None => offset.min(candidates.len()),
        };

        let page: Vec<&Node> = candidates
            .iter()
            .skip(start)
            .take(limit)
            .copied()
            .collect();

        // A `next` cursor is emitted only when rows remain beyond this
        // page; otherwise it's "" to signal the caller reached the end.
        let next = match page.last() {
            Some(last) if start + page.len() < candidates.len() => {
                Cursor::from_node(last, order_field).encode()
            }
            _ => String::new(),
        };

        Ok(QueryPage { nodes: page, next })
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

                TxOperation::ClearKind { kind, owner, is_admin } => {
                    // A clear removes every writable node of this kind,
                    // so the staged view must reflect all of them gone
                    // — otherwise a later InsertEdge in the same batch
                    // could reference an endpoint this clear deletes and
                    // wrongly validate.
                    for address in
                        self.clear_targets(kind, owner, *is_admin)
                    {
                        staged_existing.remove(address.as_str());
                    }
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

                TxOperation::ClearKind { .. } => {
                    // Nothing to validate: clearing a kind with no
                    // writable nodes (or no nodes at all) is a valid
                    // no-op, and authorization is already baked into
                    // which addresses `clear_targets` selects. A clear
                    // therefore never aborts the batch on its own —
                    // consistent with delete_node only failing on a
                    // missing target.
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

                TxOperation::ClearKind { kind, owner, is_admin } => {
                    // One tombstone per matching node, through the exact
                    // same WAL + tombstone path as a standalone
                    // delete_node — so the whole clear is durable and
                    // survives the existing recovery path.
                    for address in
                        self.clear_targets(&kind, &owner, is_admin)
                    {
                        self.delete(&address)?;
                    }
                }

                TxOperation::InsertEdge(edge) => {
                    self.insert_edge(edge)?;
                }
            }
        }

        Ok(())
    }
}

/// Extract an ordering key from a node for `query_where`'s `order`
/// field. `None` means "absent or unparsable" — such nodes sort after
/// everything with a real value, ascending, regardless of `desc` (the
/// final `if desc { reverse() }` in `query_where` still flips them to
/// the end either way, matching "missing sorts last" either direction).
fn order_key(node: &Node, field: &str) -> Option<serde_json::Value> {
    let data: serde_json::Value = serde_json::from_str(&node.data).ok()?;
    data.get(field).cloned()
}

fn compare_order_keys(
    a: &Option<serde_json::Value>,
    b: &Option<serde_json::Value>,
) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    use serde_json::Value;

    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (Some(av), Some(bv)) => match (av, bv) {
            (Value::Number(an), Value::Number(bn)) => an
                .as_f64()
                .unwrap_or(0.0)
                .partial_cmp(&bn.as_f64().unwrap_or(0.0))
                .unwrap_or(Ordering::Equal),
            (Value::String(a_s), Value::String(b_s)) => a_s.cmp(b_s),
            (Value::Bool(a_b), Value::Bool(b_b)) => a_b.cmp(b_b),
            _ => Ordering::Equal,
        },
    }
}

/// One page of `query_where` results plus the opaque keyset cursor for
/// the following page. `next` is `""` when this was the last page.
///
/// Holds borrows into the engine (same as `query`'s `Vec<&Node>`), so
/// it is serialized while the read lock is still held.
#[derive(Debug, Serialize)]
pub struct QueryPage<'a> {
    pub nodes: Vec<&'a Node>,
    pub next: String,
}

/// The decoded contents of an opaque keyset cursor: the last returned
/// row's order value (absent when ordering by `address` alone) and its
/// `address` tiebreak. Serialized to compact JSON and base64url-encoded
/// so the wire form is opaque to the client — it round-trips the cursor
/// without interpreting it.
#[derive(Serialize, Deserialize)]
struct Cursor {
    /// Order value: `o` for "order value". `None`/absent when the query
    /// orders by `address` only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    o: Option<serde_json::Value>,
    /// Address tiebreak: `a` for "address".
    a: String,
}

impl Cursor {
    fn from_node(node: &Node, order_field: Option<&str>) -> Cursor {
        let o = order_field.and_then(|field| order_key(node, field));
        Cursor { o, a: node.address.clone() }
    }

    fn encode(&self) -> String {
        let json = serde_json::to_vec(self).unwrap_or_default();
        base64url_encode(&json)
    }

    fn decode(s: &str) -> Result<Cursor, String> {
        let bytes = base64url_decode(s).map_err(|_| "invalid cursor: not valid base64url".to_string())?;
        serde_json::from_slice(&bytes).map_err(|_| "invalid cursor: malformed payload".to_string())
    }
}

/// Compare a node against a cursor in the ascending base ordering
/// `(order_key, address)`. `Greater` means the node sorts after the
/// cursor's row (i.e. it belongs on a later ascending page); `Less`
/// means before. Direction handling (`desc`) is applied by the caller.
fn cmp_node_to_cursor(
    node: &Node,
    order_field: Option<&str>,
    cur: &Cursor,
) -> std::cmp::Ordering {
    match order_field {
        Some(field) => {
            let base = compare_order_keys(&order_key(node, field), &cur.o);
            base.then_with(|| node.address.as_str().cmp(cur.a.as_str()))
        }
        None => node.address.as_str().cmp(cur.a.as_str()),
    }
}

// ── base64url (RFC 4648 §5, no padding) ──────────────────────────────
//
// Hand-rolled to keep the cursor opaque without pulling in a crate; the
// cursor payload is small and this is exercised by round-trip tests.

const B64URL: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

fn base64url_encode(input: &[u8]) -> String {
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;

        out.push(B64URL[((n >> 18) & 0x3f) as usize] as char);
        out.push(B64URL[((n >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            out.push(B64URL[((n >> 6) & 0x3f) as usize] as char);
        }
        if chunk.len() > 2 {
            out.push(B64URL[(n & 0x3f) as usize] as char);
        }
    }
    out
}

fn base64url_decode(input: &str) -> Result<Vec<u8>, ()> {
    fn val(c: u8) -> Result<u32, ()> {
        match c {
            b'A'..=b'Z' => Ok((c - b'A') as u32),
            b'a'..=b'z' => Ok((c - b'a' + 26) as u32),
            b'0'..=b'9' => Ok((c - b'0' + 52) as u32),
            b'-' => Ok(62),
            b'_' => Ok(63),
            _ => Err(()),
        }
    }

    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3 + 3);
    for chunk in bytes.chunks(4) {
        if chunk.len() < 2 {
            return Err(());
        }
        let mut n = 0u32;
        for (i, &c) in chunk.iter().enumerate() {
            n |= val(c)? << (18 - 6 * i);
        }
        out.push((n >> 16) as u8);
        if chunk.len() > 2 {
            out.push((n >> 8) as u8);
        }
        if chunk.len() > 3 {
            out.push(n as u8);
        }
    }
    Ok(out)
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

    /// Remove every live node of `kind` the caller is allowed to write,
    /// as one native, all-or-nothing step inside the transaction. Each
    /// removal takes the same WAL + tombstone path as a single
    /// `DeleteNode`, so a `ClearKind` is exactly "N deletes" that
    /// commit or roll back together with the rest of the batch.
    ///
    /// Authorization is carried on the op rather than resolved in the
    /// engine: the handler already holds the write lock and knows the
    /// caller's identity, so it stamps the resolved `owner` and
    /// `is_admin` here. A non-admin clears only nodes it owns; an admin
    /// (`is_admin == true`) clears every node of that kind regardless of
    /// owner — mirroring how the read/delete routes let an admin bypass
    /// per-node ownership.
    ClearKind {
        kind: String,
        owner: String,
        is_admin: bool,
    },
}

#[cfg(test)]
mod cursor_tests {
    use super::*;
    use crate::core::coordinate::Coordinate;
    use crate::core::node::{Node, Visibility};

    fn make_node(address: &str, score: i64) -> Node {
        let mut n = Node::new(
            Coordinate::new(0, 0, 0, 0),
            address.to_string(),
            "thing".to_string(),
            "owner".to_string(),
        );
        n.data = format!("{{\"score\": {score}}}");
        n.visibility = Visibility::Public;
        n
    }

    /// Walk every page via the returned cursor and collect addresses in
    /// order. Panics if paging fails to terminate.
    fn page_all(
        engine: &StorageEngine,
        order: Option<&str>,
        desc: bool,
        limit: usize,
    ) -> Vec<String> {
        let mut out = Vec::new();
        let mut after: Option<String> = None;
        for _ in 0..1000 {
            let page = engine
                .query_where(
                    Some("thing"),
                    None,
                    "",
                    None,
                    "item",
                    order,
                    desc,
                    after.as_deref(),
                    limit,
                    0,
                )
                .expect("query_where ok");
            out.extend(page.nodes.iter().map(|n| n.address.clone()));
            if page.next.is_empty() {
                return out;
            }
            after = Some(page.next);
        }
        panic!("paging did not terminate");
    }

    #[test]
    fn base64url_roundtrips_all_lengths() {
        for len in 0..40usize {
            let bytes: Vec<u8> = (0..len).map(|i| (i as u8).wrapping_mul(37)).collect();
            let enc = base64url_encode(&bytes);
            assert!(!enc.contains('='), "must be unpadded");
            let dec = base64url_decode(&enc).expect("decode");
            assert_eq!(dec, bytes, "roundtrip failed at len {len}");
        }
    }

    #[test]
    fn base64url_rejects_bad_input() {
        assert!(base64url_decode("!!!!").is_err());
        assert!(base64url_decode("A").is_err()); // dangling single char
    }

    #[test]
    fn keyset_pages_cover_all_rows_once_ordered_by_field() {
        let mut e = StorageEngine::new();
        for (addr, score) in [("a", 30), ("b", 10), ("c", 20), ("d", 50), ("e", 40)] {
            e.nodes.insert(addr.to_string(), make_node(addr, score));
        }
        // Ascending by score: b(10) c(20) a(30) e(40) d(50)
        let asc = page_all(&e, Some("score"), false, 2);
        assert_eq!(asc, vec!["b", "c", "a", "e", "d"]);

        // Descending by score.
        let desc = page_all(&e, Some("score"), true, 2);
        assert_eq!(desc, vec!["d", "e", "a", "c", "b"]);
    }

    #[test]
    fn keyset_tiebreak_by_address_with_equal_order_values() {
        let mut e = StorageEngine::new();
        // All identical score => order is purely by address tiebreak.
        for addr in ["a", "b", "c", "d", "e"] {
            e.nodes.insert(addr.to_string(), make_node(addr, 7));
        }
        let asc = page_all(&e, Some("score"), false, 2);
        assert_eq!(asc, vec!["a", "b", "c", "d", "e"]);
    }

    #[test]
    fn keyset_stable_when_a_row_is_deleted_between_pages() {
        let mut e = StorageEngine::new();
        for (addr, score) in [("a", 10), ("b", 20), ("c", 30), ("d", 40)] {
            e.nodes.insert(addr.to_string(), make_node(addr, score));
        }
        // First page ascending, limit 2 => a, b ; cursor sits at b(20).
        let (p1_addrs, p1_next) = {
            let page1 = e
                .query_where(Some("thing"), None, "", None, "item", Some("score"), false, None, 2, 0)
                .unwrap();
            (
                page1.nodes.iter().map(|n| n.address.clone()).collect::<Vec<_>>(),
                page1.next.clone(),
            )
        };
        assert_eq!(p1_addrs, vec!["a", "b"]);
        assert!(!p1_next.is_empty());

        // Delete an already-seen row; the next page must still be c, d
        // (offset would have skipped c here).
        e.nodes.remove("a");
        let page2 = e
            .query_where(
                Some("thing"), None, "", None, "item", Some("score"), false,
                Some(&p1_next), 2, 0,
            )
            .unwrap();
        assert_eq!(
            page2.nodes.iter().map(|n| n.address.as_str()).collect::<Vec<_>>(),
            vec!["c", "d"]
        );
        assert!(page2.next.is_empty(), "reached last page");
    }

    #[test]
    fn order_by_address_when_order_absent() {
        let mut e = StorageEngine::new();
        for addr in ["c", "a", "b"] {
            e.nodes.insert(addr.to_string(), make_node(addr, 1));
        }
        let all = page_all(&e, None, false, 1);
        assert_eq!(all, vec!["a", "b", "c"]);
    }

    #[test]
    fn invalid_cursor_is_rejected() {
        let mut e = StorageEngine::new();
        e.nodes.insert("a".to_string(), make_node("a", 1));
        let err = e
            .query_where(
                Some("thing"), None, "", None, "item", Some("score"), false,
                Some("not-valid-base64!!"), 10, 0,
            )
            .unwrap_err();
        assert!(err.contains("invalid cursor"), "got: {err}");
    }

    #[test]
    fn unpushable_predicate_errors_not_wrong_answer() {
        use crate::core::predicate::Expr;
        let mut e = StorageEngine::new();
        e.nodes.insert("a".to_string(), make_node("a", 1));
        // A bare `ref` node is not something eval can push down.
        let expr: Expr = serde_json::from_value(serde_json::json!({
            "kind": "ref", "name": "somethingElse"
        }))
        .unwrap();
        let res = e.query_where(
            Some("thing"), None, "", Some(&expr), "item", None, false, None, 10, 0,
        );
        assert!(res.is_err(), "unpushable predicate must error");
    }
}

#[cfg(test)]
mod clear_kind_tests {
    use super::*;
    use crate::core::coordinate::Coordinate;
    use crate::core::node::Node;
    use std::sync::{Mutex, OnceLock};

    // These tests exercise the real WAL/binary/tombstone paths, so they
    // need a data directory. `config` resolves it through a process-wide
    // `OnceLock`, so every test shares one temp dir (set once) and must
    // not append to the shared WAL/binary files concurrently — hence one
    // lock serializing them. Each test uses unique addresses/kinds so a
    // shared dir never crosses assertions between tests.
    fn disk_guard() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: Mutex<()> = Mutex::new(());
        static INIT: OnceLock<()> = OnceLock::new();
        INIT.get_or_init(|| {
            let dir = std::env::temp_dir()
                .join(format!("facetql-cleartest-{}", std::process::id()));
            std::fs::create_dir_all(&dir).expect("create temp data dir");
            crate::config::set_data_dir(dir);
        });
        LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn node_owned(address: &str, kind: &str, owner: &str) -> Node {
        Node::new(
            Coordinate::new(0, 0, 0, 0),
            address.to_string(),
            kind.to_string(),
            owner.to_string(),
        )
    }

    /// Admin clears every node of the kind regardless of owner, and
    /// leaves other kinds untouched.
    #[test]
    fn admin_clears_all_of_a_kind() {
        let _g = disk_guard();
        let mut e = StorageEngine::new();
        e.insert(node_owned("ck_admin:1", "CkAdminEntity", "alice")).unwrap();
        e.insert(node_owned("ck_admin:2", "CkAdminEntity", "bob")).unwrap();
        e.insert(node_owned("ck_admin:keep", "CkAdminOther", "alice")).unwrap();

        e.execute_transaction(vec![TxOperation::ClearKind {
            kind: "CkAdminEntity".to_string(),
            owner: "root".to_string(),
            is_admin: true,
        }])
        .expect("admin clear commits");

        assert!(e.get("ck_admin:1").is_none(), "admin cleared alice's node");
        assert!(e.get("ck_admin:2").is_none(), "admin cleared bob's node");
        assert!(e.get("ck_admin:keep").is_some(), "other kind untouched");
    }

    /// A non-admin clears only nodes it owns of that kind; another
    /// owner's node of the same kind stays intact.
    #[test]
    fn non_admin_clears_only_its_own_nodes() {
        let _g = disk_guard();
        let mut e = StorageEngine::new();
        e.insert(node_owned("ck_own:mine1", "CkOwnEntity", "alice")).unwrap();
        e.insert(node_owned("ck_own:mine2", "CkOwnEntity", "alice")).unwrap();
        e.insert(node_owned("ck_own:theirs", "CkOwnEntity", "bob")).unwrap();

        e.execute_transaction(vec![TxOperation::ClearKind {
            kind: "CkOwnEntity".to_string(),
            owner: "alice".to_string(),
            is_admin: false,
        }])
        .expect("non-admin clear commits");

        assert!(e.get("ck_own:mine1").is_none(), "own node cleared");
        assert!(e.get("ck_own:mine2").is_none(), "own node cleared");
        assert!(
            e.get("ck_own:theirs").is_some(),
            "other owner's node of the same kind is left intact"
        );
    }

    /// A clear is atomic with the rest of the batch: a later op that
    /// fails validation rolls the whole transaction back, including the
    /// clear — nothing is applied.
    #[test]
    fn clear_rolls_back_when_a_later_op_fails() {
        let _g = disk_guard();
        let mut e = StorageEngine::new();
        e.insert(node_owned("ck_atom:1", "CkAtomEntity", "alice")).unwrap();
        e.insert(node_owned("ck_atom:2", "CkAtomEntity", "alice")).unwrap();

        // Clear the kind, then delete a node that doesn't exist. The
        // delete fails validation (pass 2), so pass 3 never runs and the
        // clear is never applied.
        let result = e.execute_transaction(vec![
            TxOperation::ClearKind {
                kind: "CkAtomEntity".to_string(),
                owner: "alice".to_string(),
                is_admin: false,
            },
            TxOperation::DeleteNode("ck_atom:missing".to_string()),
        ]);

        assert!(result.is_err(), "batch must fail on the missing delete target");
        assert!(e.get("ck_atom:1").is_some(), "clear rolled back with the batch");
        assert!(e.get("ck_atom:2").is_some(), "clear rolled back with the batch");
    }

    /// Each cleared node is tombstoned + WAL-logged exactly like a
    /// standalone delete, so a fresh recovery from durable storage no
    /// longer sees it — while a non-cleared node recovers normally.
    #[test]
    fn cleared_nodes_do_not_survive_recovery() {
        let _g = disk_guard();
        let mut e = StorageEngine::new();
        e.insert(node_owned("ck_rec:gone", "CkRecEntity", "alice")).unwrap();
        e.insert(node_owned("ck_rec:stay", "CkRecOther", "alice")).unwrap();

        e.execute_transaction(vec![TxOperation::ClearKind {
            kind: "CkRecEntity".to_string(),
            owner: "alice".to_string(),
            is_admin: false,
        }])
        .expect("clear commits");

        // Rebuild purely from durable storage (data files + tombstones).
        let recovered = StorageEngine::load().expect("recovery load");
        assert!(
            recovered.get("ck_rec:gone").is_none(),
            "clear's tombstone survived recovery"
        );
        assert!(
            recovered.get("ck_rec:stay").is_some(),
            "non-cleared node recovered from durable storage"
        );
    }
}