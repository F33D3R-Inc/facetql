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

/// Loop-variable name a `delete_where` predicate's field accesses are
/// written against. The `delete_where` wire contract (§4b) carries no
/// `item_var` — unlike `/nodes/query` — so it uses the same default the
/// query path does (`default_item_var` in `api::routes`): `"item"`.
const DELETE_WHERE_ITEM_VAR: &str = "item";

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

    /// Process-lifetime operation counters, the rate source behind
    /// `GET /stats` (and any future health/observability surface — NOTES
    /// EPIC 08). `reads_total` counts calls to `get`/`query`/`query_where`;
    /// `writes_total` counts each applied mutation in `insert`/`delete`/
    /// `insert_edge` — which, because the transaction apply pass routes
    /// every committed op through exactly those primitives, also makes a
    /// transaction contribute one increment per node it actually mutates
    /// (a `ClearKind`/`DeleteWhere` of N nodes counts as N writes, the
    /// truthful workload signal).
    ///
    /// These are deliberately NOT persisted: they start at 0 every time
    /// the process starts and are never checkpointed or replayed. That is
    /// correct for a rate source — a Fabric poller (or any consumer)
    /// derives ops/sec and the read/write split by *differencing two
    /// samples over time*, and a restart simply resets the baseline. They
    /// are atomics rather than plain integers because `get`/`query`/
    /// `query_where` mutate them while holding only a read lock on the
    /// engine (the route layer takes `db.engine.read()` for reads), so the
    /// increment must not require `&mut self`. `Ordering::Relaxed` is used
    /// throughout: these are statistics, not a synchronization signal, so
    /// no happens-before ordering with other memory is needed.
    reads_total: AtomicU64,
    writes_total: AtomicU64,
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
            reads_total: AtomicU64::new(0),
            writes_total: AtomicU64::new(0),
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

        // Count the applied write. See the `writes_total` field doc for
        // why this lives at the mutation primitive (so a transaction op
        // is counted once, here, when it actually applies).
        self.writes_total.fetch_add(1, Ordering::Relaxed);

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
        self.reads_total.fetch_add(1, Ordering::Relaxed);
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

        // Count the applied write (see `writes_total` field doc). A bulk
        // ClearKind/DeleteWhere flows through here once per node removed,
        // so N cleared nodes count as N writes.
        self.writes_total.fetch_add(1, Ordering::Relaxed);

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

    /// Live addresses a `DeleteWhere` would remove: `clear_targets`'
    /// selection (every node whose `kind` matches and that the caller
    /// may write) narrowed further by a `where_` predicate evaluated
    /// against each candidate's decoded `data`.
    ///
    /// The predicate is run through the *same* `predicate::eval` the
    /// `/nodes/query` path uses (see `query_where`), so a predicated
    /// bulk delete selects byte-for-byte the rows the equivalent query
    /// would — one evaluator, not two. `where_ == None` degenerates to
    /// exactly `clear_targets` (all writable nodes of the kind).
    ///
    /// A predicate `eval` can't push down (or otherwise errors) is
    /// surfaced as `Err`, mirroring how `query_where` surfaces it. The
    /// transaction turns that `Err` into a whole-batch abort — never a
    /// wrong or partial delete.
    ///
    /// Read-only, like `clear_targets`: it computes addresses without
    /// touching WAL, disk, or memory, so it is safe to call during
    /// transaction staging (and from the handler, to record the exact
    /// addresses the delete will tombstone).
    pub(crate) fn delete_where_targets(
        &self,
        kind: &str,
        where_: Option<&Expr>,
        owner: &str,
        is_admin: bool,
    ) -> Result<Vec<String>, String> {
        let mut targets = Vec::new();

        for node in self.nodes.values() {
            if node.kind != kind {
                continue;
            }
            if !(is_admin || node.can_write(owner)) {
                continue;
            }

            if let Some(expr) = where_ {
                let data: serde_json::Value =
                    serde_json::from_str(&node.data).unwrap_or(serde_json::Value::Null);

                let matched = predicate::eval(expr, DELETE_WHERE_ITEM_VAR, &data)
                    .map(|v| matches!(v, serde_json::Value::Bool(true)))
                    .map_err(|e| format!("predicate evaluation failed: {e}"))?;

                if !matched {
                    continue;
                }
            }

            targets.push(node.address.clone());
        }

        Ok(targets)
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

        // Count the applied write (see `writes_total` field doc).
        self.writes_total.fetch_add(1, Ordering::Relaxed);

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
    ///
    /// `requester` controls the per-node visibility filter, the same way a
    /// real DBMS distinguishes a normal role from a superuser:
    ///
    /// * `Some(r)` — apply `n.can_read(r)`: the caller sees public nodes
    ///   plus the private nodes it owns.
    /// * `None` — admin/superuser bypass: skip the visibility filter
    ///   entirely and return every node matching `kind`/`owner`. This is
    ///   what lets an admin list Private nodes it does not own, mirroring
    ///   `get_node`'s admin bypass. A normal role must never pass `None`.
    pub fn query(
        &self,
        kind: Option<&str>,
        owner: Option<&str>,
        requester: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Vec<&Node> {
        self.reads_total.fetch_add(1, Ordering::Relaxed);
        self.nodes
            .values()
            .filter(|n| {
                kind.map_or(true, |k| n.kind == k)
            })
            .filter(|n| {
                owner.map_or(true, |o| n.owner == o)
            })
            .filter(|n| requester.map_or(true, |r| n.can_read(r)))
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
    ///
    /// `requester` carries the same admin/superuser semantics as
    /// [`query`]: `Some(r)` applies the `n.can_read(r)` visibility filter,
    /// while `None` is the admin bypass that skips it entirely and lets an
    /// admin page over Private nodes it does not own.
    pub fn query_where(
        &self,
        kind: Option<&str>,
        owner: Option<&str>,
        requester: Option<&str>,
        predicate: Option<&Expr>,
        item_var: &str,
        order: Option<&str>,
        desc: bool,
        after: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<QueryPage<'_>, String> {
        self.reads_total.fetch_add(1, Ordering::Relaxed);
        let mut candidates: Vec<&Node> = self
            .nodes
            .values()
            .filter(|n| kind.map_or(true, |k| n.kind == k))
            .filter(|n| owner.map_or(true, |o| n.owner == o))
            .filter(|n| requester.map_or(true, |r| n.can_read(r)))
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
    // Stats / observability
    // ---------------------------------------------------------------------

    /// A snapshot of the engine's own storage and operation statistics —
    /// the native source behind `GET /stats` (and any future health /
    /// readiness / capacity surface; NOTES EPIC 08).
    ///
    /// Structural counts (`node_count`, `edge_count`, `user_count`,
    /// `history_entries`, per-`kind` counts) are computed from the live
    /// in-memory collections at call time. `kinds` is grouped through a
    /// `BTreeMap`, so the returned `Vec<KindCount>` is sorted by `kind` and
    /// therefore deterministic and testable. The operation counters
    /// (`reads_total`/`writes_total`) are the process-lifetime atomics (see
    /// their field docs): monotonic since process start, not persisted, and
    /// meant to be *differenced over time* by a consumer to derive rates.
    ///
    /// Read-only (`&self`): it only reads collections and loads the
    /// atomics, so it runs under the engine's read lock like any query.
    pub fn stats(&self) -> EngineStats {
        use std::collections::BTreeMap;

        let mut by_kind: BTreeMap<String, u64> = BTreeMap::new();
        for node in self.nodes.values() {
            *by_kind.entry(node.kind.clone()).or_default() += 1;
        }

        EngineStats {
            node_count: self.nodes.len() as u64,
            edge_count: self.edges_out.values().map(|v| v.len() as u64).sum(),
            user_count: self.users.len() as u64,
            history_entries: self.history.values().map(|v| v.len() as u64).sum(),
            kinds: by_kind
                .into_iter()
                .map(|(kind, count)| KindCount { kind, count })
                .collect(),
            reads_total: self.reads_total.load(Ordering::Relaxed),
            writes_total: self.writes_total.load(Ordering::Relaxed),
        }
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

                TxOperation::DeleteWhere { kind, where_, owner, is_admin } => {
                    // Same staging rationale as ClearKind — the view
                    // must reflect every selected node gone so a later
                    // InsertEdge can't validate against an endpoint this
                    // delete removes. An unpushable/erroring predicate
                    // surfaces here via `?`, aborting the batch before
                    // pass 2/3 write anything — the query path's
                    // error-not-wrong-answer contract, applied to a
                    // bulk delete.
                    for address in self.delete_where_targets(
                        kind,
                        where_.as_ref(),
                        owner,
                        *is_admin,
                    )? {
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

                TxOperation::DeleteWhere { .. } => {
                    // Nothing to validate here: the one way a
                    // delete_where can abort — an unpushable/erroring
                    // predicate — already surfaced in pass 1 (staging),
                    // before any write. Matching zero writable nodes is
                    // a valid no-op, so a delete_where never aborts the
                    // batch on its own, same as ClearKind.
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

                TxOperation::DeleteWhere { kind, where_, owner, is_admin } => {
                    // One tombstone per selected node, through the exact
                    // same WAL + tombstone path as a standalone
                    // delete_node — so the whole predicated delete is
                    // durable and survives recovery. Re-selecting here
                    // (as ClearKind does) can't newly error: pass 1
                    // already evaluated this same predicate and any
                    // failure aborted the batch before reaching pass 3.
                    for address in self.delete_where_targets(
                        &kind,
                        where_.as_ref(),
                        &owner,
                        is_admin,
                    )? {
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

/// One entry of the per-`kind` node-count breakdown in [`EngineStats`].
#[derive(Debug, Serialize)]
pub struct KindCount {
    pub kind: String,
    pub count: u64,
}

/// A snapshot of the engine's storage/operation statistics, produced by
/// [`StorageEngine::stats`]. This owns all of its data (no borrows into
/// the engine), so the `GET /stats` handler can serialize it directly as
/// the wire response.
///
/// The field set and names are exactly the additive `GET /stats` wire
/// contract (a new endpoint, no change to any existing op — see
/// AGENT_LOG §4/§4b): this struct is the single source of truth for that
/// shape, serialized straight to JSON rather than mirrored into a second
/// response struct that could drift from it.
#[derive(Debug, Serialize)]
pub struct EngineStats {
    pub node_count: u64,
    pub edge_count: u64,
    pub user_count: u64,
    pub history_entries: u64,
    pub kinds: Vec<KindCount>,
    pub reads_total: u64,
    pub writes_total: u64,
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

    /// Predicated bulk delete — `ClearKind`'s superset. Removes every
    /// live node of `kind` the caller may write AND, when `where_` is
    /// `Some`, whose decoded `data` satisfies the predicate. Each
    /// removal takes the same WAL + tombstone path as a single
    /// `DeleteNode`, so a `DeleteWhere` is exactly "N deletes" that
    /// commit or roll back together with the rest of the batch.
    ///
    /// The predicate is evaluated by the same `predicate::eval` the
    /// `/nodes/query` path uses (via `delete_where_targets`), so a bulk
    /// delete and a query select identically. An unpushable/erroring
    /// predicate aborts the whole transaction before anything is written
    /// — never a wrong or partial delete. `where_ == None` behaves
    /// exactly like `ClearKind`.
    ///
    /// Authorization is carried on the op (resolved by the handler under
    /// the write lock), identical to `ClearKind`: a non-admin deletes
    /// only nodes it owns; an admin (`is_admin == true`) deletes every
    /// matching node regardless of owner.
    DeleteWhere {
        kind: String,
        where_: Option<Expr>,
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
                    None,
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
                .query_where(Some("thing"), None, None, None, "item", Some("score"), false, None, 2, 0)
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
                Some("thing"), None, None, None, "item", Some("score"), false,
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
                Some("thing"), None, None, None, "item", Some("score"), false,
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
            Some("thing"), None, None, Some(&expr), "item", None, false, None, 10, 0,
        );
        assert!(res.is_err(), "unpushable predicate must error");
    }

    /// Regression: an admin (`requester = None`) must list a Private node
    /// it does not own, while a non-admin (`requester = Some("bob")`) must
    /// not. This is the exact case that was broken: the route passed `""`
    /// as a fake "see everything" requester, but `can_read("")` only
    /// matches Public nodes or the owner `""`, so an admin listed nothing.
    #[test]
    fn admin_bypass_lists_private_nodes_others_cannot() {
        let mut e = StorageEngine::new();

        // A Private node owned by alice (Node::new defaults to Private).
        let mut node = Node::new(
            Coordinate::new(0, 0, 0, 0),
            "priv:1".to_string(),
            "secret".to_string(),
            "alice".to_string(),
        );
        node.data = "{}".to_string();
        assert_eq!(node.visibility, Visibility::Private, "precondition: private");
        e.nodes.insert(node.address.clone(), node);

        // query(): admin bypass (None) sees it; a non-owner does not.
        let admin = e.query(Some("secret"), None, None, 50, 0);
        assert_eq!(admin.len(), 1, "admin (None) must list the private node");
        assert_eq!(admin[0].address, "priv:1");

        let bob = e.query(Some("secret"), None, Some("bob"), 50, 0);
        assert!(bob.is_empty(), "non-admin bob must not see alice's private node");

        // query_where(): same bypass semantics.
        let admin_page = e
            .query_where(Some("secret"), None, None, None, "item", None, false, None, 50, 0)
            .expect("query_where ok");
        assert_eq!(admin_page.nodes.len(), 1, "admin (None) must list via query_where");
        assert_eq!(admin_page.nodes[0].address, "priv:1");

        let bob_page = e
            .query_where(Some("secret"), None, Some("bob"), None, "item", None, false, None, 50, 0)
            .expect("query_where ok");
        assert!(bob_page.nodes.is_empty(), "non-admin bob must not see it via query_where");
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

#[cfg(test)]
mod delete_where_tests {
    use super::*;
    use crate::core::coordinate::Coordinate;
    use crate::core::node::Node;
    use crate::core::predicate::Expr;
    use std::sync::{Mutex, OnceLock};

    // Same durable-path setup as clear_kind_tests: a process-wide temp
    // data dir set once, and one lock serializing the tests that append
    // to the shared WAL/binary/tombstone files. Every test uses unique
    // addresses/kinds so the shared dir never crosses assertions.
    fn disk_guard() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: Mutex<()> = Mutex::new(());
        static INIT: OnceLock<()> = OnceLock::new();
        INIT.get_or_init(|| {
            let dir = std::env::temp_dir()
                .join(format!("facetql-dwtest-{}", std::process::id()));
            std::fs::create_dir_all(&dir).expect("create temp data dir");
            crate::config::set_data_dir(dir);
        });
        LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn node_with(address: &str, kind: &str, owner: &str, data: &str) -> Node {
        let mut n = Node::new(
            Coordinate::new(0, 0, 0, 0),
            address.to_string(),
            kind.to_string(),
            owner.to_string(),
        );
        n.data = data.to_string();
        n
    }

    /// The predicate `item.status == status` — the same `Expr` shape a
    /// FCT-compiled `/nodes/query` predicate arrives as, evaluated by the
    /// same `predicate::eval`. Field access is written against the
    /// default loop variable `"item"` (delete_where carries no item_var).
    fn status_eq(status: &str) -> Expr {
        serde_json::from_value(serde_json::json!({
            "kind": "bin",
            "op": "==",
            "l": {
                "kind": "get",
                "field": "status",
                "obj": { "kind": "ref", "name": "item" }
            },
            "r": { "kind": "lit", "val": status, "vtype": "string" }
        }))
        .expect("valid predicate")
    }

    /// The predicate filters the kind down to only the matching rows;
    /// same-kind non-matching rows and other kinds are untouched.
    #[test]
    fn predicate_selects_only_matching_nodes() {
        let _g = disk_guard();
        let mut e = StorageEngine::new();
        e.insert(node_with("dw_sel:a", "DwSelEntity", "alice", r#"{"status":"expired"}"#)).unwrap();
        e.insert(node_with("dw_sel:b", "DwSelEntity", "alice", r#"{"status":"active"}"#)).unwrap();
        e.insert(node_with("dw_sel:c", "DwSelEntity", "alice", r#"{"status":"expired"}"#)).unwrap();
        e.insert(node_with("dw_sel:keep", "DwSelOther", "alice", r#"{"status":"expired"}"#)).unwrap();

        e.execute_transaction(vec![TxOperation::DeleteWhere {
            kind: "DwSelEntity".to_string(),
            where_: Some(status_eq("expired")),
            owner: "alice".to_string(),
            is_admin: false,
        }])
        .expect("delete_where commits");

        assert!(e.get("dw_sel:a").is_none(), "matching node deleted");
        assert!(e.get("dw_sel:c").is_none(), "matching node deleted");
        assert!(e.get("dw_sel:b").is_some(), "non-matching node of the kind survives");
        assert!(e.get("dw_sel:keep").is_some(), "other kind untouched");
    }

    /// A non-admin deletes only its own matching nodes; another owner's
    /// matching node of the same kind stays intact.
    #[test]
    fn non_admin_deletes_only_own_matching() {
        let _g = disk_guard();
        let mut e = StorageEngine::new();
        e.insert(node_with("dw_own:mine", "DwOwnEntity", "alice", r#"{"status":"expired"}"#)).unwrap();
        e.insert(node_with("dw_own:mine_ok", "DwOwnEntity", "alice", r#"{"status":"active"}"#)).unwrap();
        e.insert(node_with("dw_own:theirs", "DwOwnEntity", "bob", r#"{"status":"expired"}"#)).unwrap();

        e.execute_transaction(vec![TxOperation::DeleteWhere {
            kind: "DwOwnEntity".to_string(),
            where_: Some(status_eq("expired")),
            owner: "alice".to_string(),
            is_admin: false,
        }])
        .expect("non-admin delete_where commits");

        assert!(e.get("dw_own:mine").is_none(), "own matching node deleted");
        assert!(e.get("dw_own:mine_ok").is_some(), "own non-matching node survives");
        assert!(
            e.get("dw_own:theirs").is_some(),
            "another owner's matching node is left intact for a non-admin"
        );
    }

    /// An admin deletes every matching node of the kind regardless of
    /// owner (same admin bypass as clear_kind / the delete route).
    #[test]
    fn admin_deletes_all_matching() {
        let _g = disk_guard();
        let mut e = StorageEngine::new();
        e.insert(node_with("dw_adm:a", "DwAdmEntity", "alice", r#"{"status":"expired"}"#)).unwrap();
        e.insert(node_with("dw_adm:b", "DwAdmEntity", "bob", r#"{"status":"expired"}"#)).unwrap();
        e.insert(node_with("dw_adm:c", "DwAdmEntity", "carol", r#"{"status":"active"}"#)).unwrap();

        e.execute_transaction(vec![TxOperation::DeleteWhere {
            kind: "DwAdmEntity".to_string(),
            where_: Some(status_eq("expired")),
            owner: "root".to_string(),
            is_admin: true,
        }])
        .expect("admin delete_where commits");

        assert!(e.get("dw_adm:a").is_none(), "admin deleted alice's matching node");
        assert!(e.get("dw_adm:b").is_none(), "admin deleted bob's matching node");
        assert!(e.get("dw_adm:c").is_some(), "non-matching node survives even for admin");
    }

    /// Omitted `where` (`None`) degenerates to clear_kind: every
    /// writable node of the kind is deleted regardless of `data`.
    #[test]
    fn omitted_where_behaves_like_clear_kind() {
        let _g = disk_guard();
        let mut e = StorageEngine::new();
        e.insert(node_with("dw_all:a", "DwAllEntity", "alice", r#"{"status":"active"}"#)).unwrap();
        e.insert(node_with("dw_all:b", "DwAllEntity", "alice", r#"{"status":"expired"}"#)).unwrap();

        e.execute_transaction(vec![TxOperation::DeleteWhere {
            kind: "DwAllEntity".to_string(),
            where_: None,
            owner: "alice".to_string(),
            is_admin: false,
        }])
        .expect("omitted-where delete_where commits");

        assert!(e.get("dw_all:a").is_none(), "omitted where deletes all writable of the kind");
        assert!(e.get("dw_all:b").is_none(), "omitted where deletes all writable of the kind");
    }

    /// An unpushable predicate aborts the whole transaction and nothing
    /// is deleted — the query path's error-not-wrong-answer contract.
    #[test]
    fn unpushable_predicate_aborts_nothing_deleted() {
        let _g = disk_guard();
        let mut e = StorageEngine::new();
        e.insert(node_with("dw_bad:a", "DwBadEntity", "alice", r#"{"status":"expired"}"#)).unwrap();
        e.insert(node_with("dw_bad:b", "DwBadEntity", "alice", r#"{"status":"expired"}"#)).unwrap();

        // A bare `ref` node is not something `predicate::eval` can push
        // down — the exact shape query_where rejects.
        let expr: Expr = serde_json::from_value(serde_json::json!({
            "kind": "ref", "name": "somethingElse"
        }))
        .unwrap();

        let result = e.execute_transaction(vec![TxOperation::DeleteWhere {
            kind: "DwBadEntity".to_string(),
            where_: Some(expr),
            owner: "alice".to_string(),
            is_admin: false,
        }]);

        assert!(result.is_err(), "unpushable predicate must abort the transaction");
        assert!(e.get("dw_bad:a").is_some(), "nothing deleted on predicate error");
        assert!(e.get("dw_bad:b").is_some(), "nothing deleted on predicate error");
    }

    /// A delete_where is atomic with the rest of the batch: a later op
    /// that fails validation rolls the whole transaction back, including
    /// the delete_where — nothing is applied.
    #[test]
    fn rolls_back_when_a_sibling_op_fails() {
        let _g = disk_guard();
        let mut e = StorageEngine::new();
        e.insert(node_with("dw_atom:1", "DwAtomEntity", "alice", r#"{"status":"expired"}"#)).unwrap();
        e.insert(node_with("dw_atom:2", "DwAtomEntity", "alice", r#"{"status":"expired"}"#)).unwrap();

        // delete_where, then delete a node that doesn't exist. The
        // missing delete fails validation (pass 2), so pass 3 never runs
        // and the delete_where is never applied.
        let result = e.execute_transaction(vec![
            TxOperation::DeleteWhere {
                kind: "DwAtomEntity".to_string(),
                where_: Some(status_eq("expired")),
                owner: "alice".to_string(),
                is_admin: false,
            },
            TxOperation::DeleteNode("dw_atom:missing".to_string()),
        ]);

        assert!(result.is_err(), "batch must fail on the missing delete target");
        assert!(e.get("dw_atom:1").is_some(), "delete_where rolled back with the batch");
        assert!(e.get("dw_atom:2").is_some(), "delete_where rolled back with the batch");
    }

    /// Each deleted node is tombstoned + WAL-logged like a standalone
    /// delete, so a fresh recovery from durable storage no longer sees
    /// it — while a non-matching node of the kind recovers normally.
    #[test]
    fn deleted_nodes_do_not_survive_recovery() {
        let _g = disk_guard();
        let mut e = StorageEngine::new();
        e.insert(node_with("dw_rec:gone", "DwRecEntity", "alice", r#"{"status":"expired"}"#)).unwrap();
        e.insert(node_with("dw_rec:stay", "DwRecEntity", "alice", r#"{"status":"active"}"#)).unwrap();

        e.execute_transaction(vec![TxOperation::DeleteWhere {
            kind: "DwRecEntity".to_string(),
            where_: Some(status_eq("expired")),
            owner: "alice".to_string(),
            is_admin: false,
        }])
        .expect("delete_where commits");

        // Rebuild purely from durable storage (data files + tombstones).
        let recovered = StorageEngine::load().expect("recovery load");
        assert!(
            recovered.get("dw_rec:gone").is_none(),
            "delete_where's tombstone survived recovery"
        );
        assert!(
            recovered.get("dw_rec:stay").is_some(),
            "non-matching node recovered from durable storage"
        );
    }
}

#[cfg(test)]
mod stats_tests {
    use super::*;
    use crate::core::coordinate::Coordinate;
    use crate::core::node::Node;
    use std::sync::{Mutex, OnceLock};

    // Same durable-path discipline as clear_kind_tests / delete_where_tests:
    // `insert` exercises the real WAL/binary path, which needs a data dir
    // resolved through the process-wide `OnceLock`. One lock serializes the
    // tests that append to the shared files; unique kinds/addresses keep
    // assertions from crossing between tests.
    fn disk_guard() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: Mutex<()> = Mutex::new(());
        static INIT: OnceLock<()> = OnceLock::new();
        INIT.get_or_init(|| {
            let dir = std::env::temp_dir()
                .join(format!("facetql-statstest-{}", std::process::id()));
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

    /// Insert N nodes across two kinds and do M gets, then assert the
    /// snapshot: exact `node_count`, per-kind grouping that is both
    /// correct and sorted, and operation counters that reflect at least
    /// the writes/reads we performed (>= because these are process-
    /// lifetime counters other tests may not have touched — a fresh
    /// engine starts them at 0, but the assertion is written to hold
    /// regardless).
    #[test]
    fn stats_counts_kinds_and_operations() {
        let _g = disk_guard();
        let mut e = StorageEngine::new();

        // N = 5 nodes: 3 of "StatAlpha", 2 of "StatBeta".
        e.insert(node_owned("stat:a1", "StatAlpha", "alice")).unwrap();
        e.insert(node_owned("stat:a2", "StatAlpha", "alice")).unwrap();
        e.insert(node_owned("stat:a3", "StatAlpha", "alice")).unwrap();
        e.insert(node_owned("stat:b1", "StatBeta", "alice")).unwrap();
        e.insert(node_owned("stat:b2", "StatBeta", "alice")).unwrap();
        let n: u64 = 5;

        // M = 4 gets (reads).
        let m: u64 = 4;
        for addr in ["stat:a1", "stat:a2", "stat:b1", "stat:missing"] {
            let _ = e.get(addr);
        }

        let s = e.stats();

        assert_eq!(s.node_count, n, "node_count must equal inserted N");

        // Grouping is exact and sorted (BTreeMap → ascending by kind:
        // "StatAlpha" before "StatBeta").
        assert_eq!(s.kinds.len(), 2, "two distinct kinds");
        assert_eq!(s.kinds[0].kind, "StatAlpha");
        assert_eq!(s.kinds[0].count, 3);
        assert_eq!(s.kinds[1].kind, "StatBeta");
        assert_eq!(s.kinds[1].count, 2);

        // Fresh engine: counters started at 0, so they equal exactly what
        // we did here — but assert `>=` so the test is robust to the
        // counting placement (each insert = 1 write, each get = 1 read).
        assert!(s.writes_total >= n, "writes_total {} >= {n}", s.writes_total);
        assert!(s.reads_total >= m, "reads_total {} >= {m}", s.reads_total);

        // Structural fields we can pin exactly on a fresh engine.
        assert_eq!(s.edge_count, 0, "no edges inserted");
        assert_eq!(s.user_count, 0, "no users inserted");
        assert_eq!(s.history_entries, 0, "no overwrites, so no history");
    }
}