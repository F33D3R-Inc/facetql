use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Serialize, Deserialize};

use crate::core::edge::{Edge, EdgeId};
use crate::core::history::HistoryEntry;
use crate::core::node::Node;
use crate::core::predicate::{self, Expr};
use crate::core::user::UserRecord;
use crate::storage::binary::{self, EdgeRecord, NodeRecord, UserOpRecord};
use crate::storage::index::Index;
use crate::storage::transaction::{Operation, Transaction};
use crate::storage::wal;

/// Loop-variable name a `delete_where` predicate's field accesses are
/// written against. The `delete_where` wire contract (§4b) carries no
/// `item_var` — unlike `/nodes/query` — so it uses the same default the
/// query path does (`default_item_var` in `api::routes`): `"item"`.
const DELETE_WHERE_ITEM_VAR: &str = "item";

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
    /// `writes_total` counts each applied mutation, and it is counted in
    /// exactly one place: `apply_committed`, the single apply step every
    /// mutation now passes through — the framed transaction path and the
    /// standalone single-record path alike (see `apply_atomic`). One
    /// counting site is the point: while `insert`/`delete`/`insert_edge`
    /// each counted for themselves *and* `apply_committed` counted for
    /// the transaction path, the number a mutation contributed depended
    /// on which of the two carried it, so moving a path from one to the
    /// other would silently change the statistic. Now a mutation
    /// contributes one increment per node it actually mutates (a
    /// `ClearKind`/`DeleteWhere` of N nodes counts as N writes, the
    /// truthful workload signal) whichever path carried it. Counting at
    /// the apply step also means a mutation that fails validation, or a
    /// frame that never reaches its `COMMIT`, writes nothing and counts
    /// nothing.
    ///
    /// An `Archive` is deliberately not counted: it is the history half
    /// of the overwrite or delete that produced it rather than a
    /// mutation anyone asked for, and counting it would double every
    /// upsert. User records (`InsertUser`/`RevokeUser`) are likewise
    /// uncounted, preserving what `insert_user`/`revoke_user` have
    /// always done — identity administration is not data workload.
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
    /// Its one caller is [`Self::apply_atomic`]'s single-record branch,
    /// and it must stay that way: a mutation that decides for itself to
    /// append here is a mutation that has stepped outside the
    /// frame-vs-standalone rule, and a record appended alongside a
    /// framed operation is replayed twice by recovery — once in the
    /// frame, once as a standalone record.
    ///
    /// Returns the WAL sequence number assigned to this record so the
    /// caller can advance the durability checkpoint once the matching
    /// physical write has also completed. See `storage::checkpoint`.
    ///
    /// Sequence and operation IDs come from `wal`'s counters, which are
    /// the single source of truth for WAL identifiers. The engine must
    /// not keep a second counter: staged transactions
    /// (`storage::commit::StagedCommit`) allocate from `wal` too, and two
    /// independent counters in one process would hand out duplicate
    /// sequence numbers, breaking the strictly-increasing invariant
    /// `recovery::validate_sequence` enforces — which would make the
    /// *next* startup fail to recover at all.
    fn append_wal(
        &self,
        transaction_id: u64,
        operation: wal::WalOperation,
    ) -> Result<u64, String> {
        let sequence = wal::next_sequence();

        let record = wal::WalRecord::new(
            sequence,
            transaction_id,
            wal::next_operation_id(),
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
    // Durable apply: the frame-vs-standalone rule
    // ---------------------------------------------------------------------

    /// Apply one logical mutation — the ordered list of resolved
    /// [`Operation`]s it lowers to — durably and atomically.
    ///
    /// **The rule this function exists to hold, and the one a future
    /// reader must not accidentally violate:**
    ///
    /// > **Two or more durable records ⇒ frame.
    /// > Exactly one ⇒ standalone.**
    ///
    /// Every mutation primitive routes through here so that rule is
    /// decided in one place instead of being re-decided — and eventually
    /// mis-decided — at each call site.
    ///
    /// ## Why two records must be framed
    ///
    /// A mutation that emits two independent standalone records is not
    /// atomic no matter how carefully its halves are ordered:
    ///
    /// ```text
    /// WAL Archive(tx 0)   ← durable
    /// history record      ← durable
    /// checkpoint advanced ← the archive is now "settled"
    ///        ✗ crash
    /// WAL Insert(tx 0)    ← never written
    /// ```
    ///
    /// Recovery starts above the checkpoint, so it never revisits the
    /// archive — and there is no insert record to replay. The node keeps
    /// its old value while history claims that value was superseded: a
    /// durable half-mutation no later run can repair. Staging both
    /// records under one `BEGIN … COMMIT` frame removes the in-between,
    /// because recovery replays a frame only when it sees the `COMMIT`,
    /// so the pair lands together or not at all.
    ///
    /// ## Why one record must not be
    ///
    /// A single-record mutation is already atomic — one fsync'd WAL
    /// record, then one physical write — so a frame would buy nothing
    /// while costing two extra fsync'd control records (`BEGIN` and
    /// `COMMIT`) and a checkpoint fence on the single-writer mutation
    /// path. The cheap path stays cheap.
    ///
    /// ## Who writes what
    ///
    /// Both branches converge on [`Self::apply_committed`] for the apply
    /// itself, so memory, physical storage and `writes_total` are
    /// touched by exactly one body of code regardless of route. The
    /// standalone branch owns the WAL record and the checkpoint advance
    /// that `apply_committed` deliberately does not do; the framed
    /// branch leaves both to [`Transaction::commit`]. Neither a caller
    /// nor `apply_committed` may add its own: a framed operation that
    /// also appended a standalone record would be replayed twice by
    /// recovery — once inside the frame, once outside it — and a framed
    /// operation that advanced the checkpoint itself would re-open the
    /// very window the frame closes.
    fn apply_atomic(
        &mut self,
        operations: Vec<Operation>,
    ) -> Result<(), String> {
        match operations.len() {
            // Nothing resolved to nothing: no record, no frame, no
            // checkpoint movement. An empty mutation is a no-op, not a
            // WAL entry.
            0 => Ok(()),

            // -----------------------------------------------------
            // Exactly one durable record: standalone (tx id 0).
            // -----------------------------------------------------
            //
            // WAL intent → physical + memory → checkpoint. The
            // checkpoint moves last, once `apply_committed` has put the
            // record in physical storage, so it never claims durability
            // that recovery could not back up.
            // -----------------------------------------------------
            1 => {
                let operation = &operations[0];

                let sequence = self.append_wal(0, operation.to_wal())?;

                self.apply_committed(operation)?;

                Self::advance_checkpoint(sequence);

                Ok(())
            }

            // -----------------------------------------------------
            // Two or more: one implicit single-statement transaction.
            // -----------------------------------------------------
            //
            // The same machinery `execute_transaction` uses for a wire
            // batch; the only difference is that a mutation primitive
            // resolved this operation list instead of
            // `lower_transaction` lowering it from a request. Same
            // frame, same recovery rule, same atomicity.
            // -----------------------------------------------------
            _ => Transaction::from_operations(operations)
                .commit(|operation| self.apply_committed(operation))
                // `Transaction::commit` speaks `io::Result`; the
                // mutation API speaks `Result<_, String>`. Carry the
                // message across rather than collapsing it into a
                // generic failure — it is the only account the caller
                // gets of what went wrong.
                .map_err(|e| e.to_string()),
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

    /// Memory-only edge removal, the recovery counterpart of
    /// [`Self::delete_edge`].
    ///
    /// Removes the edge from **both** adjacency maps: they are two views
    /// of one set, and a recovery that pruned only `edges_out` would
    /// leave `edges_to` answering with a relationship that no longer
    /// exists — a difference nothing later would ever reconcile, because
    /// nothing rebuilds the maps against each other.
    ///
    /// Naturally idempotent, which recovery depends on: replaying a
    /// `DeleteEdge` against an edge that `load()` already resolved away
    /// (or that an earlier replay removed) finds nothing to retain and
    /// returns `Ok`, exactly as `replay_delete` does for a node that is
    /// already gone. Idempotence is what lets the checkpoint stay an
    /// optimization rather than a correctness barrier.
    pub(crate) fn replay_delete_edge(
        &mut self,
        id: &EdgeId,
    ) -> Result<(), String> {
        self.unindex_edge(id);
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
    /// facetql.data     — a log of `NodeRecord`s
    /// facetql.edges    — a log of `EdgeRecord`s
    /// facetql.users    — a log of `UserOpRecord`s
    /// facetql.history  — a log of `HistoryEntry`s (append-only, no deletes)
    ///
    /// Each is a **straight replay in file order**: a `Put` inserts or
    /// overwrites its key, a `Delete`/`Revoke` removes it, and the last
    /// record for a key wins. That is the whole algorithm — there is no
    /// second pass and no other log to reconcile against.
    ///
    /// ## Why that is the fix, and not just a simplification
    ///
    /// This used to end with a fifth pass over `facetql.tombstones`,
    /// removing every tombstoned address *after* the data log had been
    /// replayed. Two append-only logs share no ordering, so nothing on
    /// disk could say whether a delete happened before or after a
    /// neighbouring create — and applying the tombstones last meant a
    /// tombstone always won, whenever it was written:
    ///
    ///     create X → delete X → create X again → restart → X is gone.
    ///
    /// That is silent data loss on a perfectly ordinary sequence, and it
    /// applied to users too (revocations were `user:`-prefixed entries in
    /// the same log). Moving each entity's deletes into that entity's own
    /// log makes file order the total order for that entity type, so
    /// last-write-wins is decided by the same thing that decides it for
    /// every other write: position in the file.
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

        for (offset, record) in binary::read_all()? {
            match record {
                NodeRecord::Put(node) => {
                    engine.index.insert(node.address.clone(), offset);
                    engine.nodes.insert(node.address.clone(), node);
                }

                // The address is gone as of this point in the log. A
                // later `Put` for the same address re-creates it — that
                // is the whole point of carrying deletes here.
                NodeRecord::Delete(address) => {
                    engine.nodes.remove(&address);
                }
            }
        }

        // -------------------------------------------------------------
        // Edges
        // -------------------------------------------------------------
        //
        // Resolved to the live set FIRST, then indexed — deliberately not
        // pushed into the adjacency lists as the log is walked. The
        // adjacency lists are `Vec`s, so a blind walk would leave a
        // deleted edge sitting in `edges_out`/`edges_in` unless every
        // `Delete` also scanned and spliced both vectors, and any edge
        // re-created after a delete would have to be de-duplicated
        // against the copy still in there.
        //
        // `order` records the position of each `Put` so the rebuilt
        // adjacency lists are deterministic rather than dependent on
        // `HashMap` iteration order: walking it in reverse and emitting
        // each surviving edge the first time it is seen places every
        // edge at its most recent `Put` — i.e. exactly the file order of
        // the record that won.
        // -------------------------------------------------------------

        let mut order: Vec<EdgeId> = Vec::new();
        let mut live: HashMap<EdgeId, Edge> = HashMap::new();

        for (_offset, record) in
            binary::read_all_records::<EdgeRecord>(&binary::edges_path())?
        {
            match record {
                EdgeRecord::Put(edge) => {
                    order.push(edge.id());
                    live.insert(edge.id(), edge);
                }

                EdgeRecord::Delete(id) => {
                    live.remove(&id);
                }
            }
        }

        let mut resolved = Vec::with_capacity(live.len());

        for id in order.into_iter().rev() {
            // `remove` both fetches and consumes, so an edge that was
            // written more than once is emitted once, at its last `Put`.
            if let Some(edge) = live.remove(&id) {
                resolved.push(edge);
            }
        }

        for edge in resolved.into_iter().rev() {
            engine.index_edge(edge);
        }

        // -------------------------------------------------------------
        // Users
        // -------------------------------------------------------------
        //
        // Revocations live here now rather than as `user:`-prefixed
        // entries in a shared tombstone log. With one log per entity
        // there are no two key spaces to keep from colliding, so the
        // prefix is gone too — and a revoked token hash can be re-issued
        // and revoked again, which the old permanent tombstone made
        // impossible.
        // -------------------------------------------------------------

        for (_offset, record) in
            binary::read_all_records::<UserOpRecord>(&binary::users_path())?
        {
            match record {
                UserOpRecord::Put(user) => {
                    engine.users.insert(user.token_hash.clone(), user);
                }

                UserOpRecord::Revoke(token_hash) => {
                    engine.users.remove(&token_hash);
                }
            }
        }

        // -------------------------------------------------------------
        // History
        // -------------------------------------------------------------
        //
        // No operation enum: history is strictly additive. Nothing ever
        // supersedes or removes an archived state, so file order is
        // already the only order it has.
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
    /// archived before the new state is written. Upsert semantics are
    /// unchanged, including the absence of an ownership check here — a
    /// cross-owner overwrite is rejected on the transaction path, which
    /// is where the batch's owner context exists.
    ///
    /// Durability follows the rule in [`Self::apply_atomic`], and an
    /// overwrite is the canonical two-record case:
    ///
    /// create (one record — standalone):
    ///
    /// WAL Insert(tx 0)
    ///     ↓
    /// node durable
    ///     ↓
    /// memory
    ///     ↓
    /// checkpoint
    ///
    /// overwrite (two records — framed):
    ///
    /// BEGIN(tx)
    ///     ↓
    /// WAL Archive(tx) → WAL Insert(tx)   ← both durable, nothing visible
    ///     ↓
    /// COMMIT(tx)                         ← the atomic commit point
    ///     ↓
    /// history durable → node durable → memory
    ///     ↓
    /// checkpoint past COMMIT
    ///
    /// Before the frame, the archive was written, made durable *and*
    /// checkpointed before the insert record even existed, so a crash in
    /// between left history claiming a value had been superseded by a
    /// value that never landed — and the already-advanced checkpoint put
    /// that state permanently out of recovery's reach. The archive and
    /// the insert it precedes are one mutation, so they land as one.
    pub fn insert(&mut self, node: Node) -> Result<(), String> {
        let mut operations = Vec::with_capacity(2);

        // -------------------------------------------------------------
        // Archive the previous value.
        // -------------------------------------------------------------
        //
        // Staged immediately ahead of the insert that supersedes it —
        // the same ordering `lower_transaction` produces for an
        // overwrite — so recovery rebuilds history identically whichever
        // path wrote the frame.
        // -------------------------------------------------------------

        if let Some(previous) = self.nodes.get(&node.address) {
            operations.push(Operation::Archive(
                HistoryEntry::now(previous.clone()),
            ));
        }

        operations.push(Operation::Insert(node));

        self.apply_atomic(operations)
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
    ///
    /// The write half is a read-modify-write, and its target exists by
    /// definition (a missing node has already returned `NotFound`), so
    /// the `insert` below always archives and therefore always resolves to
    /// the two-record `Archive` + `Insert` frame (see
    /// [`Self::apply_atomic`]). A claim is where a torn mutation would
    /// hurt most: a crash between the archive and the insert used to
    /// leave the node *unclaimed* while history recorded that it had
    /// been superseded, so the slot looked free to the next worker and
    /// the archive looked like a claim that had been taken away. The
    /// frame closes that window. Nothing else about the claim contract
    /// changes — the `ClaimError` variants and the "already claimed by
    /// X" text are the caller's API and are untouched.
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
    ///
    /// The value being removed is archived to history first, exactly as
    /// an overwrite archives the value it replaces. A delete is the last
    /// thing that ever happens to a node's state, so if it is the one
    /// transition that doesn't archive, the final state is the one state
    /// nobody can ever look up again — `GET /node/:address/history`
    /// would show every version except the one that mattered when
    /// somebody asks what was deleted.
    ///
    /// That archive makes a live delete a two-record mutation, so it
    /// goes through a frame under the rule in [`Self::apply_atomic`]:
    ///
    /// BEGIN(tx) → WAL Archive(tx) → WAL Delete(tx) → COMMIT(tx)
    ///     ↓
    /// history durable → tombstone durable → memory
    ///     ↓
    /// checkpoint past COMMIT
    ///
    /// A crash can no longer leave the history entry durable with the
    /// node still live, which is what the old archive-then-checkpoint,
    /// delete-then-checkpoint pair allowed.
    ///
    /// Deleting an address that holds nothing archives nothing, so it is
    /// a single `Delete` record and stays on the cheaper standalone
    /// path. The tombstone is still written in that case, deliberately:
    /// deleting an absent address is idempotent rather than an error, so
    /// a repeated delete is harmless and a delete of something a
    /// concurrent writer already removed still ends with the address
    /// durably gone.
    pub fn delete(
        &mut self,
        address: &str,
    ) -> Result<(), String> {
        let mut operations = Vec::with_capacity(2);

        if let Some(existing) = self.nodes.get(address) {
            operations.push(Operation::Archive(
                HistoryEntry::now(existing.clone()),
            ));
        }

        operations.push(Operation::Delete(address.to_string()));

        self.apply_atomic(operations)
    }

    /// Does one node fall inside a bulk-delete selection?
    ///
    /// This is the single selection rule behind both `clear_kind` and
    /// `delete_where`: the node's `kind` must match, the caller must be
    /// allowed to write it (an admin matches all of that kind; a
    /// non-admin only what it owns, via the same `can_write` check the
    /// delete route uses), and — when a `where_` predicate is present —
    /// the predicate must hold against the node's decoded `data`.
    ///
    /// `clear_kind` is exactly this rule with `where_ == None`, which is
    /// why there is one function here and not two: the two wire ops
    /// differ only in whether a predicate participates.
    ///
    /// The predicate is run through the *same* `predicate::eval` the
    /// `/nodes/query` path uses (see `query_where`), so a predicated
    /// bulk delete selects byte-for-byte the rows the equivalent query
    /// would — one evaluator, not two. A predicate `eval` can't push
    /// down (or otherwise errors) is surfaced as `Err`, which the
    /// transaction turns into a whole-batch abort — never a wrong or
    /// partial delete.
    fn selection_matches(
        node: &Node,
        kind: &str,
        where_: Option<&Expr>,
        owner: &str,
        is_admin: bool,
    ) -> Result<bool, String> {
        if node.kind != kind {
            return Ok(false);
        }

        if !(is_admin || node.can_write(owner)) {
            return Ok(false);
        }

        let expr = match where_ {
            Some(expr) => expr,
            None => return Ok(true),
        };

        let data: serde_json::Value =
            serde_json::from_str(&node.data).unwrap_or(serde_json::Value::Null);

        predicate::eval(expr, DELETE_WHERE_ITEM_VAR, &data)
            .map(|v| matches!(v, serde_json::Value::Bool(true)))
            .map_err(|e| format!("predicate evaluation failed: {e}"))
    }

    /// The node a successful `SetIf` produces, or why its condition did
    /// not hold.
    ///
    /// One rule in one place: the transaction path calls this to build
    /// the mutation, and nothing else evaluates a CAS condition. The
    /// condition is tested against `node`'s decoded `data`, and on
    /// success `set`'s entries are *merged* into that same object rather
    /// than replacing it — so a caller can move one field without having
    /// to resend, and risk clobbering, the rest of the node it did not
    /// intend to touch.
    fn set_if_next(
        node: &Node,
        field: &str,
        expect: &Expectation,
        set: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<Node, TransactionError> {
        // A node whose data was never written is an empty object, so a
        // set_if can initialise one (`expect_absent`) rather than
        // requiring a separate seeding write.
        let mut data = if node.data.trim().is_empty() {
            serde_json::Map::new()
        } else {
            match serde_json::from_str::<serde_json::Value>(&node.data) {
                Ok(serde_json::Value::Object(map)) => map,
                Ok(serde_json::Value::Null) => serde_json::Map::new(),
                Ok(_) => {
                    return Err(TransactionError::Invalid(format!(
                        "set_if target {} does not hold a JSON object in `data`",
                        node.address
                    )))
                }
                Err(e) => {
                    return Err(TransactionError::Invalid(format!(
                        "set_if target {} has undecodable `data`: {e}",
                        node.address
                    )))
                }
            }
        };

        let current = data.get(field);

        let holds = match expect {
            // A non-numeric or missing value is not "less than" anything
            // — it fails rather than being coerced, because a lease
            // check that silently treats a malformed deadline as due
            // would hand the same slot to everyone.
            Expectation::AtMost(bound) => {
                match current.and_then(serde_json::Value::as_f64) {
                    Some(value) => value <= *bound,
                    None => false,
                }
            }

            Expectation::Equals(expected) => current == Some(expected),

            Expectation::Absent => {
                matches!(current, None | Some(serde_json::Value::Null))
            }
        };

        if !holds {
            return Err(TransactionError::Precondition(format!(
                "set_if precondition failed on {}.{field}",
                node.address
            )));
        }

        for (key, value) in set {
            data.insert(key.clone(), value.clone());
        }

        let mut next = node.clone();
        next.data = serde_json::Value::Object(data).to_string();

        Ok(next)
    }

    /// Live addresses a `DeleteWhere` would remove: every node whose
    /// `kind` matches and that the caller may write, narrowed further by
    /// a `where_` predicate evaluated against each candidate's decoded
    /// `data`.
    ///
    /// The predicate is run through the *same* `predicate::eval` the
    /// `/nodes/query` path uses (see `query_where`), so a predicated
    /// bulk delete selects byte-for-byte the rows the equivalent query
    /// would — one evaluator, not two. `where_ == None` degenerates to
    /// exactly a `clear_kind` (all writable nodes of the kind).
    ///
    /// A predicate `eval` can't push down (or otherwise errors) is
    /// surfaced as `Err`, mirroring how `query_where` surfaces it. The
    /// transaction turns that `Err` into a whole-batch abort — never a
    /// wrong or partial delete.
    ///
    /// Read-only: it computes addresses without touching WAL, disk, or
    /// memory, so the handler can call it to report the exact addresses
    /// a delete will tombstone. The transaction path resolves its own
    /// targets through `staged_selection`, which applies the same rule
    /// to the batch's in-progress view rather than to live state.
    ///
    /// Returned addresses are sorted, so the selection is deterministic
    /// rather than dependent on `HashMap` iteration order.
    pub(crate) fn delete_where_targets(
        &self,
        kind: &str,
        where_: Option<&Expr>,
        owner: &str,
        is_admin: bool,
    ) -> Result<Vec<String>, String> {
        let mut targets = Vec::new();

        for node in self.nodes.values() {
            if Self::selection_matches(node, kind, where_, owner, is_admin)? {
                targets.push(node.address.clone());
            }
        }

        targets.sort();

        Ok(targets)
    }

    // ---------------------------------------------------------------------
    // Edges
    // ---------------------------------------------------------------------

    /// Creates — or replaces — a relationship between two existing
    /// nodes.
    ///
    /// Both endpoints must exist before the edge is written.
    ///
    /// **Upsert on identity.** `(from, to, kind)` is what an edge is
    /// (see [`EdgeId`]), so re-asserting an existing relationship lands
    /// on the same edge rather than beside it. It used to push a second
    /// copy into both adjacency lists, and `edges_from` then returned
    /// the same relationship twice — a traversal double-counted it, and
    /// a delete could only ever have removed one of the copies.
    ///
    /// Replacing an edge owned by someone else is rejected, mirroring
    /// the cross-owner rejection `insert` does for nodes on the
    /// transaction path: the owner is who may *retract* the edge, so
    /// silently overwriting it would hand that right to whoever asserted
    /// the relationship most recently.
    ///
    /// One durable record, so this stays standalone under the rule in
    /// [`Self::apply_atomic`]: the WAL record and the edges-file append
    /// are already a single atomic mutation, and framing them would add
    /// two control records and a checkpoint fence to buy nothing.
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

        if let Some(existing) = self.find_edge(&edge.id()) {
            if existing.owner != edge.owner {
                return Err(format!(
                    "edge {} -[{}]-> {} is owned by {}",
                    edge.from,
                    edge.kind,
                    edge.to,
                    existing.owner
                ));
            }
        }

        self.apply_atomic(vec![Operation::InsertEdge(edge)])
    }

    /// The live edge with this identity, or `None`.
    ///
    /// Looks in `edges_out` only: every edge is indexed into `edges_out`
    /// and `edges_in` together and removed from both together, so one is
    /// a faithful witness for the other.
    ///
    /// Compares the three identity fields directly instead of building an
    /// [`EdgeId`] per candidate — `Edge::id()` clones three `String`s,
    /// which is wasted work inside a scan.
    pub fn find_edge(
        &self,
        id: &EdgeId,
    ) -> Option<&Edge> {
        self.edges_out
            .get(&id.from)?
            .iter()
            .find(|edge| edge_has_id(edge, id))
    }

    /// Removes one edge.
    ///
    /// Errors when there is no such edge, so a caller (and the
    /// `DELETE /edge` route above it) can tell "removed" from "was never
    /// there" instead of reporting success for a relationship that never
    /// existed. Authorization is the caller's: this is the storage
    /// primitive, and the ownership check lives where the requester's
    /// identity does — the route, or `TxOperation::DeleteEdge`.
    ///
    /// Goes through [`Self::apply_atomic`] like every other mutation,
    /// and must keep doing so. Hand-rolling the WAL append or the
    /// checkpoint advance here would put a second copy of the
    /// frame-vs-standalone rule in the codebase, and the copy that is
    /// only exercised by one route is the one that silently rots.
    pub fn delete_edge(
        &mut self,
        id: &EdgeId,
    ) -> Result<(), String> {
        if self.find_edge(id).is_none() {
            return Err(format!(
                "edge not found: {} -[{}]-> {}",
                id.from,
                id.kind,
                id.to
            ));
        }

        self.apply_atomic(vec![Operation::DeleteEdge(id.clone())])
    }

    /// Places `edge` in both adjacency maps, **replacing** any edge that
    /// already has its identity rather than appending a duplicate.
    ///
    /// This is the single place both maps are written, so replace-by-
    /// identity has to happen here or not at all: the live path, the
    /// `load()` rebuild and WAL replay all arrive through this function,
    /// and a duplicate introduced by any one of them is permanent —
    /// nothing scans the vectors afterwards to notice two copies of the
    /// same relationship.
    fn index_edge(
        &mut self,
        edge: Edge,
    ) {
        let id = edge.id();

        let outgoing = self.edges_out.entry(edge.from.clone()).or_default();

        match outgoing.iter_mut().find(|e| edge_has_id(e, &id)) {
            Some(slot) => *slot = edge.clone(),
            None => outgoing.push(edge.clone()),
        }

        let incoming = self.edges_in.entry(edge.to.clone()).or_default();

        match incoming.iter_mut().find(|e| edge_has_id(e, &id)) {
            Some(slot) => *slot = edge,
            None => incoming.push(edge),
        }
    }

    /// Removes the edge with this identity from both adjacency maps.
    ///
    /// The mirror of [`Self::index_edge`], and likewise the only place
    /// edges leave the maps. Empty vectors are dropped rather than left
    /// behind: `edges_out`'s keys would otherwise accumulate one entry
    /// per node that ever had an outgoing edge, and `stats()` sums the
    /// vectors it holds, so a map full of empty ones is pure overhead
    /// with no reader.
    fn unindex_edge(
        &mut self,
        id: &EdgeId,
    ) {
        if let Some(outgoing) = self.edges_out.get_mut(&id.from) {
            outgoing.retain(|edge| !edge_has_id(edge, id));

            if outgoing.is_empty() {
                self.edges_out.remove(&id.from);
            }
        }

        if let Some(incoming) = self.edges_in.get_mut(&id.to) {
            incoming.retain(|edge| !edge_has_id(edge, id));

            if incoming.is_empty() {
                self.edges_in.remove(&id.to);
            }
        }
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

    /// Creates a node and one or more outgoing edges as one crash-atomic
    /// mutation.
    ///
    /// The node and every edge are resolved and validated first, then
    /// staged into a single `BEGIN … COMMIT` frame (see
    /// [`Self::apply_atomic`]), so the whole shape — the node, the
    /// history entry if it replaced something, and all N edges — becomes
    /// visible together or not at all.
    ///
    /// This used to be explicitly best-effort: the node was inserted and
    /// checkpointed, then each edge was inserted and checkpointed, and a
    /// failing edge triggered a compensating `delete` of the node just
    /// created. That left three ways to end up with wreckage — a crash
    /// between the node and an edge, a crash between two edges, and a
    /// crash during the compensation itself — and compensation is not
    /// rollback: it wrote *more* durable records (a tombstone, another
    /// history entry) to approximate undoing records already on disk.
    /// Validating up front and committing once removes both the
    /// compensation and the windows it was papering over.
    ///
    /// Endpoint validation runs against this call's own staged view: an
    /// edge may point at any live node, or at the node being created
    /// here — which is not live yet but is staged ahead of every edge in
    /// the frame, exactly as the transaction path's `staged_node` view
    /// resolves an intra-batch reference. The error text is unchanged
    /// from the per-edge path, since it is the caller-visible contract.
    ///
    /// The error tuple keeps its shape, but its `Vec<Edge>` is now
    /// always empty: once the batch is atomic there is no such thing as
    /// "edges created before the failure" — a failure means nothing was
    /// created. An empty list is the truthful answer, where the old
    /// partial list described durable wreckage the caller then had to
    /// clean up.
    pub fn insert_with_edges(
        &mut self,
        node: Node,
        edge_targets: Vec<(String, String)>,
    ) -> Result<Vec<Edge>, (String, Vec<Edge>)> {
        let address = node.address.clone();
        let owner = node.owner.clone();

        let mut operations = Vec::with_capacity(2 + edge_targets.len());

        // An overwrite archives what it replaces, exactly as `insert`
        // does — carried inside this frame rather than settled ahead of
        // it as its own record.
        if let Some(previous) = self.nodes.get(&address) {
            operations.push(Operation::Archive(
                HistoryEntry::now(previous.clone()),
            ));
        }

        operations.push(Operation::Insert(node));

        let mut created = Vec::with_capacity(edge_targets.len());

        for (to, kind) in edge_targets {
            let edge = Edge::new(
                address.clone(),
                to,
                kind,
                owner.clone(),
            );

            // `from` is the node this call creates, so it counts as
            // present even though it is not in `self.nodes` yet: it is
            // staged ahead of every edge in the frame. The check is
            // written out rather than assumed, so the staged rule stays
            // visible if the edge source ever stops being the new node.
            if edge.from != address && !self.nodes.contains_key(&edge.from) {
                return Err((
                    format!(
                        "edge 'from' address not found: {}",
                        edge.from
                    ),
                    Vec::new(),
                ));
            }

            if edge.to != address && !self.nodes.contains_key(&edge.to) {
                return Err((
                    format!(
                        "edge 'to' address not found: {}",
                        edge.to
                    ),
                    Vec::new(),
                ));
            }

            operations.push(Operation::InsertEdge(edge.clone()));

            created.push(edge);
        }

        self.apply_atomic(operations)
            .map_err(|e| (e, Vec::new()))?;

        Ok(created)
    }

    // ---------------------------------------------------------------------
    // Users
    // ---------------------------------------------------------------------

    /// Persists a new user record.
    ///
    /// Only the token hash is persisted. Plaintext tokens are never stored
    /// in the engine.
    ///
    /// One durable record — the WAL record plus the users-file append —
    /// so this stays standalone under the rule in
    /// [`Self::apply_atomic`].
    pub fn insert_user(
        &mut self,
        record: UserRecord,
    ) -> Result<(), String> {
        self.apply_atomic(vec![Operation::InsertUser(record)])
    }

    /// Revokes a user by token hash.
    ///
    /// User tombstones are namespaced with "user:".
    ///
    /// One durable record — the WAL record plus the tombstone — so this
    /// stays standalone too. See [`Self::apply_atomic`].
    pub fn revoke_user(
        &mut self,
        token_hash: &str,
    ) -> Result<(), String> {
        self.apply_atomic(vec![Operation::RevokeUser(
            token_hash.to_string(),
        )])
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

    /// Executes a batch of operations as one crash-atomic transaction.
    ///
    /// The batch is *validated and resolved in full* before a single byte
    /// is written, then staged into a single durable
    /// `BEGIN … mutations … COMMIT` WAL frame, and only then applied to
    /// memory and physical storage:
    ///
    /// 1. [`Self::lower_transaction`] walks the ops in order against a
    ///    staged view of the data, validating each one and lowering it
    ///    into the concrete mutations it produces (expanding
    ///    `clear_kind`/`delete_where` into the exact set of deletes they
    ///    resolve to, and pairing each overwrite with its `Archive`).
    ///    Any validation failure returns here, with nothing written.
    ///
    /// 2. [`Transaction::commit`] stages every resolved mutation under
    ///    one transaction ID, writes the durable `COMMIT` marker, applies
    ///    the batch through [`Self::apply_committed`], and then settles
    ///    the durability checkpoint past the frame.
    ///
    /// Crash behaviour (FQL-003/FQL-004): a crash before the `COMMIT`
    /// record is durable leaves an incomplete frame, which recovery
    /// discards in full — the batch never happened. A crash after it
    /// leaves a complete frame, which recovery replays in full. There is
    /// no in-between state where part of a batch survives, which is what
    /// the previous per-operation apply path could not guarantee.
    ///
    /// Because every failure mode is resolved in step 1 — before the
    /// frame opens — step 2's apply cannot fail on validation grounds.
    /// That ordering is the point: nothing may fail *after* the commit
    /// marker is durable, since at that instant the batch is already
    /// promised to recovery.
    pub fn execute_transaction(
        &mut self,
        ops: Vec<TxOperation>,
    ) -> Result<(), TransactionError> {
        let transaction = Transaction::from_operations(
            self.lower_transaction(ops)?,
        );

        transaction
            .commit(|operation| self.apply_committed(operation))
            .map_err(|e| TransactionError::Storage(e.to_string()))
    }

    /// Validate a batch and lower it into the concrete mutations it
    /// produces, without writing anything.
    ///
    /// Validation walks the operations **in order** against a staged view
    /// — live state overlaid with everything the batch has done so far —
    /// so each operation is judged against the state it will actually
    /// meet when applied. That ordering is what makes the apply pass
    /// infallible, and it has to be: the apply pass runs after the
    /// `COMMIT` marker is durable, where an error can no longer roll
    /// anything back.
    ///
    /// The staged view also gives the bulk ops their honest selection: a
    /// `clear_kind` later in the batch removes nodes an earlier
    /// `insert_node` in the same batch created, and does not remove ones
    /// an earlier delete already took away.
    ///
    /// Returns the resolved mutations in apply order. An `Archive`
    /// immediately precedes the `Insert` that supersedes it, mirroring
    /// the WAL ordering the standalone `insert` path writes.
    fn lower_transaction(
        &self,
        ops: Vec<TxOperation>,
    ) -> Result<Vec<Operation>, TransactionError> {
        let mut lowered: Vec<Operation> = Vec::new();

        // The batch's in-progress overlay on live state: `Some(node)` is
        // a value this batch wrote, `None` an address it removed. An
        // address absent from the overlay is untouched so far and
        // resolves against `self.nodes`.
        let mut staged: HashMap<String, Option<Node>> = HashMap::new();

        // The same overlay for edges, keyed by identity. Edges need
        // their own because they have their own key space: an edge is
        // addressed by `(from, to, kind)`, not by a node address, so the
        // node overlay could not represent one. Without it a batch that
        // deleted an edge and then re-asserted it (or deleted it twice)
        // would be judged against live state and get the wrong answer
        // both times.
        let mut staged_edges: HashMap<EdgeId, Option<Edge>> = HashMap::new();

        for op in ops {
            match op {
                TxOperation::InsertNode(node) => {
                    if let Some(existing) =
                        staged_node(&staged, &self.nodes, &node.address)
                    {
                        // SECURITY:
                        //
                        // Do not silently allow a transaction to
                        // overwrite another owner's node. Insert is
                        // otherwise replacement semantics, matching the
                        // standalone insert path.
                        if existing.owner != node.owner {
                            return Err(TransactionError::Invalid(format!(
                                "transaction failed, nothing applied: \
                                 address {} is owned by {}",
                                node.address,
                                existing.owner
                            )));
                        }

                        // An overwrite archives the value being
                        // replaced — the same history entry the
                        // standalone `insert` path writes, staged into
                        // the frame ahead of the insert that supersedes
                        // it.
                        lowered.push(Operation::Archive(
                            HistoryEntry::now(existing.clone()),
                        ));
                    }

                    staged.insert(
                        node.address.clone(),
                        Some(node.clone()),
                    );

                    lowered.push(Operation::Insert(node));
                }

                TxOperation::DeleteNode(address) => {
                    // Already removed earlier in this same batch (by a
                    // clear_kind/delete_where or an explicit delete):
                    // the target is gone, so there is nothing left to
                    // tombstone. Idempotent rather than an error — a
                    // bulk clear followed by an explicit delete of one
                    // of the nodes it removed is a valid batch.
                    if matches!(staged.get(&address), Some(None)) {
                        continue;
                    }

                    match staged_node(&staged, &self.nodes, &address) {
                        // Archive the value being removed, as the
                        // standalone delete path does — a deleted node's
                        // final state is exactly the one an operator
                        // comes looking for.
                        Some(node) => lowered
                            .push(Operation::Archive(HistoryEntry::now(node.clone()))),
                        None => {
                            return Err(TransactionError::Invalid(format!(
                                "transaction failed, nothing applied: \
                                 delete target not found: {address}"
                            )))
                        }
                    }

                    staged.insert(address.clone(), None);

                    lowered.push(Operation::Delete(address));
                }

                TxOperation::ClearKind { kind, owner, is_admin } => {
                    // A clear never aborts the batch on its own:
                    // clearing a kind with no writable nodes (or no
                    // nodes at all) is a valid no-op, and authorization
                    // is already baked into which addresses the
                    // selection returns.
                    for address in self
                        .staged_selection(&staged, &kind, None, &owner, is_admin)
                        .map_err(TransactionError::Invalid)?
                    {
                        if let Some(node) = staged_node(&staged, &self.nodes, &address) {
                            lowered.push(Operation::Archive(
                                HistoryEntry::now(node.clone()),
                            ));
                        }

                        staged.insert(address.clone(), None);
                        lowered.push(Operation::Delete(address));
                    }
                }

                TxOperation::DeleteWhere { kind, where_, owner, is_admin } => {
                    // Same as ClearKind, plus the predicate. The one way
                    // a delete_where can abort the batch — an unpushable
                    // or erroring predicate — surfaces from the
                    // selection below, here in the validate/lower pass,
                    // before anything is written. Matching zero nodes is
                    // a valid no-op.
                    for address in self
                        .staged_selection(&staged, &kind, where_.as_ref(), &owner, is_admin)
                        .map_err(TransactionError::Invalid)?
                    {
                        if let Some(node) = staged_node(&staged, &self.nodes, &address) {
                            lowered.push(Operation::Archive(
                                HistoryEntry::now(node.clone()),
                            ));
                        }

                        staged.insert(address.clone(), None);
                        lowered.push(Operation::Delete(address));
                    }
                }

                TxOperation::SetIf {
                    address,
                    field,
                    expect,
                    set,
                    owner,
                    is_admin,
                } => {
                    // The target must exist. A missing node is reported
                    // as a failed precondition rather than an invalid
                    // batch: to a worker racing for a slot, "the node
                    // isn't there" and "someone else already took it"
                    // are the same answer — you did not win — and both
                    // want the same handling.
                    let node = match staged_node(&staged, &self.nodes, &address) {
                        Some(node) => node,
                        None => {
                            return Err(TransactionError::Precondition(format!(
                                "set_if target not found: {address}"
                            )))
                        }
                    };

                    if !(is_admin || node.can_write(&owner)) {
                        return Err(TransactionError::Invalid(format!(
                            "transaction failed, nothing applied: \
                             not authorized to set_if {address}"
                        )));
                    }

                    let next = Self::set_if_next(node, &field, &expect, &set)?;

                    // A CAS is an upsert that had to earn the right to
                    // run, so it lowers to the same pair as any other
                    // overwrite: archive what was there, then insert.
                    lowered.push(Operation::Archive(HistoryEntry::now(node.clone())));

                    staged.insert(address, Some(next.clone()));

                    lowered.push(Operation::Insert(next));
                }

                TxOperation::InsertEdge(edge) => {
                    // Endpoints are checked against the staged view, so
                    // an edge may reference a node this batch inserted
                    // earlier, but not one it already deleted — and not
                    // one inserted *later*, because by then the edge has
                    // already been applied.
                    if staged_node(&staged, &self.nodes, &edge.from).is_none() {
                        return Err(TransactionError::Invalid(format!(
                            "transaction failed, nothing applied: \
                             edge 'from' address not found: {}",
                            edge.from
                        )));
                    }

                    if staged_node(&staged, &self.nodes, &edge.to).is_none() {
                        return Err(TransactionError::Invalid(format!(
                            "transaction failed, nothing applied: \
                             edge 'to' address not found: {}",
                            edge.to
                        )));
                    }

                    // Recorded in the edge overlay so a later op in
                    // this batch — a `delete_edge` of it, or a second
                    // insert of the same identity — resolves against
                    // what the batch has done rather than against live
                    // state it has already moved past.
                    staged_edges.insert(edge.id(), Some(edge.clone()));

                    lowered.push(Operation::InsertEdge(edge));
                }

                TxOperation::DeleteEdge { id, owner, is_admin } => {
                    // Already removed earlier in this same batch:
                    // nothing left to delete. Idempotent rather than an
                    // error, exactly as `DeleteNode` is.
                    if matches!(staged_edges.get(&id), Some(None)) {
                        continue;
                    }

                    let edge = match staged_edge(&staged_edges, self, &id) {
                        Some(edge) => edge,
                        None => {
                            return Err(TransactionError::Invalid(format!(
                                "transaction failed, nothing applied: \
                                 delete target not found: edge {} -[{}]-> {}",
                                id.from, id.kind, id.to
                            )))
                        }
                    };

                    // An admin bypasses the per-edge owner check the
                    // same way it bypasses a node's `can_write`.
                    if !(is_admin || edge.can_write(&owner)) {
                        return Err(TransactionError::Invalid(format!(
                            "transaction failed, nothing applied: \
                             not authorized to delete edge {} -[{}]-> {}",
                            id.from, id.kind, id.to
                        )));
                    }

                    staged_edges.insert(id.clone(), None);

                    lowered.push(Operation::DeleteEdge(id));
                }
            }
        }

        Ok(lowered)
    }

    /// The addresses a bulk delete selects **within a batch**: the same
    /// rule as [`Self::delete_where_targets`], applied to the staged view
    /// instead of to live state.
    ///
    /// Candidates are every live address plus every address the batch has
    /// touched, each resolved through the overlay — so a node the batch
    /// inserted is eligible and a node it already removed is not. The
    /// candidate set is walked through a `BTreeSet`, making the returned
    /// order sorted and deterministic rather than dependent on `HashMap`
    /// iteration order; these addresses become WAL records, and a WAL
    /// should not vary run to run for identical input.
    fn staged_selection(
        &self,
        staged: &HashMap<String, Option<Node>>,
        kind: &str,
        where_: Option<&Expr>,
        owner: &str,
        is_admin: bool,
    ) -> Result<Vec<String>, String> {
        let mut candidates: std::collections::BTreeSet<&str> =
            self.nodes.keys().map(String::as_str).collect();

        candidates.extend(staged.keys().map(String::as_str));

        let mut targets = Vec::new();

        for address in candidates {
            let node = match staged_node(staged, &self.nodes, address) {
                Some(node) => node,
                None => continue,
            };

            if Self::selection_matches(node, kind, where_, owner, is_admin)? {
                targets.push(address.to_string());
            }
        }

        Ok(targets)
    }

    /// Apply one already-committed mutation to memory and physical
    /// storage.
    ///
    /// This is the apply half of **every** mutation path — the framed
    /// one and the standalone one both reach state through here, via
    /// [`Self::apply_atomic`] — and it is deliberately narrower than a
    /// mutation primitive:
    ///
    /// * It writes **no WAL record**. On the framed path the frame has
    ///   already logged this operation's intent under its transaction
    ///   ID, so writing again here would double-log it and (worse) log
    ///   it a second time as a standalone record that recovery would
    ///   replay outside the frame. On the standalone path `apply_atomic`
    ///   has already written the one record, before calling this.
    ///
    /// * It **does not advance the checkpoint**. In a frame the
    ///   checkpoint may only move once the whole frame is in physical
    ///   storage, and the frame settles it; advancing per-operation
    ///   would let a crash mid-apply leave a checkpoint that claims
    ///   durability for part of a batch, which is exactly the hole the
    ///   frame closes. On the standalone path `apply_atomic` advances it
    ///   afterwards, once this call has put the record in physical
    ///   storage.
    ///
    /// * It is the **only** place `writes_total` is incremented, which
    ///   is what keeps the counter path-independent (see that field's
    ///   doc). An `Archive` is not counted: it is the history half of
    ///   the mutation that carries it, not a mutation of its own.
    ///
    /// It is the counterpart to the `replay_*` methods, which apply to
    /// memory only. Those are correct for recovery, which is replaying a
    /// WAL whose records may already be in the binary files; this one is
    /// for the live path, where physical storage must actually be
    /// written.
    pub(crate) fn apply_committed(
        &mut self,
        operation: &Operation,
    ) -> Result<(), String> {
        match operation {
            Operation::Archive(entry) => {
                binary::append_record(
                    &binary::history_path(),
                    entry,
                )
                    .map_err(|e| e.to_string())?;

                self.history
                    .entry(entry.address.clone())
                    .or_default()
                    .push(entry.clone());
            }

            Operation::Insert(node) => {
                let offset =
                    binary::append_node_record(&NodeRecord::Put(node.clone()))
                        .map_err(|e| e.to_string())?;

                self.index.insert(
                    node.address.clone(),
                    offset,
                );

                self.nodes.insert(
                    node.address.clone(),
                    node.clone(),
                );

                self.writes_total.fetch_add(1, Ordering::Relaxed);
            }

            // The delete goes into the node log itself, immediately
            // after (in file order) whatever `Put` it removes. That
            // ordering IS the durable record of which happened last, so
            // a later re-create simply appends another `Put` and wins.
            Operation::Delete(address) => {
                binary::append_node_record(
                    &NodeRecord::Delete(address.clone()),
                )
                    .map_err(|e| e.to_string())?;

                self.nodes.remove(address);

                self.writes_total.fetch_add(1, Ordering::Relaxed);
            }

            Operation::InsertEdge(edge) => {
                binary::append_record(
                    &binary::edges_path(),
                    &EdgeRecord::Put(edge.clone()),
                )
                    .map_err(|e| e.to_string())?;

                self.index_edge(edge.clone());

                self.writes_total.fetch_add(1, Ordering::Relaxed);
            }

            Operation::DeleteEdge(id) => {
                binary::append_record(
                    &binary::edges_path(),
                    &EdgeRecord::Delete(id.clone()),
                )
                    .map_err(|e| e.to_string())?;

                self.unindex_edge(id);

                self.writes_total.fetch_add(1, Ordering::Relaxed);
            }

            Operation::InsertUser(record) => {
                binary::append_record(
                    &binary::users_path(),
                    &UserOpRecord::Put(record.clone()),
                )
                    .map_err(|e| e.to_string())?;

                self.users.insert(
                    record.token_hash.clone(),
                    record.clone(),
                );
            }

            Operation::RevokeUser(token_hash) => {
                binary::append_record(
                    &binary::users_path(),
                    &UserOpRecord::Revoke(token_hash.clone()),
                )
                    .map_err(|e| e.to_string())?;

                self.users.remove(token_hash);
            }
        }

        Ok(())
    }
}

/// Resolve an edge identity against a transaction's staged edge overlay,
/// falling back to the engine's live adjacency lists.
///
/// The edge counterpart of [`staged_node`], with the same three states:
/// `Some(edge)` in the overlay is an edge the batch wrote, `Some(None)`
/// one it removed, and an absent key means the batch has not touched
/// that identity, so live state answers.
fn staged_edge<'a>(
    staged: &'a HashMap<EdgeId, Option<Edge>>,
    engine: &'a StorageEngine,
    id: &EdgeId,
) -> Option<&'a Edge> {
    match staged.get(id) {
        Some(slot) => slot.as_ref(),
        None => engine.find_edge(id),
    }
}

/// Does this edge carry that identity?
///
/// Field-by-field rather than `edge.id() == *id`, which would clone
/// three `String`s for every candidate in a linear scan. `owner` is
/// deliberately absent — see [`EdgeId`] for why it is not part of what
/// an edge is.
fn edge_has_id(edge: &Edge, id: &EdgeId) -> bool {
    edge.from == id.from && edge.to == id.to && edge.kind == id.kind
}

/// Resolve an address against a transaction's staged overlay, falling
/// back to live state.
///
/// `Some(node)` in the overlay is a value the batch has written,
/// `Some(None)` an address it has removed, and an absent key means the
/// batch has not touched the address at all.
fn staged_node<'a>(
    staged: &'a HashMap<String, Option<Node>>,
    live: &'a HashMap<String, Node>,
    address: &str,
) -> Option<&'a Node> {
    match staged.get(address) {
        Some(slot) => slot.as_ref(),
        None => live.get(address),
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

    /// Remove one edge by its identity `(from, to, kind)`, as part of
    /// the batch.
    ///
    /// Authorization is carried on the op — resolved by the handler
    /// under the write lock, exactly as `ClearKind`/`DeleteWhere`/
    /// `SetIf` carry it: a non-admin may delete only an edge it owns,
    /// an admin any edge. The check runs against the batch's staged
    /// view, so it judges the edge the batch will actually meet.
    ///
    /// Deleting an edge that is not there is an invalid batch, matching
    /// `DeleteNode` — with the same exception: if *this* batch already
    /// deleted it, the op is a no-op rather than an error, because
    /// "remove it, twice" is a coherent request and the end state is
    /// the one asked for.
    DeleteEdge {
        id: EdgeId,
        owner: String,
        is_admin: bool,
    },

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

    /// Native compare-and-set on one node — the atomic conditional
    /// update behind "take this slot only if nobody else already has".
    ///
    /// `field` names a key inside the node's decoded `data` object,
    /// `expect` is the condition that key must satisfy, and `set` is the
    /// map of fields merged into `data` when it does. Check and write
    /// happen inside the same batch under the same engine write lock, so
    /// no other writer can slip between them — that indivisibility is
    /// the whole point, and is why this is an engine primitive instead
    /// of a read-then-write in a caller. A caller that "emulates" it
    /// with a get followed by a put has a race, always.
    ///
    /// When the condition does not hold, the *whole transaction* is
    /// rejected with `TransactionError::Precondition` and nothing is
    /// applied. That is how a caller learns it lost: a batch carrying a
    /// `SetIf` either commits (I won) or comes back precondition-failed
    /// (someone else won). Two outcomes, no third, and no separate
    /// result channel that could disagree with what was committed.
    ///
    /// A successful `SetIf` lowers to exactly the `Archive` + `Insert`
    /// pair an upsert produces, so it archives history, stages inside
    /// the crash-atomic frame, and replays through recovery like any
    /// other write.
    ///
    /// Authorization is carried on the op, as with the bulk ops: a
    /// non-admin may only compare-and-set a node it owns.
    SetIf {
        address: String,
        field: String,
        expect: Expectation,
        set: serde_json::Map<String, serde_json::Value>,
        owner: String,
        is_admin: bool,
    },
}

/// The condition a [`TxOperation::SetIf`] tests against one field of a
/// node's `data`.
///
/// Deliberately small. These are the comparisons a compare-and-set
/// actually needs, not a second predicate language — anything richer
/// belongs in `core::predicate`, the `where` grammar the query and
/// `delete_where` paths already share. A CAS condition has to stay
/// trivially decidable, because a caller's correctness depends on
/// knowing exactly when it wins.
#[derive(Debug, Clone)]
pub enum Expectation {
    /// The field exists, is a number, and is less than or equal to this
    /// value.
    ///
    /// The deadline form: "reserve this tick only if its `next_run` is
    /// already due". A worker passes `now`; whichever worker's
    /// transaction commits first moves `next_run` into the future, and
    /// every other worker's batch is rejected.
    AtMost(f64),

    /// The field exists and equals this JSON value exactly.
    ///
    /// The version form: "write this only if the version is still the
    /// one I read". The caller bumps the version inside `set`, so two
    /// concurrent writers cannot both succeed.
    Equals(serde_json::Value),

    /// The field is absent, or present as JSON `null`.
    ///
    /// The create-once form: "set this only if nobody has set it". Null
    /// counts as absent so that clearing a field genuinely releases it.
    Absent,
}

/// Why a transaction was rejected.
///
/// These are kept apart because they mean genuinely different things to
/// a caller: an invalid batch is the caller's to fix, a failed
/// precondition means it lost a race and should re-read (or simply
/// stop — the right answer for a scheduler that didn't win the tick),
/// and a storage failure is neither, since nothing about the request was
/// wrong. Flattening them into one string would force every caller to
/// pattern-match on error prose to tell "you're wrong" from "you lost"
/// from "the disk is full".
#[derive(Debug)]
pub enum TransactionError {
    /// The batch is invalid: a missing delete target, an edge with no
    /// endpoint, an owner conflict, an unpushable predicate.
    Invalid(String),

    /// A conditional operation's precondition did not hold. The batch is
    /// rejected and nothing is applied — "you lost the race", not "your
    /// request was malformed".
    Precondition(String),

    /// The batch was valid, but could not be made durable.
    Storage(String),
}

impl std::fmt::Display for TransactionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransactionError::Invalid(e)
            | TransactionError::Precondition(e)
            | TransactionError::Storage(e) => write!(f, "{e}"),
        }
    }
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
/// `set_if` — the native compare-and-set primitive.
///
/// These tests are about one property: **two callers racing for the same
/// slot cannot both win.** That is the entire reason this op exists in
/// the engine rather than as a get-then-put in a caller, so it is the
/// thing worth pinning down.
#[cfg(test)]
mod set_if_tests {
    use super::*;
    use crate::core::coordinate::Coordinate;
    use crate::core::node::Node;
    use std::sync::{Mutex, OnceLock};

    // Same durable-path setup as the other transaction test modules: one
    // process-wide temp data dir, one lock serializing tests that append
    // to the shared WAL/binary/tombstone files. Addresses are unique per
    // test so the shared dir never crosses assertions.
    fn disk_guard() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: Mutex<()> = Mutex::new(());
        static INIT: OnceLock<()> = OnceLock::new();
        INIT.get_or_init(|| {
            let dir = std::env::temp_dir()
                .join(format!("facetql-setiftest-{}", std::process::id()));
            std::fs::create_dir_all(&dir).expect("create temp data dir");
            crate::config::set_data_dir(dir);
        });
        LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn node_with(address: &str, owner: &str, data: &str) -> Node {
        let mut n = Node::new(
            Coordinate::new(0, 0, 0, 0),
            address.to_string(),
            "SetIfEntity".to_string(),
            owner.to_string(),
        );
        n.data = data.to_string();
        n
    }

    fn set(pairs: &[(&str, serde_json::Value)]) -> serde_json::Map<String, serde_json::Value> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect()
    }

    fn field(engine: &StorageEngine, address: &str, field: &str) -> serde_json::Value {
        let node = engine.get(address).expect("node exists");
        let data: serde_json::Value =
            serde_json::from_str(&node.data).expect("data is JSON");
        data.get(field).cloned().unwrap_or(serde_json::Value::Null)
    }

    /// The durable-scheduler case (`ReserveCron`): several workers wake
    /// at the same tick and all try to reserve it. Exactly one may win,
    /// and the losers must be told they lost rather than quietly also
    /// running the job.
    #[test]
    fn only_one_worker_reserves_a_due_tick() {
        let _g = disk_guard();
        let mut e = StorageEngine::new();
        e.insert(node_with("si_cron:nightly", "alice", r#"{"next_run":100}"#))
            .unwrap();

        let reserve = |worker: &str, at: f64| TxOperation::SetIf {
            address: "si_cron:nightly".to_string(),
            field: "next_run".to_string(),
            expect: Expectation::AtMost(at),
            set: set(&[
                ("next_run", serde_json::json!(at + 3600.0)),
                ("held_by", serde_json::json!(worker)),
            ]),
            owner: "alice".to_string(),
            is_admin: false,
        };

        // now = 150, so next_run (100) is due: the first worker wins.
        e.execute_transaction(vec![reserve("worker-a", 150.0)])
            .expect("first worker reserves the due tick");

        // The tick is no longer due, so every later worker loses — and
        // loses with Precondition, not a generic failure.
        let second = e.execute_transaction(vec![reserve("worker-b", 150.0)]);
        assert!(
            matches!(second, Err(TransactionError::Precondition(_))),
            "second worker must lose the race, got {second:?}"
        );

        assert_eq!(field(&e, "si_cron:nightly", "held_by"), serde_json::json!("worker-a"));
        assert_eq!(field(&e, "si_cron:nightly", "next_run"), serde_json::json!(3750.0));
    }

    /// The version case (compare-and-swap on a revision counter): a
    /// writer holding a stale version is rejected, and the node keeps
    /// the winner's value.
    #[test]
    fn stale_version_cannot_overwrite() {
        let _g = disk_guard();
        let mut e = StorageEngine::new();
        e.insert(node_with("si_ver:doc", "alice", r#"{"version":7,"body":"first"}"#))
            .unwrap();

        let write = |expect_version: serde_json::Value, body: &str, next: i64| {
            TxOperation::SetIf {
                address: "si_ver:doc".to_string(),
                field: "version".to_string(),
                expect: Expectation::Equals(expect_version),
                set: set(&[
                    ("version", serde_json::json!(next)),
                    ("body", serde_json::json!(body)),
                ]),
                owner: "alice".to_string(),
                is_admin: false,
            }
        };

        e.execute_transaction(vec![write(serde_json::json!(7), "second", 8)])
            .expect("writer holding the current version wins");

        let stale = e.execute_transaction(vec![write(serde_json::json!(7), "third", 8)]);
        assert!(
            matches!(stale, Err(TransactionError::Precondition(_))),
            "a stale version must be rejected, got {stale:?}"
        );

        assert_eq!(field(&e, "si_ver:doc", "body"), serde_json::json!("second"));
        assert_eq!(field(&e, "si_ver:doc", "version"), serde_json::json!(8));
    }

    /// `expect_absent` is create-once: the first setter takes the field,
    /// everyone after is refused. A field explicitly set back to `null`
    /// counts as released, so clearing it genuinely frees the slot.
    #[test]
    fn absent_claims_once_and_null_releases() {
        let _g = disk_guard();
        let mut e = StorageEngine::new();
        e.insert(node_with("si_once:slot", "alice", r#"{}"#)).unwrap();

        let claim = |worker: &str| TxOperation::SetIf {
            address: "si_once:slot".to_string(),
            field: "owner_worker".to_string(),
            expect: Expectation::Absent,
            set: set(&[("owner_worker", serde_json::json!(worker))]),
            owner: "alice".to_string(),
            is_admin: false,
        };

        e.execute_transaction(vec![claim("worker-a")]).expect("first claim wins");

        let second = e.execute_transaction(vec![claim("worker-b")]);
        assert!(
            matches!(second, Err(TransactionError::Precondition(_))),
            "an already-claimed slot must refuse a second claim, got {second:?}"
        );
        assert_eq!(
            field(&e, "si_once:slot", "owner_worker"),
            serde_json::json!("worker-a")
        );

        // Releasing by writing null makes the slot claimable again.
        e.execute_transaction(vec![TxOperation::SetIf {
            address: "si_once:slot".to_string(),
            field: "owner_worker".to_string(),
            expect: Expectation::Equals(serde_json::json!("worker-a")),
            set: set(&[("owner_worker", serde_json::Value::Null)]),
            owner: "alice".to_string(),
            is_admin: false,
        }])
        .expect("holder releases the slot");

        e.execute_transaction(vec![claim("worker-b")])
            .expect("a released slot is claimable again");
        assert_eq!(
            field(&e, "si_once:slot", "owner_worker"),
            serde_json::json!("worker-b")
        );
    }

    /// `set` merges into `data` rather than replacing it — a CAS that
    /// moves one field must not silently drop every field the caller
    /// didn't mention.
    #[test]
    fn set_merges_and_leaves_other_fields_intact() {
        let _g = disk_guard();
        let mut e = StorageEngine::new();
        e.insert(node_with(
            "si_merge:row",
            "alice",
            r#"{"version":1,"title":"keep me","tags":["a"]}"#,
        ))
        .unwrap();

        e.execute_transaction(vec![TxOperation::SetIf {
            address: "si_merge:row".to_string(),
            field: "version".to_string(),
            expect: Expectation::Equals(serde_json::json!(1)),
            set: set(&[("version", serde_json::json!(2))]),
            owner: "alice".to_string(),
            is_admin: false,
        }])
        .expect("cas applies");

        assert_eq!(field(&e, "si_merge:row", "version"), serde_json::json!(2));
        assert_eq!(field(&e, "si_merge:row", "title"), serde_json::json!("keep me"));
        assert_eq!(field(&e, "si_merge:row", "tags"), serde_json::json!(["a"]));
    }

    /// A lost CAS rejects the *whole* batch. This is what makes "did my
    /// transaction commit?" a truthful answer to "did I win?" — a losing
    /// worker must not have its other operations applied anyway.
    #[test]
    fn a_lost_cas_rolls_back_the_whole_batch() {
        let _g = disk_guard();
        let mut e = StorageEngine::new();
        e.insert(node_with("si_atom:lease", "alice", r#"{"next_run":900}"#))
            .unwrap();

        let result = e.execute_transaction(vec![
            TxOperation::InsertNode(node_with("si_atom:sideeffect", "alice", r#"{}"#)),
            TxOperation::SetIf {
                address: "si_atom:lease".to_string(),
                field: "next_run".to_string(),
                // 900 > 100, so the lease is not yet due: this loses.
                expect: Expectation::AtMost(100.0),
                set: set(&[("next_run", serde_json::json!(1000))]),
                owner: "alice".to_string(),
                is_admin: false,
            },
        ]);

        assert!(
            matches!(result, Err(TransactionError::Precondition(_))),
            "an undue lease must fail the batch, got {result:?}"
        );
        assert!(
            e.get("si_atom:sideeffect").is_none(),
            "the batch's other write must roll back with the lost CAS"
        );
        assert_eq!(field(&e, "si_atom:lease", "next_run"), serde_json::json!(900));
    }

    /// A CAS is a write, so it obeys the same ownership rule every other
    /// write does: a non-owner cannot use it to reach into someone
    /// else's node, and being refused for that reason is an invalid
    /// batch — not a lost race.
    #[test]
    fn non_owner_cannot_compare_and_set() {
        let _g = disk_guard();
        let mut e = StorageEngine::new();
        e.insert(node_with("si_auth:row", "alice", r#"{"version":1}"#))
            .unwrap();

        let result = e.execute_transaction(vec![TxOperation::SetIf {
            address: "si_auth:row".to_string(),
            field: "version".to_string(),
            expect: Expectation::Equals(serde_json::json!(1)),
            set: set(&[("version", serde_json::json!(99))]),
            owner: "mallory".to_string(),
            is_admin: false,
        }]);

        assert!(
            matches!(result, Err(TransactionError::Invalid(_))),
            "a non-owner CAS must be refused as invalid, got {result:?}"
        );
        assert_eq!(field(&e, "si_auth:row", "version"), serde_json::json!(1));
    }

    /// A won CAS archives the value it replaced, like any other
    /// overwrite — the reservation history of a slot is exactly the
    /// audit trail an operator needs when two workers disagree about who
    /// ran a job.
    #[test]
    fn a_won_cas_archives_the_previous_value() {
        let _g = disk_guard();
        let mut e = StorageEngine::new();
        e.insert(node_with("si_hist:slot", "alice", r#"{"next_run":10}"#))
            .unwrap();

        e.execute_transaction(vec![TxOperation::SetIf {
            address: "si_hist:slot".to_string(),
            field: "next_run".to_string(),
            expect: Expectation::AtMost(50.0),
            set: set(&[("next_run", serde_json::json!(60))]),
            owner: "alice".to_string(),
            is_admin: false,
        }])
        .expect("cas applies");

        let history = e.history_for("si_hist:slot");
        assert_eq!(history.len(), 1, "the replaced value was archived");
        assert!(
            history[0].node.data.contains("\"next_run\":10"),
            "history holds the pre-CAS value, got {}",
            history[0].node.data
        );
    }
}

/// History on the delete path.
///
/// An overwrite has always archived the value it replaced. A delete is
/// the *last* transition a node ever makes, so if it doesn't archive,
/// the final state — the one an operator asks about after somebody
/// deletes something — is the single state that was never recorded.
#[cfg(test)]
mod delete_history_tests {
    use super::*;
    use crate::core::coordinate::Coordinate;
    use crate::core::node::Node;
    use std::sync::{Mutex, OnceLock};

    fn disk_guard() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: Mutex<()> = Mutex::new(());
        static INIT: OnceLock<()> = OnceLock::new();
        INIT.get_or_init(|| {
            let dir = std::env::temp_dir()
                .join(format!("facetql-delhisttest-{}", std::process::id()));
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

    /// A standalone delete archives the state it removed.
    #[test]
    fn delete_archives_the_removed_state() {
        let _g = disk_guard();
        let mut e = StorageEngine::new();
        e.insert(node_with("dh_one:a", "DhEntity", "alice", r#"{"v":1}"#)).unwrap();

        e.delete("dh_one:a").unwrap();

        let history = e.history_for("dh_one:a");
        assert_eq!(history.len(), 1, "the deleted state was archived");
        assert!(history[0].node.data.contains("\"v\":1"));
        assert!(e.get("dh_one:a").is_none(), "the node is still gone");
    }

    /// The full lifecycle: create → overwrite → delete leaves both the
    /// replaced value and the deleted value in history, in order.
    #[test]
    fn overwrite_then_delete_records_both_states() {
        let _g = disk_guard();
        let mut e = StorageEngine::new();
        e.insert(node_with("dh_two:a", "DhEntity", "alice", r#"{"v":1}"#)).unwrap();
        e.insert(node_with("dh_two:a", "DhEntity", "alice", r#"{"v":2}"#)).unwrap();
        e.delete("dh_two:a").unwrap();

        let history = e.history_for("dh_two:a");
        assert_eq!(history.len(), 2, "one entry per state that stopped being current");
        assert!(history[0].node.data.contains("\"v\":1"), "oldest first");
        assert!(history[1].node.data.contains("\"v\":2"), "then the deleted state");
    }

    /// A bulk delete archives every node it removes — the audit trail
    /// for a mass deletion is exactly when you need one most.
    #[test]
    fn bulk_delete_archives_every_removed_node() {
        let _g = disk_guard();
        let mut e = StorageEngine::new();
        e.insert(node_with("dh_bulk:a", "DhBulkEntity", "alice", r#"{"v":"a"}"#)).unwrap();
        e.insert(node_with("dh_bulk:b", "DhBulkEntity", "alice", r#"{"v":"b"}"#)).unwrap();

        e.execute_transaction(vec![TxOperation::ClearKind {
            kind: "DhBulkEntity".to_string(),
            owner: "alice".to_string(),
            is_admin: false,
        }])
        .expect("clear commits");

        for (address, value) in [("dh_bulk:a", "a"), ("dh_bulk:b", "b")] {
            assert!(e.get(address).is_none(), "{address} was cleared");
            let history = e.history_for(address);
            assert_eq!(history.len(), 1, "{address} archived its removed state");
            assert!(history[0].node.data.contains(value));
        }
    }

    /// A transactional delete archives too, and the archive is part of
    /// the same crash-atomic frame: it survives a fresh recovery from
    /// the durable files rather than living only in memory.
    #[test]
    fn transactional_delete_archive_survives_recovery() {
        let _g = disk_guard();
        let mut e = StorageEngine::new();
        e.insert(node_with("dh_rec:a", "DhRecEntity", "alice", r#"{"v":"final"}"#)).unwrap();

        e.execute_transaction(vec![TxOperation::DeleteNode("dh_rec:a".to_string())])
            .expect("delete commits");

        let recovered = StorageEngine::load().expect("recovery load");
        assert!(recovered.get("dh_rec:a").is_none(), "still deleted");
        let history = recovered.history_for("dh_rec:a");
        assert_eq!(history.len(), 1, "the archive is durable, not just in memory");
        assert!(history[0].node.data.contains("final"));
    }
}
