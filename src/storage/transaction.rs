//! Multi-operation transaction model and the crash-safe commit driver.
//!
//! A `POST /transaction` batch is *validate-all-then-apply*: the engine
//! (`StorageEngine::execute_transaction`) first resolves and validates
//! the whole batch — expanding `clear_kind`/`delete_where` into the
//! concrete set of node addresses they remove, and rejecting the batch
//! as a unit if anything is invalid — before a single byte is written.
//! That part is unchanged and stays in the engine.
//!
//! What this module owns is the **apply** half: turning the validated,
//! resolved batch into one atomic, crash-safe unit of durability. The
//! previous apply path wrote each operation as a standalone WAL record
//! (`transaction_id == 0`) and advanced the checkpoint per-operation, so
//! a crash mid-batch could persist some operations and drop others. Here
//! the resolved operations are staged into a single `BEGIN … COMMIT`
//! frame via [`crate::storage::commit::StagedCommit`], so recovery's
//! existing "replay iff BEGIN…COMMIT, else discard" rule makes the batch
//! all-or-nothing across a crash.
//!
//! The [`Operation`] set here is the *resolved mutation* granularity —
//! the individual WAL-level effects a validated batch produces, after
//! `clear_kind`/`delete_where` have been expanded to concrete deletes.
//! It maps one-to-one onto [`wal::WalOperation`] mutation records. The
//! transaction *wire* contract (`insert_node` / `delete_node` /
//! `insert_edge` / `delete_edge` / `clear_kind` / `delete_where` /
//! `set_if`) lives in the engine's `TxOperation`; this is the internal
//! shape the engine lowers a validated batch into before staging it.

use std::io;

use crate::core::edge::{Edge, EdgeId};
use crate::core::history::HistoryEntry;
use crate::core::node::Node;
use crate::core::user::UserRecord;
use crate::storage::commit::StagedCommit;
use crate::storage::index::{self as keys, IndexDef};
use crate::storage::wal::WalOperation;

/// One resolved mutation within a transaction.
///
/// These are the concrete, post-validation effects that get staged to
/// the WAL — the same effects the engine's mutation primitives produce,
/// but carried as data so they can be framed under a single transaction
/// id instead of written as independent standalone records. Each variant
/// maps directly onto a [`wal::WalOperation`] mutation.
pub enum Operation {
    /// Archive a node's previous state (the history entry an overwrite
    /// produces). Staged before the replacing `Insert` so recovery
    /// rebuilds history in the same order the live path did.
    Archive(HistoryEntry),

    /// Insert or replace a node (upsert semantics, matching
    /// `StorageEngine::insert`).
    Insert(Node),

    /// Delete the node at this address.
    ///
    /// Lowers to the removal of the address from the primary index and
    /// the secondary ones. That removal *is* the delete: nothing
    /// resolves the address afterwards, so no read path can reach the
    /// record even though its bytes stay in the heap until compaction
    /// reclaims them. There is no tombstone to outrank a later
    /// re-create, which is what makes create → delete → create resolve
    /// correctly with nothing to reconcile.
    Delete(String),

    /// Insert or replace a directional edge (upsert on `(from, to,
    /// kind)`, matching `StorageEngine::insert_edge`).
    InsertEdge(Edge),

    /// Delete the edge with this identity.
    ///
    /// The edge counterpart of [`Operation::Delete`]: the identity is
    /// removed from both edge indexes, so follow → unfollow → follow
    /// again is three ordinary index writes and survives a restart.
    DeleteEdge(EdgeId),

    /// Insert or replace a persistent user.
    InsertUser(UserRecord),

    /// Revoke a persistent user by token hash.
    RevokeUser(String),

    /// Declare an index over a `data` field, and populate it from the
    /// nodes that already exist.
    ///
    /// A mutation rather than an operator side-effect because it is one:
    /// it adds a durable access path that every later write must
    /// maintain, and a half-created index is a wrong answer waiting to
    /// happen. Logging it means recovery re-declares and re-populates it
    /// — both idempotent by key — rather than restarting into a database
    /// whose definition log and trees disagree.
    CreateIndex(IndexDef),

    /// Drop a declared index and remove its tree.
    DropIndex(String),
}

impl Operation {
    /// Refuse a mutation whose durable keys the indexes could not
    /// accept, *before* it is written to the WAL.
    ///
    /// This is a durability check disguised as a validation. The write
    /// path logs intent first and applies second, so an operation that
    /// the index layer rejects at apply time has already been made
    /// durable: recovery replays it on the next start, hits the same
    /// rejection, and startup fails for good. One oversized address in
    /// one request would be enough. Checking here — at the last point
    /// before anything is logged — turns that into an ordinary rejected
    /// request.
    ///
    /// The bounds are the index layer's own (see
    /// [`crate::storage::index`]), so there is one definition of an
    /// admissible key rather than a copy of it here that can drift.
    ///
    /// `indexes` is the set of declared `data`-field indexes the
    /// mutation will have to maintain: a node carries one key per index
    /// over its kind, and those keys are subject to the same
    /// "admissible before it is logged" rule as the built-in ones.
    pub fn validate(&self, indexes: &[IndexDef]) -> Result<(), String> {
        match self {
            Operation::Archive(entry) => keys::check_history_keys(
                &entry.address,
                &entry.node.address,
                &entry.node.kind,
                &entry.node.owner,
            ),

            Operation::Insert(node) => {
                keys::check_node_keys(&node.address, &node.kind, &node.owner)?;

                keys::check_data_keys(
                    indexes.iter().filter(|def| def.kind == node.kind),
                    &node.address,
                    &node.data,
                )
            }

            Operation::Delete(address) => {
                keys::check_key_admissible("primary", address.as_bytes())
            }

            Operation::InsertEdge(edge) => {
                keys::check_edge_keys(&edge.from, &edge.to, &edge.kind)
            }

            Operation::DeleteEdge(id) => {
                keys::check_edge_keys(&id.from, &id.to, &id.kind)
            }

            // Users live in their own flat log, not in an index, so
            // there is no key to admit.
            Operation::InsertUser(_) | Operation::RevokeUser(_) => Ok(()),

            // A definition, not a row: what has to hold is that the name
            // can be a filename and the kind/field can be key
            // components. The keys its backfill will write are checked
            // by the backfill itself, which is the only thing that knows
            // what values are out there.
            Operation::CreateIndex(def) => def.validate(),

            Operation::DropIndex(_) => Ok(()),
        }
    }

    /// Lower this resolved mutation to its WAL representation.
    ///
    /// Cloning is deliberate: the caller keeps the [`Operation`] to apply
    /// it to memory + physical storage after the frame commits, so the
    /// WAL record needs its own copy of the payload.
    pub fn to_wal(&self) -> WalOperation {
        match self {
            Operation::Archive(entry) => WalOperation::Archive(entry.clone()),
            Operation::Insert(node) => WalOperation::Insert(node.clone()),
            Operation::Delete(address) => WalOperation::Delete(address.clone()),
            Operation::InsertEdge(edge) => WalOperation::InsertEdge(edge.clone()),
            Operation::DeleteEdge(id) => WalOperation::DeleteEdge(id.clone()),
            Operation::InsertUser(user) => WalOperation::InsertUser(user.clone()),
            Operation::RevokeUser(hash) => WalOperation::RevokeUser(hash.clone()),
            Operation::CreateIndex(def) => WalOperation::CreateIndex(def.clone()),
            Operation::DropIndex(name) => WalOperation::DropIndex(name.clone()),
        }
    }
}

/// A validated, resolved multi-operation transaction ready to be staged
/// and committed as one atomic, crash-safe unit.
pub struct Transaction {
    /// The resolved mutations, in the exact order they must be applied.
    /// Order matters: an `Archive` precedes the `Insert` that supersedes
    /// it, and an edge's endpoints must exist (or be staged) before the
    /// edge — the engine's validation/lowering guarantees this ordering
    /// before handing the batch here.
    pub operations: Vec<Operation>,
}

impl Transaction {
    /// A transaction with no operations yet.
    pub fn new() -> Self {
        Self { operations: Vec::new() }
    }

    /// A transaction over an already-resolved list of mutations.
    pub fn from_operations(operations: Vec<Operation>) -> Self {
        Self { operations }
    }

    /// Stage, commit, and apply this transaction atomically.
    ///
    /// This is the crash-safe apply path for every mutation that
    /// produces two or more durable records: the engine's
    /// `execute_transaction` routes its validated batch through here,
    /// and its single-statement primitives route through here too, via
    /// `apply_atomic`, as implicit one-statement transactions.
    /// Sequence:
    ///
    /// 1. Open a [`StagedCommit`] frame (durable `BEGIN`, checkpoint
    ///    fenced).
    /// 2. Stage every operation's WAL record under the transaction id
    ///    (all durable) — **before** anything is applied to state.
    /// 3. Write the durable `COMMIT` marker — the atomic commit point.
    /// 4. Apply each operation to physical storage + indexes via
    ///    `apply`.
    /// 5. Settle: release the checkpoint fence and hand the caller the
    ///    `COMMIT` sequence.
    ///
    /// `apply` is supplied by the engine and must apply one resolved
    /// [`Operation`] to physical state **without** writing its own WAL
    /// record and **without** advancing the checkpoint — the frame has
    /// already durably logged the intent, and double-logging or
    /// early-checkpointing would break the atomicity this path
    /// guarantees. `StorageEngine::apply_committed` is that primitive,
    /// and it is the only thing callers pass here.
    ///
    /// # Who moves the checkpoint
    ///
    /// Not this. Applying a batch puts it in the buffer pool, which is
    /// not the same as putting it on the disk, and the checkpoint may
    /// only ever name a sequence whose effects are genuinely durable.
    /// So this returns the `COMMIT` sequence and the engine advances the
    /// checkpoint to it later, at the flush that makes the heap and the
    /// indexes durable (`StorageEngine::checkpoint`). Returning it —
    /// rather than advancing it here — is what keeps that decision with
    /// the layer that knows what has actually been written.
    ///
    /// ## Failure handling
    ///
    /// * A failure during staging or commit (steps 1–3, before `COMMIT`
    ///   is durable): the frame is dropped, which releases the fence and
    ///   writes a best-effort `ABORT`. Recovery discards the incomplete
    ///   frame — nothing becomes visible. The error is returned.
    ///
    /// * A failure in `apply` (step 4, *after* `COMMIT` is durable): the
    ///   transaction is already durably committed, so this is not rolled
    ///   back. The frame is dropped without settling, so the checkpoint
    ///   is left below the frame and recovery replays the committed batch
    ///   from the WAL on the next start, reconstructing the state `apply`
    ///   failed to write. The error is returned so the caller learns the
    ///   apply did not complete.
    ///
    /// An empty transaction is a no-op: it opens no frame, writes no
    /// records, and returns sequence 0 — a value below every real
    /// sequence, so it can never move the checkpoint forward.
    pub fn commit<F>(self, indexes: &[IndexDef], mut apply: F) -> io::Result<u64>
    where
        F: FnMut(&Operation) -> Result<(), String>,
    {
        if self.operations.is_empty() {
            return Ok(0);
        }

        // Admissibility first, for the whole batch, before the frame is
        // opened. An operation the indexes would refuse must never
        // become durable — see [`Operation::validate`] — and refusing
        // the batch as a unit here keeps the all-or-nothing contract:
        // one inadmissible operation rejects the request rather than
        // committing its neighbours.
        for operation in &self.operations {
            operation
                .validate(indexes)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        }

        let mut frame = StagedCommit::open()?;

        // Step 2: durably log the full intent before touching state.
        for operation in &self.operations {
            frame.stage(operation.to_wal())?;
        }

        // Step 3: the atomic commit point. From here the batch is
        // durable; a crash replays it in full.
        frame.commit()?;

        // Step 4: make it visible. If this fails after COMMIT, we do NOT
        // settle — leaving the checkpoint below the frame so recovery
        // replays the committed batch and repairs state on restart.
        for operation in &self.operations {
            apply(operation)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        }

        // Step 5: the batch is applied — release the fence and report
        // how far the checkpoint may eventually move.
        frame.settle()
    }
}

impl Default for Transaction {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------
// THE RULE THIS MACHINERY EXISTS FOR
//
// Two or more durable records ⇒ frame. Exactly one ⇒ standalone.
//
// A mutation that lands as a single fsync'd WAL record plus its one
// physical write is already atomic, and framing it would only add a
// BEGIN, a COMMIT and a checkpoint fence. A mutation that lands as two
// or more records is not atomic on its own, however carefully its
// halves are ordered: a crash between them leaves half of it durable —
// and once the checkpoint has advanced over that half, permanently
// beyond recovery's reach. Those go through `Transaction::commit`.
//
// The rule is decided in one place: the engine's `apply_atomic`, which
// every single-statement mutation primitive routes through. It sends a
// lone operation down the cheap standalone (tx id 0) path and anything
// longer here, as an implicit single-statement transaction — an
// `insert`/`delete` of an address that already holds a value (archive
// + insert/delete), `claim` (which is exactly such an insert), and
// `insert_with_edges` (node + N edges). `execute_transaction` reaches
// `Transaction::commit` directly instead, because a lowered wire batch
// is a transaction by construction.
//
// Both routes pass the same apply closure,
// `StorageEngine::apply_committed`, which writes physical storage and
// the indexes, writes no WAL, and never touches the checkpoint.
// ---------------------------------------------------------------------
