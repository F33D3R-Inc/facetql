use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use serde::{Serialize, Deserialize};

use crate::core::edge::{Edge, EdgeId};
use crate::core::history::HistoryEntry;
use crate::core::node::Node;
use crate::core::predicate::{self, Expr};
use crate::core::user::UserRecord;
use crate::storage::binary::{self, UserOpRecord};
use crate::storage::cache::RecordCache;
use crate::storage::catalog::Catalog;
use crate::storage::heap::{HeapRecord, RecordStore};
use crate::storage::index::{self as keys, Indexes};
use crate::storage::location::RecordLocation;
use crate::storage::transaction::{Operation, Transaction};
use crate::storage::wal;

/// Loop-variable name a `delete_where` predicate's field accesses are
/// written against. The `delete_where` wire contract (§4b) carries no
/// `item_var` — unlike `/nodes/query` — so it uses the same default the
/// query path does (`default_item_var` in `api::routes`): `"item"`.
const DELETE_WHERE_ITEM_VAR: &str = "item";

/// Mutations applied before the engine checkpoints: pushes the heap,
/// the catalog and every index to stable storage and then advances the
/// WAL checkpoint past them.
///
/// This is a knob on how much WAL a restart replays, not on durability.
/// A committed mutation is durable the instant its WAL record is
/// fsync'd, which happens before this counter is even touched; the
/// checkpoint only decides how much of that WAL a restart has to redo.
/// Checkpointing on every mutation — which is what the engine used to
/// do, because a mutation *was* one appended record — would now mean
/// fsyncing the heap and six index files per write.
const DEFAULT_CHECKPOINT_INTERVAL: u64 = 256;

const CHECKPOINT_INTERVAL_ENV: &str = "FACETQL_CHECKPOINT_INTERVAL";

/// How dead a heap segment must be before a checkpoint drains it.
const COMPACTION_RATIO: f64 = 0.5;

/// Segments drained per checkpoint. Compaction runs inline on the write
/// path, so it is deliberately rationed: one bounded piece of work per
/// checkpoint keeps a large reclaim from becoming a long stall.
const SEGMENTS_PER_COMPACTION: usize = 1;

/// Rows one request may accumulate in memory before the engine refuses
/// to continue.
///
/// # Which reads this exists for
///
/// Most access paths here are already bounded by the caller's `limit`:
/// an index range scan stops as soon as the page is full, so the work
/// and the memory are proportional to the answer. A handful are not,
/// and they are not for a structural reason rather than an oversight —
/// they have to hold the whole result before they can produce any of
/// it:
///
/// ```text
///   order by a `data` field   no index on `data`, so every candidate
///                             is materialized and then sorted
///   a node's history          the full version list
///   a node's edges            the full adjacency list
///   an owner's nodes          every node that owner holds
///   delete_where targets      every address the batch will remove
/// ```
///
/// For those, the size of the answer is chosen by the *data*, not by the
/// request, so one ordinary-looking query over a large kind is a
/// heap allocation the size of that kind. That is the difference between
/// slow and fatal, and it is reachable without any hostile intent at all.
///
/// # Why it errors rather than truncating
///
/// Silently returning the first N rows would make a query answer a
/// question nobody asked, and — for `delete_where` — would delete a
/// subset of what the caller named. A refusal is recoverable: the
/// caller narrows the query, or an operator raises the bound. A wrong
/// answer is not.
///
/// 100_000 nodes is far past any interactive page and still a bounded,
/// modest allocation.
const DEFAULT_MAX_SCAN_ROWS: usize = 100_000;

const MAX_SCAN_ROWS_ENV: &str = "FACETQL_MAX_SCAN_ROWS";

/// Resolved mutations one transaction may carry.
///
/// A batch is staged into a single `BEGIN … COMMIT` frame, which means
/// every one of its records is fsync'd to the WAL before any of them is
/// applied and none of them may be checkpointed away until the frame
/// settles. So a batch is not merely a lot of work — it is work that
/// pins the checkpoint boundary and holds its whole resolved form in
/// memory while it runs.
///
/// The wire limit is not the same number, and cannot be: one wire
/// operation can lower into many. `clear_kind` and `delete_where` expand
/// to one `Delete` per matching node (each with an `Archive` in front of
/// it), so a three-operation request can resolve into a batch bounded
/// only by the size of a kind. That is why the bound is checked here, on
/// the *lowered* list, rather than on the request: this is the only
/// place that knows how large the batch actually became.
///
/// 50_000 mutations is far past any application batch and still a frame
/// that stages and applies in bounded time.
const DEFAULT_MAX_TRANSACTION_OPS: usize = 50_000;

const MAX_TRANSACTION_OPS_ENV: &str = "FACETQL_MAX_TRANSACTION_OPS";

/// Rows a query may skip before its first result.
///
/// Offset paging is the one query parameter whose cost is set by a
/// number the caller picks with no relation to the answer it wants.
/// Reaching row `offset` means walking the access path and *discarding*
/// every row before it — and a row here is not cheap: this tree has no
/// leaf links, so each step is a fresh root-to-leaf descent, and each
/// candidate is a heap read and a record decode before the filter can
/// reject it. Page 200_000 therefore costs ten million reads to return
/// fifty rows, from a request that is forty bytes long.
///
/// The keyset cursor (`after`) exists precisely because of that: it
/// resumes at the row itself, so page 200_000 costs what page 1 costs.
/// It is already the wire contract for `POST /nodes/query`. Bounding
/// offset is what makes the cheap path the one that scales, rather than
/// leaving a trap that only shows up under a client that pages deeply.
///
/// 10_000 is past any offset a UI produces and cheap to serve.
const DEFAULT_MAX_QUERY_OFFSET: usize = 10_000;

const MAX_QUERY_OFFSET_ENV: &str = "FACETQL_MAX_QUERY_OFFSET";

fn max_query_offset() -> usize {
    static OFFSET: std::sync::OnceLock<usize> = std::sync::OnceLock::new();

    *OFFSET.get_or_init(|| {
        std::env::var(MAX_QUERY_OFFSET_ENV)
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .unwrap_or(DEFAULT_MAX_QUERY_OFFSET)
    })
}

fn offset_too_large(offset: usize) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!(
            "offset {offset} is above the maximum of {}. Deep offset \
             paging re-reads and discards every row before the page, so \
             its cost grows with the page number; use the keyset cursor \
             (`after`, from a previous page's `next`) on POST /nodes/query, \
             which costs the same at any depth. Raise \
             {MAX_QUERY_OFFSET_ENV} to override.",
            max_query_offset()
        ),
    )
}

fn max_transaction_ops() -> usize {
    static OPS: std::sync::OnceLock<usize> = std::sync::OnceLock::new();

    *OPS.get_or_init(|| {
        std::env::var(MAX_TRANSACTION_OPS_ENV)
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .filter(|ops| *ops > 0)
            .unwrap_or(DEFAULT_MAX_TRANSACTION_OPS)
    })
}

fn max_scan_rows() -> usize {
    static ROWS: std::sync::OnceLock<usize> = std::sync::OnceLock::new();

    *ROWS.get_or_init(|| {
        std::env::var(MAX_SCAN_ROWS_ENV)
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .filter(|rows| *rows > 0)
            .unwrap_or(DEFAULT_MAX_SCAN_ROWS)
    })
}

/// The refusal a scan raises when it would have to hold more than
/// [`max_scan_rows`] rows.
///
/// `InvalidInput` rather than a generic failure: nothing is broken, the
/// request asked for more than this engine will materialize, and the
/// API layer turns that into a 4xx so a caller can tell "narrow your
/// query" from "the database is unwell".
fn scan_limit_exceeded(what: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!(
            "{what} would materialize more than {} rows, the maximum one \
             request may hold in memory. Narrow the query (a smaller \
             `kind`/`owner` range, or a predicate), page through it, or \
             raise {MAX_SCAN_ROWS_ENV}.",
            max_scan_rows()
        ),
    )
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

/// The storage engine.
///
/// # Where the database actually is
///
/// On disk, and nowhere else. The engine holds no map of nodes, no
/// adjacency lists and no history; it holds the machinery for reaching
/// them:
///
/// ```text
///   StorageEngine
///     ├── catalog    what segments exist, how long they are
///     ├── store      the record heap: segments → pages → records
///     ├── indexes    six durable B+trees (storage::index)
///     └── cache      a bounded LRU of recently read nodes
/// ```
///
/// This is the change that makes the rest of the engine a database
/// rather than a persisted program. Reads resolve
/// `address → RecordLocation → page → record`; filtered queries walk an
/// index instead of every node; opening the database reads metadata and
/// index roots instead of every record ever written. Memory is bounded
/// by the caches, not by the data.
///
/// # The one thing still resident
///
/// `users` is a `HashMap`, deliberately. It is bounded by the number of
/// identities rather than by the amount of data, it is consulted on
/// every single authenticated request, and it is the one structure whose
/// absence fails closed in the wrong direction (a token that cannot be
/// resolved is a request that cannot be authorized). Nodes, edges and
/// history — the three that grow without bound — are not resident.
pub struct StorageEngine {
    /// Physical metadata: format, page size, segments. Shared with the
    /// heap, which updates segment lengths as it appends.
    catalog: Arc<Catalog>,

    /// The record heap.
    store: RecordStore,

    /// The durable access paths. Authoritative: a query answers from
    /// these, so every mutation must maintain every one of them.
    indexes: Indexes,

    /// Bounded LRU over recently read/written nodes. A pure accelerator
    /// — every entry can be re-derived from the heap through the primary
    /// index, and a cold cache changes latency, never answers.
    cache: RecordCache,

    /// Persistent users, keyed by token_hash.
    pub users: HashMap<String, UserRecord>,

    /// Process-lifetime operation counters, the rate source behind
    /// `GET /stats`. `reads_total` counts calls to `get`/`query`/
    /// `query_where`; `writes_total` counts each applied mutation, and
    /// it is counted in exactly one place: `apply_committed`, the single
    /// apply step every mutation passes through — the framed transaction
    /// path and the standalone single-record path alike (see
    /// `apply_atomic`). One counting site is the point: the number a
    /// mutation contributes must not depend on which path carried it.
    ///
    /// An `Archive` is deliberately not counted: it is the history half
    /// of the overwrite or delete that produced it rather than a
    /// mutation anyone asked for. User records are likewise uncounted —
    /// identity administration is not data workload.
    ///
    /// Not persisted: they start at 0 every time the process starts.
    /// That is correct for a rate source — a consumer differences two
    /// samples over time, and a restart simply resets the baseline.
    /// Atomics rather than plain integers because the read paths
    /// increment them while holding only a read lock on the engine.
    reads_total: AtomicU64,
    writes_total: AtomicU64,

    /// Highest WAL sequence whose effects have been applied to the heap
    /// and indexes — in the buffer pool, not necessarily on disk. The
    /// checkpoint may advance to this value, and only to this value,
    /// once a flush has made those effects durable.
    applied_sequence: u64,

    /// Mutations applied since the last checkpoint.
    pending_mutations: u64,

    /// How many mutations to let accumulate before checkpointing.
    checkpoint_interval: u64,

    /// Records currently in `facetql.users`, tracked so the log can be
    /// compacted when it has grown far past the identities it describes.
    /// See [`StorageEngine::compact_user_log`].
    user_log_records: usize,
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
    /// Its one caller is [`Self::apply_atomic`]'s single-record branch,
    /// and it must stay that way: a mutation that decides for itself to
    /// append here is a mutation that has stepped outside the
    /// frame-vs-standalone rule, and a record appended alongside a
    /// framed operation is replayed twice by recovery — once in the
    /// frame, once as a standalone record.
    ///
    /// Returns the WAL sequence number assigned to this record so the
    /// caller can record how far the durability checkpoint may
    /// eventually move. See `storage::checkpoint`.
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

    // ---------------------------------------------------------------------
    // Durable apply: the frame-vs-standalone rule
    // ---------------------------------------------------------------------

    /// Apply one logical mutation — the ordered list of resolved
    /// [`Operation`]s it lowers to — durably and atomically.
    ///
    /// **The rule this function exists to hold:**
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
    ///        ✗ crash
    /// WAL Insert(tx 0)    ← never written
    /// ```
    ///
    /// Recovery replays the archive and has no insert to pair it with:
    /// the node keeps its old value while history claims that value was
    /// superseded. Staging both records under one `BEGIN … COMMIT` frame
    /// removes the in-between, because recovery replays a frame only
    /// when it sees the `COMMIT`, so the pair lands together or not at
    /// all.
    ///
    /// ## Why one record must not be
    ///
    /// A single-record mutation is already atomic — one fsync'd WAL
    /// record — so a frame would buy nothing while costing two extra
    /// fsync'd control records (`BEGIN` and `COMMIT`) and a checkpoint
    /// fence. The cheap path stays cheap.
    ///
    /// ## What "applied" means now
    ///
    /// [`Self::apply_committed`] writes the record into the heap and the
    /// entry into every index — in the buffer pool. Neither is fsync'd
    /// here, and the WAL checkpoint is *not* advanced here. Both happen
    /// together at [`Self::checkpoint`]. That ordering is the durability
    /// contract: WAL first (already fsync'd above), physical state next,
    /// checkpoint last and only over state that has actually reached the
    /// disk.
    fn apply_atomic(
        &mut self,
        operations: Vec<Operation>,
    ) -> Result<(), String> {
        match operations.len() {
            // Nothing resolved to nothing: no record, no frame, no
            // checkpoint movement. An empty mutation is a no-op, not a
            // WAL entry.
            0 => Ok(()),

            // Exactly one durable record: standalone (tx id 0).
            1 => {
                let operation = &operations[0];

                // Before the WAL, never after it. An operation whose
                // keys the indexes would refuse cannot be allowed to
                // become durable: recovery would replay it into the
                // same refusal on every subsequent start. See
                // `Operation::validate`.
                operation.validate()?;

                let sequence = self.append_wal(0, operation.to_wal())?;

                self.apply_committed(operation)?;

                self.note_applied(sequence);

                Ok(())
            }

            // Two or more: one implicit single-statement transaction.
            // The same machinery `execute_transaction` uses for a wire
            // batch; the only difference is that a mutation primitive
            // resolved this operation list instead of
            // `lower_transaction` lowering it from a request.
            _ => {
                let sequence = Transaction::from_operations(operations)
                    .commit(|operation| self.apply_committed(operation))
                    // `Transaction::commit` speaks `io::Result`; the
                    // mutation API speaks `Result<_, String>`. Carry the
                    // message across rather than collapsing it into a
                    // generic failure — it is the only account the
                    // caller gets of what went wrong.
                    .map_err(|e| e.to_string())?;

                self.note_applied(sequence);

                Ok(())
            }
        }
    }

    /// Record that everything up to `sequence` is now reflected in the
    /// heap and indexes, and checkpoint if enough has accumulated.
    ///
    /// A checkpoint failure is logged, not returned: the mutation that
    /// triggered it is already durable in the WAL, so failing it here
    /// would report a committed write as failed. What a stuck checkpoint
    /// actually costs is a longer replay at the next startup.
    fn note_applied(&mut self, sequence: u64) {
        self.applied_sequence = self.applied_sequence.max(sequence);
        self.pending_mutations += 1;

        if self.pending_mutations < self.checkpoint_interval {
            return;
        }

        if let Err(e) = self.checkpoint() {
            eprintln!(
                "warning: failed to checkpoint physical storage at WAL \
                 sequence {}: {e}. The mutation is durable in the WAL and \
                 will be replayed on the next start.",
                self.applied_sequence
            );
        }
    }

    /// Push everything to stable storage and move the durability
    /// boundary.
    ///
    /// The order is the contract, and every step is load-bearing:
    ///
    /// ```text
    ///   1. compact      drain a dead heap segment into the live one
    ///   2. heap + catalog fsync'd  records exist, and are described
    ///   3. index metas fsync'd     entries that point at those records
    ///   4. WAL checkpoint          "everything ≤ N is on disk"
    ///   5. retire drained segments  now that nothing points into them
    /// ```
    ///
    /// A crash anywhere in 1–3 leaves the checkpoint where it was, so
    /// recovery replays those operations and reproduces exactly the
    /// state that did not make it. Every replay is idempotent by key —
    /// a re-applied insert rewrites the same key, a re-applied archive
    /// lands on the same `(address, version)` — so redoing work that
    /// *did* survive costs a superseded record and nothing else.
    pub fn checkpoint(&mut self) -> io::Result<()> {
        let drained = self.compact()?;

        self.store.sync()?;
        self.indexes.commit()?;

        crate::storage::checkpoint::advance(self.applied_sequence)?;

        self.pending_mutations = 0;

        self.rotate_wal()?;

        // Only now: the index entries that used to point into these
        // segments are committed elsewhere, so the bytes are genuinely
        // unreferenced. A crash before this leaves a segment full of
        // dead records that the next pass drains for free.
        for segment in drained {
            self.store.drop_segment(segment)?;
        }

        Ok(())
    }

    /// Reclaim the part of the WAL a future recovery would skip, once
    /// the log has grown past the rotation threshold.
    ///
    /// Called from [`Self::checkpoint`], immediately after the
    /// checkpoint boundary has moved and never before it: the records
    /// this drops are exactly the ones at or below the boundary, so
    /// dropping them before the boundary was durable would discard
    /// operations recovery still needs.
    ///
    /// Rotation is the counterpart of compaction. Compaction bounds the
    /// heap against a workload that rewrites the same rows forever;
    /// this bounds the WAL against a workload that simply keeps running.
    /// Without it the log is the one structure whose size tracks the
    /// database's *lifetime* rather than its contents, and since startup
    /// reads the whole log, an old database eventually cannot be opened
    /// at all.
    ///
    /// Safe to run here and only here: every mutation path holds `&mut
    /// self`, so no append can be in flight, and no transaction frame
    /// can be open — the staged-commit driver holds the same exclusive
    /// borrow for the whole of its frame.
    fn rotate_wal(&mut self) -> io::Result<()> {
        if wal::size()? < wal::rotate_threshold() {
            return Ok(());
        }

        // Re-read rather than reuse `applied_sequence`: `advance` clamps
        // to the lowest open transaction fence, so the value that
        // actually reached disk may be lower than the one requested, and
        // it is the durable value that defines what recovery will skip.
        let durable = crate::storage::checkpoint::read()?;

        wal::rotate(durable)?;

        Ok(())
    }

    // ---------------------------------------------------------------------
    // Compaction
    // ---------------------------------------------------------------------

    /// Drain heap segments that are mostly dead, returning the ones that
    /// are now safe to retire.
    ///
    /// Liveness is decided by asking the indexes, never by trusting the
    /// obsolete-byte counter: the counter picks *which* segment is worth
    /// the work, and then every record in it is checked against the
    /// index entry that would address it. A record the index no longer
    /// points at is garbage by definition, whatever the counter said.
    fn compact(&mut self) -> io::Result<Vec<u32>> {
        let candidates = self.store.compaction_candidates(COMPACTION_RATIO);

        let mut drained = Vec::new();

        for segment in candidates.into_iter().take(SEGMENTS_PER_COMPACTION) {
            self.drain_segment(segment)?;
            drained.push(segment);
        }

        Ok(drained)
    }

    fn drain_segment(&mut self, segment: u32) -> io::Result<()> {
        // Collect the live records' identities first and move them
        // afterwards. Scanning and appending at the same time would be
        // appending to a structure being walked, and holding whole
        // records would make the working set the size of the segment;
        // an identity is a key, and keys are small.
        let mut live: Vec<(RecordLocation, LiveKey)> = Vec::new();

        {
            let indexes = &self.indexes;

            self.store.scan_segment(segment, |location, record| {
                let key = LiveKey::of(&record);

                if key.current_location(indexes)? == Some(location) {
                    live.push((location, key));
                }

                Ok(())
            })?;
        }

        for (old, key) in live {
            let record = self.store.read(old)?;
            let fresh = self.store.append(&record)?;

            key.repoint(&self.indexes, fresh)?;
        }

        Ok(())
    }

    // ---------------------------------------------------------------------
    // Recovery-only mutation path
    // ---------------------------------------------------------------------
    //
    // IMPORTANT:
    //
    // These methods MUST NOT call append_wal().
    //
    // Recovery reads the WAL and applies these operations directly.
    // Calling normal mutation methods during recovery would append
    // another WAL record while replaying the original WAL.
    //
    // They DO write to physical storage, unlike the memory-only replay
    // this engine used to do. That is a consequence of the indexes being
    // authoritative: there is no in-memory state to reconstruct
    // separately from the durable one, so "replay" and "apply" are the
    // same operation minus the WAL append. Every one of them is
    // idempotent by key, which is what makes replaying an operation
    // whose effects already reached disk harmless.
    // ---------------------------------------------------------------------

    pub(crate) fn replay_archive(
        &mut self,
        entry: HistoryEntry,
    ) -> Result<(), String> {
        self.apply_committed(&Operation::Archive(entry))
    }

    pub(crate) fn replay_insert(
        &mut self,
        node: Node,
    ) -> Result<(), String> {
        self.apply_committed(&Operation::Insert(node))
    }

    pub(crate) fn replay_delete(
        &mut self,
        address: &str,
    ) -> Result<(), String> {
        self.apply_committed(&Operation::Delete(address.to_string()))
    }

    pub(crate) fn replay_insert_edge(
        &mut self,
        edge: Edge,
    ) -> Result<(), String> {
        self.apply_committed(&Operation::InsertEdge(edge))
    }

    pub(crate) fn replay_delete_edge(
        &mut self,
        id: &EdgeId,
    ) -> Result<(), String> {
        self.apply_committed(&Operation::DeleteEdge(id.clone()))
    }

    pub(crate) fn replay_insert_user(
        &mut self,
        record: UserRecord,
    ) -> Result<(), String> {
        self.apply_committed(&Operation::InsertUser(record))
    }

    pub(crate) fn replay_revoke_user(
        &mut self,
        token_hash: &str,
    ) -> Result<(), String> {
        self.apply_committed(&Operation::RevokeUser(token_hash.to_string()))
    }

    /// Note how far recovery replayed, so the checkpoint it takes
    /// afterwards moves the durability boundary across the work it just
    /// redid instead of leaving it to be redone again next time.
    pub(crate) fn note_recovered(&mut self, sequence: u64) {
        self.applied_sequence = self.applied_sequence.max(sequence);
    }

    // ---------------------------------------------------------------------
    // Construction / loading
    // ---------------------------------------------------------------------

    /// Open the physical database: catalog, heap, indexes.
    ///
    /// Cost is proportional to metadata — one catalog read and two meta
    /// pages per index — and not to the number of records stored. That
    /// is the property this whole layer exists for: a database with a
    /// billion nodes opens as fast as one with ten.
    pub fn open() -> io::Result<Self> {
        // Before a single file is touched. Everything below this line —
        // the page allocator, the index meta slots, the WAL counters —
        // is process-local state describing shared files, so a second
        // process opening the same directory does not race, it corrupts.
        // See `storage::lock`.
        crate::storage::lock::acquire()?;

        let catalog = Arc::new(Catalog::open()?);
        let store = RecordStore::open(Arc::clone(&catalog));
        let indexes = Indexes::open()?;

        let checkpoint_interval = std::env::var(CHECKPOINT_INTERVAL_ENV)
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .filter(|interval| *interval > 0)
            .unwrap_or(DEFAULT_CHECKPOINT_INTERVAL);

        Ok(Self {
            catalog,
            store,
            indexes,
            cache: RecordCache::new(),
            users: HashMap::new(),
            reads_total: AtomicU64::new(0),
            writes_total: AtomicU64::new(0),
            applied_sequence: 0,
            pending_mutations: 0,
            checkpoint_interval,
            user_log_records: 0,
        })
    }

    /// Open the database and load the identities needed to serve it.
    ///
    /// This used to be the full-database load: every node, edge and
    /// history entry replayed out of four append-only logs into
    /// `HashMap`s before the server could accept a request. It is now
    /// the catalog, the index roots, and one replay of `facetql.users`
    /// — which is bounded by the number of identities, not by the amount
    /// of data.
    ///
    /// Users keep their own append-only log with last-write-wins replay
    /// (`Put` inserts, `Revoke` removes) because they are the one thing
    /// that must be fully resident anyway; putting them in the heap
    /// would add an index without removing a scan.
    ///
    /// WAL recovery is deliberately separate from this. The recovery
    /// layer subsequently applies committed WAL operations through the
    /// replay_* methods without generating new WAL records.
    pub fn load() -> io::Result<Self> {
        let mut engine = Self::open()?;

        let log = binary::read_all_records::<UserOpRecord>(&binary::users_path())?;

        engine.user_log_records = log.len();

        for (_offset, record) in log {
            match record {
                UserOpRecord::Put(user) => {
                    engine.users.insert(user.token_hash.clone(), user);
                }

                UserOpRecord::Revoke(token_hash) => {
                    engine.users.remove(&token_hash);
                }
            }
        }

        Ok(engine)
    }

    /// True when the database holds no nodes.
    ///
    /// Read off the primary index's entry count rather than by counting
    /// anything.
    pub fn is_empty(&self) -> bool {
        self.indexes.primary.len() == 0
    }

    // ---------------------------------------------------------------------
    // Record access
    // ---------------------------------------------------------------------

    /// The node at `address`, through the primary index.
    ///
    /// ```text
    ///   cache hit  → done
    ///   cache miss → primary index → RecordLocation → page → record
    /// ```
    ///
    /// A miss is ordinary and correct; the cache changes how long this
    /// takes and never what it returns.
    fn read_node(&self, address: &str) -> io::Result<Option<Node>> {
        if let Some(node) = self.cache.get(address) {
            return Ok(Some((*node).clone()));
        }

        let Some(location) = self.node_location(address)? else {
            return Ok(None);
        };

        let node = self.node_at(location)?;

        self.cache.put(address, Arc::new(node.clone()));

        Ok(Some(node))
    }

    fn node_location(&self, address: &str) -> io::Result<Option<RecordLocation>> {
        match self.indexes.primary.get(address.as_bytes())? {
            Some(raw) => Ok(Some(RecordLocation::decode(&raw)?)),
            None => Ok(None),
        }
    }

    fn node_at(&self, location: RecordLocation) -> io::Result<Node> {
        match self.store.read(location)? {
            HeapRecord::Node(node) => Ok(node),
            other => Err(mismatched_record("node", &other)),
        }
    }

    fn edge_at(&self, location: RecordLocation) -> io::Result<Edge> {
        match self.store.read(location)? {
            HeapRecord::Edge(edge) => Ok(edge),
            other => Err(mismatched_record("edge", &other)),
        }
    }

    fn history_at(&self, location: RecordLocation) -> io::Result<HistoryEntry> {
        match self.store.read(location)? {
            HeapRecord::History(entry) => Ok(entry),
            other => Err(mismatched_record("history entry", &other)),
        }
    }

    fn contains_node(&self, address: &str) -> io::Result<bool> {
        if self.cache.get(address).is_some() {
            return Ok(true);
        }

        Ok(self.indexes.primary.get(address.as_bytes())?.is_some())
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
    /// overwrite is the canonical two-record case: `Archive` + `Insert`
    /// go into one frame, so a crash can never leave history claiming a
    /// value was superseded by a value that never landed.
    pub fn insert(&mut self, node: Node) -> Result<(), String> {
        let mut operations = Vec::with_capacity(2);

        // Archive the previous value, staged immediately ahead of the
        // insert that supersedes it — the same ordering
        // `lower_transaction` produces for an overwrite — so recovery
        // rebuilds history identically whichever path wrote the frame.
        if let Some(previous) = self.read_node(&node.address).map_err(io_message)? {
            operations.push(Operation::Archive(HistoryEntry::now(previous)));
        }

        operations.push(Operation::Insert(node));

        self.apply_atomic(operations)
    }

    /// Every archived previous state for `address`, oldest first.
    ///
    /// A prefix scan of the history index, so reading one node's history
    /// costs its own versions and nothing else — it does not touch, and
    /// is not slowed by, any other node's. The index is keyed
    /// `(address, version)` with the version big-endian, so the scan
    /// comes back in chronological order without sorting.
    ///
    /// Does not include the current live value.
    pub fn history_for(&self, address: &str) -> io::Result<Vec<HistoryEntry>> {
        let mut entries = Vec::new();

        let cap = max_scan_rows();

        self.indexes.history.for_each_range(
            &keys::history_prefix(address),
            None,
            false,
            |_key, value| {
                if entries.len() >= cap {
                    return Err(scan_limit_exceeded("this node's history"));
                }

                entries.push(self.history_at(RecordLocation::decode(value)?)?);
                Ok(true)
            },
        )?;

        Ok(entries)
    }

    /// The node at `address`, or `None`.
    pub fn get(&self, address: &str) -> io::Result<Option<Node>> {
        self.reads_total.fetch_add(1, Ordering::Relaxed);

        self.read_node(address)
    }

    // ---------------------------------------------------------------------
    // Claims
    // ---------------------------------------------------------------------

    /// Atomically claims a node for `worker`.
    ///
    /// StorageEngine itself does not provide an independent concurrency
    /// primitive. The database layer serializes mutations through its
    /// write lock, so the check and update occur within one mutation.
    ///
    /// The write half is a read-modify-write whose target exists by
    /// definition, so the `insert` below always archives and therefore
    /// always resolves to the two-record `Archive` + `Insert` frame. A
    /// claim is where a torn mutation would hurt most: a crash between
    /// the archive and the insert would leave the node *unclaimed* while
    /// history recorded that it had been superseded.
    pub fn claim(
        &mut self,
        address: &str,
        worker: &str,
    ) -> Result<(), ClaimError> {
        let mut node = match self
            .read_node(address)
            .map_err(|e| ClaimError::StorageError(e.to_string()))?
        {
            Some(node) => node,
            None => return Err(ClaimError::NotFound),
        };

        if let Some(existing) = &node.claimed_by {
            return Err(ClaimError::AlreadyClaimed(existing.clone()));
        }

        node.claimed_by = Some(worker.to_string());

        self.insert(node).map_err(ClaimError::StorageError)?;

        Ok(())
    }

    // ---------------------------------------------------------------------
    // Delete
    // ---------------------------------------------------------------------

    /// Removes a node.
    ///
    /// The value being removed is archived to history first, exactly as
    /// an overwrite archives the value it replaces. A delete is the last
    /// thing that ever happens to a node's state, so if it is the one
    /// transition that doesn't archive, the final state is the one state
    /// nobody can ever look up again.
    ///
    /// That archive makes a live delete a two-record mutation, so it
    /// goes through a frame under the rule in [`Self::apply_atomic`]:
    /// `BEGIN → Archive → Delete → COMMIT`.
    ///
    /// Deleting an address that holds nothing archives nothing and
    /// resolves to a single `Delete`, which the apply step treats as a
    /// no-op. Deleting an absent address is idempotent rather than an
    /// error, so a repeated delete is harmless.
    ///
    /// The record bytes are not erased. The primary index stops pointing
    /// at them, which is what makes the node gone; compaction reclaims
    /// the space later.
    pub fn delete(&mut self, address: &str) -> Result<(), String> {
        let mut operations = Vec::with_capacity(2);

        if let Some(existing) = self.read_node(address).map_err(io_message)? {
            operations.push(Operation::Archive(HistoryEntry::now(existing)));
        }

        operations.push(Operation::Delete(address.to_string()));

        self.apply_atomic(operations)
    }

    /// Does one node fall inside a bulk-delete selection?
    ///
    /// This is the single selection rule behind both `clear_kind` and
    /// `delete_where`: the node's `kind` must match, the caller must be
    /// allowed to write it (an admin matches all of that kind; a
    /// non-admin only what it owns), and — when a `where_` predicate is
    /// present — the predicate must hold against the node's decoded
    /// `data`.
    ///
    /// The predicate is run through the *same* `predicate::eval` the
    /// `/nodes/query` path uses, so a predicated bulk delete selects
    /// byte-for-byte the rows the equivalent query would. A predicate
    /// `eval` can't push down is surfaced as `Err`, which the
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
    /// the mutation, and nothing else evaluates a CAS condition. On
    /// success `set`'s entries are *merged* into the node's data rather
    /// than replacing it — so a caller can move one field without having
    /// to resend, and risk clobbering, the rest of the node.
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

    /// Live addresses a `DeleteWhere` would remove.
    ///
    /// Driven by the **kind index**: a bulk delete always names a kind,
    /// so the candidates are that kind's prefix range and nothing else.
    /// This used to walk every node in the database to find them, which
    /// made the cost of clearing one small kind proportional to the size
    /// of everything else.
    ///
    /// Read-only: it computes addresses without touching WAL, disk or
    /// index state, so the handler can call it to report the exact
    /// addresses a delete will remove. Returned addresses are sorted,
    /// because the index scan is already in address order within the
    /// kind.
    pub(crate) fn delete_where_targets(
        &self,
        kind: &str,
        where_: Option<&Expr>,
        owner: &str,
        is_admin: bool,
    ) -> Result<Vec<String>, String> {
        let mut targets = Vec::new();
        let mut failure: Option<String> = None;
        let cap = max_scan_rows();

        let prefix = keys::kind_prefix(kind);

        self.indexes
            .kind
            .for_each_range(&prefix, None, false, |key, _value| {
                let address = address_from_key(key, &prefix);

                let Some(node) = self.read_node(&address)? else {
                    // The kind index and the primary index are written
                    // in the same apply step, so this cannot happen in a
                    // consistent database; skipping rather than failing
                    // keeps one stale entry from making a whole query
                    // unanswerable.
                    return Ok(true);
                };

                match Self::selection_matches(&node, kind, where_, owner, is_admin) {
                    Ok(true) => {
                        // A bulk delete resolves to a concrete address
                        // list before anything is logged, so the list is
                        // held whole. Refusing is the only safe answer:
                        // truncating it would delete a subset of what the
                        // caller named.
                        if targets.len() >= cap {
                            return Err(scan_limit_exceeded("this bulk delete"));
                        }

                        targets.push(address)
                    }
                    Ok(false) => {}
                    Err(e) => {
                        failure = Some(e);
                        return Ok(false);
                    }
                }

                Ok(true)
            })
            .map_err(io_message)?;

        if let Some(e) = failure {
            return Err(e);
        }

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
    /// (see [`EdgeId`]), and it is the index key, so re-asserting an
    /// existing relationship lands on the same entry rather than beside
    /// it.
    ///
    /// Replacing an edge owned by someone else is rejected, mirroring
    /// the cross-owner rejection `insert` does for nodes on the
    /// transaction path: the owner is who may *retract* the edge, so
    /// silently overwriting it would hand that right to whoever asserted
    /// the relationship most recently.
    ///
    /// One durable record, so this stays standalone under the rule in
    /// [`Self::apply_atomic`].
    pub fn insert_edge(&mut self, edge: Edge) -> Result<(), String> {
        if !self.contains_node(&edge.from).map_err(io_message)? {
            return Err(format!("edge 'from' address not found: {}", edge.from));
        }

        if !self.contains_node(&edge.to).map_err(io_message)? {
            return Err(format!("edge 'to' address not found: {}", edge.to));
        }

        if let Some(existing) = self.find_edge(&edge.id()).map_err(io_message)? {
            if existing.owner != edge.owner {
                return Err(format!(
                    "edge {} -[{}]-> {} is owned by {}",
                    edge.from, edge.kind, edge.to, existing.owner
                ));
            }
        }

        self.apply_atomic(vec![Operation::InsertEdge(edge)])
    }

    /// The live edge with this identity, or `None`.
    ///
    /// A single point lookup in the outgoing-edge index, which is keyed
    /// by exactly this identity. It used to be a linear scan of the
    /// source node's adjacency list.
    pub fn find_edge(&self, id: &EdgeId) -> io::Result<Option<Edge>> {
        let key = keys::edge_out_key(&id.from, &id.kind, &id.to);

        match self.indexes.edge_out.get(&key)? {
            Some(raw) => Ok(Some(self.edge_at(RecordLocation::decode(&raw)?)?)),
            None => Ok(None),
        }
    }

    /// Removes one edge.
    ///
    /// Errors when there is no such edge, so a caller (and the
    /// `DELETE /edge` route above it) can tell "removed" from "was never
    /// there" instead of reporting success for a relationship that never
    /// existed. Authorization is the caller's: this is the storage
    /// primitive, and the ownership check lives where the requester's
    /// identity does — the route, or `TxOperation::DeleteEdge`.
    pub fn delete_edge(&mut self, id: &EdgeId) -> Result<(), String> {
        if self.find_edge(id).map_err(io_message)?.is_none() {
            return Err(format!(
                "edge not found: {} -[{}]-> {}",
                id.from, id.kind, id.to
            ));
        }

        self.apply_atomic(vec![Operation::DeleteEdge(id.clone())])
    }

    /// Every edge leaving `address`.
    ///
    /// A prefix scan of the outgoing-edge index. The graph is not
    /// resident: traversing one node's edges reads that node's range and
    /// the records it names, and is unaffected by how many edges the
    /// rest of the graph has.
    pub fn edges_from(&self, address: &str) -> io::Result<Vec<Edge>> {
        self.edges_in_range(&self.indexes.edge_out, &keys::edge_out_prefix(address))
    }

    /// Every edge arriving at `address`, through the mirror index.
    pub fn edges_to(&self, address: &str) -> io::Result<Vec<Edge>> {
        self.edges_in_range(&self.indexes.edge_in, &keys::edge_in_prefix(address))
    }

    fn edges_in_range(
        &self,
        index: &crate::storage::btree::BTree,
        prefix: &[u8],
    ) -> io::Result<Vec<Edge>> {
        let mut edges = Vec::new();
        let cap = max_scan_rows();

        index.for_each_range(prefix, None, false, |_key, value| {
            if edges.len() >= cap {
                return Err(scan_limit_exceeded("this node's edge list"));
            }

            edges.push(self.edge_at(RecordLocation::decode(value)?)?);
            Ok(true)
        })?;

        Ok(edges)
    }

    // ---------------------------------------------------------------------
    // Queries
    // ---------------------------------------------------------------------

    /// Every live node owned by `owner`, through the owner index.
    pub fn nodes_by_owner(&self, owner: &str) -> io::Result<Vec<Node>> {
        let mut nodes = Vec::new();
        let prefix = keys::owner_prefix(owner);
        let cap = max_scan_rows();

        self.indexes
            .owner
            .for_each_range(&prefix, None, false, |key, _value| {
                if nodes.len() >= cap {
                    return Err(scan_limit_exceeded("this owner's node list"));
                }

                let address = address_from_key(key, &prefix);

                if let Some(node) = self.read_node(&address)? {
                    nodes.push(node);
                }

                Ok(true)
            })?;

        Ok(nodes)
    }

    /// General-purpose listing.
    ///
    /// The access path is chosen from the filters, which is the whole
    /// difference between this and the scan it replaced:
    ///
    /// ```text
    ///   kind given   → kind index prefix range
    ///   owner given  → owner index prefix range
    ///   neither      → primary index scan (no better path exists)
    /// ```
    ///
    /// `requester` controls the per-node visibility filter, the same way
    /// a real DBMS distinguishes a normal role from a superuser:
    ///
    /// * `Some(r)` — apply `n.can_read(r)`: the caller sees public nodes
    ///   plus the private nodes it owns.
    /// * `None` — admin/superuser bypass: skip the visibility filter
    ///   entirely and return every node matching `kind`/`owner`. A
    ///   normal role must never pass `None`.
    pub fn query(
        &self,
        kind: Option<&str>,
        owner: Option<&str>,
        requester: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> io::Result<Vec<Node>> {
        self.reads_total.fetch_add(1, Ordering::Relaxed);

        // Asking for nothing reads nothing. Checked up front because the
        // scan's stop condition is "the page is full", which a zero-row
        // page is from the start — without this, the first candidate
        // would be pushed before the condition could be tested.
        if limit == 0 {
            return Ok(Vec::new());
        }

        if offset > max_query_offset() {
            return Err(offset_too_large(offset));
        }

        let mut nodes = Vec::new();
        let mut skipped = 0usize;

        self.scan_candidates(kind, owner, None, false, |node| {
            if let Some(k) = kind {
                if node.kind != k {
                    return Ok(true);
                }
            }

            if let Some(o) = owner {
                if node.owner != o {
                    return Ok(true);
                }
            }

            if let Some(r) = requester {
                if !node.can_read(r) {
                    return Ok(true);
                }
            }

            if skipped < offset {
                skipped += 1;
                return Ok(true);
            }

            nodes.push(node);

            Ok(nodes.len() < limit)
        })?;

        Ok(nodes)
    }

    /// Walk candidate nodes in address order through the narrowest
    /// index the filters allow, stopping as soon as `visit` says so.
    ///
    /// `start_after` resumes strictly past an address, which is what
    /// makes keyset pagination read only the page it returns rather than
    /// re-walking everything before it.
    fn scan_candidates<F>(
        &self,
        kind: Option<&str>,
        owner: Option<&str>,
        start_after: Option<&str>,
        reverse: bool,
        mut visit: F,
    ) -> io::Result<()>
    where
        F: FnMut(Node) -> io::Result<bool>,
    {
        // Kind is the more selective of the two in every workload this
        // engine serves (a kind is an entity type; an owner is a
        // person), so it wins when both are present. The other filter is
        // still applied to each row by the caller.
        let (index, prefix) = match (kind, owner) {
            (Some(k), _) => (&self.indexes.kind, keys::kind_prefix(k)),
            (None, Some(o)) => (&self.indexes.owner, keys::owner_prefix(o)),
            (None, None) => {
                // No index narrows this: walk the primary index itself,
                // which is still an ordered index scan rather than an
                // unordered walk of every record, and still reads only
                // as far as the caller asks it to.
                let after = start_after.map(|a| a.as_bytes().to_vec());

                return self.indexes.primary.for_each_range(
                    &[],
                    after.as_deref(),
                    reverse,
                    |key, value| {
                        let _ = key;
                        visit(self.node_at(RecordLocation::decode(value)?)?)
                    },
                );
            }
        };

        let after = start_after.map(|address| {
            let mut key = prefix.clone();
            key.extend_from_slice(address.as_bytes());
            key
        });

        index.for_each_range(&prefix, after.as_deref(), reverse, |key, _value| {
            let address = address_from_key(key, &prefix);

            match self.read_node(&address)? {
                Some(node) => visit(node),
                None => Ok(true),
            }
        })
    }

    /// Predicate-pushdown query: the `kind`/`owner`/visibility filtering
    /// of `query()`, plus a pushable `Expr` predicate evaluated against
    /// each candidate node's decoded `data`, plus in-engine ordering.
    ///
    /// `item_var` is the loop-variable name the predicate's field
    /// accesses are written against (mirrors FCT's `Query.ItemVar`).
    ///
    /// # Two access paths, and why there are two
    ///
    /// * **Ordered by address** (`order` absent, or `"id"`): served
    ///   entirely by an index range scan. The index is already in
    ///   address order, so the page is read by walking forward (or
    ///   backward) from the cursor and stopping at `limit` — nothing
    ///   outside the returned page is read at all.
    ///
    /// * **Ordered by a `data` field**: there is no index on `data`, so
    ///   the matching candidates have to be materialized and sorted.
    ///   This is the one query shape whose working set is proportional
    ///   to the result, and it is the honest cost of ordering by an
    ///   unindexed field — the alternative would be pretending an access
    ///   path exists that does not.
    ///
    /// Pagination is an **opaque keyset cursor**, matching FCT's
    /// `Query.After` contract: the ordering is the composite
    /// `(order_field, address)` — `address` is the stable tiebreak that
    /// makes the total order deterministic even when the `order_field`
    /// values collide — and a cursor encodes the last returned row's
    /// `(order_value, address)`. The next page selects rows *strictly
    /// past* that point in the requested direction, so paging stays
    /// stable under concurrent inserts and deletes the way a plain
    /// offset does not.
    ///
    /// `requester` carries the same admin/superuser semantics as
    /// [`Self::query`].
    #[allow(clippy::too_many_arguments)]
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
    ) -> Result<QueryPage, String> {
        self.reads_total.fetch_add(1, Ordering::Relaxed);

        // The cursor supersedes offset, so only an offset that will
        // actually be walked has to be bounded.
        if after.filter(|c| !c.is_empty()).is_none() && offset > max_query_offset()
        {
            return Err(offset_too_large(offset).to_string());
        }

        // Normalize the order field: `None` or `"id"` both mean "order
        // by address alone" (the tiebreak becomes the whole key).
        let order_field = order.filter(|o| *o != "id");

        let cursor = match after.filter(|c| !c.is_empty()) {
            Some(encoded) => Some(Cursor::decode(encoded)?),
            None => None,
        };

        let matches = |node: &Node| -> Result<bool, String> {
            if let Some(k) = kind {
                if node.kind != k {
                    return Ok(false);
                }
            }

            if let Some(o) = owner {
                if node.owner != o {
                    return Ok(false);
                }
            }

            if let Some(r) = requester {
                if !node.can_read(r) {
                    return Ok(false);
                }
            }

            let Some(expr) = predicate else {
                return Ok(true);
            };

            let data: serde_json::Value =
                serde_json::from_str(&node.data).unwrap_or(serde_json::Value::Null);

            predicate::eval(expr, item_var, &data)
                .map(|v| matches!(v, serde_json::Value::Bool(true)))
                .map_err(|e| format!("predicate evaluation failed: {e}"))
        };

        if order_field.is_none() {
            return self.query_by_address(
                kind, owner, cursor, desc, limit, offset, matches,
            );
        }

        self.query_sorted(
            kind,
            owner,
            order_field,
            cursor,
            desc,
            limit,
            offset,
            matches,
        )
    }

    /// The index-ordered page: walk the access path from the cursor and
    /// stop at `limit`.
    #[allow(clippy::too_many_arguments)]
    fn query_by_address<F>(
        &self,
        kind: Option<&str>,
        owner: Option<&str>,
        cursor: Option<Cursor>,
        desc: bool,
        limit: usize,
        offset: usize,
        matches: F,
    ) -> Result<QueryPage, String>
    where
        F: Fn(&Node) -> Result<bool, String>,
    {
        let start_after = cursor.as_ref().map(|c| c.a.clone());

        let mut nodes: Vec<Node> = Vec::new();
        let mut skipped = 0usize;
        let mut more = false;
        let mut failure: Option<String> = None;

        // `offset` only applies when no cursor was supplied; it is the
        // pre-keyset fallback the wire contract still allows.
        let to_skip = if start_after.is_some() { 0 } else { offset };

        self.scan_candidates(
            kind,
            owner,
            start_after.as_deref(),
            desc,
            |node| {
                match matches(&node) {
                    Ok(true) => {}
                    Ok(false) => return Ok(true),
                    Err(e) => {
                        failure = Some(e);
                        return Ok(false);
                    }
                }

                if skipped < to_skip {
                    skipped += 1;
                    return Ok(true);
                }

                if nodes.len() == limit {
                    // One row past the page: proof that a next cursor is
                    // worth emitting, and the last row this scan reads.
                    more = true;
                    return Ok(false);
                }

                nodes.push(node);

                Ok(true)
            },
        )
        .map_err(io_message)?;

        if let Some(e) = failure {
            return Err(e);
        }

        let next = match nodes.last() {
            Some(last) if more => Cursor::from_node(last, None).encode(),
            _ => String::new(),
        };

        Ok(QueryPage { nodes, next })
    }

    /// The sorted page: materialize the matching candidates, order them
    /// by `(order_field, address)`, then cut the page out.
    #[allow(clippy::too_many_arguments)]
    fn query_sorted<F>(
        &self,
        kind: Option<&str>,
        owner: Option<&str>,
        order_field: Option<&str>,
        cursor: Option<Cursor>,
        desc: bool,
        limit: usize,
        offset: usize,
        matches: F,
    ) -> Result<QueryPage, String>
    where
        F: Fn(&Node) -> Result<bool, String>,
    {
        let mut candidates: Vec<Node> = Vec::new();
        let mut failure: Option<String> = None;
        let cap = max_scan_rows();

        self.scan_candidates(kind, owner, None, false, |node| {
            match matches(&node) {
                Ok(true) => {
                    // Ordering by an unindexed `data` field is the one
                    // query shape whose working set is the whole result
                    // rather than the page — see `max_scan_rows`.
                    if candidates.len() >= cap {
                        return Err(scan_limit_exceeded(
                            "ordering by an unindexed field",
                        ));
                    }

                    candidates.push(node);
                }
                Ok(false) => {}
                Err(e) => {
                    failure = Some(e);
                    return Ok(false);
                }
            }

            Ok(true)
        })
        .map_err(io_message)?;

        if let Some(e) = failure {
            return Err(e);
        }

        // Sort into the ascending base order `(order_key, address)`,
        // then flip for `desc`. The `address` tiebreak is what makes the
        // keyset cursor well-defined when `order_field` values repeat.
        candidates.sort_by(|a, b| {
            let base = match order_field {
                Some(field) => {
                    compare_order_keys(&order_key(a, field), &order_key(b, field))
                }
                None => std::cmp::Ordering::Equal,
            };

            base.then_with(|| a.address.cmp(&b.address))
        });

        if desc {
            candidates.reverse();
        }

        let start = match &cursor {
            Some(cursor) => candidates
                .iter()
                .position(|n| {
                    let c = cmp_node_to_cursor(n, order_field, cursor);

                    if desc {
                        c == std::cmp::Ordering::Less
                    } else {
                        c == std::cmp::Ordering::Greater
                    }
                })
                .unwrap_or(candidates.len()),
            None => offset.min(candidates.len()),
        };

        let total = candidates.len();

        let page: Vec<Node> = candidates
            .into_iter()
            .skip(start)
            .take(limit)
            .collect();

        let next = match page.last() {
            Some(last) if start + page.len() < total => {
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
    /// staged into a single `BEGIN … COMMIT` frame, so the whole shape —
    /// the node, the history entry if it replaced something, and all N
    /// edges — becomes visible together or not at all.
    ///
    /// Endpoint validation runs against this call's own staged view: an
    /// edge may point at any live node, or at the node being created
    /// here — which is not live yet but is staged ahead of every edge in
    /// the frame.
    ///
    /// The error tuple keeps its shape, but its `Vec<Edge>` is always
    /// empty: once the batch is atomic there is no such thing as "edges
    /// created before the failure".
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
        match self.read_node(&address) {
            Ok(Some(previous)) => {
                operations.push(Operation::Archive(HistoryEntry::now(previous)));
            }
            Ok(None) => {}
            Err(e) => return Err((e.to_string(), Vec::new())),
        }

        operations.push(Operation::Insert(node));

        let mut created = Vec::with_capacity(edge_targets.len());

        for (to, kind) in edge_targets {
            let edge = Edge::new(address.clone(), to, kind, owner.clone());

            // `from` is the node this call creates, so it counts as
            // present even though it is not indexed yet: it is staged
            // ahead of every edge in the frame. The check is written out
            // rather than assumed, so the staged rule stays visible if
            // the edge source ever stops being the new node.
            for (endpoint, label) in [(&edge.from, "from"), (&edge.to, "to")] {
                if endpoint == &address {
                    continue;
                }

                match self.contains_node(endpoint) {
                    Ok(true) => {}
                    Ok(false) => {
                        return Err((
                            format!("edge '{label}' address not found: {endpoint}"),
                            Vec::new(),
                        ));
                    }
                    Err(e) => return Err((e.to_string(), Vec::new())),
                }
            }

            operations.push(Operation::InsertEdge(edge.clone()));

            created.push(edge);
        }

        self.apply_atomic(operations).map_err(|e| (e, Vec::new()))?;

        Ok(created)
    }

    // ---------------------------------------------------------------------
    // Users
    // ---------------------------------------------------------------------

    /// Persists a new user record.
    ///
    /// Only the token hash is persisted. Plaintext tokens are never
    /// stored in the engine. One durable record, so this stays
    /// standalone under the rule in [`Self::apply_atomic`].
    pub fn insert_user(&mut self, record: UserRecord) -> Result<(), String> {
        self.apply_atomic(vec![Operation::InsertUser(record)])
    }

    /// Revokes a user by token hash. One durable record, standalone.
    pub fn revoke_user(&mut self, token_hash: &str) -> Result<(), String> {
        self.apply_atomic(vec![Operation::RevokeUser(token_hash.to_string())])
    }

    /// Rewrite `facetql.users` as one `Put` per live identity, when the
    /// log has grown far past the identities it describes.
    ///
    /// # Why this exists
    ///
    /// The user log is append-only and was the last durable structure
    /// with no reclamation at all. The heap has compaction, the WAL has
    /// rotation, the indexes reuse freed pages — this grew forever. Its
    /// *content* is bounded by the number of identities, but its
    /// *length* is bounded by the number of administrative operations
    /// ever performed, and those are not the same thing for any
    /// deployment that rotates credentials on a schedule. Every startup
    /// reads the whole file, so the cost of opening the database grew
    /// with the history of its identities rather than with how many it
    /// has.
    ///
    /// Rewriting is lossless because replay is last-write-wins over a
    /// total order: a log of one `Put` per surviving identity replays to
    /// exactly the map that is live right now. Revocations are not
    /// dropped — they are *applied*, which is the same thing once no
    /// earlier record survives to be outranked.
    ///
    /// # When it runs
    ///
    /// Once, at startup, after WAL recovery — the first moment the set
    /// of live identities is final. Never while serving: an append
    /// racing the rename would land in the file being replaced.
    ///
    /// A failure is reported and non-fatal. The old log is still intact
    /// and still correct; the only cost is that it stays long.
    pub fn compact_user_log(&mut self) -> io::Result<()> {
        // Rewriting costs one pass over the identities, so it should be
        // rare relative to appends. Four times the live count means an
        // idle database never rewrites and a heavily-rotated one
        // amortizes to a constant factor. The floor keeps a database
        // with a handful of users from rewriting on every restart.
        let threshold = (self.users.len() * 4).max(64);

        if self.user_log_records <= threshold {
            return Ok(());
        }

        let live: Vec<UserOpRecord> = self
            .users
            .values()
            .cloned()
            .map(UserOpRecord::Put)
            .collect();

        binary::rewrite_records(&binary::users_path(), &live)?;

        self.user_log_records = live.len();

        Ok(())
    }

    pub fn find_user_by_hash(&self, token_hash: &str) -> Option<&UserRecord> {
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
    /// the native source behind `GET /stats`.
    ///
    /// The structural counts are read straight off the indexes' entry
    /// counters, which each tree maintains in its meta page, so counting
    /// nodes or edges costs nothing. `kinds` is the exception: there is
    /// no per-kind counter, so it walks the kind index — an index-only
    /// scan that never reads a record, but still proportional to the
    /// number of nodes, which is why it lives on an explicit stats
    /// endpoint and not on a hot path.
    pub fn stats(&self) -> io::Result<EngineStats> {
        use std::collections::BTreeMap;

        let mut by_kind: BTreeMap<String, u64> = BTreeMap::new();

        self.indexes.kind.for_each_range(&[], None, false, |key, _value| {
            if let Some((kind, _)) = keys::split_component(key) {
                *by_kind
                    .entry(String::from_utf8_lossy(kind).into_owned())
                    .or_default() += 1;
            }

            Ok(true)
        })?;

        Ok(EngineStats {
            node_count: self.indexes.primary.len(),
            edge_count: self.indexes.edge_out.len(),
            user_count: self.users.len() as u64,
            history_entries: self.indexes.history.len(),
            kinds: by_kind
                .into_iter()
                .map(|(kind, count)| KindCount { kind, count })
                .collect(),
            reads_total: self.reads_total.load(Ordering::Relaxed),
            writes_total: self.writes_total.load(Ordering::Relaxed),
            storage: self.storage_stats(),
        })
    }

    /// Physical storage statistics: how many segments the heap has, how
    /// much of them is dead, and what compaction would reclaim.
    fn storage_stats(&self) -> StorageStats {
        self.catalog.with(|data| StorageStats {
            page_size: data.page_size,
            segments: data.segments.len() as u64,
            pages: data.segments.iter().map(|s| s.pages as u64).sum(),
            obsolete_bytes: data.segments.iter().map(|s| s.obsolete_bytes).sum(),
        })
    }

    // ---------------------------------------------------------------------
    // Transactions
    // ---------------------------------------------------------------------

    /// Executes a batch of operations as one crash-atomic transaction.
    ///
    /// The batch is *validated and resolved in full* before a single
    /// byte is written, then staged into a single durable
    /// `BEGIN … mutations … COMMIT` WAL frame, and only then applied:
    ///
    /// 1. [`Self::lower_transaction`] walks the ops in order against a
    ///    staged view of the data, validating each one and lowering it
    ///    into the concrete mutations it produces (expanding
    ///    `clear_kind`/`delete_where` into the exact set of deletes they
    ///    resolve to, and pairing each overwrite with its `Archive`).
    ///    Any validation failure returns here, with nothing written.
    ///
    /// 2. [`Transaction::commit`] stages every resolved mutation under
    ///    one transaction ID, writes the durable `COMMIT` marker, and
    ///    applies the batch through [`Self::apply_committed`].
    ///
    /// Crash behaviour: a crash before the `COMMIT` record is durable
    /// leaves an incomplete frame, which recovery discards in full — the
    /// batch never happened. A crash after it leaves a complete frame,
    /// which recovery replays in full. There is no in-between state
    /// where part of a batch survives.
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
        let lowered = self.lower_transaction(ops)?;

        // Checked after lowering, because lowering is where a batch's
        // real size becomes known — see `max_transaction_ops`. Checked
        // before `commit`, because past that point the first record is
        // durable and the batch can no longer be refused.
        if lowered.len() > max_transaction_ops() {
            return Err(TransactionError::Invalid(format!(
                "transaction failed, nothing applied: it resolves to {}                  mutations, over the {} this engine will stage in one                  frame. Split the batch, or raise {MAX_TRANSACTION_OPS_ENV}.",
                lowered.len(),
                max_transaction_ops()
            )));
        }

        let transaction = Transaction::from_operations(lowered);

        let sequence = transaction
            .commit(|operation| self.apply_committed(operation))
            .map_err(|e| TransactionError::Storage(e.to_string()))?;

        self.note_applied(sequence);

        Ok(())
    }

    /// Validate a batch and lower it into the concrete mutations it
    /// produces, without writing anything.
    ///
    /// Validation walks the operations **in order** against a staged
    /// view — live state overlaid with everything the batch has done so
    /// far — so each operation is judged against the state it will
    /// actually meet when applied. That ordering is what makes the apply
    /// pass infallible, and it has to be: the apply pass runs after the
    /// `COMMIT` marker is durable, where an error can no longer roll
    /// anything back.
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
        // resolves through the primary index.
        let mut staged: HashMap<String, Option<Node>> = HashMap::new();

        // The same overlay for edges, keyed by identity. Edges need
        // their own because they have their own key space.
        let mut staged_edges: HashMap<EdgeId, Option<Edge>> = HashMap::new();

        for op in ops {
            match op {
                TxOperation::InsertNode(node) => {
                    if let Some(existing) =
                        self.staged_node(&staged, &node.address).map_err(storage)?
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
                                node.address, existing.owner
                            )));
                        }

                        lowered.push(Operation::Archive(HistoryEntry::now(existing)));
                    }

                    staged.insert(node.address.clone(), Some(node.clone()));

                    lowered.push(Operation::Insert(node));
                }

                TxOperation::DeleteNode(address) => {
                    // Already removed earlier in this same batch: the
                    // target is gone, so there is nothing left to
                    // remove. Idempotent rather than an error — a bulk
                    // clear followed by an explicit delete of one of the
                    // nodes it removed is a valid batch.
                    if matches!(staged.get(&address), Some(None)) {
                        continue;
                    }

                    match self.staged_node(&staged, &address).map_err(storage)? {
                        // Archive the value being removed, as the
                        // standalone delete path does — a deleted node's
                        // final state is exactly the one an operator
                        // comes looking for.
                        Some(node) => {
                            lowered.push(Operation::Archive(HistoryEntry::now(node)))
                        }
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
                    self.lower_selection(
                        &mut lowered,
                        &mut staged,
                        &kind,
                        None,
                        &owner,
                        is_admin,
                    )?;
                }

                TxOperation::DeleteWhere { kind, where_, owner, is_admin } => {
                    // Same as ClearKind, plus the predicate. The one way
                    // a delete_where can abort the batch — an unpushable
                    // or erroring predicate — surfaces from the
                    // selection, here in the validate/lower pass, before
                    // anything is written.
                    self.lower_selection(
                        &mut lowered,
                        &mut staged,
                        &kind,
                        where_.as_ref(),
                        &owner,
                        is_admin,
                    )?;
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
                    let node = match self
                        .staged_node(&staged, &address)
                        .map_err(storage)?
                    {
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

                    let next = Self::set_if_next(&node, &field, &expect, &set)?;

                    // A CAS is an upsert that had to earn the right to
                    // run, so it lowers to the same pair as any other
                    // overwrite: archive what was there, then insert.
                    lowered.push(Operation::Archive(HistoryEntry::now(node)));

                    staged.insert(address, Some(next.clone()));

                    lowered.push(Operation::Insert(next));
                }

                TxOperation::InsertEdge(edge) => {
                    // Endpoints are checked against the staged view, so
                    // an edge may reference a node this batch inserted
                    // earlier, but not one it already deleted — and not
                    // one inserted *later*, because by then the edge has
                    // already been applied.
                    for (endpoint, label) in
                        [(&edge.from, "from"), (&edge.to, "to")]
                    {
                        if self
                            .staged_node(&staged, endpoint)
                            .map_err(storage)?
                            .is_none()
                        {
                            return Err(TransactionError::Invalid(format!(
                                "transaction failed, nothing applied: \
                                 edge '{label}' address not found: {endpoint}"
                            )));
                        }
                    }

                    // Recorded in the edge overlay so a later op in this
                    // batch — a `delete_edge` of it, or a second insert
                    // of the same identity — resolves against what the
                    // batch has done rather than against live state it
                    // has already moved past.
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

                    let edge = match self
                        .staged_edge(&staged_edges, &id)
                        .map_err(storage)?
                    {
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

    /// Expand a `clear_kind`/`delete_where` into the archives and
    /// deletes it resolves to, against the batch's staged view.
    fn lower_selection(
        &self,
        lowered: &mut Vec<Operation>,
        staged: &mut HashMap<String, Option<Node>>,
        kind: &str,
        where_: Option<&Expr>,
        owner: &str,
        is_admin: bool,
    ) -> Result<(), TransactionError> {
        let targets = self
            .staged_selection(staged, kind, where_, owner, is_admin)
            .map_err(TransactionError::Invalid)?;

        for address in targets {
            if let Some(node) =
                self.staged_node(staged, &address).map_err(storage)?
            {
                lowered.push(Operation::Archive(HistoryEntry::now(node)));
            }

            staged.insert(address.clone(), None);
            lowered.push(Operation::Delete(address));
        }

        Ok(())
    }

    /// The addresses a bulk delete selects **within a batch**: the same
    /// rule as [`Self::delete_where_targets`], applied to the staged
    /// view instead of to live state.
    ///
    /// Candidates come from the kind index — the same access path the
    /// live selection uses — plus every address the batch has touched,
    /// each resolved through the overlay. So a node the batch inserted
    /// is eligible and a node it already removed is not. Walked through
    /// a `BTreeSet`, making the returned order sorted and deterministic
    /// rather than dependent on `HashMap` iteration order; these
    /// addresses become WAL records, and a WAL should not vary run to
    /// run for identical input.
    fn staged_selection(
        &self,
        staged: &HashMap<String, Option<Node>>,
        kind: &str,
        where_: Option<&Expr>,
        owner: &str,
        is_admin: bool,
    ) -> Result<Vec<String>, String> {
        let mut candidates: std::collections::BTreeSet<String> =
            std::collections::BTreeSet::new();

        let prefix = keys::kind_prefix(kind);

        self.indexes
            .kind
            .for_each_range(&prefix, None, false, |key, _value| {
                candidates.insert(address_from_key(key, &prefix));
                Ok(true)
            })
            .map_err(io_message)?;

        candidates.extend(staged.keys().cloned());

        let mut targets = Vec::new();

        for address in candidates {
            let node = match self.staged_node(staged, &address).map_err(io_message)? {
                Some(node) => node,
                None => continue,
            };

            if Self::selection_matches(&node, kind, where_, owner, is_admin)? {
                targets.push(address);
            }
        }

        Ok(targets)
    }

    /// Resolve an address against a transaction's staged overlay,
    /// falling back to durable state.
    ///
    /// `Some(node)` in the overlay is a value the batch has written,
    /// `Some(None)` an address it has removed, and an absent key means
    /// the batch has not touched the address at all.
    fn staged_node(
        &self,
        staged: &HashMap<String, Option<Node>>,
        address: &str,
    ) -> io::Result<Option<Node>> {
        match staged.get(address) {
            Some(slot) => Ok(slot.clone()),
            None => self.read_node(address),
        }
    }

    /// The edge counterpart of [`Self::staged_node`].
    fn staged_edge(
        &self,
        staged: &HashMap<EdgeId, Option<Edge>>,
        id: &EdgeId,
    ) -> io::Result<Option<Edge>> {
        match staged.get(id) {
            Some(slot) => Ok(slot.clone()),
            None => self.find_edge(id),
        }
    }

    // ---------------------------------------------------------------------
    // Apply
    // ---------------------------------------------------------------------

    /// Apply one already-committed mutation to physical storage and the
    /// indexes.
    ///
    /// This is the apply half of **every** mutation path — the framed
    /// one, the standalone one, and WAL replay — and it is deliberately
    /// narrower than a mutation primitive:
    ///
    /// * It writes **no WAL record**. On the framed path the frame has
    ///   already logged this operation's intent under its transaction
    ///   ID; on the standalone path `apply_atomic` has already written
    ///   the one record before calling this.
    ///
    /// * It **does not advance the checkpoint** and does not fsync. The
    ///   writes land in the buffer pool and become durable at
    ///   [`Self::checkpoint`], which is also the only place the
    ///   checkpoint moves — and it moves only after the flush.
    ///
    /// * It is the **only** place `writes_total` is incremented, which
    ///   is what keeps the counter path-independent.
    ///
    /// # The index discipline
    ///
    /// Each arm below maintains *every* access path the operation
    /// affects, in one place, because the indexes are authoritative: a
    /// node written to the heap but missing from the kind index is not a
    /// slow query, it is a node that `GET /nodes?kind=…` says does not
    /// exist. An update that changes a node's `kind` or `owner` must
    /// therefore also retract the old membership, which is why the
    /// previous version is read before the new one is written.
    pub(crate) fn apply_committed(
        &mut self,
        operation: &Operation,
    ) -> Result<(), String> {
        self.apply_operation(operation).map_err(io_message)
    }

    fn apply_operation(&mut self, operation: &Operation) -> io::Result<()> {
        match operation {
            Operation::Archive(entry) => {
                let location = self.store.append(&HeapRecord::History(entry.clone()))?;

                let key = keys::history_key(&entry.address, entry.version);

                // Keyed by (address, version), so replaying an archive
                // whose record already reached disk repoints the same
                // key at an identical copy instead of appending a second
                // history entry — the duplication that used to make WAL
                // replay non-idempotent.
                if let Some(previous) = self.indexes.history.get(&key)? {
                    self.store.mark_obsolete(RecordLocation::decode(&previous)?);
                }

                self.indexes.history.put(&key, &location.encode())?;
            }

            Operation::Insert(node) => {
                let previous = self.node_location(&node.address)?;

                let previous_node = match previous {
                    Some(location) => Some(self.node_at(location)?),
                    None => None,
                };

                let location = self.store.append(&HeapRecord::Node(node.clone()))?;

                self.indexes
                    .primary
                    .put(node.address.as_bytes(), &location.encode())?;

                // Retract stale secondary memberships before asserting
                // the new ones. Only when they actually changed: an
                // update that keeps the same kind and owner rewrites
                // nothing here.
                if let Some(previous_node) = &previous_node {
                    if previous_node.kind != node.kind {
                        self.indexes
                            .kind
                            .remove(&keys::kind_key(&previous_node.kind, &node.address))?;
                    }

                    if previous_node.owner != node.owner {
                        self.indexes.owner.remove(&keys::owner_key(
                            &previous_node.owner,
                            &node.address,
                        ))?;
                    }
                }

                self.indexes
                    .kind
                    .put(&keys::kind_key(&node.kind, &node.address), &[])?;

                self.indexes
                    .owner
                    .put(&keys::owner_key(&node.owner, &node.address), &[])?;

                if let Some(location) = previous {
                    self.store.mark_obsolete(location);
                }

                self.cache.put(&node.address, Arc::new(node.clone()));

                self.writes_total.fetch_add(1, Ordering::Relaxed);
            }

            // Removing the primary index entry is what makes the node
            // gone: nothing resolves the address any more, so no read
            // path can reach the record even though its bytes are still
            // in the heap until compaction reclaims them.
            Operation::Delete(address) => {
                if let Some(location) = self.node_location(address)? {
                    let node = self.node_at(location)?;

                    self.indexes.primary.remove(address.as_bytes())?;
                    self.indexes
                        .kind
                        .remove(&keys::kind_key(&node.kind, address))?;
                    self.indexes
                        .owner
                        .remove(&keys::owner_key(&node.owner, address))?;

                    self.store.mark_obsolete(location);
                }

                self.cache.invalidate(address);

                self.writes_total.fetch_add(1, Ordering::Relaxed);
            }

            Operation::InsertEdge(edge) => {
                let out_key = keys::edge_out_key(&edge.from, &edge.kind, &edge.to);
                let in_key = keys::edge_in_key(&edge.to, &edge.kind, &edge.from);

                if let Some(previous) = self.indexes.edge_out.get(&out_key)? {
                    self.store.mark_obsolete(RecordLocation::decode(&previous)?);
                }

                let location = self.store.append(&HeapRecord::Edge(edge.clone()))?;

                self.indexes.edge_out.put(&out_key, &location.encode())?;
                self.indexes.edge_in.put(&in_key, &location.encode())?;

                self.writes_total.fetch_add(1, Ordering::Relaxed);
            }

            Operation::DeleteEdge(id) => {
                let out_key = keys::edge_out_key(&id.from, &id.kind, &id.to);
                let in_key = keys::edge_in_key(&id.to, &id.kind, &id.from);

                if let Some(previous) = self.indexes.edge_out.get(&out_key)? {
                    self.store.mark_obsolete(RecordLocation::decode(&previous)?);
                }

                self.indexes.edge_out.remove(&out_key)?;
                self.indexes.edge_in.remove(&in_key)?;

                self.writes_total.fetch_add(1, Ordering::Relaxed);
            }

            // Users keep their own append-only log: they are fully
            // resident by design (see the `users` field), so an index
            // would buy nothing.
            Operation::InsertUser(record) => {
                binary::append_record(
                    &binary::users_path(),
                    &UserOpRecord::Put(record.clone()),
                )?;

                self.user_log_records += 1;

                self.users.insert(record.token_hash.clone(), record.clone());
            }

            Operation::RevokeUser(token_hash) => {
                binary::append_record(
                    &binary::users_path(),
                    &UserOpRecord::Revoke(token_hash.clone()),
                )?;

                self.user_log_records += 1;

                self.users.remove(token_hash);
            }
        }

        Ok(())
    }
}

/// The identity of a record, used by compaction to ask the right index
/// whether the copy it found is still the live one.
enum LiveKey {
    Node(String),
    Edge(EdgeId),
    History(String, u64),
}

impl LiveKey {
    fn of(record: &HeapRecord) -> LiveKey {
        match record {
            HeapRecord::Node(node) => LiveKey::Node(node.address.clone()),
            HeapRecord::Edge(edge) => LiveKey::Edge(edge.id()),
            HeapRecord::History(entry) => {
                LiveKey::History(entry.address.clone(), entry.version)
            }
        }
    }

    /// Where the index currently says this record is. A record whose
    /// index entry points somewhere else — or nowhere — is garbage.
    fn current_location(
        &self,
        indexes: &Indexes,
    ) -> io::Result<Option<RecordLocation>> {
        let raw = match self {
            LiveKey::Node(address) => indexes.primary.get(address.as_bytes())?,
            LiveKey::Edge(id) => indexes
                .edge_out
                .get(&keys::edge_out_key(&id.from, &id.kind, &id.to))?,
            LiveKey::History(address, version) => {
                indexes.history.get(&keys::history_key(address, *version))?
            }
        };

        match raw {
            Some(bytes) => Ok(Some(RecordLocation::decode(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Point every index that addresses this record at its new home.
    fn repoint(
        &self,
        indexes: &Indexes,
        location: RecordLocation,
    ) -> io::Result<()> {
        let encoded = location.encode();

        match self {
            LiveKey::Node(address) => {
                indexes.primary.put(address.as_bytes(), &encoded)
            }

            // An edge is reachable through both directions, so both have
            // to move together or a reverse traversal would resolve to a
            // location in a segment that no longer exists.
            LiveKey::Edge(id) => {
                indexes
                    .edge_out
                    .put(&keys::edge_out_key(&id.from, &id.kind, &id.to), &encoded)?;

                indexes
                    .edge_in
                    .put(&keys::edge_in_key(&id.to, &id.kind, &id.from), &encoded)
            }

            LiveKey::History(address, version) => indexes
                .history
                .put(&keys::history_key(address, *version), &encoded),
        }
    }
}

/// The address half of a secondary-index key, which is everything after
/// the length-prefixed component the scan filtered on.
fn address_from_key(key: &[u8], prefix: &[u8]) -> String {
    String::from_utf8_lossy(&key[prefix.len().min(key.len())..]).into_owned()
}

fn mismatched_record(expected: &str, found: &HeapRecord) -> io::Error {
    let actual = match found {
        HeapRecord::Node(_) => "node",
        HeapRecord::Edge(_) => "edge",
        HeapRecord::History(_) => "history entry",
    };

    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "an index entry for a {expected} resolved to a {actual} record — \
             the index and the heap disagree about what is stored where"
        ),
    )
}

fn io_message(error: io::Error) -> String {
    error.to_string()
}

fn storage(error: io::Error) -> TransactionError {
    TransactionError::Storage(error.to_string())
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
/// Owns its nodes. It used to hold borrows into the engine's node map,
/// which is not something an out-of-core engine can offer: a record read
/// through the primary index into the heap is materialized for the read
/// that asked for it, and there is no long-lived map for a reference to
/// point into.
#[derive(Debug, Serialize)]
pub struct QueryPage {
    pub nodes: Vec<Node>,
    pub next: String,
}

/// Physical storage statistics, produced by
/// [`StorageEngine::storage_stats`]: the shape of the heap rather than
/// the shape of the data in it.
#[derive(Debug, Serialize)]
pub struct StorageStats {
    pub page_size: u32,
    pub segments: u64,
    pub pages: u64,
    pub obsolete_bytes: u64,
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
    /// The shape of the physical storage under all of the above.
    ///
    /// Additive: every field the endpoint already reported is unchanged
    /// and still means what it meant. This is here because the heap is
    /// now something that can be in a bad state — segments accumulating
    /// dead bytes faster than compaction reclaims them — and a subsystem
    /// whose health cannot be observed is a subsystem that fails
    /// silently.
    pub storage: StorageStats,
}

/// Longest `after` cursor this server will look at.
///
/// A cursor holds an address (bounded by the index key limit) and one
/// JSON order value, base64url'd — comfortably under a kilobyte in
/// practice. Four is generous headroom for a large order value and still
/// rejects a hostile cursor before anything is allocated from it.
const MAX_CURSOR_LEN: usize = 4 * 1024;

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
        // A cursor this engine issued is a base64url'd `{o, a}` pair, so
        // it is bounded by the address plus one order value. Checking the
        // length before decoding keeps a caller from handing back a
        // megabyte of base64 — which the body limit permits — and making
        // the server decode and JSON-parse it just to reject it.
        if s.len() > MAX_CURSOR_LEN {
            return Err(format!(
                "invalid cursor: {} bytes, over the {MAX_CURSOR_LEN}-byte \
                 maximum — this is not a cursor this server issued",
                s.len()
            ));
        }

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
    /// removal takes the same WAL + index path as a single
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
    /// removal takes the same WAL + index path as a single
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
pub(crate) mod test_support {
    //! One data directory, and one writer into it at a time.
    //!
    //! Both halves of that are now load-bearing. `config` resolves the
    //! data directory through a process-wide `OnceLock`, so every test
    //! in this binary shares one directory whatever it asks for — and a
    //! `StorageEngine` is a *writer* over that directory's index files
    //! and heap segments, not a private in-memory map. Two engines open
    //! at once are two writers on the same B-tree files, each publishing
    //! its own root over the other's.
    //!
    //! So the lock is global rather than per-module: a per-module lock
    //! serializes a module against itself and lets the module next door
    //! run concurrently, which is exactly the case that corrupts. Tests
    //! still use distinct kinds and addresses, because the directory
    //! persists across tests within a run.
    use std::sync::{Mutex, MutexGuard, OnceLock};

    pub fn disk_guard() -> MutexGuard<'static, ()> {
        static LOCK: Mutex<()> = Mutex::new(());
        static INIT: OnceLock<()> = OnceLock::new();

        INIT.get_or_init(|| {
            let dir = std::env::temp_dir()
                .join(format!("facetql-test-{}", std::process::id()));

            std::fs::create_dir_all(&dir).expect("create temp data dir");

            crate::config::set_data_dir(dir);
        });

        LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod cursor_tests {
    //! Keyset pagination, the cursor codec, and the admin visibility
    //! bypass.
    //!
    //! These used to build state by writing straight into the engine's
    //! node map, which no longer exists — the database is the heap and
    //! the indexes on disk, so state is built through `insert`/`delete`
    //! like any other caller builds it. Two consequences show up in
    //! every test below: they need a data directory, and they share one
    //! with every other test module in this binary (`config` resolves it
    //! through a process-wide `OnceLock`). So each test works in its own
    //! `kind`, which is also the access path a `kind`-filtered query
    //! uses, and therefore the thing that keeps one test's rows out of
    //! another's results.
    use super::*;
    use crate::core::coordinate::Coordinate;
    use crate::core::node::{Node, Visibility};

    use crate::storage::engine::test_support::disk_guard;

    fn make_node(kind: &str, address: &str, score: i64) -> Node {
        let mut n = Node::new(
            Coordinate::new(0, 0, 0, 0),
            address.to_string(),
            kind.to_string(),
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
        kind: &str,
        order: Option<&str>,
        desc: bool,
        limit: usize,
    ) -> Vec<String> {
        let mut out = Vec::new();
        let mut after: Option<String> = None;

        for _ in 0..1000 {
            let page = engine
                .query_where(
                    Some(kind),
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
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        for (addr, score) in
            [("ks1:a", 30), ("ks1:b", 10), ("ks1:c", 20), ("ks1:d", 50), ("ks1:e", 40)]
        {
            e.insert(make_node("KsOrdered", addr, score)).expect("insert");
        }

        // Ascending by score: b(10) c(20) a(30) e(40) d(50)
        let asc = page_all(&e, "KsOrdered", Some("score"), false, 2);
        assert_eq!(asc, vec!["ks1:b", "ks1:c", "ks1:a", "ks1:e", "ks1:d"]);

        // Descending by score.
        let desc = page_all(&e, "KsOrdered", Some("score"), true, 2);
        assert_eq!(desc, vec!["ks1:d", "ks1:e", "ks1:a", "ks1:c", "ks1:b"]);
    }

    #[test]
    fn keyset_tiebreak_by_address_with_equal_order_values() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        // All identical score => order is purely by address tiebreak.
        for addr in ["ks2:a", "ks2:b", "ks2:c", "ks2:d", "ks2:e"] {
            e.insert(make_node("KsTiebreak", addr, 7)).expect("insert");
        }

        let asc = page_all(&e, "KsTiebreak", Some("score"), false, 2);
        assert_eq!(asc, vec!["ks2:a", "ks2:b", "ks2:c", "ks2:d", "ks2:e"]);
    }

    #[test]
    fn keyset_stable_when_a_row_is_deleted_between_pages() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        for (addr, score) in [("ks3:a", 10), ("ks3:b", 20), ("ks3:c", 30), ("ks3:d", 40)] {
            e.insert(make_node("KsStable", addr, score)).expect("insert");
        }

        // First page ascending, limit 2 => a, b ; cursor sits at b(20).
        let page1 = e
            .query_where(
                Some("KsStable"), None, None, None, "item", Some("score"), false, None, 2, 0,
            )
            .expect("query_where ok");

        let p1_addrs: Vec<String> =
            page1.nodes.iter().map(|n| n.address.clone()).collect();
        let p1_next = page1.next.clone();

        assert_eq!(p1_addrs, vec!["ks3:a", "ks3:b"]);
        assert!(!p1_next.is_empty());

        // Delete an already-seen row; the next page must still be c, d
        // (offset would have skipped c here).
        e.delete("ks3:a").expect("delete");

        let page2 = e
            .query_where(
                Some("KsStable"), None, None, None, "item", Some("score"), false,
                Some(&p1_next), 2, 0,
            )
            .expect("query_where ok");

        assert_eq!(
            page2.nodes.iter().map(|n| n.address.as_str()).collect::<Vec<_>>(),
            vec!["ks3:c", "ks3:d"]
        );
        assert!(page2.next.is_empty(), "reached last page");
    }

    #[test]
    fn order_by_address_when_order_absent() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        for addr in ["ks4:c", "ks4:a", "ks4:b"] {
            e.insert(make_node("KsAddressOrder", addr, 1)).expect("insert");
        }

        let all = page_all(&e, "KsAddressOrder", None, false, 1);
        assert_eq!(all, vec!["ks4:a", "ks4:b", "ks4:c"]);
    }

    #[test]
    fn invalid_cursor_is_rejected() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        e.insert(make_node("KsBadCursor", "ks5:a", 1)).expect("insert");

        let err = e
            .query_where(
                Some("KsBadCursor"), None, None, None, "item", Some("score"), false,
                Some("not-valid-base64!!"), 10, 0,
            )
            .unwrap_err();

        assert!(err.contains("invalid cursor"), "got: {err}");
    }

    #[test]
    fn unpushable_predicate_errors_not_wrong_answer() {
        use crate::core::predicate::Expr;

        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        e.insert(make_node("KsUnpushable", "ks6:a", 1)).expect("insert");

        // A bare `ref` node is not something eval can push down.
        let expr: Expr = serde_json::from_value(serde_json::json!({
            "kind": "ref", "name": "somethingElse"
        }))
        .unwrap();

        let res = e.query_where(
            Some("KsUnpushable"), None, None, Some(&expr), "item", None, false, None, 10, 0,
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
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        let mut private = make_node("KsSecret", "ks7:priv", 1);
        private.visibility = Visibility::Private;
        private.owner = "alice".to_string();

        e.insert(private).expect("insert");

        // query(): admin bypass (None) sees it; a non-owner does not.
        let admin = e
            .query(Some("KsSecret"), None, None, 50, 0)
            .expect("query");
        assert_eq!(admin.len(), 1, "admin (None) must see the private node");

        let bob = e
            .query(Some("KsSecret"), None, Some("bob"), 50, 0)
            .expect("query");
        assert!(bob.is_empty(), "non-admin bob must not see it");

        // query_where(): same bypass.
        let admin_page = e
            .query_where(Some("KsSecret"), None, None, None, "item", None, false, None, 50, 0)
            .expect("query_where ok");
        assert_eq!(admin_page.nodes.len(), 1, "admin (None) must list via query_where");
        assert_eq!(admin_page.nodes[0].address, "ks7:priv");

        let bob_page = e
            .query_where(Some("KsSecret"), None, Some("bob"), None, "item", None, false, None, 50, 0)
            .expect("query_where ok");
        assert!(bob_page.nodes.is_empty(), "non-admin bob must not see it via query_where");
    }
}
#[cfg(test)]
mod clear_kind_tests {
    use super::*;
    use crate::core::coordinate::Coordinate;
    use crate::core::node::Node;

    use crate::storage::engine::test_support::disk_guard;

    /// Close this engine and reopen the database the way a restart does:
    /// read the catalog and the index roots, then replay the WAL past
    /// the durability checkpoint.
    ///
    /// The old engine is consumed rather than left alive beside the new
    /// one, and that is not tidiness: two engines over one data
    /// directory are two writers on the same index files. What is being
    /// asserted is the state a *restart* sees, which is exactly the
    /// state the WAL and the last checkpoint can reconstruct.
    fn reopen(engine: StorageEngine) -> StorageEngine {
        drop(engine);

        let mut recovered =
            StorageEngine::load().expect("reopen storage engine");

        crate::storage::recovery::recover(&mut recovered)
            .expect("wal recovery");

        recovered
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
        let mut e = StorageEngine::open().expect("open storage engine");
        e.insert(node_owned("ck_admin:1", "CkAdminEntity", "alice")).unwrap();
        e.insert(node_owned("ck_admin:2", "CkAdminEntity", "bob")).unwrap();
        e.insert(node_owned("ck_admin:keep", "CkAdminOther", "alice")).unwrap();

        e.execute_transaction(vec![TxOperation::ClearKind {
            kind: "CkAdminEntity".to_string(),
            owner: "root".to_string(),
            is_admin: true,
        }])
        .expect("admin clear commits");

        assert!(e.get("ck_admin:1").expect("read").is_none(), "admin cleared alice's node");
        assert!(e.get("ck_admin:2").expect("read").is_none(), "admin cleared bob's node");
        assert!(e.get("ck_admin:keep").expect("read").is_some(), "other kind untouched");
    }

    /// A non-admin clears only nodes it owns of that kind; another
    /// owner's node of the same kind stays intact.
    #[test]
    fn non_admin_clears_only_its_own_nodes() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");
        e.insert(node_owned("ck_own:mine1", "CkOwnEntity", "alice")).unwrap();
        e.insert(node_owned("ck_own:mine2", "CkOwnEntity", "alice")).unwrap();
        e.insert(node_owned("ck_own:theirs", "CkOwnEntity", "bob")).unwrap();

        e.execute_transaction(vec![TxOperation::ClearKind {
            kind: "CkOwnEntity".to_string(),
            owner: "alice".to_string(),
            is_admin: false,
        }])
        .expect("non-admin clear commits");

        assert!(e.get("ck_own:mine1").expect("read").is_none(), "own node cleared");
        assert!(e.get("ck_own:mine2").expect("read").is_none(), "own node cleared");
        assert!(
            e.get("ck_own:theirs").expect("read").is_some(),
            "other owner's node of the same kind is left intact"
        );
    }

    /// A clear is atomic with the rest of the batch: a later op that
    /// fails validation rolls the whole transaction back, including the
    /// clear — nothing is applied.
    #[test]
    fn clear_rolls_back_when_a_later_op_fails() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");
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
        assert!(e.get("ck_atom:1").expect("read").is_some(), "clear rolled back with the batch");
        assert!(e.get("ck_atom:2").expect("read").is_some(), "clear rolled back with the batch");
    }

    /// Each cleared node is removed + WAL-logged exactly like a
    /// standalone delete, so a fresh recovery from durable storage no
    /// longer sees it — while a non-cleared node recovers normally.
    #[test]
    fn cleared_nodes_do_not_survive_recovery() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");
        e.insert(node_owned("ck_rec:gone", "CkRecEntity", "alice")).unwrap();
        e.insert(node_owned("ck_rec:stay", "CkRecOther", "alice")).unwrap();

        e.execute_transaction(vec![TxOperation::ClearKind {
            kind: "CkRecEntity".to_string(),
            owner: "alice".to_string(),
            is_admin: false,
        }])
        .expect("clear commits");

        // Reopen from durable storage and replay the WAL, which is what
        // a restart does. `e` is dropped first: two engines over one
        // data directory would be two writers on the same index files,
        // and the point here is the state a *restart* sees, not the
        // state a second live handle would.
        let recovered = reopen(e);
        assert!(
            recovered.get("ck_rec:gone").expect("read").is_none(),
            "a cleared node came back after recovery"
        );
        assert!(
            recovered.get("ck_rec:stay").expect("read").is_some(),
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

    use crate::storage::engine::test_support::disk_guard;

    /// Close this engine and reopen the database the way a restart does:
    /// read the catalog and the index roots, then replay the WAL past
    /// the durability checkpoint.
    ///
    /// The old engine is consumed rather than left alive beside the new
    /// one, and that is not tidiness: two engines over one data
    /// directory are two writers on the same index files. What is being
    /// asserted is the state a *restart* sees, which is exactly the
    /// state the WAL and the last checkpoint can reconstruct.
    fn reopen(engine: StorageEngine) -> StorageEngine {
        drop(engine);

        let mut recovered =
            StorageEngine::load().expect("reopen storage engine");

        crate::storage::recovery::recover(&mut recovered)
            .expect("wal recovery");

        recovered
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
            // "text" is what FCT emits for a string literal; "string"
            // is not one of its vtypes.
            "r": { "kind": "lit", "val": status, "vtype": "text" }
        }))
        .expect("valid predicate")
    }

    /// The predicate filters the kind down to only the matching rows;
    /// same-kind non-matching rows and other kinds are untouched.
    #[test]
    fn predicate_selects_only_matching_nodes() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");
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

        assert!(e.get("dw_sel:a").expect("read").is_none(), "matching node deleted");
        assert!(e.get("dw_sel:c").expect("read").is_none(), "matching node deleted");
        assert!(e.get("dw_sel:b").expect("read").is_some(), "non-matching node of the kind survives");
        assert!(e.get("dw_sel:keep").expect("read").is_some(), "other kind untouched");
    }

    /// A non-admin deletes only its own matching nodes; another owner's
    /// matching node of the same kind stays intact.
    #[test]
    fn non_admin_deletes_only_own_matching() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");
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

        assert!(e.get("dw_own:mine").expect("read").is_none(), "own matching node deleted");
        assert!(e.get("dw_own:mine_ok").expect("read").is_some(), "own non-matching node survives");
        assert!(
            e.get("dw_own:theirs").expect("read").is_some(),
            "another owner's matching node is left intact for a non-admin"
        );
    }

    /// An admin deletes every matching node of the kind regardless of
    /// owner (same admin bypass as clear_kind / the delete route).
    #[test]
    fn admin_deletes_all_matching() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");
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

        assert!(e.get("dw_adm:a").expect("read").is_none(), "admin deleted alice's matching node");
        assert!(e.get("dw_adm:b").expect("read").is_none(), "admin deleted bob's matching node");
        assert!(e.get("dw_adm:c").expect("read").is_some(), "non-matching node survives even for admin");
    }

    /// Omitted `where` (`None`) degenerates to clear_kind: every
    /// writable node of the kind is deleted regardless of `data`.
    #[test]
    fn omitted_where_behaves_like_clear_kind() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");
        e.insert(node_with("dw_all:a", "DwAllEntity", "alice", r#"{"status":"active"}"#)).unwrap();
        e.insert(node_with("dw_all:b", "DwAllEntity", "alice", r#"{"status":"expired"}"#)).unwrap();

        e.execute_transaction(vec![TxOperation::DeleteWhere {
            kind: "DwAllEntity".to_string(),
            where_: None,
            owner: "alice".to_string(),
            is_admin: false,
        }])
        .expect("omitted-where delete_where commits");

        assert!(e.get("dw_all:a").expect("read").is_none(), "omitted where deletes all writable of the kind");
        assert!(e.get("dw_all:b").expect("read").is_none(), "omitted where deletes all writable of the kind");
    }

    /// An unpushable predicate aborts the whole transaction and nothing
    /// is deleted — the query path's error-not-wrong-answer contract.
    #[test]
    fn unpushable_predicate_aborts_nothing_deleted() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");
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
        assert!(e.get("dw_bad:a").expect("read").is_some(), "nothing deleted on predicate error");
        assert!(e.get("dw_bad:b").expect("read").is_some(), "nothing deleted on predicate error");
    }

    /// A delete_where is atomic with the rest of the batch: a later op
    /// that fails validation rolls the whole transaction back, including
    /// the delete_where — nothing is applied.
    #[test]
    fn rolls_back_when_a_sibling_op_fails() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");
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
        assert!(e.get("dw_atom:1").expect("read").is_some(), "delete_where rolled back with the batch");
        assert!(e.get("dw_atom:2").expect("read").is_some(), "delete_where rolled back with the batch");
    }

    /// Each deleted node is removed + WAL-logged like a standalone
    /// delete, so a fresh recovery from durable storage no longer sees
    /// it — while a non-matching node of the kind recovers normally.
    #[test]
    fn deleted_nodes_do_not_survive_recovery() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");
        e.insert(node_with("dw_rec:gone", "DwRecEntity", "alice", r#"{"status":"expired"}"#)).unwrap();
        e.insert(node_with("dw_rec:stay", "DwRecEntity", "alice", r#"{"status":"active"}"#)).unwrap();

        e.execute_transaction(vec![TxOperation::DeleteWhere {
            kind: "DwRecEntity".to_string(),
            where_: Some(status_eq("expired")),
            owner: "alice".to_string(),
            is_admin: false,
        }])
        .expect("delete_where commits");

        // Reopen from durable storage and replay the WAL, which is what
        // a restart does. `e` is dropped first: two engines over one
        // data directory would be two writers on the same index files,
        // and the point here is the state a *restart* sees, not the
        // state a second live handle would.
        let recovered = reopen(e);
        assert!(
            recovered.get("dw_rec:gone").expect("read").is_none(),
            "a deleted node came back after recovery"
        );
        assert!(
            recovered.get("dw_rec:stay").expect("read").is_some(),
            "non-matching node recovered from durable storage"
        );
    }
}

#[cfg(test)]
mod stats_tests {
    use super::*;
    use crate::core::coordinate::Coordinate;
    use crate::core::node::Node;

    use crate::storage::engine::test_support::disk_guard;

    fn node_owned(address: &str, kind: &str, owner: &str) -> Node {
        Node::new(
            Coordinate::new(0, 0, 0, 0),
            address.to_string(),
            kind.to_string(),
            owner.to_string(),
        )
    }

    /// Insert N nodes across two kinds and do M gets, then assert the
    /// snapshot.
    ///
    /// What can be pinned exactly is this test's own kinds, because a
    /// kind is a key range of its own. What cannot is any database-wide
    /// total: the counts come off the durable indexes now, and the data
    /// directory is shared with every other test in this binary, so
    /// `node_count` legitimately includes their rows. Asserting `>=` on
    /// the totals and `==` on the kinds keeps the test about what
    /// `stats` claims rather than about what else happens to be stored.
    #[test]
    fn stats_counts_kinds_and_operations() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

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
            let _ = e.get(addr).expect("read");
        }

        let s = e.stats().expect("stats");

        assert!(s.node_count >= n, "node_count {} >= {n}", s.node_count);

        // Grouping is exact for this test's own kinds, and the whole
        // list is sorted ascending (it is built through a BTreeMap), so
        // "StatAlpha" precedes "StatBeta" wherever they land.
        let count_of = |kind: &str| {
            s.kinds
                .iter()
                .find(|entry| entry.kind == kind)
                .map(|entry| entry.count)
                .unwrap_or(0)
        };

        assert_eq!(count_of("StatAlpha"), 3);
        assert_eq!(count_of("StatBeta"), 2);

        let kinds: Vec<&str> = s.kinds.iter().map(|k| k.kind.as_str()).collect();
        let mut sorted = kinds.clone();
        sorted.sort();
        assert_eq!(kinds, sorted, "kinds must come back sorted");

        // Each insert is one write and each get one read; the engine's
        // counters are process-lifetime, so these are lower bounds.
        assert!(s.writes_total >= n, "writes_total {} >= {n}", s.writes_total);
        assert!(s.reads_total >= m, "reads_total {} >= {m}", s.reads_total);

        // The physical storage block is populated, not a placeholder.
        assert!(s.storage.page_size > 0, "page size reported");
        assert!(s.storage.segments >= 1, "at least one heap segment");
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

    use crate::storage::engine::test_support::disk_guard;

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
        let node = engine.get(address).expect("read").expect("node exists");
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
        let mut e = StorageEngine::open().expect("open storage engine");
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
        let mut e = StorageEngine::open().expect("open storage engine");
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
        let mut e = StorageEngine::open().expect("open storage engine");
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
        let mut e = StorageEngine::open().expect("open storage engine");
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
        let mut e = StorageEngine::open().expect("open storage engine");
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
            e.get("si_atom:sideeffect").expect("read").is_none(),
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
        let mut e = StorageEngine::open().expect("open storage engine");
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
        let mut e = StorageEngine::open().expect("open storage engine");
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

        let history = e.history_for("si_hist:slot").expect("read history");
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

    use crate::storage::engine::test_support::disk_guard;

    /// Close this engine and reopen the database the way a restart does:
    /// read the catalog and the index roots, then replay the WAL past
    /// the durability checkpoint.
    ///
    /// The old engine is consumed rather than left alive beside the new
    /// one, and that is not tidiness: two engines over one data
    /// directory are two writers on the same index files. What is being
    /// asserted is the state a *restart* sees, which is exactly the
    /// state the WAL and the last checkpoint can reconstruct.
    fn reopen(engine: StorageEngine) -> StorageEngine {
        drop(engine);

        let mut recovered =
            StorageEngine::load().expect("reopen storage engine");

        crate::storage::recovery::recover(&mut recovered)
            .expect("wal recovery");

        recovered
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
        let mut e = StorageEngine::open().expect("open storage engine");
        e.insert(node_with("dh_one:a", "DhEntity", "alice", r#"{"v":1}"#)).unwrap();

        e.delete("dh_one:a").unwrap();

        let history = e.history_for("dh_one:a").expect("read history");
        assert_eq!(history.len(), 1, "the deleted state was archived");
        assert!(history[0].node.data.contains("\"v\":1"));
        assert!(e.get("dh_one:a").expect("read").is_none(), "the node is still gone");
    }

    /// The full lifecycle: create → overwrite → delete leaves both the
    /// replaced value and the deleted value in history, in order.
    #[test]
    fn overwrite_then_delete_records_both_states() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");
        e.insert(node_with("dh_two:a", "DhEntity", "alice", r#"{"v":1}"#)).unwrap();
        e.insert(node_with("dh_two:a", "DhEntity", "alice", r#"{"v":2}"#)).unwrap();
        e.delete("dh_two:a").unwrap();

        let history = e.history_for("dh_two:a").expect("read history");
        assert_eq!(history.len(), 2, "one entry per state that stopped being current");
        assert!(history[0].node.data.contains("\"v\":1"), "oldest first");
        assert!(history[1].node.data.contains("\"v\":2"), "then the deleted state");
    }

    /// A bulk delete archives every node it removes — the audit trail
    /// for a mass deletion is exactly when you need one most.
    #[test]
    fn bulk_delete_archives_every_removed_node() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");
        e.insert(node_with("dh_bulk:a", "DhBulkEntity", "alice", r#"{"v":"a"}"#)).unwrap();
        e.insert(node_with("dh_bulk:b", "DhBulkEntity", "alice", r#"{"v":"b"}"#)).unwrap();

        e.execute_transaction(vec![TxOperation::ClearKind {
            kind: "DhBulkEntity".to_string(),
            owner: "alice".to_string(),
            is_admin: false,
        }])
        .expect("clear commits");

        for (address, value) in [("dh_bulk:a", "a"), ("dh_bulk:b", "b")] {
            assert!(e.get(address).expect("read").is_none(), "{address} was cleared");
            let history = e.history_for(address).expect("read history");
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
        let mut e = StorageEngine::open().expect("open storage engine");
        e.insert(node_with("dh_rec:a", "DhRecEntity", "alice", r#"{"v":"final"}"#)).unwrap();

        e.execute_transaction(vec![TxOperation::DeleteNode("dh_rec:a".to_string())])
            .expect("delete commits");

        let recovered = reopen(e);
        assert!(recovered.get("dh_rec:a").expect("read").is_none(), "still deleted");
        let history = recovered.history_for("dh_rec:a").expect("read history");
        assert_eq!(history.len(), 1, "the archive is durable, not just in memory");
        assert!(history[0].node.data.contains("final"));
    }
}
