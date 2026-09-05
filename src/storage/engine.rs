use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock};
use std::sync::Arc;

use serde::{Serialize, Deserialize};

use crate::core::aggregate::{Accumulator, AggFunc, AggSpec};
use crate::core::edge::{Edge, EdgeId};
use crate::core::history::HistoryEntry;
use crate::core::node::Node;
use crate::core::predicate::{self, Expr};
use crate::core::user::UserRecord;
use crate::metrics::{self, CellAttribution, CellTable, RuntimeStats};
use crate::storage::binary::{self, UserOpRecord};
use crate::storage::cache::RecordCache;
use crate::storage::catalog::Catalog;
use crate::storage::heap::{HeapRecord, RecordStore};
use crate::storage::index::{
    self as keys, Declared, IndexDef, IndexInfo, IndexOpRecord, Indexes,
};
use crate::storage::text::{self as text, TextIndex, TextIndexDef, TextIndexOpRecord};
use crate::storage::reference::{
    ReferenceDef, ReferenceOpRecord, ReferentialAction, References,
};
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
/// The reserved kind sequences live under.
///
/// A sequence is an ordinary node, which is deliberate: it gets the WAL
/// record, the crash recovery, the ownership check and the history that
/// every other write gets, rather than a second durability story written
/// specially for a counter.
const SEQUENCE_KIND: &str = "__sequence";

/// Longest sequence name.
const MAX_SEQUENCE_NAME: usize = 128;

/// Largest block one allocation may take.
///
/// Big enough that a bulk import takes its ids in one call; small enough
/// that a typo cannot burn a meaningful part of the range.
const MAX_SEQUENCE_BLOCK: u64 = 100_000;

/// Addresses one `multi_get` may name.
///
/// Each one costs an index descent and a record read, so this is the
/// same kind of bound as the scan-row cap — it limits the work a single
/// request can buy. A feed page is twenty rows and the enrichment for it
/// a few hundred addresses, so a thousand is comfortably above what a
/// page needs and well below what a scan costs.
const MAX_MULTI_GET: usize = 1_000;

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

// ---------------------------------------------------------------------
// Inverted-index probe budget
// ---------------------------------------------------------------------
//
// Three bounds on how hard the planner tries to narrow a substring
// search before it gives up and reads the candidates it has. All three
// are safe to hit in *either* direction, which is the property that
// makes them tunable numbers rather than correctness decisions: dropping
// a trigram only ever widens the candidate set, and a wider candidate
// set is still a superset of the answer because the predicate is
// re-evaluated on every candidate regardless.

/// How many of a needle's trigrams are tried as the *seed* — the one
/// whose whole posting list is read.
///
/// The seed decides the cost of everything after it, so a rare trigram
/// is worth looking for. But looking is not free: finding the smallest
/// of N lists costs N times the size of the smallest, because each
/// attempt has to be read far enough to know it is not smaller. Four is
/// where that stops paying — past it the hunt costs more than a slightly
/// better seed saves.
const MAX_TEXT_SEED_GRAMS: usize = 4;

/// A posting list at most this long ends the seed hunt immediately.
///
/// The hunt exists to avoid seeding from a trigram that half the corpus
/// holds. Once a list this short is in hand that danger is gone, and
/// reading three more lists to find one marginally shorter is pure loss
/// — the refinement probes below narrow it the rest of the way for one
/// B+tree descent per candidate, which is cheaper than another list
/// scan. Measured on a 20 000-row corpus, taking the first list under
/// this bound instead of hunting for the smallest cut the query from
/// 385 ms to 92 ms without changing a single row of the answer.
const MAX_TEXT_SEED_ENOUGH: usize = 1024;

/// How many trigrams are then probed against the seed's candidates.
///
/// Each probe is one B+tree descent per surviving candidate and can only
/// shrink the set, so this is purely a cost ceiling.
const MAX_TEXT_PROBE_GRAMS: usize = 16;

/// The candidate count below which probing stops.
///
/// Under this many candidates it is cheaper to read the records and let
/// the predicate decide than to spend another trigram's worth of
/// descents removing a few of them — and the predicate has to run on the
/// survivors either way.
const MAX_TEXT_PROBE_FLOOR: usize = 64;

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

    /// The declared references: which `data` field points at which
    /// kind, and what deleting the referenced node does to the nodes
    /// referencing it.
    ///
    /// Beside the indexes rather than inside them because it is a
    /// different kind of thing — an index is an access path, a reference
    /// is a rule *about* mutations that happens to need one. Resident
    /// for the same reason the index definitions are: every delete has
    /// to ask whether anything points at what it is removing, and a rule
    /// that costs a read to discover is a rule every write pays for.
    references: References,

    /// Bounded LRU over recently read/written nodes. A pure accelerator
    /// — every entry can be re-derived from the heap through the primary
    /// index, and a cold cache changes latency, never answers.
    cache: RecordCache,

    /// Persistent users, keyed by token_hash.
    /// Persistent identities, keyed by token hash.
    ///
    /// Behind a lock rather than owned exclusively: authentication reads
    /// this on every request, and a read must not have to wait for a
    /// write to some unrelated node. Bounded by identity count, not by
    /// data volume, which is why it is resident at all.
    users: RwLock<HashMap<String, UserRecord>>,

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

    /// Per-coordinate attribution of the two counters above.
    ///
    /// `reads_total` and `writes_total` describe the whole instance, and
    /// an instance is not the unit anyone places data in — a *cell* is.
    /// A control plane that can only see the instance total can only
    /// ever move everything or nothing, however precisely it reasons.
    /// This is the same traffic, broken down by the coordinate the
    /// records actually live at.
    ///
    /// Fixed-size and lock-free by construction: see [`CellTable`] for
    /// the bound, what happens past it, and why it is counted rather
    /// than concealed. It is engine state rather than process state
    /// because a coordinate is a property of *this* database's records.
    cells: CellTable,

    /// Highest WAL sequence whose effects have been applied to the heap
    /// and indexes — in the buffer pool, not necessarily on disk. The
    /// checkpoint may advance to this value, and only to this value,
    /// once a flush has made those effects durable.
    /// Atomic only so that every method can take `&self`; it is written
    /// exclusively under [`StorageEngine::write_lock`], so there is no
    /// interleaving to reason about.
    applied_sequence: AtomicU64,

    /// Mutations applied since the last checkpoint. Same story.
    pending_mutations: AtomicU64,

    /// How many mutations to let accumulate before checkpointing.
    checkpoint_interval: u64,

    /// Records currently in `facetql.users`, tracked so the log can be
    /// compacted when it has grown far past the identities it describes.
    /// See [`StorageEngine::compact_user_log`].
    user_log_records: AtomicUsize,

    /// Bumped by every checkpoint. A reader pins the current value for
    /// the length of its work; a heap segment retired at epoch `E` is
    /// deleted only once no reader is pinned below `E`.
    epoch: AtomicU64,

    /// Epochs currently pinned by live readers, and how many hold each.
    /// The same shape, and the same job, as the B+tree's reader registry
    /// — that one defers page reuse, this one defers file deletion.
    readers: Mutex<BTreeMap<u64, usize>>,

    /// Segments drained by compaction, with the epoch at which the
    /// committed indexes stopped pointing into them.
    retired: Mutex<Vec<(u32, u64)>>,

    /// Serializes writers against each other.
    ///
    /// The engine used to be wrapped in one `RwLock` at the `Database`
    /// level, which did two jobs at once: it made writes exclusive of
    /// each other (which they must be — this is a single-writer engine)
    /// and it made writes exclusive of *reads* (which they need not be,
    /// now that the B+tree serves snapshots and the record cache is
    /// keyed by version).
    ///
    /// Splitting them means this mutex keeps the property that matters
    /// and drops the one that only cost throughput. It is taken by the
    /// public mutation entry points and by nothing else; the `_locked`
    /// variants exist for the two places a mutation calls another one
    /// (`claim` → `insert`, `note_applied` → `checkpoint`), which would
    /// otherwise deadlock on a non-reentrant lock.
    writer: Mutex<()>,
}

/// A live read's claim on the engine's current epoch.
///
/// While one exists, no heap segment retired at or after the epoch it
/// pinned is deleted. Created by [`StorageEngine::pin_read`]; the pin
/// lasts exactly as long as the value.
pub struct ReadPin<'a> {
    engine: &'a StorageEngine,
    epoch: u64,
}

impl ReadPin<'_> {
    /// The epoch this read is pinned to.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
}

impl Drop for ReadPin<'_> {
    fn drop(&mut self) {
        self.engine.release_read(self.epoch);
    }
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
    /// The identity map, for reading.
    fn users_read(&self) -> std::sync::RwLockReadGuard<'_, HashMap<String, UserRecord>> {
        self.users.read().unwrap_or_else(|e| e.into_inner())
    }

    /// The identity map, for writing. Callers already hold the writer
    /// lock; this guards the map against concurrent authentication
    /// reads, which do not.
    fn users_write(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<String, UserRecord>> {
        self.users.write().unwrap_or_else(|e| e.into_inner())
    }

    /// Serialize this writer against every other one.
    ///
    /// Taken by the public mutation entry points. A mutation that calls
    /// another mutation must call its `_locked` form instead — this lock
    /// is not reentrant, and `std::sync::Mutex` deadlocks rather than
    /// reporting the mistake.
    /// Pin the current epoch for the length of a read.
    ///
    /// # What this protects
    ///
    /// Reads no longer exclude writes, so a `checkpoint` — including its
    /// compaction — can run while a query is in flight. Compaction
    /// drains a mostly-dead segment by copying its live records
    /// elsewhere and repointing the indexes, then deletes the file. That
    /// is safe against the *committed* index generation, which no longer
    /// names it. It is not safe against a reader that resolved a
    /// `RecordLocation` a moment earlier and is about to read it.
    ///
    /// So deletion is deferred, exactly the way the B+tree defers page
    /// reuse: the segment is retired with the epoch that unreferenced
    /// it, and the file goes away once no reader is pinned below that.
    /// Holding a pin for a long time costs disk, never correctness.
    pub fn pin_read(&self) -> ReadPin<'_> {
        let epoch = self.epoch.load(Ordering::Relaxed);

        *self.readers_lock().entry(epoch).or_insert(0) += 1;

        ReadPin {
            engine: self,
            epoch,
        }
    }

    fn readers_lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<u64, usize>> {
        self.readers.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Release a pin. Called from [`ReadPin::drop`].
    fn release_read(&self, epoch: u64) {
        let mut readers = self.readers_lock();

        if let std::collections::btree_map::Entry::Occupied(mut slot) = readers.entry(epoch) {
            *slot.get_mut() -= 1;

            if *slot.get() == 0 {
                slot.remove();
            }
        }
    }

    /// The oldest epoch any live reader is pinned to.
    fn oldest_reader(&self) -> Option<u64> {
        self.readers_lock().keys().next().copied()
    }

    /// Delete every retired segment no reader can still reach.
    ///
    /// Called at each checkpoint. A segment held back by a long-running
    /// read is simply reconsidered at the next one — the list is the
    /// only thing that grows, and it grows by one `(u32, u64)` per
    /// deferred segment.
    fn drop_unreferenced_segments(&self) -> io::Result<()> {
        let oldest = self.oldest_reader();

        let ready: Vec<u32> = {
            let mut retired = self.retired.lock().unwrap_or_else(|e| e.into_inner());
            let mut ready = Vec::new();

            retired.retain(|(segment, epoch)| {
                if oldest.is_none_or(|pinned| pinned >= *epoch) {
                    ready.push(*segment);
                    false
                } else {
                    true
                }
            });

            ready
        };

        for segment in ready {
            self.store.drop_segment(segment)?;
        }

        Ok(())
    }

    fn write_lock(&self) -> std::sync::MutexGuard<'_, ()> {
        // A poisoned writer lock means a thread panicked *partway
        // through a mutation*: some indexes updated, others not. Ending
        // the process is the repair, because every acknowledged write is
        // already in the WAL and startup recovery replays it. Continuing
        // would serve from state that is inconsistent with the disk.
        //
        // This used to be the `Database`-level `RwLock`'s job. It moved
        // here with the exclusion it enforces.
        // Every mutation path in this file acquires the writer through
        // here, which makes it the one place that can see a writer
        // *queue*. Two relaxed adds around a lock acquisition that is
        // itself an atomic RMW: the measurement costs less than the
        // thing it measures.
        metrics::enter_write_queue();

        let guard =
            self.writer.lock().unwrap_or_else(|_| crate::database::poisoned());

        metrics::leave_write_queue();

        guard
    }

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

        // Written, not flushed. Every mutation — standalone or framed —
        // is made durable by the `wal::sync_pending` its entry point
        // runs after releasing the writer lock, which is where
        // concurrent writers meet and share one fsync.
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
        &self,
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
                let declared = self.declared_indexes();
                operation.validate(&declared)?;
                self.check_unique(&operations)?;
                self.check_references(&operations)?;

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
                let declared = self.declared_indexes();
                self.check_unique(&operations)?;
                self.check_references(&operations)?;

                let sequence = Transaction::from_operations(operations)
                    .commit(&declared, |operation| self.apply_committed(operation))
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
    fn note_applied(&self, sequence: u64) {
        self.applied_sequence.fetch_max(sequence, Ordering::Relaxed);
        let pending = self.pending_mutations.fetch_add(1, Ordering::Relaxed) + 1;

        if pending < self.checkpoint_interval {
            return;
        }

        if let Err(e) = self.checkpoint_locked() {
            eprintln!(
                "warning: failed to checkpoint physical storage at WAL \
                 sequence {}: {e}. The mutation is durable in the WAL and \
                 will be replayed on the next start.",
                self.applied_sequence.load(Ordering::Relaxed)
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
    pub fn checkpoint(&self) -> io::Result<()> {
        let _writer = self.write_lock();

        self.checkpoint_locked()
    }

    /// The checkpoint itself, for callers already holding the writer
    /// lock. `note_applied` reaches this from inside a mutation, which
    /// is why the lock is not taken here.
    fn checkpoint_locked(&self) -> io::Result<()> {
        let drained = self.compact()?;

        self.store.sync()?;
        self.indexes.commit()?;

        crate::storage::checkpoint::advance(self.applied_sequence.load(Ordering::Relaxed))?;

        self.pending_mutations.store(0, Ordering::Relaxed);

        self.rotate_wal()?;

        // Only now: the index entries that used to point into these
        // segments are committed elsewhere, so the bytes are genuinely
        // unreferenced *by the committed generation*. A crash before
        // this leaves a segment full of dead records that the next pass
        // drains for free.
        //
        // A reader pinned at an earlier epoch may still hold a location
        // into one of them, so the epoch advances and the segments are
        // retired against it rather than deleted outright. See
        // `pin_read`.
        let epoch = self.epoch.fetch_add(1, Ordering::Relaxed) + 1;

        if !drained.is_empty() {
            let mut retired = self.retired.lock().unwrap_or_else(|e| e.into_inner());

            retired.extend(drained.into_iter().map(|segment| (segment, epoch)));
        }

        self.drop_unreferenced_segments()?;

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
    fn rotate_wal(&self) -> io::Result<()> {
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
    fn compact(&self) -> io::Result<Vec<u32>> {
        let candidates = self.store.compaction_candidates(COMPACTION_RATIO);

        let mut drained = Vec::new();

        for segment in candidates.into_iter().take(SEGMENTS_PER_COMPACTION) {
            self.drain_segment(segment)?;
            drained.push(segment);
        }

        Ok(drained)
    }

    fn drain_segment(&self, segment: u32) -> io::Result<()> {
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
        &self,
        entry: HistoryEntry,
    ) -> Result<(), String> {
        self.apply_committed(&Operation::Archive(entry))
    }

    pub(crate) fn replay_insert(
        &self,
        node: Node,
    ) -> Result<(), String> {
        self.apply_committed(&Operation::Insert(node))
    }

    pub(crate) fn replay_delete(
        &self,
        address: &str,
    ) -> Result<(), String> {
        self.apply_committed(&Operation::Delete(address.to_string()))
    }

    pub(crate) fn replay_insert_edge(
        &self,
        edge: Edge,
    ) -> Result<(), String> {
        self.apply_committed(&Operation::InsertEdge(edge))
    }

    pub(crate) fn replay_delete_edge(
        &self,
        id: &EdgeId,
    ) -> Result<(), String> {
        self.apply_committed(&Operation::DeleteEdge(id.clone()))
    }

    pub(crate) fn replay_insert_user(
        &self,
        record: UserRecord,
    ) -> Result<(), String> {
        self.apply_committed(&Operation::InsertUser(record))
    }

    pub(crate) fn replay_revoke_user(
        &self,
        token_hash: &str,
    ) -> Result<(), String> {
        self.apply_committed(&Operation::RevokeUser(token_hash.to_string()))
    }

    pub(crate) fn replay_create_index(
        &self,
        def: IndexDef,
    ) -> Result<(), String> {
        self.apply_committed(&Operation::CreateIndex(def))
    }

    pub(crate) fn replay_drop_index(
        &self,
        name: &str,
    ) -> Result<(), String> {
        self.apply_committed(&Operation::DropIndex(name.to_string()))
    }

    pub(crate) fn replay_create_reference(
        &self,
        def: ReferenceDef,
    ) -> Result<(), String> {
        self.apply_committed(&Operation::CreateReference(def))
    }

    pub(crate) fn replay_drop_reference(
        &self,
        name: &str,
    ) -> Result<(), String> {
        self.apply_committed(&Operation::DropReference(name.to_string()))
    }

    pub(crate) fn replay_create_text_index(
        &self,
        def: TextIndexDef,
    ) -> Result<(), String> {
        self.apply_committed(&Operation::CreateTextIndex(def))
    }

    pub(crate) fn replay_drop_text_index(
        &self,
        name: &str,
    ) -> Result<(), String> {
        self.apply_committed(&Operation::DropTextIndex(name.to_string()))
    }

    /// Note how far recovery replayed, so the checkpoint it takes
    /// afterwards moves the durability boundary across the work it just
    /// redid instead of leaving it to be redone again next time.
    pub(crate) fn note_recovered(&self, sequence: u64) {
        self.applied_sequence.fetch_max(sequence, Ordering::Relaxed);
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

        // Starts the clock behind `uptime_seconds` and the first
        // observation window. Here, because "up" means "has a database
        // open", not "has served a request".
        metrics::init();

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
            references: References::new(),
            cache: RecordCache::new(),
            users: RwLock::new(HashMap::new()),
            reads_total: AtomicU64::new(0),
            writes_total: AtomicU64::new(0),
            cells: CellTable::new(),
            applied_sequence: AtomicU64::new(0),
            pending_mutations: AtomicU64::new(0),
            checkpoint_interval,
            user_log_records: AtomicUsize::new(0),
            epoch: AtomicU64::new(1),
            readers: Mutex::new(BTreeMap::new()),
            retired: Mutex::new(Vec::new()),
            writer: Mutex::new(()),
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
        let engine = Self::open()?;

        let log = binary::read_all_records::<UserOpRecord>(&binary::users_path())?;

        engine.user_log_records.store(log.len(), Ordering::Relaxed);

        for (_offset, record) in log {
            match record {
                UserOpRecord::Put(user) => {
                    engine.users_write().insert(user.token_hash.clone(), user);
                }

                UserOpRecord::Revoke(token_hash) => {
                    engine.users_write().remove(&token_hash);
                }
            }
        }

        // Declared `data`-field indexes, replayed the same last-write-
        // wins way and for the same reason: the set of indexes is
        // bounded by how many an operator declared, is consulted on
        // every write, and has to be known before the first request is
        // served — a write applied without maintaining an index it
        // does not know about is a silently wrong index.
        let declared =
            binary::read_all_records::<IndexOpRecord>(&keys::definitions_path())?;

        let mut definitions: HashMap<String, IndexDef> = HashMap::new();

        for (_offset, record) in declared {
            match record {
                IndexOpRecord::Put(def) => {
                    definitions.insert(def.name.clone(), def);
                }

                IndexOpRecord::Drop(name) => {
                    definitions.remove(&name);
                }
            }
        }

        // Name order so opening is deterministic and a failure names the
        // same index every time.
        let mut definitions: Vec<IndexDef> = definitions.into_values().collect();
        definitions.sort_by(|a, b| a.name.cmp(&b.name));

        for def in definitions {
            engine.indexes.open_data(def)?;
        }

        // Declared inverted indexes, replayed exactly the same way and
        // for the same reason: an index the engine does not know about
        // at the first write is an index that is silently missing rows
        // from then on.
        let declared = binary::read_all_records::<TextIndexOpRecord>(
            &text::definitions_path(),
        )?;

        let mut definitions: HashMap<String, TextIndexDef> = HashMap::new();

        for (_offset, record) in declared {
            match record {
                TextIndexOpRecord::Put(def) => {
                    definitions.insert(def.name.clone(), def);
                }

                TextIndexOpRecord::Drop(name) => {
                    definitions.remove(&name);
                }
            }
        }

        let mut definitions: Vec<TextIndexDef> = definitions.into_values().collect();
        definitions.sort_by(|a, b| a.name.cmp(&b.name));

        for def in definitions {
            engine.indexes.open_text(def)?;
        }

        // Declared references, replayed the same way and for a sharper
        // version of the same reason: a delete applied without knowing
        // about a reference does not merely leave an index stale, it
        // leaves rows referencing a node that is gone — and nothing
        // later will ever find them to clean up.
        let declared = binary::read_all_records::<ReferenceOpRecord>(
            &crate::storage::reference::definitions_path(),
        )?;

        for (_offset, record) in declared {
            match record {
                ReferenceOpRecord::Put(def) => engine.references.put(def),
                ReferenceOpRecord::Drop(name) => engine.references.remove(&name),
            }
        }

        Ok(engine)
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
        let Some(location) = self.node_location(address)? else {
            return Ok(None);
        };

        Ok(Some(self.node_at(location)?))
    }

    fn node_location(&self, address: &str) -> io::Result<Option<RecordLocation>> {
        match self.indexes.primary.get(address.as_bytes())? {
            Some(raw) => Ok(Some(RecordLocation::decode(&raw)?)),
            None => Ok(None),
        }
    }

    /// The node at one physical location, through the record cache.
    ///
    /// Every path that resolves an address to a location comes through
    /// here, so the cache now serves the query paths too — before, only
    /// the by-address read consulted it.
    fn node_at(&self, location: RecordLocation) -> io::Result<Node> {
        // Per-cell read attribution happens here and only here, for the
        // same reason `writes_total` is incremented in exactly one
        // place: this is the funnel every path that turns an address
        // into a record passes through, so no query plan can under-report
        // a cell by forgetting to count. A cache hit is counted too — it
        // is still a read of that cell's data, and a cell whose working
        // set fits in cache is not a cell nobody is using.
        if let Some(node) = self.cache.get(location) {
            self.cells.record_read(node.coordinate, node.data.len() as u64);

            return Ok((*node).clone());
        }

        match self.store.read(location)? {
            HeapRecord::Node(node) => {
                self.cells.record_read(node.coordinate, node.data.len() as u64);
                self.cache.put(location, Arc::new(node.clone()));
                Ok(node)
            }
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
    pub fn insert(&self, node: Node) -> Result<(), String> {
        // The durable flush deliberately happens *after* this block,
        // outside the writer lock. Holding the lock across the fsync
        // means the next writer cannot even begin, so there is never a
        // second writer to share the flush with — see `wal::sync_pending`.
        let outcome = {
            let _writer = self.write_lock();

            self.insert_locked(node)
        };

        wal::sync_pending().map_err(|e| e.to_string())?;

        outcome
    }

    /// The insert itself, for callers already holding the writer lock —
    /// `claim` reserves a node by inserting it.
    fn insert_locked(&self, node: Node) -> Result<(), String> {
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

    /// Read many nodes by address in one call.
    ///
    /// # Why this is a primitive and not a loop
    ///
    /// Rendering a page of a feed needs the viewer's own state for every
    /// row on it — did I like this, did I repost it, do I follow the
    /// author. Done one address at a time that is a round trip per row,
    /// and the round trip dominates: twenty per-row reads measured 13.7 ms
    /// against 0.93 ms for the same answers fetched together. The
    /// Postgres implementation of this product writes the same thing as
    /// `WHERE id = ANY($1::uuid[])` and issues exactly two queries per
    /// page.
    ///
    /// Addresses that do not exist are skipped rather than reported: the
    /// caller asked which of these exist, and a missing row is an answer,
    /// not a failure. Addresses the caller may not read are skipped for
    /// the same reason `query_where` filters them — a caller must not be
    /// able to probe for the existence of a private node.
    ///
    /// Results come back in the order asked for, so a caller can zip them
    /// against its own list without matching on address.
    pub fn multi_get(
        &self,
        addresses: &[String],
        requester: Option<&str>,
    ) -> Result<Vec<Node>, String> {
        if addresses.len() > MAX_MULTI_GET {
            return Err(format!(
                "multi-get asked for {} addresses; the maximum is \
                 {MAX_MULTI_GET}. Each one is an index descent and a record \
                 read, so the batch is bounded like any other scan.",
                addresses.len(),
            ));
        }

        self.reads_total.fetch_add(1, Ordering::Relaxed);

        let mut found = Vec::with_capacity(addresses.len());

        for address in addresses {
            let Some(node) = self.read_node(address).map_err(io_message)? else {
                continue;
            };

            let visible = match requester {
                Some(owner) => node.can_read(owner),
                None => true,
            };

            if visible {
                found.push(node);
            }
        }

        Ok(found)
    }

    /// Allocate the next `count` values of a named sequence.
    ///
    /// Returns the first value allocated; the caller owns
    /// `[first, first + count)` and no other caller will ever be given
    /// them.
    ///
    /// # Why the engine has to own this
    ///
    /// An application that allocates ids by asking "what is the largest
    /// one so far" has to read every row to find out, and then two
    /// callers that read at the same moment allocate the same id. The
    /// fct runtime does exactly that: its `nextID` is a high-water mark
    /// derived from loading every row of every table at boot, which is a
    /// large part of why it holds the whole database in memory.
    ///
    /// A sequence is the standard answer and it is small: one durable
    /// record per allocation, taken under the writer lock, so the read
    /// and the increment cannot interleave.
    ///
    /// # Blocks
    ///
    /// `count` above one hands out a range in a single round trip. A
    /// client inserting a thousand rows takes a thousand ids once rather
    /// than paying a durable write per id — the same reason Postgres
    /// sequences have a cache.
    ///
    /// Gaps are normal and are not an error: a caller that takes a block
    /// and uses three of it has burned the rest. A sequence guarantees
    /// uniqueness and monotonicity, never density.
    pub fn sequence_next(
        &self,
        name: &str,
        count: u64,
        owner: &str,
        is_admin: bool,
    ) -> Result<u64, String> {
        if count == 0 || count > MAX_SEQUENCE_BLOCK {
            return Err(format!(
                "sequence block must be between 1 and {MAX_SEQUENCE_BLOCK}, got {count}"
            ));
        }

        if name.is_empty() || name.len() > MAX_SEQUENCE_NAME || name.contains(':') {
            return Err(format!(
                "sequence name must be 1..={MAX_SEQUENCE_NAME} characters and \
                 contain no ':', got {name:?}"
            ));
        }

        let address = format!("{SEQUENCE_KIND}:{name}");

        let outcome = (|| {
            let _writer = self.write_lock();

            let previous = self.read_node(&address).map_err(io_message)?;

            let start = match &previous {
                Some(node) => {
                    if !(is_admin || node.can_write(owner)) {
                        return Err(format!(
                            "not authorized to advance sequence {name:?}: it \
                             belongs to another owner"
                        ));
                    }

                    serde_json::from_str::<serde_json::Value>(&node.data)
                        .ok()
                        .and_then(|v| v.get("next").and_then(serde_json::Value::as_u64))
                        .unwrap_or(1)
                }
                None => 1,
            };

            let next = start.checked_add(count).ok_or_else(|| {
                format!("sequence {name:?} is exhausted at {start}")
            })?;

            let mut node = Node::new(
                crate::core::coordinate::Coordinate::new(0, 0, 0, 0),
                address.clone(),
                SEQUENCE_KIND.to_string(),
                previous
                    .as_ref()
                    .map(|n| n.owner.clone())
                    .unwrap_or_else(|| owner.to_string()),
            );

            node.data = format!(r#"{{"next":{next}}}"#);

            self.insert_locked(node)?;

            Ok(start)
        })();

        wal::sync_pending().map_err(|e| e.to_string())?;

        outcome
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
        &self,
        address: &str,
        worker: &str,
    ) -> Result<(), ClaimError> {
        // The durable flush deliberately happens *after* this block,
        // outside the writer lock. Holding the lock across the fsync
        // means the next writer cannot even begin, so there is never a
        // second writer to share the flush with — see `wal::sync_pending`.
        let outcome = (|| {
            let _writer = self.write_lock();

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

            self.insert_locked(node).map_err(ClaimError::StorageError)?;

            Ok(())
        })();

        wal::sync_pending().map_err(|e| ClaimError::StorageError(e.to_string()))?;

        outcome
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
    pub fn delete(&self, address: &str) -> Result<(), TransactionError> {
        // The durable flush deliberately happens *after* this block,
        // outside the writer lock. Holding the lock across the fsync
        // means the next writer cannot even begin, so there is never a
        // second writer to share the flush with — see `wal::sync_pending`.
        let outcome = (|| {
            let _writer = self.write_lock();

            let existing = self
                .read_node(address)
                .map_err(|e| TransactionError::Storage(e.to_string()))?;

            let Some(existing) = existing else {
                // Nothing there: nothing to archive, and nothing that
                // could be referencing it either, since a reference
                // resolves to a node. Stays a single no-op `Delete`.
                return self
                    .apply_atomic(vec![Operation::Delete(address.to_string())])
                    .map_err(TransactionError::Storage);
            };

            // Not "archive it and remove it" but "lower this delete",
            // because what a delete means is now a property of the
            // declared references rather than of this call site. With
            // none declared this resolves to exactly the two operations
            // it always did.
            let mut operations = Vec::with_capacity(2);
            let mut staged: HashMap<String, Option<Node>> = HashMap::new();

            self.lower_delete_closure(
                vec![existing],
                &mut operations,
                &mut staged,
            )?;

            // Every failure the closure could raise has been raised;
            // what is left is the durable apply, whose only remaining
            // failure mode is the storage itself.
            self.apply_atomic(operations).map_err(TransactionError::Storage)
        })();

        wal::sync_pending()
            .map_err(|e| TransactionError::Storage(e.to_string()))?;

        outcome
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
    pub fn insert_edge(&self, edge: Edge) -> Result<(), String> {
        // The durable flush deliberately happens *after* this block,
        // outside the writer lock. Holding the lock across the fsync
        // means the next writer cannot even begin, so there is never a
        // second writer to share the flush with — see `wal::sync_pending`.
        let outcome = (|| {
            let _writer = self.write_lock();

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
        })();

        wal::sync_pending().map_err(|e| e.to_string())?;

        outcome
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
    pub fn delete_edge(&self, id: &EdgeId) -> Result<(), String> {
        // The durable flush deliberately happens *after* this block,
        // outside the writer lock. Holding the lock across the fsync
        // means the next writer cannot even begin, so there is never a
        // second writer to share the flush with — see `wal::sync_pending`.
        let outcome = (|| {
            let _writer = self.write_lock();

            if self.find_edge(id).map_err(io_message)?.is_none() {
                return Err(format!(
                    "edge not found: {} -[{}]-> {}",
                    id.from, id.kind, id.to
                ));
            }

            self.apply_atomic(vec![Operation::DeleteEdge(id.clone())])
        })();

        wal::sync_pending().map_err(|e| e.to_string())?;

        outcome
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
    /// `requester` carries the same visibility semantics as
    /// [`Self::query`]: `None` means an internal caller that is not
    /// filtering, `Some(owner)` means only what that identity may read,
    /// and an admin is passed as `None` because an admin bypasses
    /// visibility the way a superuser does.
    ///
    /// The filter is not optional decoration. This used to have no
    /// `requester` at all, which was safe only because its single caller
    /// could ask about one owner: the caller's own. The moment the
    /// endpoint above it can name somebody else's nodes, an unfiltered
    /// listing hands out every private node that owner has.
    pub fn nodes_by_owner(
        &self,
        owner: &str,
        requester: Option<&str>,
    ) -> io::Result<Vec<Node>> {
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
                    match requester {
                        Some(r) if !node.can_read(r) => {}
                        _ => nodes.push(node),
                    }
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

        // Counted here because this is the single point every access
        // path funnels its candidates through: a node reaches `matches`
        // exactly when a plan has read it in order to decide about it.
        // Counting inside each plan instead would count whatever each
        // plan happened to think was worth counting.
        let examined = std::cell::Cell::new(0u64);

        let matches = |node: &Node| -> Result<bool, String> {
            examined.set(examined.get() + 1);

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

        // The plan runs inside a closure so every branch's page
        // passes one exit, where the candidate count is attached to
        // it. Five `return`s and five places to remember would be
        // five chances for one plan to under-report what it read.
        let mut page = (|| -> Result<QueryPage, String> {
            // Does the predicate pin an indexed field to one value? Then
            // the answer lives under a single prefix of that index, and the
            // rest of the kind never has to be read at all.
            //
            // Entries under one prefix are ordered by address — the value
            // part of the key is identical for all of them — which is why
            // this serves both orderings without a re-sort: it *is* address
            // order, and it is also `(value, address)` order for the field
            // it pins, because that value does not vary within the prefix.
            if let Some((index, literal)) =
                self.equality_prefix_plan(kind, order_field, predicate, item_var)
            {
                return self.query_by_data_prefix(
                    &index, &literal, cursor, desc, limit, offset, matches,
                );
            }

            // Does the predicate pin an indexed *string* field to a prefix?
            // Then the answer lives under a byte prefix of that index.
            //
            // Unlike the equality case this is only sound when the ordering
            // is that same field: entries under a string prefix vary in
            // value, so their order is `(value, address)` and not address
            // order. Asking for address order and being handed value order
            // would be a wrong answer that looks like a right one.
            if let Some((index, literal)) =
                self.string_prefix_plan(kind, order_field, predicate, item_var)
            {
                let prefix = keys::encode_string_prefix(&literal);

                return self.query_by_key_prefix(
                    &index, prefix, cursor, desc, limit, offset, matches,
                );
            }

            // Does the predicate require a substring of a field an
            // inverted index covers? Then the rows that can possibly
            // match are the intersection of that substring's trigram
            // postings — a superset of the answer, never a subset, so
            // `matches` still decides every row (see
            // `text_candidate_plan`).
            //
            // Only for the address ordering here. The candidate set
            // arrives sorted by address, which *is* the contract when no
            // `order` was asked for; any other ordering needs these rows
            // sorted by a field the postings say nothing about, so it
            // goes to `query_sorted` below, which takes the same
            // candidates as its enumeration source.
            if order_field.is_none()
                && let Some(candidates) = self
                    .text_candidate_plan(kind, predicate, item_var)
                    .map_err(io_message)?
            {
                return self.query_by_text(
                    candidates, cursor, desc, limit, offset, matches,
                );
            }

            if order_field.is_none() {
                return self.query_by_address(
                    kind, owner, cursor, desc, limit, offset, matches,
                );
            }

            // An index over exactly this `(kind, field)` turns the sort into
            // a range scan: the entries are already in `(value, address)`
            // order, so the page is read by walking from the cursor and
            // stopping at `limit`. Nothing outside the page is read, and the
            // `max_scan_rows` refusal below never comes up — which is the
            // whole reason to declare one.
            if let (Some(k), Some(field)) = (kind, order_field)
                && self.indexes.data_find(k, field).is_some()
            {
                return self.query_by_data_index(
                    k, field, cursor, desc, limit, offset, matches,
                );
            }

            // The sorted path enumerates through the narrowest source
            // it has. An equality prefix is narrower than a trigram
            // intersection — it selects on a whole value rather than on
            // windows of one — so it wins when both apply, and the
            // postings are only resolved when it does not.
            let text_candidates =
                match self.equality_selection(kind, predicate, item_var) {
                    Some(_) => None,
                    None => self
                        .text_candidate_plan(kind, predicate, item_var)
                        .map_err(io_message)?,
                };

            self.query_sorted(
                kind,
                owner,
                order_field,
                cursor,
                desc,
                limit,
                offset,
                predicate,
                item_var,
                text_candidates,
                matches,
            )
        })()?;

        page.examined = examined.get();

        Ok(page)
    }

    /// How many nodes match — without materializing any of them.
    ///
    /// The same `kind`/`owner`/visibility filtering and the same pushable
    /// predicate as [`Self::query_where`], answered as a number.
    ///
    /// This is [`Self::aggregate_where`] with the fold that ignores every
    /// field, kept as its own call because a count is the one aggregate
    /// whose answer is an integer rather than a JSON value, and because
    /// it is by far the most asked. It shares the traversal, which is the
    /// part that must not fork: a count and a sum over the same filter
    /// have to agree about which rows exist.
    ///
    /// # Why this exists rather than "query and count the rows"
    ///
    /// A caller that wants a count and only has a paged read has two bad
    /// options: ask for one enormous page, which is the allocation
    /// `max_scan_rows` exists to refuse, or walk the cursor to the end,
    /// which is one network round trip per page and turns "how many
    /// replies does this post have" into an N+1 across the wire. Both are
    /// worse than the scan they are avoiding. `SELECT count(*)` is a
    /// primitive in every database for the same reason.
    ///
    /// # Why it is not bounded by `max_scan_rows`
    ///
    /// That bound exists because a *result set* is held in memory, and
    /// the size of the answer is chosen by the data rather than by the
    /// request. A count holds one integer no matter how many rows it
    /// visits, so the reason does not apply. What it does cost is time
    /// proportional to the candidates, and time is bounded elsewhere and
    /// deliberately — the per-request deadline and the per-identity rate
    /// limit on the `bulk` endpoint class (see `api::limits`). Refusing a
    /// count because a kind is large would make `count(Post)` fail on
    /// exactly the databases where the number is most worth having.
    pub fn count_where(
        &self,
        kind: Option<&str>,
        owner: Option<&str>,
        requester: Option<&str>,
        predicate: Option<&Expr>,
        item_var: &str,
    ) -> Result<u64, String> {
        let mut acc = Accumulator::new(AggFunc::Count);

        self.fold_where(
            kind,
            owner,
            requester,
            predicate,
            item_var,
            &AggSpec::count(),
            &mut acc,
        )?;

        Ok(acc.rows())
    }

    /// One aggregate over the rows a filter selects — `sum`, `avg`,
    /// `min`, `max`, or the `count` [`Self::count_where`] returns as an
    /// integer.
    ///
    /// # Why the engine computes this rather than the caller
    ///
    /// For the same reason it computes the count. "The total of this
    /// order's lines" answered by a query is a page of rows crossing the
    /// wire — or several pages — to produce one number, and that is worse
    /// on every axis than the scan it replaces: more bytes, more round
    /// trips, more memory, and a total that is wrong the moment the rows
    /// do not fit in one page. The rows never have to leave the engine.
    ///
    /// # What it costs
    ///
    /// The same three access paths [`Self::count_where`] documents, with
    /// one exception: the cheapest of them counts index keys without
    /// reading a single record, and an aggregate over a *field* has to
    /// read the field. So `count` can be answered from the index alone
    /// and `sum` cannot — see [`AggFunc::needs_field`], which is the one
    /// place that rule is written down.
    ///
    /// Not bounded by `max_scan_rows`, for the reason a count is not: the
    /// answer is one value however many rows it visits.
    pub fn aggregate_where(
        &self,
        kind: Option<&str>,
        owner: Option<&str>,
        requester: Option<&str>,
        predicate: Option<&Expr>,
        item_var: &str,
        spec: &AggSpec,
    ) -> Result<serde_json::Value, String> {
        let mut acc = Accumulator::new(spec.func);

        self.fold_where(kind, owner, requester, predicate, item_var, spec, &mut acc)?;

        Ok(acc.finish())
    }

    /// Walk the rows a filter selects and fold each one into `acc`.
    ///
    /// The shared body of [`Self::count_where`] and
    /// [`Self::aggregate_where`]: the access paths live here once, so
    /// which rows an aggregate sees is decided in one place for every
    /// aggregate. What differs between them is entirely inside `acc`.
    ///
    /// # The three access paths, cheapest first
    ///
    /// 1. **Index keys only.** With no predicate, no visibility filtering
    ///    and nothing to read out of each row, the answer is how many
    ///    entries the `kind` (or `owner`) index holds under its prefix.
    ///    No record is read and no JSON is decoded. Available to `count`
    ///    alone — every other aggregate needs the record.
    /// 2. **An equality prefix**, when the predicate pins a field a
    ///    declared index covers: only the entries holding that value are
    ///    walked, so an aggregate over fifty matches in a million-row
    ///    kind costs the fifty.
    /// 3. **The inverted index**, when the predicate requires a
    ///    substring one covers: the postings hand back a superset, which
    ///    is read and re-tested — the same access path `query_where`
    ///    takes, which is what keeps an aggregate and its query agreeing.
    /// 4. **The candidate scan**, the general case: the narrowest index
    ///    the filters allow, decoding each candidate to test it.
    #[allow(clippy::too_many_arguments)]
    fn fold_where(
        &self,
        kind: Option<&str>,
        owner: Option<&str>,
        requester: Option<&str>,
        predicate: Option<&Expr>,
        item_var: &str,
        spec: &AggSpec,
        acc: &mut Accumulator,
    ) -> Result<(), String> {
        self.reads_total.fetch_add(1, Ordering::Relaxed);

        let field = spec.field.as_deref().unwrap_or("");

        // One closure decodes, tests and folds, so a row's JSON is parsed
        // at most once even when both the predicate and the aggregate
        // need it.
        let mut visit = |node: &Node| -> Result<(), String> {
            if let Some(k) = kind
                && node.kind != k
            {
                return Ok(());
            }

            if let Some(o) = owner
                && node.owner != o
            {
                return Ok(());
            }

            if let Some(r) = requester
                && !node.can_read(r)
            {
                return Ok(());
            }

            if predicate.is_none() && !spec.needs_field() {
                return acc.fold(field, None);
            }

            let data: serde_json::Value =
                serde_json::from_str(&node.data).unwrap_or(serde_json::Value::Null);

            if let Some(expr) = predicate {
                let verdict = predicate::eval(expr, item_var, &data)
                    .map_err(|e| format!("predicate evaluation failed: {e}"))?;

                if !matches!(verdict, serde_json::Value::Bool(true)) {
                    return Ok(());
                }
            }

            let value = if spec.needs_field() {
                data.get(field)
            } else {
                None
            };

            acc.fold(field, value)
        };

        // Path 1: nothing to test per row and nothing to read out of it,
        // so no record is touched. The membership index already knows the
        // answer.
        if predicate.is_none()
            && requester.is_none()
            && !spec.needs_field()
            && let Some((index, prefix)) = match (kind, owner) {
                (Some(k), None) => Some((&self.indexes.kind, keys::kind_prefix(k))),
                (None, Some(o)) => Some((&self.indexes.owner, keys::owner_prefix(o))),
                _ => None,
            }
        {
            let mut total = 0u64;

            index
                .for_each_range(&prefix, None, false, |_key, _value| {
                    total += 1;
                    Ok(true)
                })
                .map_err(io_message)?;

            acc.fold_rows(total);

            return Ok(());
        }

        let mut failure: Option<String> = None;

        // Path 2: the predicate pins an indexed field. `None` for the
        // ordering, because an aggregate has no order to preserve — any
        // access path that visits each matching row exactly once will do.
        if let Some((index, literal)) =
            self.equality_prefix_plan(kind, None, predicate, item_var)
        {
            let prefix = keys::encode_order_value(Some(&literal));

            index
                .tree
                .for_each_range(&prefix, None, false, |key, _value| {
                    let Some(raw) = keys::address_from_data_key(key) else {
                        return Ok(true);
                    };

                    let address = String::from_utf8_lossy(raw).into_owned();

                    let Some(node) = self.read_node(&address)? else {
                        return Ok(true);
                    };

                    if let Err(e) = visit(&node) {
                        failure = Some(e);
                        return Ok(false);
                    }

                    Ok(true)
                })
                .map_err(io_message)?;

            return match failure {
                Some(e) => Err(e),
                None => Ok(()),
            };
        }

        // Path 3: the predicate requires a substring an inverted index
        // covers. The postings hand back a superset of the matching
        // rows, so this reads those and lets `visit` decide — the same
        // access path and the same guarantee `query_where` gets, which
        // is what keeps an aggregate and its query agreeing.
        if let Some(candidates) = self
            .text_candidate_plan(kind, predicate, item_var)
            .map_err(io_message)?
        {
            for address in &candidates {
                let Some(node) = self.read_node(address).map_err(io_message)? else {
                    continue;
                };

                visit(&node)?;
            }

            return Ok(());
        }

        // Path 4: the general scan.
        self.scan_candidates(kind, owner, None, false, |node| {
            if let Err(e) = visit(&node) {
                failure = Some(e);
                return Ok(false);
            }

            Ok(true)
        })
        .map_err(io_message)?;

        match failure {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Every distinct value of one `data` field, with how many nodes
    /// carry it.
    ///
    /// # Why this exists
    ///
    /// [`Self::count_where`] answers one question per call, which is
    /// wrong for the shape that asks it most. A feed rendering twenty
    /// posts, each showing its like/reply/repost totals, is sixty counts
    /// — sixty round trips, sixty rate-limit tokens, and sixty scans —
    /// to fill in numbers that all come from the same three predicates
    /// with one field varying. That is an N+1, and moving it from SQL to
    /// HTTP does not stop it being one.
    ///
    /// Grouping is the fix: one call per *predicate shape* per page
    /// instead of one per row. The caller asks "how many Likes per
    /// tweet" once and indexes the answer itself.
    ///
    /// # One entry per value
    ///
    /// The reply holds **at most one entry per distinct value**, on
    /// every path. A caller indexes this answer by value — fct's reads
    /// it straight into a map — so a second entry for a value is not a
    /// redundancy it can merge, it is a count it silently drops.
    #[allow(clippy::too_many_arguments)]
    pub fn count_by(
        &self,
        kind: Option<&str>,
        owner: Option<&str>,
        requester: Option<&str>,
        predicate: Option<&Expr>,
        item_var: &str,
        group_by: &str,
        values: Option<&[serde_json::Value]>,
    ) -> Result<Vec<GroupCount>, String> {
        let groups = self.fold_by(
            kind,
            owner,
            requester,
            predicate,
            item_var,
            group_by,
            values,
            &AggSpec::count(),
        )?;

        Ok(groups
            .into_iter()
            .map(|(value, acc)| GroupCount { value, count: acc.rows() })
            .collect())
    }

    /// One aggregate per distinct value of one `data` field — the grouped
    /// form of [`Self::aggregate_where`].
    ///
    /// Same reason to exist as [`Self::count_by`], one step further: a
    /// page showing each seller's revenue is one `sum` grouped by seller,
    /// not one `sum` per seller. The `values` argument is what makes it
    /// cheap for a rendered page — see [`Self::count_by`] on why asking
    /// about the twenty values a page renders is a different question
    /// from grouping the whole kind.
    ///
    /// The index-only paths `count_by` can take are not available here,
    /// for the reason stated on [`AggFunc::needs_field`]: the value being
    /// aggregated is in the record, so the record is read.
    #[allow(clippy::too_many_arguments)]
    pub fn aggregate_by(
        &self,
        kind: Option<&str>,
        owner: Option<&str>,
        requester: Option<&str>,
        predicate: Option<&Expr>,
        item_var: &str,
        group_by: &str,
        values: Option<&[serde_json::Value]>,
        spec: &AggSpec,
    ) -> Result<Vec<GroupAggregate>, String> {
        let groups = self.fold_by(
            kind, owner, requester, predicate, item_var, group_by, values, spec,
        )?;

        Ok(groups
            .into_iter()
            .map(|(value, acc)| GroupAggregate { value, result: acc.finish() })
            .collect())
    }

    /// Walk the rows a filter selects, split them by `group_by`, and fold
    /// each group.
    ///
    /// The shared body of [`Self::count_by`] and [`Self::aggregate_by`].
    ///
    /// # Why this one IS bounded by `max_scan_rows`
    ///
    /// Unlike a plain count, the answer here is a result set whose size
    /// the data chooses — one entry per distinct value — so the reason
    /// that bound exists applies in full. A group-by over a field that
    /// is nearly unique is a request for a copy of the table with the
    /// rows replaced by ones. Refusing is recoverable; a multi-gigabyte
    /// map is not.
    ///
    /// # The two access paths
    ///
    /// With a declared index over the grouped field, nothing that needs
    /// a record to decide — no predicate, no visibility filtering — and
    /// an aggregate that reads no field, the index keys already carry
    /// the value: entries sharing a value are adjacent, so the counting
    /// is a walk of the index and the only records read are **one per
    /// distinct group**, to recover the value in its own JSON type.
    /// Otherwise every candidate is decoded, which is the same cost as
    /// the scan the caller was going to do anyway, paid once instead of
    /// once per row.
    ///
    /// The index paths accumulate their runs through the same grouping
    /// the scan uses instead of emitting one entry per run: a run whose
    /// representative record was deleted underneath the walk cannot name
    /// its value, and two such runs would otherwise both be reported as
    /// `null`. They are counted under `null` alongside the rows that
    /// genuinely carry no value there — the same bucket the scan path
    /// puts those in, and the only choice that keeps the groups summing
    /// to the count.
    #[allow(clippy::too_many_arguments)]
    fn fold_by(
        &self,
        kind: Option<&str>,
        owner: Option<&str>,
        requester: Option<&str>,
        predicate: Option<&Expr>,
        item_var: &str,
        group_by: &str,
        values: Option<&[serde_json::Value]>,
        spec: &AggSpec,
    ) -> Result<Vec<(Option<serde_json::Value>, Accumulator)>, String> {
        self.reads_total.fetch_add(1, Ordering::Relaxed);

        let cap = max_scan_rows();
        let func = spec.func;
        let field = spec.field.as_deref().unwrap_or("");

        // Groups are accumulated under the field value's order-preserving
        // encoding rather than under the value itself, for the same
        // reason the index is keyed that way: a JSON value is not
        // hashable and its text form is not canonical, while this
        // encoding is exactly one byte string per distinct value and is
        // already what "the same value" means everywhere else here.
        let mut groups: HashMap<
            Vec<u8>,
            (Option<serde_json::Value>, Accumulator),
        > = HashMap::new();

        // Whichever path filled `groups`, the answer leaves through
        // here. Two paths that each shaped their own reply is how they
        // came to disagree about what a well-formed one is, so the
        // completion and the ordering are written once.
        let finish = |mut groups: HashMap<
            Vec<u8>,
            (Option<serde_json::Value>, Accumulator),
        >|
         -> Vec<(Option<serde_json::Value>, Accumulator)> {
            // A requested value with no matching row still gets an
            // entry: the caller asked about it, so "no rows" is an
            // answer, and an absent key would be indistinguishable from
            // one the engine forgot. What that entry holds is the
            // aggregate's own empty answer — `0` for a count or a sum,
            // `null` for an average that has nothing to average.
            if let Some(wanted) = values {
                for value in wanted {
                    groups
                        .entry(keys::encode_order_value(Some(value)))
                        .or_insert_with(|| {
                            (Some(value.clone()), Accumulator::new(func))
                        });
                }
            }

            // Sorted so the reply is deterministic — a caller diffing
            // two samples, or a test asserting one, should not be
            // reading hash iteration order.
            let mut out: Vec<(Option<serde_json::Value>, Accumulator)> =
                groups.into_values().collect();

            out.sort_by(|a, b| {
                keys::compare_order_values(a.0.as_ref(), b.0.as_ref())
            });

            out
        };

        // The caller named the values it cares about, an index covers the
        // field, and nothing needs a record to decide — so each answer is
        // the length of one prefix range and NO record is read at all.
        //
        // This is the shape that motivated the whole endpoint, and
        // measuring it is what showed the unrestricted form is the wrong
        // tool for it: a feed rendering twenty posts wants twenty counts,
        // and grouping the whole kind to get them computed twenty
        // thousand. One call, twenty prefix walks, nothing decoded.
        if let Some(wanted) = values {
            if wanted.len() > MAX_GROUP_VALUES {
                return Err(format!(
                    "grouping was given {} values; the maximum is \
                     {MAX_GROUP_VALUES}. Ask for the values a page actually \
                     renders, or omit `values` to group the whole kind.",
                    wanted.len()
                ));
            }

            if predicate.is_none()
                && requester.is_none()
                && owner.is_none()
                && !spec.needs_field()
                && let Some(k) = kind
                && let Some(index) = self.indexes.data_find(k, group_by)
            {
                let mut answered: std::collections::HashSet<Vec<u8>> =
                    std::collections::HashSet::with_capacity(wanted.len());

                for value in wanted {
                    let prefix = keys::encode_order_value(Some(value));

                    // The same value named twice is one question. Walking
                    // its range again would answer it with twice the rows
                    // — and `1` and `1.0` are the same question here, for
                    // the same reason they are one key in the index.
                    if !answered.insert(prefix.clone()) {
                        continue;
                    }

                    let mut count = 0u64;

                    index
                        .tree
                        .for_each_range(&prefix, None, false, |_key, _value| {
                            count += 1;
                            Ok(true)
                        })
                        .map_err(io_message)?;

                    // Every requested value comes back, zero included: the
                    // caller asked about it, so "no rows" is an answer and
                    // an absent key would be indistinguishable from one
                    // the engine forgot.
                    group_entry(&mut groups, cap, func, Some(value))?
                        .fold_rows(count);
                }

                return Ok(finish(groups));
            }
        }

        // The index path: adjacency in the index *is* the grouping, so
        // the walk never reads a record to decide which group an entry
        // belongs to — only once per group, to recover the value.
        if predicate.is_none()
            && requester.is_none()
            && owner.is_none()
            && !spec.needs_field()
            && let Some(k) = kind
            && let Some(index) = self.indexes.data_find(k, group_by)
        {
            let mut runs: Vec<(Vec<u8>, String, u64)> = Vec::new();

            index
                .tree
                .for_each_range(&[], None, false, |key, _value| {
                    let Some(raw) = keys::address_from_data_key(key) else {
                        return Ok(true);
                    };

                    // Everything before the address is the encoded value.
                    let encoded = key[..key.len() - raw.len()].to_vec();

                    match runs.last_mut() {
                        Some((last, _, count)) if *last == encoded => *count += 1,
                        _ => {
                            if runs.len() >= cap {
                                return Err(scan_limit_exceeded(
                                    "grouping by a field with this many distinct values",
                                ));
                            }

                            runs.push((
                                encoded,
                                String::from_utf8_lossy(raw).into_owned(),
                                1,
                            ));
                        }
                    }

                    Ok(true)
                })
                .map_err(io_message)?;

            for (_encoded, representative, count) in runs {
                // One read per group, purely to recover the value with
                // its JSON type intact. A representative deleted since
                // the walk leaves its run unable to name itself: those
                // rows were read, so they are counted under `null`
                // rather than dropped, which is where a row with no
                // value for the field lands anyway.
                let value = match self.read_node(&representative).map_err(io_message)? {
                    Some(node) => serde_json::from_str::<serde_json::Value>(&node.data)
                        .ok()
                        .and_then(|d| d.get(group_by).cloned()),
                    None => None,
                };

                // Through the accumulator the scan path uses, not into a
                // vector of its own: runs are distinct by encoded key,
                // but the values they recover need not be, and a second
                // entry for a value is a count the caller loses.
                group_entry(&mut groups, cap, func, value.as_ref())?
                    .fold_rows(count);
            }

            return Ok(finish(groups));
        }

        let matches = |node: &Node,
                       data: &serde_json::Value|
         -> Result<bool, String> {
            if let Some(k) = kind
                && node.kind != k
            {
                return Ok(false);
            }

            if let Some(o) = owner
                && node.owner != o
            {
                return Ok(false);
            }

            if let Some(r) = requester
                && !node.can_read(r)
            {
                return Ok(false);
            }

            let Some(expr) = predicate else {
                return Ok(true);
            };

            predicate::eval(expr, item_var, data)
                .map(|v| matches!(v, serde_json::Value::Bool(true)))
                .map_err(|e| format!("predicate evaluation failed: {e}"))
        };

        let mut failure: Option<String> = None;

        self.scan_candidates(kind, owner, None, false, |node| {
            let data: serde_json::Value =
                serde_json::from_str(&node.data).unwrap_or(serde_json::Value::Null);

            match matches(&node, &data) {
                Ok(true) => {}
                Ok(false) => return Ok(true),
                Err(e) => {
                    failure = Some(e);
                    return Ok(false);
                }
            }

            let value = data.get(group_by);

            // With `values` given, everything outside the asked-for set is
            // not a group the caller wanted — counting it would make the
            // reply an answer to a different question.
            if let Some(wanted) = values
                && !wanted.iter().any(|w| {
                    keys::compare_order_values(Some(w), value)
                        == std::cmp::Ordering::Equal
                })
            {
                return Ok(true);
            }

            let folded = group_entry(&mut groups, cap, func, value).and_then(|acc| {
                acc.fold(
                    field,
                    if spec.needs_field() {
                        data.get(field)
                    } else {
                        None
                    },
                )
            });

            if let Err(e) = folded {
                failure = Some(e);
                return Ok(false);
            }

            Ok(true)
        })
        .map_err(io_message)?;

        if let Some(e) = failure {
            return Err(e);
        }

        Ok(finish(groups))
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

        Ok(QueryPage { nodes, next, examined: 0 })
    }

    /// The rows a substring predicate can possibly match, from an
    /// inverted index — or `None`, meaning "no index serves this, use
    /// the scan".
    ///
    /// # The correctness claim, stated once
    ///
    /// The returned addresses are a **superset** of the rows the scan
    /// would return, never a subset. That is not a hope about the
    /// tokenizer, it follows from one property of trigrams: if
    /// `v.contains(s)` then every trigram of `s` is a trigram of `v`
    /// ([`crate::storage::text`] proves it directly). So the
    /// intersection of the needle's posting lists contains every true
    /// match, plus rows where the trigrams occur scattered rather than
    /// adjacent. Those are removed by the caller's `matches` closure,
    /// which evaluates the *whole* original predicate on every candidate
    /// exactly as every other access path does.
    ///
    /// Everything this function does to bound its own cost — trying only
    /// a few trigrams as the seed, probing only some of the rest, giving
    /// up on a probe budget — drops trigrams from the intersection, and
    /// dropping a trigram can only make the candidate set *larger*. So
    /// no tuning decision in here can turn a superset into a subset, and
    /// none of them can change the query's answer.
    ///
    /// # When it declines
    ///
    /// * no `kind` (the index is per-kind), or no predicate;
    /// * no inverted index over a field the predicate constrains;
    /// * every literal shorter than one trigram — `contains(x, "hi")`
    ///   has no window to look up, so there is nothing to intersect;
    /// * every candidate trigram's posting list is longer than
    ///   [`max_scan_rows`] — the substring is so common that the index
    ///   is not narrowing anything, and the scan is the honest path.
    ///
    /// Declining is always safe: it costs a scan, which is what the
    /// query cost before this index existed.
    fn text_candidate_plan(
        &self,
        kind: Option<&str>,
        predicate: Option<&Expr>,
        item_var: &str,
    ) -> io::Result<Option<Vec<String>>> {
        let (Some(kind), Some(predicate)) = (kind, predicate) else {
            return Ok(None);
        };

        // Name order, so a database with two applicable inverted indexes
        // plans the same way on every request rather than following hash
        // iteration order.
        for index in self.indexes.text_all() {
            if index.def.kind != kind {
                continue;
            }

            let literals =
                predicate::substring_literals(predicate, item_var, &index.def.field);

            if literals.is_empty() {
                continue;
            }

            // Every literal is a requirement, so every literal's trigrams
            // are required: the union of their windows is the set the
            // postings have to agree on.
            let mut grams: Vec<[u8; text::GRAM_LEN]> = literals
                .iter()
                .flat_map(|literal| text::grams(literal))
                .collect();

            grams.sort_unstable();
            grams.dedup();

            if grams.is_empty() {
                continue;
            }

            if let Some(candidates) = self.text_candidates(&index, &grams)? {
                return Ok(Some(candidates));
            }
        }

        Ok(None)
    }

    /// Intersect posting lists: read the rarest trigram's list, then
    /// probe the others against it.
    ///
    /// Seeding from the smallest list and probing the rest — rather than
    /// reading every list and intersecting — is what keeps the cost
    /// proportional to the *answer* instead of to the most common
    /// trigram in the needle. `"the quick brown fox"` contains `"the"`,
    /// whose list is most of the corpus, and `"qui"`, whose list is
    /// tiny; reading the tiny one and asking the tree eleven questions
    /// about each survivor is the difference between the two.
    ///
    /// Returns `None` when no trigram's list was small enough to seed
    /// from, which is the planner's signal to use the scan.
    fn text_candidates(
        &self,
        index: &TextIndex,
        grams: &[[u8; text::GRAM_LEN]],
    ) -> io::Result<Option<Vec<String>>> {
        let cap = max_scan_rows();

        // The seed: a short posting list among the first few trigrams.
        //
        // Three bounds, and each one is a cost bound rather than a
        // correctness one. Each attempt stops as soon as it is longer
        // than the best already found, so a rare trigram found early
        // makes the rest of the hunt nearly free. The hunt as a whole
        // stops at the first list short enough that a better seed could
        // not repay the reading. And `budget` caps what the hunt may
        // read in total, so a needle made entirely of common trigrams
        // gives up and lets the scan answer instead of paying several
        // scans' worth of index reads to discover that it cannot help.
        let mut best: Option<BTreeSet<String>> = None;
        let mut best_len = cap;
        let mut budget = cap;

        for gram in grams.iter().take(MAX_TEXT_SEED_GRAMS) {
            let limit = best_len.min(budget);

            if limit == 0 {
                break;
            }

            let mut list: BTreeSet<String> = BTreeSet::new();
            let mut read = 0usize;
            let mut over = false;

            index.tree.for_each_range(&gram[..], None, false, |key, _value| {
                if read >= limit {
                    over = true;
                    return Ok(false);
                }

                read += 1;

                if let Some(raw) = text::address_from_key(key) {
                    list.insert(String::from_utf8_lossy(raw).into_owned());
                }

                Ok(true)
            })?;

            budget -= read;

            if over {
                continue;
            }

            best_len = list.len();
            best = Some(list);

            // Nothing holds this trigram, so nothing holds the needle:
            // no later trigram can improve on an empty intersection. And
            // a list already short enough is not worth improving on.
            if best_len <= MAX_TEXT_SEED_ENOUGH {
                break;
            }
        }

        let Some(mut candidates) = best else {
            return Ok(None);
        };

        // The refinement: every remaining trigram must also be present,
        // asked one candidate at a time. `budget` is what stops an
        // unselective query from paying for probes that are not removing
        // anything — abandoning them leaves a wider candidate set, which
        // is still a superset.
        let mut budget = cap;

        for gram in grams.iter().take(MAX_TEXT_PROBE_GRAMS) {
            if candidates.len() <= MAX_TEXT_PROBE_FLOOR
                || budget < candidates.len()
            {
                break;
            }

            budget -= candidates.len();

            let mut kept: BTreeSet<String> = BTreeSet::new();

            for address in &candidates {
                if index.tree.get(&text::key(gram, address))?.is_some() {
                    kept.insert(address.clone());
                }
            }

            candidates = kept;
        }

        // A `BTreeSet<String>` orders by the address bytes, which is the
        // order the `kind` index walks in — so this list *is* address
        // order, and the address-ordered plan can page through it
        // without sorting anything.
        Ok(Some(candidates.into_iter().collect()))
    }

    /// The page over an inverted index's candidates, in address order.
    ///
    /// The counterpart of [`Self::query_by_address`] for a substring
    /// search: same contract, same cursor shape, same `matches` filter —
    /// the only difference is that the candidates came from posting
    /// lists rather than from a walk of the whole kind. A cursor issued
    /// here is exactly one [`Self::query_by_address`] would issue, so
    /// dropping the index mid-page leaves an outstanding cursor valid.
    #[allow(clippy::too_many_arguments)]
    fn query_by_text<F>(
        &self,
        candidates: Vec<String>,
        cursor: Option<Cursor>,
        desc: bool,
        limit: usize,
        offset: usize,
        matches: F,
    ) -> Result<QueryPage, String>
    where
        F: Fn(&Node) -> Result<bool, String>,
    {
        let start_after = cursor.as_ref().map(|c| c.a.as_str());

        // As in `query_by_address`: a cursor supersedes `offset`.
        let to_skip = if start_after.is_some() { 0 } else { offset };

        // The candidates are ascending, so resuming past the cursor is a
        // binary search rather than a walk — which is what keeps a deep
        // page from re-reading the shallow ones.
        let slice: &[String] = match start_after {
            Some(after) if desc => {
                &candidates[..candidates.partition_point(|a| a.as_str() < after)]
            }
            Some(after) => {
                &candidates[candidates.partition_point(|a| a.as_str() <= after)..]
            }
            None => &candidates[..],
        };

        let mut nodes: Vec<Node> = Vec::new();
        let mut skipped = 0usize;
        let mut more = false;

        let mut visit = |address: &String| -> Result<bool, String> {
            // A candidate whose record is gone is not an error: the
            // posting outlives the row only inside this one plan, and
            // every other access path skips a missing record the same
            // way.
            let Some(node) = self.read_node(address).map_err(io_message)? else {
                return Ok(true);
            };

            if !matches(&node)? {
                return Ok(true);
            }

            if skipped < to_skip {
                skipped += 1;
                return Ok(true);
            }

            if nodes.len() == limit {
                // One row past the page: proof that a next cursor is
                // worth emitting, and the last row this plan reads.
                more = true;
                return Ok(false);
            }

            nodes.push(node);

            Ok(true)
        };

        if desc {
            for address in slice.iter().rev() {
                if !visit(address)? {
                    break;
                }
            }
        } else {
            for address in slice.iter() {
                if !visit(address)? {
                    break;
                }
            }
        }

        let next = match nodes.last() {
            Some(last) if more => Cursor::from_node(last, None).encode(),
            _ => String::new(),
        };

        Ok(QueryPage { nodes, next, examined: 0 })
    }

    /// Pick a declared index whose field the predicate pins to a single
    /// value, if the requested ordering allows using it.
    ///
    /// Two orderings qualify. With no `order`, the contract is address
    /// order, which is what a prefix scan produces. With `order` on the
    /// pinned field itself, every row in the prefix shares that value,
    /// so the tiebreak — address — is the whole ordering, and a prefix
    /// scan produces that too. Any other `order` needs the rows sorted
    /// by a field this prefix does not vary, so it goes elsewhere.
    ///
    /// Candidates are considered in name order so that a database with
    /// two applicable indexes plans the same way on every request rather
    /// than following hash iteration order.
    fn equality_prefix_plan(
        &self,
        kind: Option<&str>,
        order_field: Option<&str>,
        predicate: Option<&Expr>,
        item_var: &str,
    ) -> Option<(std::sync::Arc<crate::storage::index::DataIndex>, serde_json::Value)> {
        let (index, literal) = self.equality_selection(kind, predicate, item_var)?;

        // The prefix has to serve the *ordering* as well as the
        // selection, which it does only when the ordering is absent (the
        // contract is then address order, and entries under one prefix
        // are in address order) or is the pinned field itself (every row
        // in the prefix shares that value, so the tiebreak is the whole
        // ordering). Any other ordering needs these rows sorted by a
        // field this prefix does not vary — see `query_sorted`, which
        // uses the selection without the ordering claim.
        match order_field {
            None => Some((index, literal)),
            Some(order) if order == index.def.field => Some((index, literal)),
            Some(_) => None,
        }
    }

    /// The narrowest index that can *enumerate* the rows a predicate
    /// admits, with no claim about what order they come out in.
    ///
    /// Separating this from [`Self::equality_prefix_plan`] is what lets a
    /// query select through an index and still be ordered by something
    /// else. Conflating them cost exactly that: with `idx_Tweet_author`
    /// declared, `where author == 'u7'` took 2.4 ms unordered and
    /// **1.83 s** ordered by `created`, because the ordering disqualified
    /// the index for selection too and the query fell back to reading all
    /// fifty thousand rows of the kind. Selecting a hundred rows through
    /// the index and sorting those is what any planner does, and is the
    /// difference between the two numbers.
    /// The declared index and prefix serving a `starts_with`, if one
    /// applies.
    ///
    /// Requires the ordering to be the pinned field itself. A prefix of a
    /// string index is a range whose entries are ordered by value, so it
    /// satisfies `order by <that field>` exactly and satisfies nothing
    /// else — including the absent ordering, whose contract is address
    /// order. This is the same distinction that made the equality plan
    /// wrong once: an index that serves the *selection* does not
    /// automatically serve the *ordering*, and conflating the two is how
    /// a query comes back fast and in the wrong order.
    fn string_prefix_plan(
        &self,
        kind: Option<&str>,
        order_field: Option<&str>,
        predicate: Option<&Expr>,
        item_var: &str,
    ) -> Option<(std::sync::Arc<crate::storage::index::DataIndex>, String)> {
        let kind = kind?;
        let predicate = predicate?;
        let order = order_field?;

        let index = self.indexes.data_find(kind, order)?;
        let literal = predicate::prefix_literal(predicate, item_var, &index.def.field)?;

        Some((index, literal))
    }

    fn equality_selection(
        &self,
        kind: Option<&str>,
        predicate: Option<&Expr>,
        item_var: &str,
    ) -> Option<(std::sync::Arc<crate::storage::index::DataIndex>, serde_json::Value)> {
        let kind = kind?;
        let predicate = predicate?;

        for index in self.indexes.data_all() {
            if index.def.kind != kind
            {
                continue;
            }

            if let Some(literal) =
                predicate::equality_literal(predicate, item_var, &index.def.field)
            {
                return Some((index, literal));
            }
        }

        None
    }

    /// The page under one value of one declared index.
    ///
    /// The narrowest access path this engine has: it reads the entries
    /// holding that value and nothing else, so a query over a kind with
    /// a million rows and fifty matches costs the fifty. `matches` still
    /// runs on every candidate — the prefix answers one conjunct of the
    /// predicate, not all of it.
    #[allow(clippy::too_many_arguments)]
    fn query_by_data_prefix<F>(
        &self,
        index: &crate::storage::index::DataIndex,
        literal: &serde_json::Value,
        cursor: Option<Cursor>,
        desc: bool,
        limit: usize,
        offset: usize,
        matches: F,
    ) -> Result<QueryPage, String>
    where
        F: Fn(&Node) -> Result<bool, String>,
    {
        let prefix = keys::encode_order_value(Some(literal));

        self.query_by_key_prefix(index, prefix, cursor, desc, limit, offset, matches)
    }

    /// The page under one raw key prefix of one declared index.
    ///
    /// Split from [`Self::query_by_data_prefix`] because two different
    /// predicates reduce to a prefix and they compute different ones: an
    /// equality pins the whole encoded value, a `starts_with` pins only
    /// its leading bytes. What happens after — the cursor, the page, the
    /// visibility filter — is the same walk either way.
    #[allow(clippy::too_many_arguments)]
    fn query_by_key_prefix<F>(
        &self,
        index: &crate::storage::index::DataIndex,
        prefix: Vec<u8>,
        cursor: Option<Cursor>,
        desc: bool,
        limit: usize,
        offset: usize,
        matches: F,
    ) -> Result<QueryPage, String>
    where
        F: Fn(&Node) -> Result<bool, String>,
    {
        // The resume key is the last row's full `(value, address)`, not
        // `prefix + address`. Those coincide when the prefix is a whole
        // encoded value — the equality case — and differ the moment it
        // is not: under a *string* prefix the values vary, so a key built
        // from the prefix lands in the wrong place and the scan stops
        // after one page.
        let after = cursor
            .as_ref()
            .map(|c| keys::data_key(c.o.as_ref(), &c.a));

        let to_skip = if cursor.is_some() { 0 } else { offset };

        let order_field = Some(index.def.field.as_str());

        let mut nodes: Vec<Node> = Vec::new();
        let mut skipped = 0usize;
        let mut more = false;
        let mut failure: Option<String> = None;

        index
            .tree
            .for_each_range(&prefix, after.as_deref(), desc, |key, _value| {
                let Some(raw) = keys::address_from_data_key(key) else {
                    return Ok(true);
                };

                let address = String::from_utf8_lossy(raw).into_owned();

                let Some(node) = self.read_node(&address)? else {
                    return Ok(true);
                };

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
                    more = true;
                    return Ok(false);
                }

                nodes.push(node);

                Ok(true)
            })
            .map_err(io_message)?;

        if let Some(e) = failure {
            return Err(e);
        }

        // The cursor carries the order value even when the request did
        // not ask for an ordering, so that a later page of the same
        // query — which re-derives the same prefix from the same
        // predicate — resumes on the same key either way.
        let next = match nodes.last() {
            Some(last) if more => Cursor::from_node(last, order_field).encode(),
            _ => String::new(),
        };

        Ok(QueryPage { nodes, next, examined: 0 })
    }

    /// The declared-index page: walk the `data` index in its own order
    /// and stop at `limit`.
    ///
    /// The counterpart of [`Self::query_by_address`] for an ordering the
    /// primary index cannot serve. Both read exactly one page; the
    /// difference is only which access path is already in the requested
    /// order.
    ///
    /// The cursor is the same opaque `(order_value, address)` pair the
    /// sorted path issues, and it re-encodes to exactly the key the
    /// index holds for that row — so a caller can page through this
    /// index with a cursor a previous, index-less build handed them, and
    /// declaring or dropping an index never invalidates an outstanding
    /// cursor.
    #[allow(clippy::too_many_arguments)]
    fn query_by_data_index<F>(
        &self,
        kind: &str,
        field: &str,
        cursor: Option<Cursor>,
        desc: bool,
        limit: usize,
        offset: usize,
        matches: F,
    ) -> Result<QueryPage, String>
    where
        F: Fn(&Node) -> Result<bool, String>,
    {
        let Some(index) = self.indexes.data_find(kind, field) else {
            return Err(format!("no index over {kind}.{field}"));
        };

        let after = cursor
            .as_ref()
            .map(|c| keys::data_key(c.o.as_ref(), &c.a));

        // As in `query_by_address`: a cursor supersedes `offset`.
        let to_skip = if cursor.is_some() { 0 } else { offset };

        let mut nodes: Vec<Node> = Vec::new();
        let mut skipped = 0usize;
        let mut more = false;
        let mut failure: Option<String> = None;

        index
            .tree
            .for_each_range(&[], after.as_deref(), desc, |key, _value| {
                let Some(raw) = keys::address_from_data_key(key) else {
                    // A key with no terminator is not one this encoding
                    // produced. Skip it rather than fail the query: the
                    // row is still reachable through every other path.
                    return Ok(true);
                };

                let address = String::from_utf8_lossy(raw).into_owned();

                let Some(node) = self.read_node(&address)? else {
                    return Ok(true);
                };

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
                    more = true;
                    return Ok(false);
                }

                nodes.push(node);

                Ok(true)
            })
            .map_err(io_message)?;

        if let Some(e) = failure {
            return Err(e);
        }

        let next = match nodes.last() {
            Some(last) if more => Cursor::from_node(last, Some(field)).encode(),
            _ => String::new(),
        };

        Ok(QueryPage { nodes, next, examined: 0 })
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
        predicate_for_selection: Option<&Expr>,
        item_var_for_selection: &str,
        text_candidates: Option<Vec<String>>,
        matches: F,
    ) -> Result<QueryPage, String>
    where
        F: Fn(&Node) -> Result<bool, String>,
    {
        let mut candidates: Vec<Node> = Vec::new();
        let mut failure: Option<String> = None;
        let cap = max_scan_rows();

        // Collect one candidate. Shared by both enumeration paths so the
        // bound and the failure handling cannot differ between them.
        let take = |node: Node,
                        candidates: &mut Vec<Node>,
                        failure: &mut Option<String>|
         -> io::Result<bool> {
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
                    *failure = Some(e);
                    return Ok(false);
                }
            }

            Ok(true)
        };

        // Selection through an index when the predicate allows it, even
        // though the ordering does not come from it: read the rows the
        // predicate admits, then sort those. The alternative — which this
        // used to do — is reading the whole kind to find them.
        if let Some((index, literal)) =
            self.equality_selection(kind, predicate_for_selection, item_var_for_selection)
        {
            let prefix = keys::encode_order_value(Some(&literal));

            index
                .tree
                .for_each_range(&prefix, None, false, |key, _value| {
                    let Some(raw) = keys::address_from_data_key(key) else {
                        return Ok(true);
                    };

                    let address = String::from_utf8_lossy(raw).into_owned();

                    match self.read_node(&address)? {
                        Some(node) => take(node, &mut candidates, &mut failure),
                        None => Ok(true),
                    }
                })
                .map_err(io_message)?;
        } else if let Some(addresses) = text_candidates {
            // Selection through an inverted index, for the same reason:
            // read the rows the substring admits and sort those, rather
            // than reading the whole kind to find them. The set is a
            // superset, and `take` runs the full predicate on each row,
            // so the sorted page is identical to the scan's.
            for address in &addresses {
                let Some(node) = self.read_node(address).map_err(io_message)?
                else {
                    continue;
                };

                if !take(node, &mut candidates, &mut failure)
                    .map_err(io_message)?
                {
                    break;
                }
            }
        } else {
            self.scan_candidates(kind, owner, None, false, |node| {
                take(node, &mut candidates, &mut failure)
            })
            .map_err(io_message)?;
        }

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

        Ok(QueryPage { nodes: page, next, examined: 0 })
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
        &self,
        node: Node,
        edge_targets: Vec<(String, String)>,
        is_admin: bool,
    ) -> Result<Vec<Edge>, (String, Vec<Edge>)> {
        // The durable flush deliberately happens *after* this block,
        // outside the writer lock. Holding the lock across the fsync
        // means the next writer cannot even begin, so there is never a
        // second writer to share the flush with — see `wal::sync_pending`.
        let outcome = (|| {
            let _writer = self.write_lock();

            let address = node.address.clone();
            let owner = node.owner.clone();

            let mut operations = Vec::with_capacity(2 + edge_targets.len());

            // An overwrite archives what it replaces, exactly as `insert`
            // does — carried inside this frame rather than settled ahead of
            // it as its own record.
            //
            // And an overwrite is authorized here, against the node it
            // replaces. This is the same rule the transaction path applies
            // in `lower_transaction`, and it lives in the engine for the
            // same reason: an ownership check that only exists in a handler
            // is one that the next handler can forget. This one *was*
            // forgotten — `POST /node` reached this method with no check at
            // all, so writing to an address another identity owned silently
            // replaced their node and took ownership of it, while `PUT`,
            // `DELETE` and `insert_node` inside a transaction all refused
            // the same write. A rule enforced in three places out of four is
            // not a rule.
            match self.read_node(&address) {
                Ok(Some(previous)) => {
                    if !(is_admin || previous.can_write(&owner)) {
                        return Err((
                            format!(
                                "not authorized to overwrite node '{address}':                              it belongs to another owner"
                            ),
                            Vec::new(),
                        ));
                    }

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
        })();

        wal::sync_pending().map_err(|e| (e.to_string(), Vec::new()))?;

        outcome
    }

    // ---------------------------------------------------------------------
    // Users
    // ---------------------------------------------------------------------

    // ---------------------------------------------------------------------
    // Index administration
    // ---------------------------------------------------------------------

    /// Every declared `data`-field index, in name order.
    pub fn list_indexes(&self) -> Vec<IndexDef> {
        self.indexes
            .data_all()
            .into_iter()
            .map(|index| index.def.clone())
            .collect()
    }

    /// Every declared inverted index, in name order.
    pub fn list_text_indexes(&self) -> Vec<TextIndexDef> {
        self.indexes
            .text_all()
            .into_iter()
            .map(|index| index.def.clone())
            .collect()
    }

    /// Both kinds of declared index as one listing, in name order.
    ///
    /// What the admin endpoint answers with: an operator asking "is this
    /// field covered?" is asking one question, and two lists to merge is
    /// the answer to a different one.
    pub fn list_all_indexes(&self) -> Vec<IndexInfo> {
        let mut all: Vec<IndexInfo> = self
            .indexes
            .data_all()
            .iter()
            .map(|index| IndexInfo::ordered(&index.def))
            .chain(
                self.indexes
                    .text_all()
                    .iter()
                    .map(|index| IndexInfo::text(&index.def)),
            )
            .collect();

        all.sort_by(|a, b| a.name.cmp(&b.name));
        all
    }

    /// The declared definitions as a plain snapshot.
    ///
    /// Taken before the write path borrows the engine mutably, because
    /// validating a batch needs to know which indexes a node's keys will
    /// land in while the apply closure needs `&mut self`.
    fn declared_indexes(&self) -> Declared {
        Declared {
            data: self
                .indexes
                .data_all()
                .into_iter()
                .map(|index| index.def.clone())
                .collect(),
            text: self
                .indexes
                .text_all()
                .into_iter()
                .map(|index| index.def.clone())
                .collect(),
        }
    }

    /// Declare an index over one `data` field of one kind.
    ///
    /// Re-declaring the identical index is a no-op rather than an error:
    /// an operator re-running their schema setup should converge, and
    /// the alternative — failing on the second run — makes "make sure
    /// this index exists" impossible to express.
    ///
    /// # Why the whole kind is read before anything is logged
    ///
    /// The backfill writes one key per existing row, and a key the tree
    /// would refuse cannot be discovered *after* the create is durable:
    /// recovery would replay the create, hit the same refusal, and fail
    /// startup. So every existing row is measured against the index's
    /// key bound first, while the index can still simply not be created.
    /// This is the read Postgres does when it builds an index, for the
    /// reason Postgres reports as "index row size exceeds maximum".
    pub fn create_index(&self, def: IndexDef) -> Result<(), String> {
        // The durable flush deliberately happens *after* this block,
        // outside the writer lock. Holding the lock across the fsync
        // means the next writer cannot even begin, so there is never a
        // second writer to share the flush with — see `wal::sync_pending`.
        let outcome = (|| {
            let _writer = self.write_lock();

            def.validate()?;

            if let Some(existing) = self.indexes.data_get(&def.name) {
                if existing.def == def {
                    return Ok(());
                }

                return Err(format!(
                    "index '{}' already exists, over {}.{} — drop it before                  redefining it",
                    existing.def.name, existing.def.kind, existing.def.field
                ));
            }

            if let Some(existing) = self.indexes.text_get(&def.name) {
                return Err(format!(
                    "index '{}' already exists as an inverted index over \
                     {}.{} — one name names one index",
                    existing.def.name, existing.def.kind, existing.def.field
                ));
            }

            if let Some(other) = self.indexes.data_find(&def.kind, &def.field) {
                return Err(format!(
                    "index '{}' already covers {}.{}",
                    other.def.name, def.kind, def.field
                ));
            }

            self.check_backfill_admissible(&def)?;

            self.apply_atomic(vec![Operation::CreateIndex(def)])
        })();

        wal::sync_pending().map_err(|e| e.to_string())?;

        outcome
    }

    /// Declare an inverted index over one `data` field's text.
    ///
    /// The same shape as [`Self::create_index`] — validate, refuse a
    /// contradiction, prove the backfill is admissible, then log and
    /// apply as one crash-atomic mutation — because it is the same
    /// operational act: it adds a durable access path every later write
    /// has to maintain.
    ///
    /// Names are checked against **both** index catalogs. `DELETE
    /// /admin/indexes/:name` names one index, so a name that resolved to
    /// two would be a drop with no defined meaning.
    pub fn create_text_index(&self, def: TextIndexDef) -> Result<(), String> {
        // The durable flush deliberately happens *after* this block,
        // outside the writer lock — see `create_index`.
        let outcome = (|| {
            let _writer = self.write_lock();

            def.validate()?;

            if let Some(existing) = self.indexes.text_get(&def.name) {
                if existing.def == def {
                    return Ok(());
                }

                return Err(format!(
                    "index '{}' already exists, over {}.{} — drop it before \
                     redefining it",
                    existing.def.name, existing.def.kind, existing.def.field
                ));
            }

            if let Some(existing) = self.indexes.data_get(&def.name) {
                return Err(format!(
                    "index '{}' already exists as an ordered index over \
                     {}.{} — one name names one index",
                    existing.def.name, existing.def.kind, existing.def.field
                ));
            }

            if let Some(other) = self.indexes.text_find(&def.kind, &def.field) {
                return Err(format!(
                    "index '{}' already covers the text of {}.{}",
                    other.def.name, def.kind, def.field
                ));
            }

            self.check_text_backfill_admissible(&def)?;

            self.apply_atomic(vec![Operation::CreateTextIndex(def)])
        })();

        wal::sync_pending().map_err(|e| e.to_string())?;

        outcome
    }

    /// Every existing row of the kind, measured against the bound the
    /// inverted index imposes — before anything is logged.
    ///
    /// The same read [`Self::check_backfill_admissible`] does and for the
    /// identical reason: a posting the tree would refuse cannot be
    /// discovered after the create is durable, because recovery would
    /// replay the create, hit the same refusal, and fail startup for
    /// good.
    fn check_text_backfill_admissible(
        &self,
        def: &TextIndexDef,
    ) -> Result<(), String> {
        let mut failure: Option<String> = None;

        self.scan_candidates(Some(&def.kind), None, None, false, |node| {
            if let Err(e) = text::check_text_keys(
                std::iter::once(def),
                &node.address,
                &node.data,
            ) {
                failure = Some(format!(
                    "cannot index {}.{}: node '{}' {}",
                    def.kind, def.field, node.address, e
                ));

                return Ok(false);
            }

            Ok(true)
        })
        .map_err(io_message)?;

        match failure {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Drop a declared index, removing its tree.
    pub fn drop_index(&self, name: &str) -> Result<(), String> {
        // The durable flush deliberately happens *after* this block,
        // outside the writer lock. Holding the lock across the fsync
        // means the next writer cannot even begin, so there is never a
        // second writer to share the flush with — see `wal::sync_pending`.
        let outcome = (|| {
            let _writer = self.write_lock();

            // One name names one index, so this is a lookup in both
            // catalogs rather than two endpoints — an operator dropping
            // an index should not have to know which sort it was.
            let Some(index) = self.indexes.data_get(name) else {
                if self.indexes.text_get(name).is_some() {
                    // Nothing depends on an inverted index the way a
                    // reference depends on an ordered one: a reference
                    // resolves through a value, and this index does not
                    // store values. So there is no dependent to check.
                    return self
                        .apply_atomic(vec![Operation::DropTextIndex(name.to_string())]);
                }

                return Err(format!("no index named '{name}'"));
            };

            // A reference is only accepted because the access paths
            // behind it exist. Dropping one out from under it would
            // leave a durable rule with no way to be enforced — a
            // cascade that cannot find its targets, or a referenced
            // value nothing keeps unique.
            let dependents = self
                .references
                .depending_on_index(&index.def.kind, &index.def.field);

            if let Some(dependent) = dependents.first() {
                return Err(format!(
                    "index '{name}' cannot be dropped: reference {:?} is \
                     enforced through it ({} in total). Drop the reference \
                     first.",
                    dependent.name,
                    dependents.len(),
                ));
            }

            self.apply_atomic(vec![Operation::DropIndex(name.to_string())])
        })();

        wal::sync_pending().map_err(|e| e.to_string())?;

        outcome
    }

    // ---------------------------------------------------------------------
    // Reference administration
    // ---------------------------------------------------------------------

    /// Every declared reference, in name order.
    pub fn list_references(&self) -> Vec<ReferenceDef> {
        self.references.all()
    }

    /// Declare a reference: what one kind's `data` field points at, and
    /// what deleting the referenced node does to the nodes referencing
    /// it.
    ///
    /// Re-declaring the identical reference is a no-op, for the same
    /// reason re-declaring an index is: "make sure this exists" has to
    /// be expressible.
    ///
    /// # Why the whole referencing kind is read first
    ///
    /// A rule accepted over data that already breaks it is false from
    /// the instant it is created, and every read that trusts it is
    /// wrong. So every existing referencing node is resolved before
    /// anything is logged — the same read `create_index` does for a
    /// unique index, and the same one Postgres does when a foreign key
    /// is added without `NOT VALID`.
    pub fn create_reference(&self, def: ReferenceDef) -> Result<(), String> {
        // The durable flush deliberately happens *after* this block,
        // outside the writer lock. Holding the lock across the fsync
        // means the next writer cannot even begin, so there is never a
        // second writer to share the flush with — see `wal::sync_pending`.
        let outcome = (|| {
            let _writer = self.write_lock();

            def.validate()?;

            if let Some(existing) = self.references.get(&def.name) {
                if existing == def {
                    return Ok(());
                }

                return Err(format!(
                    "reference '{}' already exists, over {}.{} → {} — drop it \
                     before redefining it",
                    existing.name, existing.kind, existing.field,
                    existing.parent_kind,
                ));
            }

            // The referencing side. Without this index, finding what
            // points at a node being deleted is a scan of the whole
            // referencing kind, on every delete — which is the cost this
            // feature exists to remove, not to impose.
            if self.indexes.data_find(&def.kind, &def.field).is_none() {
                return Err(format!(
                    "reference {:?} needs an index over {}.{}: without one, \
                     deleting a {} would have to read every {}. Declare that \
                     index first.",
                    def.name, def.kind, def.field, def.parent_kind, def.kind,
                ));
            }

            // The referenced side. An address is unique by construction,
            // so referencing by address needs nothing; referencing a
            // `data` field needs that field to actually identify one
            // node, which is what a unique index means here.
            if let Some(parent_field) = &def.parent_field {
                match self.indexes.data_find(&def.parent_kind, parent_field) {
                    Some(index) if index.def.unique => {}

                    Some(index) => {
                        return Err(format!(
                            "reference {:?} points at {}.{}, which index '{}' \
                             covers but does not make unique — a reference has \
                             to name exactly one node, and a value two nodes \
                             can hold names neither",
                            def.name, def.parent_kind, parent_field,
                            index.def.name,
                        ))
                    }

                    None => {
                        return Err(format!(
                            "reference {:?} points at {}.{}, which needs a \
                             unique index before anything can reference it",
                            def.name, def.parent_kind, parent_field,
                        ))
                    }
                }
            }

            self.check_reference_satisfied(&def)?;

            self.apply_atomic(vec![Operation::CreateReference(def)])
        })();

        wal::sync_pending().map_err(|e| e.to_string())?;

        outcome
    }

    /// Drop a declared reference.
    ///
    /// The nodes it governed are untouched: what stops is the
    /// enforcement, so children of a later-deleted parent survive it
    /// as orphans. That is the same trade dropping a foreign key makes.
    pub fn drop_reference(&self, name: &str) -> Result<(), String> {
        // The durable flush deliberately happens *after* this block,
        // outside the writer lock. Holding the lock across the fsync
        // means the next writer cannot even begin, so there is never a
        // second writer to share the flush with — see `wal::sync_pending`.
        let outcome = (|| {
            let _writer = self.write_lock();

            if self.references.get(name).is_none() {
                return Err(format!("no reference named '{name}'"));
            }

            self.apply_atomic(vec![Operation::DropReference(name.to_string())])
        })();

        wal::sync_pending().map_err(|e| e.to_string())?;

        outcome
    }

    /// Refuse a reference the data does not already satisfy. See
    /// [`Self::create_reference`].
    fn check_reference_satisfied(&self, def: &ReferenceDef) -> Result<(), String> {
        let mut failure: Option<String> = None;

        self.scan_candidates(Some(&def.kind), None, None, false, |node| {
            let Some(key) = Self::referencing_key(&node, def) else {
                return Ok(true);
            };

            match self.resolves_live(def, &key) {
                Ok(true) => Ok(true),

                Ok(false) => {
                    failure = Some(format!(
                        "reference {:?} cannot be declared: {} holds {}={} \
                         which is not a live {}",
                        def.name, node.address, def.field, key, def.parent_kind,
                    ));

                    Ok(false)
                }

                Err(e) => {
                    failure = Some(e);
                    Ok(false)
                }
            }
        })
        .map_err(io_message)?;

        match failure {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    // ---------------------------------------------------------------------
    // Reference resolution
    // ---------------------------------------------------------------------

    /// The value a node *holds* in its referencing field, or `None` when
    /// it holds nothing.
    ///
    /// Absent and `null` are the same answer on purpose: both mean "this
    /// node references nothing", which is always admissible. That is a
    /// nullable foreign key, and it is what makes `set_null` a usable
    /// action rather than one that produces rows the rule then rejects.
    fn referencing_key(node: &Node, def: &ReferenceDef) -> Option<serde_json::Value> {
        serde_json::from_str::<serde_json::Value>(&node.data)
            .ok()?
            .get(&def.field)
            .filter(|value| !value.is_null())
            .cloned()
    }

    /// The value a node is *referenced by* — its address, or the parent
    /// field the reference names.
    fn referenced_key(node: &Node, def: &ReferenceDef) -> Option<serde_json::Value> {
        match &def.parent_field {
            None => Some(serde_json::Value::String(node.address.clone())),

            Some(field) => serde_json::from_str::<serde_json::Value>(&node.data)
                .ok()?
                .get(field)
                .filter(|value| !value.is_null())
                .cloned(),
        }
    }

    /// Does this value name a live node of the referenced kind?
    ///
    /// Two access paths, both O(log n): the primary index when the
    /// reference is by address, and the parent's unique index when it is
    /// by a `data` field. Neither reads more than the one node it is
    /// asking about.
    fn resolves_live(
        &self,
        def: &ReferenceDef,
        key: &serde_json::Value,
    ) -> Result<bool, String> {
        match &def.parent_field {
            None => {
                let Some(address) = key.as_str() else {
                    // A reference by address whose value is not a string
                    // cannot name a node at all. Reported as unresolved
                    // rather than as a type error: the answer to "does
                    // this exist" is no.
                    return Ok(false);
                };

                Ok(self
                    .read_node(address)
                    .map_err(io_message)?
                    .is_some_and(|node| node.kind == def.parent_kind))
            }

            Some(field) => {
                let index = self
                    .indexes
                    .data_find(&def.parent_kind, field)
                    .ok_or_else(|| {
                        format!(
                            "reference {:?} cannot be enforced: the unique \
                             index over {}.{} it resolves through is gone",
                            def.name, def.parent_kind, field,
                        )
                    })?;

                let prefix = keys::encode_order_value(Some(key));
                let mut found = false;

                index
                    .tree
                    .for_each_range(&prefix, None, false, |_key, _value| {
                        found = true;
                        Ok(false)
                    })
                    .map_err(io_message)?;

                Ok(found)
            }
        }
    }

    /// Every node that references `key` through `def`, judged against
    /// the batch's staged view rather than only against the index.
    ///
    /// The index alone is not enough inside a transaction, in both
    /// directions: a child the batch inserted is not in the index yet,
    /// and a child the batch already changed may no longer hold the
    /// value the index still remembers. So index candidates and the
    /// batch's own writes of this kind are unioned, each resolved
    /// through the overlay, and the field is re-read from the resolved
    /// node — the index proposes, the current value decides.
    ///
    /// `staged_kinds` is what keeps that union from being quadratic. The
    /// obvious version — union in every address the batch has touched —
    /// costs the size of the batch on *every* lookup, and a cascade does
    /// one lookup per removed node per reference, so clearing a large
    /// kind would spend the batch squared inside the writer lock. Only
    /// the batch's *written* nodes can be candidates (an address it
    /// removed resolves to nothing), and they are known before the
    /// closure starts, so they are grouped by kind once.
    ///
    /// Walked through a `BTreeSet` so the order is sorted and
    /// deterministic: these become WAL records, and a WAL should not
    /// vary run to run for identical input.
    fn referencing_nodes(
        &self,
        def: &ReferenceDef,
        key: &serde_json::Value,
        staged: &HashMap<String, Option<Node>>,
        staged_kinds: &HashMap<String, Vec<String>>,
    ) -> Result<Vec<Node>, String> {
        let index = self
            .indexes
            .data_find(&def.kind, &def.field)
            .ok_or_else(|| {
                format!(
                    "reference {:?} cannot be enforced: the index over {}.{} \
                     it finds referencing nodes through is gone",
                    def.name, def.kind, def.field,
                )
            })?;

        let prefix = keys::encode_order_value(Some(key));

        let mut candidates: std::collections::BTreeSet<String> =
            std::collections::BTreeSet::new();

        index
            .tree
            .for_each_range(&prefix, None, false, |entry, _value| {
                if let Some(raw) = keys::address_from_data_key(entry) {
                    candidates.insert(String::from_utf8_lossy(raw).into_owned());
                }

                Ok(true)
            })
            .map_err(io_message)?;

        if let Some(written) = staged_kinds.get(&def.kind) {
            candidates.extend(written.iter().cloned());
        }

        let mut nodes = Vec::new();

        for address in candidates {
            let Some(node) = self.staged_node(staged, &address).map_err(io_message)?
            else {
                continue;
            };

            if node.kind != def.kind {
                continue;
            }

            if Self::referencing_key(&node, def).as_ref() != Some(key) {
                continue;
            }

            nodes.push(node);
        }

        Ok(nodes)
    }

    // ---------------------------------------------------------------------
    // Referential actions
    // ---------------------------------------------------------------------

    /// Lower a delete into every mutation the declared references make
    /// it entail: the node itself, whatever cascades from it, whatever
    /// cascades from *that*, and any `set_null` updates along the way.
    ///
    /// This is the one place a delete becomes operations, for every
    /// path — the standalone `delete`, `delete_node`, `clear_kind` and
    /// `delete_where` — so the referential rules cannot hold on one
    /// route and not another.
    ///
    /// # Why the whole closure is one frame
    ///
    /// A parent deleted in one transaction and its children in the next
    /// is exactly the failure the rule exists to prevent: a crash
    /// between them leaves rows pointing at a node that is gone, which
    /// nothing later will find. So the closure is expanded here, before
    /// anything is written, and staged into the same frame as the delete
    /// that triggered it.
    ///
    /// That is also why it is bounded rather than unbounded: an
    /// atomic cascade has to fit in one frame, and a cascade that does
    /// not fit is refused *before* the WAL rather than half-applied
    /// after it.
    ///
    /// # Authority
    ///
    /// A referential action runs with the authority of the declaration,
    /// not of the caller: cascading into another owner's node is what
    /// makes it an integrity rule rather than a request. Declaring a
    /// reference across owners is an admin-only act for exactly that
    /// reason.
    fn lower_delete_closure(
        &self,
        seeds: Vec<Node>,
        lowered: &mut Vec<Operation>,
        staged: &mut HashMap<String, Option<Node>>,
    ) -> Result<(), TransactionError> {
        // The common case: no references declared at all. Costs one
        // lock-free check rather than a work queue and a visited set.
        if self.references.is_empty() {
            for node in seeds {
                lowered.push(Operation::Archive(HistoryEntry::now(node.clone())));
                staged.insert(node.address.clone(), None);
                lowered.push(Operation::Delete(node.address));
            }

            return Ok(());
        }

        let bound = max_transaction_ops();

        // The batch's own written nodes, grouped by kind — the
        // candidates no index knows about yet. Taken once, and complete
        // for the life of the closure: lowering is sequential, so every
        // insert the batch makes has already happened, and the only
        // entries the closure itself adds are removals (which resolve to
        // nothing) and `set_null` updates (whose field is now null, so
        // they reference nothing). A stale copy here is harmless in any
        // case — this proposes candidates, and each one is re-resolved
        // through the overlay before it is believed.
        let mut staged_kinds: HashMap<String, Vec<String>> = HashMap::new();

        for (address, slot) in staged.iter() {
            if let Some(node) = slot {
                staged_kinds
                    .entry(node.kind.clone())
                    .or_default()
                    .push(address.clone());
            }
        }

        let mut queue: std::collections::VecDeque<Node> = seeds.into();

        // Addresses already scheduled for removal. Also the cycle guard:
        // a reference graph may contain one, and a closure that revisits
        // a node it has already removed does not terminate.
        let mut removed: std::collections::HashSet<String> =
            std::collections::HashSet::new();

        while let Some(node) = queue.pop_front() {
            if !removed.insert(node.address.clone()) {
                continue;
            }

            lowered.push(Operation::Archive(HistoryEntry::now(node.clone())));
            staged.insert(node.address.clone(), None);
            lowered.push(Operation::Delete(node.address.clone()));

            for def in self.references.for_parent(&node.kind) {
                let Some(key) = Self::referenced_key(&node, &def) else {
                    // Nothing can reference a node that holds no
                    // referenced value.
                    continue;
                };

                let children: Vec<Node> = self
                    .referencing_nodes(&def, &key, staged, &staged_kinds)
                    .map_err(TransactionError::Storage)?
                    .into_iter()
                    .filter(|child| !removed.contains(&child.address))
                    .collect();

                if children.is_empty() {
                    continue;
                }

                match def.on_delete {
                    ReferentialAction::Cascade => queue.extend(children),

                    // A conflict, not a malformed request: the batch
                    // is well formed and the state says no. That is the
                    // same shape as a lost `set_if` race, and it gets
                    // the same answer.
                    ReferentialAction::Restrict => {
                        return Err(TransactionError::Precondition(format!(
                            "delete refused, nothing applied: {} is still \
                             referenced by {} through {:?} ({} in total), and \
                             that reference is declared restrict",
                            node.address,
                            children[0].address,
                            def.name,
                            children.len(),
                        )))
                    }

                    ReferentialAction::SetNull => {
                        for child in children {
                            let cleared = Self::with_field_null(&child, &def.field)
                                .map_err(TransactionError::Invalid)?;

                            lowered.push(Operation::Archive(HistoryEntry::now(
                                child.clone(),
                            )));

                            staged.insert(
                                cleared.address.clone(),
                                Some(cleared.clone()),
                            );

                            lowered.push(Operation::Insert(cleared));
                        }
                    }
                }
            }

            // Checked at the *end* of the iteration, because the node's
            // own archive and removal are not everything one iteration
            // stages: a `set_null` reference adds an archive and a
            // rewrite for each child on top of them. A bound checked
            // before those bounds nothing — which is how deleting one
            // parent with enough `set_null` children used to walk
            // straight past the refusal below and stage 2N+2 mutations
            // in a frame that admits far fewer.
            //
            // Every operation this loop appends is appended inside this
            // body, so a check here is a check on the whole lowered
            // result — the same thing `execute_transaction` checks after
            // `lower_transaction`, for the same reason: lowering is
            // where the real size becomes known.
            if lowered.len() > bound {
                return Err(TransactionError::Invalid(format!(
                    "delete refused, nothing applied: deleting {} cascades to \
                     more than the {} mutations this engine will stage in one \
                     frame. A cascade is atomic or it is not a cascade, so it \
                     is refused rather than split. Delete the referencing \
                     nodes in batches first, or raise \
                     {MAX_TRANSACTION_OPS_ENV}.",
                    node.address, bound,
                )));
            }
        }

        Ok(())
    }

    /// The same node with one `data` field set to `null`.
    ///
    /// Set to `null` rather than removed so the field keeps existing:
    /// a caller reading the row sees that it *has* a reference field and
    /// that it currently points at nothing, which is the distinction
    /// `set_null` is for. Removing the key would make a cleared
    /// reference indistinguishable from a kind that never had one.
    fn with_field_null(node: &Node, field: &str) -> Result<Node, String> {
        let mut data: serde_json::Value = serde_json::from_str(&node.data)
            .map_err(|e| {
                format!(
                    "cannot clear {}.{}: its data is not a JSON object ({e})",
                    node.address, field,
                )
            })?;

        let Some(object) = data.as_object_mut() else {
            return Err(format!(
                "cannot clear {}.{}: its data is not a JSON object",
                node.address, field,
            ));
        };

        object.insert(field.to_string(), serde_json::Value::Null);

        let mut cleared = node.clone();
        cleared.data = data.to_string();

        Ok(cleared)
    }

    /// Refuse a batch that would leave a node referencing something that
    /// is not there.
    ///
    /// The delete half of referential integrity is enforced where a
    /// delete is *lowered*, because it produces mutations. This is the
    /// insert half, which only ever refuses — so it runs here, beside
    /// [`Self::check_unique`], before the WAL and for the same reason:
    /// a record that becomes durable and is then refused would be
    /// replayed into the same refusal on every subsequent start.
    ///
    /// # Deferred, like SQL's
    ///
    /// The whole batch's net effect is computed before anything is
    /// checked, so a transaction that inserts a comment before the post
    /// it belongs to is accepted. Checking in order would make the
    /// constraint depend on the order a caller happened to serialize its
    /// writes in, which is not a property of the data.
    fn check_references(&self, operations: &[Operation]) -> Result<(), String> {
        if self.references.is_empty() {
            return Ok(());
        }

        // The batch's net effect: what it leaves present, and what it
        // leaves gone. Built in operation order so a batch that deletes
        // and re-creates an address ends up with the re-creation.
        let mut inserted: HashMap<&str, &Node> = HashMap::new();
        let mut deleted: std::collections::HashSet<&str> =
            std::collections::HashSet::new();

        for operation in operations {
            match operation {
                Operation::Insert(node) => {
                    inserted.insert(&node.address, node);
                    deleted.remove(node.address.as_str());
                }

                Operation::Delete(address) => {
                    inserted.remove(address.as_str());
                    deleted.insert(address);
                }

                _ => {}
            }
        }

        // The keys the batch's own writes make referenceable, per
        // reference, built the first time a reference is consulted.
        //
        // Without it this is quadratic: every child would scan every
        // insert looking for its parent, so a bulk load of n rows costs
        // n² inside the writer lock — and a bulk load is exactly when
        // this check runs most. One pass per reference instead.
        let mut satisfied: HashMap<String, std::collections::HashSet<String>> =
            HashMap::new();

        for node in inserted.values() {
            for def in self.references.for_child(&node.kind) {
                let Some(key) = Self::referencing_key(node, &def) else {
                    continue;
                };

                if !satisfied.contains_key(&def.name) {
                    let keys = inserted
                        .values()
                        .filter(|candidate| candidate.kind == def.parent_kind)
                        .filter_map(|candidate| Self::referenced_key(candidate, &def))
                        .map(|value| value.to_string())
                        .collect();

                    satisfied.insert(def.name.clone(), keys);
                }

                // A parent this batch writes satisfies the reference
                // whether or not it existed before — that is the whole
                // point of checking the net effect rather than the
                // starting state.
                if satisfied[&def.name].contains(&key.to_string()) {
                    continue;
                }

                if self.resolves_committed(&def, &key, &deleted)? {
                    continue;
                }

                return Err(format!(
                    "reference {:?}: {} holds {}={} which is not a live {}",
                    def.name, node.address, def.field, key, def.parent_kind,
                ));
            }
        }

        Ok(())
    }

    /// [`Self::resolves_live`], minus the parents this batch is
    /// removing.
    ///
    /// The parents it is *adding* are answered before this is called —
    /// see the memo in [`Self::check_references`] — so this only has to
    /// consult committed state and subtract.
    fn resolves_committed(
        &self,
        def: &ReferenceDef,
        key: &serde_json::Value,
        deleted: &std::collections::HashSet<&str>,
    ) -> Result<bool, String> {
        match &def.parent_field {
            None => {
                let Some(address) = key.as_str() else {
                    return Ok(false);
                };

                if deleted.contains(address) {
                    return Ok(false);
                }

                self.resolves_live(def, key)
            }

            Some(field) => {
                let index = self
                    .indexes
                    .data_find(&def.parent_kind, field)
                    .ok_or_else(|| {
                        format!(
                            "reference {:?} cannot be enforced: the unique \
                             index over {}.{} it resolves through is gone",
                            def.name, def.parent_kind, field,
                        )
                    })?;

                let prefix = keys::encode_order_value(Some(key));
                let mut found = false;

                index
                    .tree
                    .for_each_range(&prefix, None, false, |entry, _value| {
                        let Some(raw) = keys::address_from_data_key(entry) else {
                            return Ok(true);
                        };

                        let address = String::from_utf8_lossy(raw);

                        // A holder this batch is removing does not
                        // satisfy anything.
                        if deleted.contains(address.as_ref()) {
                            return Ok(true);
                        }

                        found = true;
                        Ok(false)
                    })
                    .map_err(io_message)?;

                Ok(found)
            }
        }
    }

    /// Refuse a definition whose backfill could not be applied, before
    /// the create is logged. See [`Self::create_index`].
    fn check_backfill_admissible(&self, def: &IndexDef) -> Result<(), String> {
        let mut failure: Option<String> = None;

        // For a unique index, the rows that already exist have to satisfy
        // the rule being declared. Accepting the declaration and then
        // enforcing it only on later writes would leave the constraint
        // false the moment it was created — and every read that trusted
        // it wrong.
        let mut seen: std::collections::HashMap<Vec<u8>, String> =
            std::collections::HashMap::new();

        self.scan_candidates(Some(&def.kind), None, None, false, |node| {
            if let Err(e) = keys::check_data_keys(
                std::iter::once(def),
                &node.address,
                &node.data,
            ) {
                failure = Some(format!(
                    "cannot index {}.{}: node '{}' {}",
                    def.kind, def.field, node.address, e
                ));

                return Ok(false);
            }

            if def.unique {
                let data: Option<serde_json::Value> =
                    serde_json::from_str(&node.data).ok();
                let value = data.as_ref().and_then(|d| d.get(&def.field));
                let encoded = keys::encode_order_value(value);

                if let Some(first) = seen.get(&encoded) {
                    failure = Some(format!(
                        "cannot declare {}.{} unique: '{}' and '{}' already \
                         share a value. Resolve the duplicates first — a \
                         constraint that is false when it is created is worse \
                         than none, because reads start trusting it.",
                        def.kind, def.field, first, node.address,
                    ));

                    return Ok(false);
                }

                seen.insert(encoded, node.address.clone());
            }

            Ok(true)
        })
        .map_err(io_message)?;

        match failure {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Persists a new user record.
    ///
    /// Only the token hash is persisted. Plaintext tokens are never
    /// stored in the engine. One durable record, so this stays
    /// standalone under the rule in [`Self::apply_atomic`].
    /// Put an identity straight into the in-memory map, with no WAL
    /// record and no durable write.
    ///
    /// For tests that need a caller to authenticate as. The durable path
    /// is [`StorageEngine::insert_user`], which is what a running server
    /// uses; this exists so a test does not have to fake one.
    #[cfg(test)]
    pub(crate) fn seed_user(&self, record: UserRecord) {
        self.users_write().insert(record.token_hash.clone(), record);
    }

    pub fn insert_user(&self, record: UserRecord) -> Result<(), String> {
        // The durable flush deliberately happens *after* this block,
        // outside the writer lock. Holding the lock across the fsync
        // means the next writer cannot even begin, so there is never a
        // second writer to share the flush with — see `wal::sync_pending`.
        let outcome = {
            let _writer = self.write_lock();

            self.apply_atomic(vec![Operation::InsertUser(record)])
        };

        wal::sync_pending().map_err(|e| e.to_string())?;

        outcome
    }

    /// Revokes a user by token hash. One durable record, standalone.
    pub fn revoke_user(&self, token_hash: &str) -> Result<(), String> {
        // The durable flush deliberately happens *after* this block,
        // outside the writer lock. Holding the lock across the fsync
        // means the next writer cannot even begin, so there is never a
        // second writer to share the flush with — see `wal::sync_pending`.
        let outcome = {
            let _writer = self.write_lock();

            self.apply_atomic(vec![Operation::RevokeUser(token_hash.to_string())])
        };

        wal::sync_pending().map_err(|e| e.to_string())?;

        outcome
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
    pub fn compact_user_log(&self) -> io::Result<()> {
        let _writer = self.write_lock();

        // Rewriting costs one pass over the identities, so it should be
        // rare relative to appends. Four times the live count means an
        // idle database never rewrites and a heavily-rotated one
        // amortizes to a constant factor. The floor keeps a database
        // with a handful of users from rewriting on every restart.
        let threshold = (self.users_read().len() * 4).max(64);

        if self.user_log_records.load(Ordering::Relaxed) <= threshold {
            return Ok(());
        }

        let live: Vec<UserOpRecord> = self
            .users_read()
            .values()
            .cloned()
            .map(UserOpRecord::Put)
            .collect();

        binary::rewrite_records(&binary::users_path(), &live)?;

        self.user_log_records.store(live.len(), Ordering::Relaxed);

        Ok(())
    }

    pub fn find_user_by_hash(&self, token_hash: &str) -> Option<UserRecord> {
        self.users_read().get(token_hash).cloned()
    }

    /// Returns every persistent user.
    ///
    /// Bootstrap identities that exist only in environment configuration
    /// are not represented here.
    pub fn list_users(&self) -> Vec<UserRecord> {
        self.users_read().values().cloned().collect()
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
            user_count: self.users_read().len() as u64,
            history_entries: self.indexes.history.len(),
            kinds: by_kind
                .into_iter()
                .map(|(kind, count)| KindCount { kind, count })
                .collect(),
            reads_total: self.reads_total.load(Ordering::Relaxed),
            writes_total: self.writes_total.load(Ordering::Relaxed),
            storage: self.storage_stats(),
            version: env!("CARGO_PKG_VERSION"),
            runtime: metrics::snapshot(),
            cells: metrics::cell_stats(&self.cells),
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
        &self,
        ops: Vec<TxOperation>,
    ) -> Result<(), TransactionError> {
        // The durable flush deliberately happens *after* this block,
        // outside the writer lock. Holding the lock across the fsync
        // means the next writer cannot even begin, so there is never a
        // second writer to share the flush with — see `wal::sync_pending`.
        let outcome = (|| {
            let _writer = self.write_lock();

            let lowered = self.lower_transaction(ops)?;

            // Checked after lowering, because lowering is where a batch's
            // real size becomes known — see `max_transaction_ops`. Checked
            // before `commit`, because past that point the first record is
            // durable and the batch can no longer be refused.
            if lowered.len() > max_transaction_ops() {
                return Err(TransactionError::Invalid(format!(
                    "transaction failed, nothing applied: it resolves to {} \
                     mutations, over the {} this engine will stage in one \
                     frame. Split the batch, or raise {MAX_TRANSACTION_OPS_ENV}.",
                    lowered.len(),
                    max_transaction_ops()
                )));
            }

            // Before the frame opens, for the same reason the size bound
            // is checked here: past `commit` the first record is durable
            // and the batch can no longer be refused.
            self.check_unique(&lowered)
                .map_err(TransactionError::Invalid)?;

            self.check_references(&lowered)
                .map_err(TransactionError::Invalid)?;

            let declared = self.declared_indexes();

            let transaction = Transaction::from_operations(lowered);

            let sequence = transaction
                .commit(&declared, |operation| self.apply_committed(operation))
                .map_err(|e| TransactionError::Storage(e.to_string()))?;

            self.note_applied(sequence);

            Ok(())
        })();

        wal::sync_pending().map_err(|e| TransactionError::Storage(e.to_string()))?;

        outcome
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
                        // Archives the value being removed, as the
                        // standalone delete path does — a deleted node's
                        // final state is exactly the one an operator
                        // comes looking for — and expands whatever the
                        // declared references make this delete entail.
                        Some(node) => self.lower_delete_closure(
                            vec![node],
                            &mut lowered,
                            &mut staged,
                        )?,

                        None => {
                            return Err(TransactionError::Invalid(format!(
                                "transaction failed, nothing applied: \
                                 delete target not found: {address}"
                            )))
                        }
                    }
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

        let mut seeds = Vec::with_capacity(targets.len());

        for address in targets {
            match self.staged_node(staged, &address).map_err(storage)? {
                Some(node) => seeds.push(node),

                // A selected address that resolves to nothing can only
                // come from a race with the overlay; removing it is
                // still the right answer and costs one no-op record.
                None => {
                    staged.insert(address.clone(), None);
                    lowered.push(Operation::Delete(address));
                }
            }
        }

        // A bulk delete is a delete: it cascades, restricts and clears
        // exactly like the single one, because it lowers through the
        // same function.
        self.lower_delete_closure(seeds, lowered, staged)?;

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
        &self,
        operation: &Operation,
    ) -> Result<(), String> {
        self.apply_operation(operation).map_err(io_message)
    }

    fn apply_operation(&self, operation: &Operation) -> io::Result<()> {
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

                // Declared `data` indexes. The old entry has to be
                // retracted from the *previous* node's kind and value —
                // an update can change either — and the retraction has
                // to happen before the assertion, or an update that
                // leaves the value alone would remove the entry it just
                // wrote.
                if let Some(previous_node) = &previous_node {
                    self.retract_data_keys(previous_node)?;
                    self.retract_text_keys(previous_node)?;
                }

                self.assert_data_keys(node)?;
                self.assert_text_keys(node)?;

                if let Some(location) = previous {
                    self.store.mark_obsolete(location);
                }

                self.cache.put(location, Arc::new(node.clone()));

                self.writes_total.fetch_add(1, Ordering::Relaxed);
                self.cells.record_write(node.coordinate, node.data.len() as u64);
            }

            // Removing the primary index entry is what makes the node
            // gone: nothing resolves the address any more, so no read
            // path can reach the record even though its bytes are still
            // in the heap until compaction reclaims them.
            Operation::Delete(address) => {
                let mut coordinate = None;

                if let Some(location) = self.node_location(address)? {
                    let node = self.node_at(location)?;

                    coordinate = Some((node.coordinate, node.data.len() as u64));

                    self.indexes.primary.remove(address.as_bytes())?;
                    self.indexes
                        .kind
                        .remove(&keys::kind_key(&node.kind, address))?;
                    self.indexes
                        .owner
                        .remove(&keys::owner_key(&node.owner, address))?;

                    self.retract_data_keys(&node)?;
                    self.retract_text_keys(&node)?;

                    self.store.mark_obsolete(location);
                    self.cache.invalidate(location);
                }

                self.writes_total.fetch_add(1, Ordering::Relaxed);

                // A delete of an address that was already gone still
                // counts as a mutation — `writes_total` has always
                // counted it — but there is no record, so there is no
                // coordinate. It goes to the unattributed counter rather
                // than to a guess.
                match coordinate {
                    Some((coordinate, bytes)) => {
                        self.cells.record_write(coordinate, bytes)
                    }
                    None => self.cells.record_unattributed_write(),
                }
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

                // An edge is not addressed by a coordinate — it is a
                // relation between two addresses, and the two can live
                // in different cells. Charging it to either one would be
                // an invention, so it is counted as unattributed.
                self.cells.record_unattributed_write();
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
                self.cells.record_unattributed_write();
            }

            // Users keep their own append-only log: they are fully
            // resident by design (see the `users` field), so an index
            // would buy nothing.
            Operation::InsertUser(record) => {
                binary::append_record(
                    &binary::users_path(),
                    &UserOpRecord::Put(record.clone()),
                )?;

                self.user_log_records.fetch_add(1, Ordering::Relaxed);

                self.users_write().insert(record.token_hash.clone(), record.clone());
            }

            Operation::RevokeUser(token_hash) => {
                binary::append_record(
                    &binary::users_path(),
                    &UserOpRecord::Revoke(token_hash.clone()),
                )?;

                self.user_log_records.fetch_add(1, Ordering::Relaxed);

                self.users_write().remove(token_hash);
            }

            // Declaration, tree and contents in one operation, in that
            // order. The definition is what a restart reads, the tree is
            // what writes maintain, and the backfill is what makes the
            // index answer for rows that predate it — an index missing
            // any one of the three is an index that lies.
            Operation::CreateIndex(def) => {
                binary::append_record(
                    &keys::definitions_path(),
                    &IndexOpRecord::Put(def.clone()),
                )?;

                self.indexes.open_data(def.clone())?;

                self.backfill_index(&def.name)?;
            }

            Operation::DropIndex(name) => {
                binary::append_record(
                    &keys::definitions_path(),
                    &IndexOpRecord::Drop(name.clone()),
                )?;

                self.indexes.drop_data(name)?;
            }

            // Log first, then the resident set — the same order as an
            // index, so a crash between them leaves a rule that is
            // durable but not yet enforced, which the next start
            // replays into place. The reverse order would enforce a
            // rule no restart can remember.
            Operation::CreateReference(def) => {
                binary::append_record(
                    &crate::storage::reference::definitions_path(),
                    &ReferenceOpRecord::Put(def.clone()),
                )?;

                self.references.put(def.clone());
            }

            Operation::DropReference(name) => {
                binary::append_record(
                    &crate::storage::reference::definitions_path(),
                    &ReferenceOpRecord::Drop(name.clone()),
                )?;

                self.references.remove(name);
            }

            // Declaration, tree and postings in one operation, in that
            // order, for exactly the reasons `CreateIndex` does it that
            // way — with one extra consequence: a search index that is
            // declared but not backfilled does not merely answer slowly,
            // it answers "no such row" for everything written before it.
            Operation::CreateTextIndex(def) => {
                binary::append_record(
                    &text::definitions_path(),
                    &TextIndexOpRecord::Put(def.clone()),
                )?;

                self.indexes.open_text(def.clone())?;

                self.backfill_text_index(&def.name)?;
            }

            Operation::DropTextIndex(name) => {
                binary::append_record(
                    &text::definitions_path(),
                    &TextIndexOpRecord::Drop(name.clone()),
                )?;

                self.indexes.drop_text(name)?;
            }
        }

        Ok(())
    }

    // ---------------------------------------------------------------------
    // Declared `data`-field indexes
    // ---------------------------------------------------------------------

    /// Write this node's entry into every index declared over its kind.
    ///
    /// Takes `&self` — `BTree::put` does — so it composes with the read
    /// paths a backfill needs. The empty check comes first so a database
    /// with no declared indexes does not decode a node's JSON on every
    /// write to discover there was nothing to index.
    /// Refuse a batch that would break a declared uniqueness rule.
    ///
    /// Runs **before the WAL**, like every other admissibility check, and
    /// for the same reason: a record that becomes durable and is then
    /// refused by the indexes would be replayed into the same refusal on
    /// every subsequent start.
    ///
    /// Two sources of conflict, and both matter:
    ///
    /// * **the index** — some other node already holds the value; and
    /// * **the batch itself** — two inserts in one transaction claim the
    ///   same value. Checking only the index would let those through,
    ///   because neither is committed yet when the other is checked.
    ///
    /// A value freed earlier in the same batch is not a conflict: a batch
    /// that deletes the old holder and inserts a new one is exactly how a
    /// unique value is *moved*, and refusing it would make the constraint
    /// unusable rather than strict.
    fn check_unique(&self, operations: &[Operation]) -> Result<(), String> {
        // Addresses this batch removes, so a value they hold is free.
        let mut released: Vec<&str> = Vec::new();

        for operation in operations {
            match operation {
                Operation::Delete(address) => released.push(address),
                Operation::Insert(node) => released.push(&node.address),
                _ => {}
            }
        }

        // (index name, encoded value) → the address claiming it here.
        let mut claimed: Vec<(String, Vec<u8>, &str)> = Vec::new();

        for operation in operations {
            let Operation::Insert(node) = operation else {
                continue;
            };

            let declared = self.indexes.data_for_kind(&node.kind);
            let data: Option<serde_json::Value> =
                serde_json::from_str(&node.data).ok();

            for index in declared.iter().filter(|i| i.def.unique) {
                let value = data.as_ref().and_then(|d| d.get(&index.def.field));
                let prefix = keys::encode_order_value(value);

                if let Some((_, _, other)) = claimed.iter().find(|(name, key, _)| {
                    *name == index.def.name && *key == prefix
                }) {
                    return Err(format!(
                        "unique index {:?}: this batch gives both {:?} and {:?}                          the same {:?}",
                        index.def.name, other, node.address, index.def.field,
                    ));
                }

                let mut conflict: Option<String> = None;

                index
                    .tree
                    .for_each_range(&prefix, None, false, |key, _| {
                        let holder = keys::address_from_data_key(key)
                            .map(|raw| String::from_utf8_lossy(raw).into_owned());

                        match holder {
                            // The node updating itself, or one this batch
                            // is removing, does not conflict.
                            Some(address)
                                if address == node.address
                                    || released.iter().any(|r| *r == address) =>
                            {
                                Ok(true)
                            }
                            Some(address) => {
                                conflict = Some(address);
                                Ok(false)
                            }
                            None => Ok(true),
                        }
                    })
                    .map_err(io_message)?;

                if let Some(holder) = conflict {
                    return Err(format!(
                        "unique index {:?}: {:?} is already held by {:?}",
                        index.def.name, index.def.field, holder,
                    ));
                }

                claimed.push((index.def.name.clone(), prefix, &node.address));
            }
        }

        Ok(())
    }

    fn assert_data_keys(&self, node: &Node) -> io::Result<()> {
        let declared = self.indexes.data_for_kind(&node.kind);

        if declared.is_empty() {
            return Ok(());
        }

        let data: Option<serde_json::Value> =
            serde_json::from_str(&node.data).ok();

        for index in declared {
            let value = data.as_ref().and_then(|d| d.get(&index.def.field));

            index
                .tree
                .put(&keys::data_key(value, &node.address), &[])?;
        }

        Ok(())
    }

    /// Remove this node's entry from every index declared over its kind.
    fn retract_data_keys(&self, node: &Node) -> io::Result<()> {
        let declared = self.indexes.data_for_kind(&node.kind);

        if declared.is_empty() {
            return Ok(());
        }

        let data: Option<serde_json::Value> =
            serde_json::from_str(&node.data).ok();

        for index in declared {
            let value = data.as_ref().and_then(|d| d.get(&index.def.field));

            index
                .tree
                .remove(&keys::data_key(value, &node.address))?;
        }

        Ok(())
    }

    /// Write this node's postings into every inverted index declared
    /// over its kind.
    ///
    /// One `put` per distinct trigram of the field's text. Idempotent by
    /// key, which is what makes a replayed insert converge instead of
    /// duplicating.
    fn assert_text_keys(&self, node: &Node) -> io::Result<()> {
        let declared = self.indexes.text_for_kind(&node.kind);

        if declared.is_empty() {
            return Ok(());
        }

        let data: Option<serde_json::Value> =
            serde_json::from_str(&node.data).ok();

        for index in declared {
            let Some(value) = text::indexed_text(data.as_ref(), &index.def.field)
            else {
                continue;
            };

            for gram in text::grams(value) {
                index.tree.put(&text::key(&gram, &node.address), &[])?;
            }
        }

        Ok(())
    }

    /// Remove this node's postings from every inverted index declared
    /// over its kind.
    ///
    /// Driven by the node's *own* text, which is why the caller must
    /// hand it the version being replaced rather than the one replacing
    /// it: the postings on disk are the ones the old text produced, and
    /// a retraction computed from the new text would leave every
    /// trigram the edit removed still pointing at the row. That is the
    /// stale posting that resurrects deleted content into a search
    /// result — the one failure mode this whole index has to be designed
    /// against.
    fn retract_text_keys(&self, node: &Node) -> io::Result<()> {
        let declared = self.indexes.text_for_kind(&node.kind);

        if declared.is_empty() {
            return Ok(());
        }

        let data: Option<serde_json::Value> =
            serde_json::from_str(&node.data).ok();

        for index in declared {
            let Some(value) = text::indexed_text(data.as_ref(), &index.def.field)
            else {
                continue;
            };

            for gram in text::grams(value) {
                index.tree.remove(&text::key(&gram, &node.address))?;
            }
        }

        Ok(())
    }

    /// Populate a freshly declared inverted index from the rows that
    /// already exist.
    ///
    /// Reads through the `kind` index, so it costs the kind rather than
    /// the database. Every write is a `put` keyed by `(gram, address)`,
    /// which is what makes running it twice — as recovery does — land on
    /// the keys the interrupted pass already wrote.
    fn backfill_text_index(&self, name: &str) -> io::Result<()> {
        let Some(index) = self.indexes.text_get(name) else {
            return Ok(());
        };

        let kind = index.def.kind.clone();
        let field = index.def.field.clone();

        self.scan_candidates(Some(&kind), None, None, false, |node| {
            let data: Option<serde_json::Value> =
                serde_json::from_str(&node.data).ok();

            if let Some(value) = text::indexed_text(data.as_ref(), &field) {
                for gram in text::grams(value) {
                    index.tree.put(&text::key(&gram, &node.address), &[])?;
                }
            }

            Ok(true)
        })
    }

    /// Populate a freshly declared index from the rows that already
    /// exist.
    ///
    /// Reads through the `kind` index, so it costs the kind rather than
    /// the database, and writes one entry per row. Every write is a
    /// `put` keyed by `(value, address)`, which is what makes running it
    /// twice — as recovery does — converge instead of duplicating.
    fn backfill_index(&self, name: &str) -> io::Result<()> {
        let Some(index) = self.indexes.data_get(name) else {
            return Ok(());
        };

        let kind = index.def.kind.clone();
        let field = index.def.field.clone();

        self.scan_candidates(Some(&kind), None, None, false, |node| {
            let data: Option<serde_json::Value> =
                serde_json::from_str(&node.data).ok();

            let value = data.as_ref().and_then(|d| d.get(&field));

            index.tree.put(&keys::data_key(value, &node.address), &[])?;

            Ok(true)
        })
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
    // One definition of "in order", shared with the byte encoding a
    // declared index is keyed by — see `storage::index`. Two definitions
    // would mean a sorted read and an index scan could disagree about
    // where a row belongs, which is exactly the bug an index is supposed
    // not to have.
    keys::compare_order_values(a.as_ref(), b.as_ref())
}

/// How many values one `count_by` may be asked about at once.
///
/// `values` exists so a caller can ask about the rows a page renders
/// rather than the whole kind, so the bound is set by what a page is: far
/// past any feed, and small enough that the reply is a handful of
/// kilobytes and the work is a handful of index range scans.
const MAX_GROUP_VALUES: usize = 1_000;

/// Find (or start) the running total for one group.
///
/// The single place a group is created, so the cap on distinct values is
/// enforced once rather than at each path that discovers a new one — and
/// so an index run and a scanned row that recover the same value land in
/// the same accumulator instead of two entries the caller cannot merge.
fn group_entry<'a>(
    groups: &'a mut HashMap<Vec<u8>, (Option<serde_json::Value>, Accumulator)>,
    cap: usize,
    func: AggFunc,
    value: Option<&serde_json::Value>,
) -> Result<&'a mut Accumulator, String> {
    use std::collections::hash_map::Entry;

    let key = keys::encode_order_value(value);
    let occupied = groups.len();

    match groups.entry(key) {
        Entry::Occupied(e) => Ok(&mut e.into_mut().1),

        Entry::Vacant(e) => {
            if occupied >= cap {
                return Err(scan_limit_exceeded(
                    "grouping by a field with this many distinct values",
                )
                .to_string());
            }

            Ok(&mut e
                .insert((value.cloned(), Accumulator::new(func)))
                .1)
        }
    }
}

/// One distinct value of a grouped field, and how many nodes carry it.
///
/// `value` keeps the field's own JSON type rather than being stringified
/// into a map key: `17` and `"17"` are different values to every other
/// part of this engine, and a reply that could not tell them apart would
/// make a caller guess which one it had counted. `null` here means the
/// field was absent or explicitly null — the same conflation the
/// ordering makes, for the same reason.
#[derive(Debug, Serialize)]
pub struct GroupCount {
    pub value: Option<serde_json::Value>,
    pub count: u64,
}

/// One distinct value of a grouped field, and the aggregate over the rows
/// carrying it.
///
/// `value` identifies the group exactly as [`GroupCount::value`] does;
/// `result` is the aggregate's own JSON — an integer for a `count` or an
/// integer `sum`, a float for an `avg`, `null` for an aggregate with
/// nothing to fold. Its type is the *field's*, not a number the reply
/// coerced: `min` over a text column answers with text.
#[derive(Debug, Serialize)]
pub struct GroupAggregate {
    pub value: Option<serde_json::Value>,
    pub result: serde_json::Value,
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

    /// How many candidate nodes the plan actually read and tested to
    /// produce this page.
    ///
    /// The one number that distinguishes a plan from its answer. Twenty
    /// rows can come back from an index scan that stopped at the page or
    /// from a read of every node of the kind, and the rows are identical
    /// either way — the difference only shows in what it cost, which is
    /// exactly what a regression can silently change.
    ///
    /// Reported rather than inferred from a stopwatch because a clock
    /// measures the machine as much as the plan: a timing assertion
    /// fails on a loaded CI box and passes on a quiet one, whatever the
    /// query planner is doing. This is the same quantity Postgres
    /// reports as `EXPLAIN ANALYZE`'s "Rows Removed by Filter" plus the
    /// rows returned, and it is a fact about the plan alone.
    ///
    /// Serialized with the page, so it is also the operator's answer to
    /// "why is this query slow" without a second endpoint.
    pub examined: u64,
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

    /// This server's build, from `CARGO_PKG_VERSION` — the same string
    /// `cargo` stamps on the binary, read at compile time rather than
    /// restated as a literal that could drift from `Cargo.toml`.
    ///
    /// Here because a fleet is not homogeneous and a control plane has
    /// to know what it is talking to: a rollout is half-done for a
    /// while, and a decision made on the assumption that every instance
    /// speaks the same dialect is a decision made on a guess. Consumers
    /// previously had to register instances as `"unknown"`, since the
    /// only other place the version appeared was a human-readable
    /// banner nobody should be string-matching.
    pub version: &'static str,

    /// What the process is *doing*: request throughput, latency
    /// percentiles over the most recent observation window, writer-queue
    /// contention, and this process's own CPU and memory use.
    ///
    /// Everything above this line is a census of stored data. None of it
    /// can say whether the server is under pressure — a database holding
    /// ten nodes can be saturated and one holding a billion can be idle.
    /// See [`crate::metrics`] for how each figure is measured and, just
    /// as importantly, which ones are `null` because they could not be
    /// measured honestly.
    pub runtime: RuntimeStats,

    /// The same traffic, attributed to the coordinates it touched.
    ///
    /// Bounded and lock-free, and explicit about what it could not
    /// attribute — see [`crate::metrics::CellTable`].
    pub cells: CellAttribution,
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

#[cfg(test)]
mod data_index_tests {
    //! Operator-declared indexes over a `data` field.
    //!
    //! The property under test throughout is *equivalence*: a query
    //! served by a declared index must return exactly what the same
    //! query returns with no index at all. An index that is merely fast
    //! is worthless — the whole reason to have one is that it answers
    //! the question the scan would have answered, and the only way to
    //! know that is to ask both and compare.
    //!
    //! Like every other module here, these share one data directory with
    //! the rest of the binary's tests, so each test works in its own
    //! `kind` and its own index name.

    use super::*;
    use crate::core::coordinate::Coordinate;
    use crate::core::node::{Node, Visibility};
    use crate::storage::engine::test_support::disk_guard;
    use crate::storage::index::{IndexDef, MAX_INDEX_VALUE_LEN};

    fn reopen(engine: StorageEngine) -> StorageEngine {
        drop(engine);

        let mut recovered = StorageEngine::load().expect("reopen storage engine");

        crate::storage::recovery::recover(&mut recovered).expect("wal recovery");

        recovered
    }

    fn node(kind: &str, address: &str, data: &str) -> Node {
        let mut n = Node::new(
            Coordinate::new(0, 0, 0, 0),
            address.to_string(),
            kind.to_string(),
            "owner".to_string(),
        );

        n.data = data.to_string();
        n.visibility = Visibility::Public;

        n
    }

    /// The declared indexes whose names start with `prefix`.
    ///
    /// Every test module in this binary shares one data directory, so a
    /// bare `list_indexes()` also sees whatever the neighbouring tests
    /// declared. Scoping by name is the listing equivalent of the
    /// per-test `kind` the query tests use.
    fn declared(engine: &StorageEngine, prefix: &str) -> Vec<IndexDef> {
        engine
            .list_indexes()
            .into_iter()
            .filter(|d| d.name.starts_with(prefix))
            .collect()
    }

    fn def(name: &str, kind: &str, field: &str) -> IndexDef {
        IndexDef {
            name: name.to_string(),
            kind: kind.to_string(),
            field: field.to_string(),
            unique: false,
        }
    }

    /// Every address the query returns, paged all the way through.
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

    // -----------------------------------------------------------------
    // Encoding
    // -----------------------------------------------------------------

    /// The one property the whole feature rests on: comparing two values
    /// and comparing their encodings must give the same answer. If they
    /// ever disagree, an index scan and a sort return different orders
    /// for the same query and only one of them can be right.
    #[test]
    fn byte_order_matches_value_order() {
        use serde_json::json;

        let values: Vec<Option<serde_json::Value>> = vec![
            Some(json!(null)),
            Some(json!(false)),
            Some(json!(true)),
            Some(json!(-1e300)),
            Some(json!(-2.5)),
            Some(json!(-1)),
            Some(json!(0)),
            Some(json!(0.5)),
            Some(json!(1)),
            Some(json!(2)),
            Some(json!(10)),
            Some(json!(1e300)),
            Some(json!("")),
            Some(json!("a")),
            Some(json!("aa")),
            Some(json!("b")),
            // A NUL inside a string is exactly what the escape exists
            // for: unescaped, it would read as this value's terminator
            // and the rest of the key would be parsed as an address.
            Some(json!("b\u{0}c")),
            Some(json!("c")),
            Some(json!([1, 2])),
            None,
        ];

        for (i, a) in values.iter().enumerate() {
            for (j, b) in values.iter().enumerate() {
                let by_value = keys::compare_order_values(a.as_ref(), b.as_ref());

                let by_bytes = keys::encode_order_value(a.as_ref())
                    .cmp(&keys::encode_order_value(b.as_ref()));

                assert_eq!(
                    by_value, by_bytes,
                    "values[{i}] vs values[{j}]: comparator says \
                     {by_value:?}, encoding says {by_bytes:?}"
                );

                assert_eq!(
                    by_value,
                    i.cmp(&j),
                    "values[{i}] vs values[{j}] is out of the declared order"
                );
            }
        }
    }

    /// The address has to come back out of the key intact, including
    /// when the indexed value contained the byte the terminator is made
    /// of.
    #[test]
    fn address_survives_the_key_encoding() {
        use serde_json::json;

        for value in [
            Some(json!("plain")),
            Some(json!("has\u{0}nul")),
            Some(json!(42)),
            Some(json!(null)),
            None,
        ] {
            let key = keys::data_key(value.as_ref(), "kind:some-address");

            assert_eq!(
                keys::address_from_data_key(&key),
                Some(&b"kind:some-address"[..]),
                "address lost for {value:?}"
            );
        }
    }

    // -----------------------------------------------------------------
    // Equivalence with the unindexed path
    // -----------------------------------------------------------------

    /// The same rows, in the same order, whether or not an index exists.
    /// Run over deliberately messy data — repeated values, mixed types,
    /// and rows missing the field entirely — because that is where a
    /// sort and an index are most likely to drift apart.
    #[test]
    fn an_index_returns_exactly_what_the_scan_returns() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        let rows = [
            ("dix1:a", r#"{"score": 30}"#),
            ("dix1:b", r#"{"score": 10}"#),
            ("dix1:c", r#"{"score": 20}"#),
            ("dix1:d", r#"{"score": 10}"#),
            ("dix1:e", r#"{"score": -5}"#),
            ("dix1:f", r#"{"other": 1}"#),
            ("dix1:g", r#"{"score": "text"}"#),
            ("dix1:h", r#"{"score": null}"#),
            ("dix1:i", r#"{"score": 2.5}"#),
        ];

        for (address, data) in rows {
            e.insert(node("DixOne", address, data)).expect("insert");
        }

        let unindexed_asc = page_all(&e, "DixOne", Some("score"), false, 2);
        let unindexed_desc = page_all(&e, "DixOne", Some("score"), true, 2);

        e.create_index(def("dix1_score", "DixOne", "score"))
            .expect("create index");

        assert_eq!(
            page_all(&e, "DixOne", Some("score"), false, 2),
            unindexed_asc,
            "ascending order changed when the index appeared"
        );

        assert_eq!(
            page_all(&e, "DixOne", Some("score"), true, 2),
            unindexed_desc,
            "descending order changed when the index appeared"
        );

        // Every row exactly once, whichever path served it.
        assert_eq!(unindexed_asc.len(), rows.len());

        let mut sorted = unindexed_asc.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), rows.len(), "a row was returned twice");
    }

    /// A page size that does not divide the result, walked by cursor,
    /// through the index. The cursor is re-encoded into an index key on
    /// the next request, so this is what proves that round trip lands on
    /// the same row the previous page ended on.
    #[test]
    fn index_paging_covers_every_row_once() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        for i in 0..25 {
            e.insert(node(
                "DixPage",
                &format!("dixp:{i:02}"),
                // Deliberately repeating values, so the address
                // tiebreak is what makes the order total.
                &format!(r#"{{"bucket": {}}}"#, i % 4),
            ))
            .expect("insert");
        }

        e.create_index(def("dixp_bucket", "DixPage", "bucket"))
            .expect("create index");

        for limit in [1usize, 3, 7, 25, 100] {
            let addresses = page_all(&e, "DixPage", Some("bucket"), false, limit);

            assert_eq!(addresses.len(), 25, "limit {limit} lost or repeated rows");

            let mut unique = addresses.clone();
            unique.sort();
            unique.dedup();
            assert_eq!(unique.len(), 25, "limit {limit} returned a row twice");
        }
    }

    // -----------------------------------------------------------------
    // Maintenance
    // -----------------------------------------------------------------

    /// An index is only as good as the writes that maintain it: an
    /// update has to retract the old value's entry, and a delete has to
    /// remove the row entirely. Both are checked by reading through the
    /// index, which is the only place the stale entry would show up.
    #[test]
    fn updates_and_deletes_keep_the_index_true() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        for (address, rank) in [("dix2:a", 1), ("dix2:b", 2), ("dix2:c", 3)] {
            e.insert(node("DixTwo", address, &format!(r#"{{"rank": {rank}}}"#)))
                .expect("insert");
        }

        e.create_index(def("dix2_rank", "DixTwo", "rank"))
            .expect("create index");

        assert_eq!(
            page_all(&e, "DixTwo", Some("rank"), false, 10),
            vec!["dix2:a", "dix2:b", "dix2:c"]
        );

        // Move `a` to the end. If the old entry survived, `a` would come
        // back twice.
        e.insert(node("DixTwo", "dix2:a", r#"{"rank": 9}"#))
            .expect("update");

        assert_eq!(
            page_all(&e, "DixTwo", Some("rank"), false, 10),
            vec!["dix2:b", "dix2:c", "dix2:a"]
        );

        e.delete("dix2:c").expect("delete");

        assert_eq!(
            page_all(&e, "DixTwo", Some("rank"), false, 10),
            vec!["dix2:b", "dix2:a"]
        );
    }

    /// A node inserted after the index was declared has to appear in it
    /// without anyone rebuilding anything.
    #[test]
    fn later_writes_land_in_an_existing_index() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        e.insert(node("DixThree", "dix3:b", r#"{"n": 2}"#))
            .expect("insert");

        e.create_index(def("dix3_n", "DixThree", "n"))
            .expect("create index");

        e.insert(node("DixThree", "dix3:a", r#"{"n": 1}"#))
            .expect("insert after");

        e.insert(node("DixThree", "dix3:c", r#"{"n": 3}"#))
            .expect("insert after");

        assert_eq!(
            page_all(&e, "DixThree", Some("n"), false, 10),
            vec!["dix3:a", "dix3:b", "dix3:c"]
        );
    }

    // -----------------------------------------------------------------
    // Durability
    // -----------------------------------------------------------------

    /// The definition and its contents both have to survive a restart —
    /// a definition without a populated tree is an index that silently
    /// returns nothing, which is worse than not having one.
    #[test]
    fn a_declared_index_survives_a_restart() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        for (address, n) in [("dix4:a", 3), ("dix4:b", 1), ("dix4:c", 2)] {
            e.insert(node("DixFour", address, &format!(r#"{{"n": {n}}}"#)))
                .expect("insert");
        }

        e.create_index(def("dix4_n", "DixFour", "n"))
            .expect("create index");

        let mut e = reopen(e);

        assert_eq!(
            declared(&e, "dix4_"),
            vec![def("dix4_n", "DixFour", "n")],
            "the definition did not survive"
        );

        assert_eq!(
            page_all(&e, "DixFour", Some("n"), false, 10),
            vec!["dix4:b", "dix4:c", "dix4:a"],
            "the index contents did not survive"
        );

        // And it is still being maintained on the far side of the
        // restart, which is the part a replayed definition could get
        // wrong by opening the tree but not registering it.
        e.insert(node("DixFour", "dix4:d", r#"{"n": 0}"#))
            .expect("insert after restart");

        assert_eq!(
            page_all(&e, "DixFour", Some("n"), false, 10),
            vec!["dix4:d", "dix4:b", "dix4:c", "dix4:a"]
        );
    }

    /// A drop has to survive too, and has to leave the query answering
    /// correctly through the sort path rather than failing.
    #[test]
    fn a_dropped_index_stays_dropped_and_queries_still_work() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        for (address, n) in [("dix5:a", 2), ("dix5:b", 1)] {
            e.insert(node("DixFive", address, &format!(r#"{{"n": {n}}}"#)))
                .expect("insert");
        }

        e.create_index(def("dix5_n", "DixFive", "n"))
            .expect("create index");

        e.drop_index("dix5_n").expect("drop index");

        assert!(declared(&e, "dix5_").is_empty(), "still declared after drop");

        let e = reopen(e);

        assert!(declared(&e, "dix5_").is_empty(), "the drop did not survive");

        assert_eq!(
            page_all(&e, "DixFive", Some("n"), false, 10),
            vec!["dix5:b", "dix5:a"],
            "the query stopped working without its index"
        );
    }

    // -----------------------------------------------------------------
    // Declaration rules
    // -----------------------------------------------------------------

    #[test]
    fn redeclaring_the_same_index_is_a_no_op_but_conflicts_are_errors() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        e.create_index(def("dix6_n", "DixSix", "n"))
            .expect("create index");

        e.create_index(def("dix6_n", "DixSix", "n"))
            .expect("re-declaring the identical index must converge");

        let same_name = e.create_index(def("dix6_n", "DixSix", "other"));
        assert!(same_name.is_err(), "a redefinition under a live name");

        let same_field = e.create_index(def("dix6_again", "DixSix", "n"));
        assert!(same_field.is_err(), "a second index over the same field");

        assert_eq!(declared(&e, "dix6_").len(), 1);
    }

    #[test]
    fn a_name_that_is_not_a_name_is_refused() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        // A path, not a name: this is the one that would escape the data
        // directory if names were interpolated into filenames unchecked.
        assert!(e.create_index(def("../escape", "DixSeven", "n")).is_err());

        assert!(e.create_index(def("", "DixSeven", "n")).is_err());
        assert!(e.create_index(def("ok", "", "n")).is_err());
        assert!(e.create_index(def("ok", "DixSeven", "")).is_err());

        assert!(e.drop_index("never-declared").is_err());
    }

    /// A value too large for an index key must be refused *before* the
    /// create is logged — both when the oversized row is already there
    /// and when it arrives later. Either way the refusal is an ordinary
    /// error, never a mutation the WAL has already accepted.
    #[test]
    fn an_unindexable_value_is_refused_not_committed() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        let huge = "x".repeat(MAX_INDEX_VALUE_LEN + 64);

        e.insert(node(
            "DixEight",
            "dix8:big",
            &serde_json::json!({ "blob": huge }).to_string(),
        ))
        .expect("an unindexed oversized field is ordinary data");

        let refused = e.create_index(def("dix8_blob", "DixEight", "blob"));

        assert!(refused.is_err(), "created an index it cannot maintain");
        assert!(declared(&e, "dix8_").is_empty(), "a refused index was declared");

        // The reverse order: index a field that is small today, then try
        // to write a row whose value is too large for it.
        e.insert(node("DixNine", "dix9:a", r#"{"blob": "small"}"#))
            .expect("insert");

        e.create_index(def("dix9_blob", "DixNine", "blob"))
            .expect("create index");

        let refused = e.insert(node(
            "DixNine",
            "dix9:b",
            &serde_json::json!({ "blob": huge }).to_string(),
        ));

        assert!(refused.is_err(), "wrote a row the index cannot hold");

        assert_eq!(
            page_all(&e, "DixNine", Some("blob"), false, 10),
            vec!["dix9:a"],
            "the refused write left something behind"
        );
    }

    /// An index covers one kind. A node of another kind that happens to
    /// carry the same field name must not appear in it — `data` has no
    /// schema across kinds, so two same-named fields are unrelated.
    #[test]
    fn an_index_covers_only_its_own_kind() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        e.insert(node("DixTenA", "dix10:a", r#"{"n": 1}"#))
            .expect("insert");

        e.insert(node("DixTenB", "dix10:b", r#"{"n": 2}"#))
            .expect("insert");

        e.create_index(def("dix10_n", "DixTenA", "n"))
            .expect("create index");

        assert_eq!(
            page_all(&e, "DixTenA", Some("n"), false, 10),
            vec!["dix10:a"]
        );

        assert_eq!(
            page_all(&e, "DixTenB", Some("n"), false, 10),
            vec!["dix10:b"],
            "the other kind stopped answering"
        );
    }

    /// The index narrows the access path; it does not widen who may
    /// read. A private row belonging to someone else must not become
    /// visible just because an ordered read now walks an index.
    #[test]
    fn an_index_does_not_leak_a_private_row() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        let mut public = node("DixEleven", "dix11:pub", r#"{"n": 1}"#);
        public.owner = "alice".to_string();

        let mut private = node("DixEleven", "dix11:priv", r#"{"n": 2}"#);
        private.owner = "alice".to_string();
        private.visibility = Visibility::Private;

        e.insert(public).expect("insert");
        e.insert(private).expect("insert");

        e.create_index(def("dix11_n", "DixEleven", "n"))
            .expect("create index");

        let page = e
            .query_where(
                Some("DixEleven"),
                None,
                Some("bob"),
                None,
                "item",
                Some("n"),
                false,
                None,
                10,
                0,
            )
            .expect("query_where ok");

        assert_eq!(
            page.nodes.iter().map(|n| n.address.as_str()).collect::<Vec<_>>(),
            vec!["dix11:pub"],
            "an index scan bypassed visibility"
        );
    }

    // -----------------------------------------------------------------
    // Equality pushdown: the index as a filter, not only an ordering
    // -----------------------------------------------------------------

    fn eq(field: &str, value: serde_json::Value) -> Expr {
        serde_json::from_value(serde_json::json!({
            "kind": "bin",
            "op": "==",
            "l": {
                "kind": "get",
                "field": field,
                "obj": { "kind": "ref", "name": "item" }
            },
            "r": { "kind": "lit", "val": value }
        }))
        .expect("valid predicate")
    }

    fn bin(op: &str, l: Expr, r: Expr) -> Expr {
        Expr {
            kind: "bin".to_string(),
            op: Some(op.to_string()),
            l: Some(Box::new(l)),
            r: Some(Box::new(r)),
            val: None,
            vtype: None,
            name: None,
            field: None,
            obj: None,
            key: None,
            args: None,
            x: None,
            var: None,
            where_: None,
        }
    }

    /// Run one query both ways — with the index declared and with it
    /// dropped — and require identical answers. The index is a plan
    /// choice; a plan choice that changes the result is a bug.
    fn same_with_and_without_index(
        engine: &mut StorageEngine,
        kind: &str,
        index: IndexDef,
        predicate: &Expr,
        order: Option<&str>,
        desc: bool,
        limit: usize,
    ) -> Vec<String> {
        let name = index.name.clone();

        let unindexed = page_filtered(engine, kind, predicate, order, desc, limit);

        engine.create_index(index).expect("create index");

        let indexed = page_filtered(engine, kind, predicate, order, desc, limit);

        engine.drop_index(&name).expect("drop index");

        assert_eq!(
            indexed, unindexed,
            "the index changed the answer for order={order:?} desc={desc} \
             limit={limit}"
        );

        unindexed
    }

    /// Page a predicated query all the way through, collecting
    /// addresses.
    fn page_filtered(
        engine: &StorageEngine,
        kind: &str,
        predicate: &Expr,
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
                    Some(predicate),
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

    /// The headline case: `where status == "open"` over an index on
    /// `status`. Same rows, same order, every page size, both
    /// directions, ordered and unordered.
    #[test]
    fn an_equality_predicate_is_served_by_a_prefix_of_the_index() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        for i in 0..20 {
            let status = if i % 3 == 0 { "open" } else { "closed" };

            e.insert(node(
                "DixEq",
                &format!("dixeq:{i:02}"),
                &format!(r#"{{"status": "{status}", "n": {i}}}"#),
            ))
            .expect("insert");
        }

        let predicate = eq("status", serde_json::json!("open"));

        for (order, desc, limit) in [
            (None, false, 2usize),
            (None, true, 3),
            (Some("status"), false, 1),
            (Some("status"), true, 4),
            (None, false, 100),
        ] {
            let got = same_with_and_without_index(
                &mut e,
                "DixEq",
                def("dixeq_status", "DixEq", "status"),
                &predicate,
                order,
                desc,
                limit,
            );

            assert_eq!(got.len(), 7, "wrong number of matches");
        }
    }

    /// An ordering on a *different* indexed field, filtered by equality
    /// on this one. The prefix cannot serve it — every row in the prefix
    /// shares the pinned value but varies in the ordered one — so the
    /// planner must not take it, and the answer must still be right.
    #[test]
    fn an_ordering_on_another_field_is_not_served_by_the_prefix() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        for (address, group, n) in [
            ("dixord:a", "x", 3),
            ("dixord:b", "x", 1),
            ("dixord:c", "y", 2),
            ("dixord:d", "x", 2),
        ] {
            e.insert(node(
                "DixOrd",
                address,
                &format!(r#"{{"group": "{group}", "n": {n}}}"#),
            ))
            .expect("insert");
        }

        let predicate = eq("group", serde_json::json!("x"));

        let got = same_with_and_without_index(
            &mut e,
            "DixOrd",
            def("dixord_group", "DixOrd", "group"),
            &predicate,
            Some("n"),
            false,
            2,
        );

        assert_eq!(got, vec!["dixord:b", "dixord:d", "dixord:a"]);
    }

    /// `a == 1 || b == 2` is not a requirement on either field: a row
    /// can satisfy one branch and not the other. Pushing either side
    /// into a prefix would drop the rows that matched only the other,
    /// so the analysis must refuse to descend through `||`.
    #[test]
    fn a_disjunction_is_not_pushed_into_a_prefix() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        for (address, tag, n) in [
            ("dixor:a", "keep", 1),
            ("dixor:b", "drop", 9),
            ("dixor:c", "drop", 1),
        ] {
            e.insert(node(
                "DixOr",
                address,
                &format!(r#"{{"tag": "{tag}", "n": {n}}}"#),
            ))
            .expect("insert");
        }

        // tag == "keep" || n == 9  →  a and b, never c.
        let predicate = bin(
            "||",
            eq("tag", serde_json::json!("keep")),
            eq("n", serde_json::json!(9)),
        );

        let got = same_with_and_without_index(
            &mut e,
            "DixOr",
            def("dixor_tag", "DixOr", "tag"),
            &predicate,
            None,
            false,
            10,
        );

        assert_eq!(got, vec!["dixor:a", "dixor:b"]);
    }

    /// A conjunct *is* a requirement, so `tag == "keep" && n == 1`
    /// may be served by a prefix on either field — and must give the
    /// same answer whichever one the planner picks.
    #[test]
    fn a_conjunction_is_pushed_and_still_applies_the_rest() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        for (address, tag, n) in [
            ("dixand:a", "keep", 1),
            ("dixand:b", "keep", 2),
            ("dixand:c", "other", 1),
        ] {
            e.insert(node(
                "DixAnd",
                address,
                &format!(r#"{{"tag": "{tag}", "n": {n}}}"#),
            ))
            .expect("insert");
        }

        let predicate = bin(
            "&&",
            eq("tag", serde_json::json!("keep")),
            eq("n", serde_json::json!(1)),
        );

        for index in [
            def("dixand_tag", "DixAnd", "tag"),
            def("dixand_n", "DixAnd", "n"),
        ] {
            let got = same_with_and_without_index(
                &mut e,
                "DixAnd",
                index,
                &predicate,
                None,
                false,
                10,
            );

            assert_eq!(got, vec!["dixand:a"]);
        }
    }

    /// `item.x == null` matches a row whose `x` is null *and* a row with
    /// no `x` at all, because a field access yields null for both. The
    /// index keys them apart, so no single prefix covers the predicate
    /// and it must stay on the scan path.
    #[test]
    fn a_null_comparison_is_not_pushed_into_a_prefix() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        e.insert(node("DixNull", "dixnull:explicit", r#"{"x": null}"#))
            .expect("insert");

        e.insert(node("DixNull", "dixnull:absent", r#"{"y": 1}"#))
            .expect("insert");

        e.insert(node("DixNull", "dixnull:present", r#"{"x": 1}"#))
            .expect("insert");

        let predicate = eq("x", serde_json::json!(null));

        let got = same_with_and_without_index(
            &mut e,
            "DixNull",
            def("dixnull_x", "DixNull", "x"),
            &predicate,
            None,
            false,
            10,
        );

        assert_eq!(got, vec!["dixnull:absent", "dixnull:explicit"]);
    }

    /// An integer literal and the same value stored as a float are the
    /// same value to `==`, and must land in the same prefix — otherwise
    /// the indexed path would answer a question the scan path does not.
    #[test]
    fn numeric_equality_ignores_the_written_representation() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        e.insert(node("DixNum", "dixnum:int", r#"{"n": 7}"#))
            .expect("insert");

        e.insert(node("DixNum", "dixnum:float", r#"{"n": 7.0}"#))
            .expect("insert");

        e.insert(node("DixNum", "dixnum:other", r#"{"n": 8}"#))
            .expect("insert");

        let got = same_with_and_without_index(
            &mut e,
            "DixNum",
            def("dixnum_n", "DixNum", "n"),
            &eq("n", serde_json::json!(7)),
            None,
            false,
            10,
        );

        assert_eq!(got, vec!["dixnum:float", "dixnum:int"]);
    }
}

#[cfg(test)]
mod text_index_tests {
    //! The inverted index over a `data` field's text.
    //!
    //! One property is under test throughout, and it is the same one the
    //! ordered indexes are held to: **equivalence**. A `contains` served
    //! by postings must return exactly the rows the row-by-row test
    //! returns — not a subset, which would be a silently short answer,
    //! and not a superset, which would be a wrong one. Everything else
    //! here (the maintenance cases, the restart) exists because a
    //! derived structure that drifts breaks that property in a way no
    //! amount of speed makes up for.
    //!
    //! Like every other module here, these share one data directory with
    //! the rest of the binary's tests, so each test works in its own
    //! `kind` and its own index name.

    use super::*;
    use crate::core::coordinate::Coordinate;
    use crate::core::node::{Node, Visibility};
    use crate::storage::engine::test_support::disk_guard;
    use crate::storage::text::TextIndexDef;

    fn reopen(engine: StorageEngine) -> StorageEngine {
        drop(engine);

        let mut recovered = StorageEngine::load().expect("reopen storage engine");

        crate::storage::recovery::recover(&mut recovered).expect("wal recovery");

        recovered
    }

    fn node(kind: &str, address: &str, body: &str) -> Node {
        let mut n = Node::new(
            Coordinate::new(0, 0, 0, 0),
            address.to_string(),
            kind.to_string(),
            "owner".to_string(),
        );

        n.data = serde_json::json!({ "body": body }).to_string();
        n.visibility = Visibility::Public;

        n
    }

    fn def(name: &str, kind: &str) -> TextIndexDef {
        TextIndexDef {
            name: name.to_string(),
            kind: kind.to_string(),
            field: "body".to_string(),
        }
    }

    fn substring(op: &str, literal: &str) -> Expr {
        serde_json::from_value(serde_json::json!({
            "kind": "bin",
            "op": op,
            "l": {
                "kind": "get",
                "field": "body",
                "obj": { "kind": "ref", "name": "item" },
            },
            "r": { "kind": "lit", "val": literal },
        }))
        .expect("build predicate")
    }

    /// Every address the query returns, paged all the way through, plus
    /// how many candidates the plan read to produce them.
    fn search(engine: &StorageEngine, kind: &str, op: &str, literal: &str)
        -> (Vec<String>, u64)
    {
        let predicate = substring(op, literal);

        let mut addresses = Vec::new();
        let mut examined = 0u64;
        let mut after: Option<String> = None;

        loop {
            let page = engine
                .query_where(
                    Some(kind), None, None, Some(&predicate), "item",
                    None, false, after.as_deref(), 3, 0,
                )
                .expect("query");

            examined += page.examined;
            addresses.extend(page.nodes.iter().map(|n| n.address.clone()));

            if page.next.is_empty() {
                break;
            }

            after = Some(page.next);
        }

        (addresses, examined)
    }

    /// The answer computed the only way that cannot be wrong: read every
    /// node of the kind and run Rust's own `str` test on it.
    fn by_brute_force(
        engine: &StorageEngine,
        kind: &str,
        op: &str,
        literal: &str,
    ) -> Vec<String> {
        let mut addresses: Vec<String> = engine
            .query(Some(kind), None, None, 10_000, 0)
            .expect("list")
            .into_iter()
            .filter(|n| {
                let data: serde_json::Value =
                    serde_json::from_str(&n.data).expect("json");

                let Some(body) = data.get("body").and_then(|v| v.as_str()) else {
                    return false;
                };

                match op {
                    "contains" => body.contains(literal),
                    "starts_with" => body.starts_with(literal),
                    "ends_with" => body.ends_with(literal),
                    other => panic!("unknown op {other}"),
                }
            })
            .map(|n| n.address)
            .collect();

        addresses.sort();
        addresses
    }

    /// A corpus chosen to break a lazy implementation:
    ///
    /// * `"hello"` holds `"ell"` *inside* a word, which a token index
    ///   would miss;
    /// * `"abcxbcd"` holds the trigrams `abc` and `bcd` but not the
    ///   substring `"abcd"` — a false candidate the intersection cannot
    ///   remove and the recheck must;
    /// * `"HELLO"` differs from `"hello"` only in case, which the
    ///   folded postings cannot tell apart and the recheck must;
    /// * one node whose `body` is a number rather than a string, which
    ///   has no postings and matches no substring test.
    fn seed(engine: &StorageEngine, kind: &str) {
        for (address, body) in [
            ("a", "hello world"),
            ("b", "HELLO WORLD"),
            ("c", "abcxbcd"),
            ("d", "abcd"),
            ("e", "the quick brown fox"),
            ("f", "well hello there"),
        ] {
            engine
                .insert(node(kind, &format!("{kind}:{address}"), body))
                .expect("insert");
        }

        let mut numeric = node(kind, &format!("{kind}:g"), "");
        numeric.data = r#"{"body": 42}"#.to_string();
        engine.insert(numeric).expect("insert");
    }

    /// The whole claim, checked against the only oracle there is.
    #[test]
    fn the_index_returns_exactly_what_the_row_by_row_test_returns() {
        let _g = disk_guard();
        let e = StorageEngine::open().expect("open storage engine");

        seed(&e, "TixEq");

        let probes = [
            ("contains", "ell"),      // inside a word
            ("contains", "abcd"),     // the scattered-trigram false candidate
            ("contains", "hello"),    // case-sensitive against "HELLO"
            ("contains", "HELLO"),
            ("contains", "quick brown"), // a phrase, spaces and all
            ("contains", "zzz"),      // nothing at all
            ("contains", "el"),       // shorter than one window: the scan
            ("starts_with", "hello"),
            ("ends_with", "world"),
        ];

        // Before the index: this is the scan, and it is the answer every
        // later comparison is made against.
        for (op, literal) in probes {
            assert_eq!(
                sorted(search(&e, "TixEq", op, literal).0),
                by_brute_force(&e, "TixEq", op, literal),
                "the scan itself disagrees with `str::{op}` for {literal:?}",
            );
        }

        e.create_text_index(def("tix_eq", "TixEq"))
            .expect("declare text index");

        for (op, literal) in probes {
            assert_eq!(
                sorted(search(&e, "TixEq", op, literal).0),
                by_brute_force(&e, "TixEq", op, literal),
                "the index and the scan disagree on `{op}` {literal:?}",
            );
        }
    }

    fn sorted(mut addresses: Vec<String>) -> Vec<String> {
        addresses.sort();
        addresses
    }

    /// What the index is *for*: the plan stops reading the kind and
    /// starts reading the matches. `examined` is how an operator sees
    /// that, so it has to move.
    #[test]
    fn the_index_reads_the_matches_and_not_the_kind() {
        let _g = disk_guard();
        let e = StorageEngine::open().expect("open storage engine");

        // One needle in a thousand rows of hay. The hay deliberately
        // shares a prefix with the needle, so a rarer trigram than the
        // first one has to be the seed for this to pay off.
        for n in 0..1_000 {
            e.insert(node(
                "TixCost",
                &format!("TixCost:{n:04}"),
                &format!("post number {n} about ordinary things"),
            ))
            .expect("insert");
        }

        e.insert(node(
            "TixCost",
            "TixCost:needle",
            "post number 1000 about xylophones",
        ))
        .expect("insert");

        let (before, scanned) = search(&e, "TixCost", "contains", "xylophone");

        assert_eq!(before, vec!["TixCost:needle".to_string()]);
        assert_eq!(
            scanned, 1_001,
            "without an index every node of the kind is read",
        );

        e.create_text_index(def("tix_cost", "TixCost"))
            .expect("declare text index");

        let (after, read) = search(&e, "TixCost", "contains", "xylophone");

        assert_eq!(after, before, "the index changed the cost, not the answer");
        assert_eq!(
            read, 1,
            "the postings should have narrowed this to the one row that \
             matches; read {read} of 1001",
        );
    }

    /// An update has to retract the *old* text's postings. Leaving them
    /// is the failure mode this index is designed against: the row keeps
    /// answering a search for words it no longer contains.
    #[test]
    fn an_update_retracts_the_postings_of_the_text_it_replaced() {
        let _g = disk_guard();
        let e = StorageEngine::open().expect("open storage engine");

        e.insert(node("TixUpd", "TixUpd:1", "alpha centauri"))
            .expect("insert");

        e.create_text_index(def("tix_upd", "TixUpd"))
            .expect("declare text index");

        assert_eq!(
            search(&e, "TixUpd", "contains", "alpha").0,
            vec!["TixUpd:1".to_string()],
        );

        e.insert(node("TixUpd", "TixUpd:1", "bravo cluster"))
            .expect("overwrite");

        assert!(
            search(&e, "TixUpd", "contains", "alpha").0.is_empty(),
            "the replaced text is still answering searches",
        );

        assert_eq!(
            search(&e, "TixUpd", "contains", "bravo").0,
            vec!["TixUpd:1".to_string()],
        );

        // And the postings the two texts share — "a c" appears in both —
        // must survive the retract-then-assert, not be removed by it.
        assert_eq!(
            search(&e, "TixUpd", "contains", "o cl").0,
            vec!["TixUpd:1".to_string()],
        );
    }

    /// Every path that removes a row has to remove its postings: the
    /// single delete, the predicated bulk delete, and the whole-kind
    /// clear. A posting that outlives its row is a deleted node
    /// resurrected into a search result.
    #[test]
    fn no_delete_path_leaves_a_posting_behind() {
        let _g = disk_guard();
        let e = StorageEngine::open().expect("open storage engine");

        for (address, body) in [
            ("TixDel:1", "singular removal"),
            ("TixDel:2", "predicated removal"),
            ("TixDel:3", "wholesale removal"),
        ] {
            e.insert(node("TixDel", address, body)).expect("insert");
        }

        e.create_text_index(def("tix_del", "TixDel"))
            .expect("declare text index");

        assert_eq!(search(&e, "TixDel", "contains", "removal").0.len(), 3);

        e.delete("TixDel:1").expect("single delete");

        e.execute_transaction(vec![TxOperation::DeleteWhere {
            kind: "TixDel".to_string(),
            where_: Some(substring("contains", "predicated")),
            owner: "owner".to_string(),
            is_admin: true,
        }])
        .expect("delete_where commits");

        assert_eq!(
            search(&e, "TixDel", "contains", "removal").0,
            vec!["TixDel:3".to_string()],
            "a deleted row is still reachable through its postings",
        );

        e.execute_transaction(vec![TxOperation::ClearKind {
            kind: "TixDel".to_string(),
            owner: "owner".to_string(),
            is_admin: true,
        }])
        .expect("clear_kind commits");

        assert!(
            search(&e, "TixDel", "contains", "removal").0.is_empty(),
            "clearing the kind left its postings behind",
        );

        // A row written after the kind was cleared must be found, which
        // it cannot be if the clear also took the tree with it.
        e.insert(node("TixDel", "TixDel:4", "later removal"))
            .expect("insert after clear");

        assert_eq!(
            search(&e, "TixDel", "contains", "removal").0,
            vec!["TixDel:4".to_string()],
        );
    }

    /// The definition and the postings both have to survive a restart —
    /// a definition whose tree did not come back is a search that
    /// silently returns nothing, which is worse than having no index.
    #[test]
    fn a_text_index_survives_a_restart() {
        let _g = disk_guard();
        let e = StorageEngine::open().expect("open storage engine");

        e.insert(node("TixRec", "TixRec:1", "durable haystack"))
            .expect("insert");

        e.create_text_index(def("tix_rec", "TixRec"))
            .expect("declare text index");

        let e = reopen(e);

        assert!(
            e.list_text_indexes().iter().any(|d| d.name == "tix_rec"),
            "the definition did not survive",
        );

        assert_eq!(
            search(&e, "TixRec", "contains", "haystack").0,
            vec!["TixRec:1".to_string()],
            "the postings did not survive",
        );

        // Still maintained on the far side of the restart — the part a
        // replayed definition gets wrong by opening the tree without
        // registering it.
        e.insert(node("TixRec", "TixRec:2", "later haystack"))
            .expect("insert after restart");

        assert_eq!(
            sorted(search(&e, "TixRec", "contains", "haystack").0),
            vec!["TixRec:1".to_string(), "TixRec:2".to_string()],
        );

        // And a delete on the far side retracts through the reopened
        // tree rather than a fresh empty one.
        e.delete("TixRec:1").expect("delete");

        assert_eq!(
            search(&e, "TixRec", "contains", "haystack").0,
            vec!["TixRec:2".to_string()],
        );
    }

    /// A count and its query are answered through the same access path,
    /// so they cannot disagree — including when that path is a posting
    /// intersection.
    #[test]
    fn a_count_agrees_with_the_query_it_counts() {
        let _g = disk_guard();
        let e = StorageEngine::open().expect("open storage engine");

        seed(&e, "TixCount");

        e.create_text_index(def("tix_count", "TixCount"))
            .expect("declare text index");

        for literal in ["ell", "abcd", "hello", "zzz"] {
            let predicate = substring("contains", literal);

            assert_eq!(
                e.count_where(Some("TixCount"), None, None, Some(&predicate), "item")
                    .expect("count") as usize,
                by_brute_force(&e, "TixCount", "contains", literal).len(),
                "count and scan disagree on {literal:?}",
            );
        }
    }

    /// Ordering by another field still selects through the postings and
    /// still returns the sorted answer the scan would have.
    #[test]
    fn an_ordering_on_another_field_still_selects_through_the_index() {
        let _g = disk_guard();
        let e = StorageEngine::open().expect("open storage engine");

        for (address, rank, body) in [
            ("TixOrd:a", 3, "findable alpha"),
            ("TixOrd:b", 1, "findable bravo"),
            ("TixOrd:c", 2, "findable charlie"),
            ("TixOrd:d", 0, "missing delta"),
        ] {
            let mut n = node("TixOrd", address, body);
            n.data = serde_json::json!({ "body": body, "rank": rank }).to_string();
            e.insert(n).expect("insert");
        }

        e.create_text_index(def("tix_ord", "TixOrd"))
            .expect("declare text index");

        let predicate = substring("contains", "findable");

        let page = e
            .query_where(
                Some("TixOrd"), None, None, Some(&predicate), "item",
                Some("rank"), false, None, 10, 0,
            )
            .expect("ordered query");

        assert_eq!(
            page.nodes.iter().map(|n| n.address.clone()).collect::<Vec<_>>(),
            vec![
                "TixOrd:b".to_string(),
                "TixOrd:c".to_string(),
                "TixOrd:a".to_string(),
            ],
            "ordered by rank, selected through the postings",
        );

        assert_eq!(
            page.examined, 3,
            "the sorted plan should have read the three candidates, not the \
             kind; read {}",
            page.examined,
        );
    }

    /// Both index kinds live under one name space and one drop, because
    /// `DELETE /admin/indexes/:name` names exactly one index.
    #[test]
    fn one_name_names_one_index_and_a_drop_finds_either() {
        let _g = disk_guard();
        let e = StorageEngine::open().expect("open storage engine");

        e.create_text_index(def("tix_name", "TixName"))
            .expect("declare text index");

        let clash = e.create_index(IndexDef {
            name: "tix_name".to_string(),
            kind: "TixName".to_string(),
            field: "other".to_string(),
            unique: false,
        });

        assert!(
            clash.is_err_and(|e| e.contains("already exists")),
            "an ordered index must not be able to take a text index's name",
        );

        // The same field may carry both kinds, under two names: they
        // answer different questions and neither subsumes the other.
        e.create_index(IndexDef {
            name: "tix_name_ordered".to_string(),
            kind: "TixName".to_string(),
            field: "body".to_string(),
            unique: false,
        })
        .expect("an ordered index over the same field is not a conflict");

        e.drop_index("tix_name").expect("one drop finds either kind");

        assert!(
            !e.list_text_indexes().iter().any(|d| d.name == "tix_name"),
            "the text index is still declared after being dropped",
        );

        // Dropped means the queries it served fall back, not break.
        e.insert(node("TixName", "TixName:1", "still searchable"))
            .expect("insert");

        assert_eq!(
            search(&e, "TixName", "contains", "searchable").0,
            vec!["TixName:1".to_string()],
        );
    }
}

#[cfg(test)]
mod count_tests {
    //! `count_where`, and the one property that makes it trustworthy: it
    //! must equal the number of rows the same selection actually
    //! returns. A count is a summary of a query, so a count that
    //! disagrees with its query is worse than no count at all — it is a
    //! number a caller will act on without being able to see it is wrong.
    //!
    //! It has three access paths (index keys only, an equality prefix, a
    //! candidate scan), and the tests below drive each of them and then
    //! compare against paging the query to exhaustion.

    use super::*;
    use crate::core::coordinate::Coordinate;
    use crate::core::node::{Node, Visibility};
    use crate::storage::engine::test_support::disk_guard;
    use crate::storage::index::IndexDef;

    fn node(kind: &str, address: &str, owner: &str, data: &str, public: bool) -> Node {
        let mut n = Node::new(
            Coordinate::new(0, 0, 0, 0),
            address.to_string(),
            kind.to_string(),
            owner.to_string(),
        );

        n.data = data.to_string();

        if public {
            n.visibility = Visibility::Public;
        }

        n
    }

    fn author_is(who: &str) -> Expr {
        serde_json::from_value(serde_json::json!({
            "kind": "bin",
            "op": "==",
            "l": {
                "kind": "get",
                "field": "author",
                "obj": { "kind": "ref", "name": "item" }
            },
            "r": { "kind": "lit", "val": who, "vtype": "text" }
        }))
        .expect("valid predicate")
    }

    /// Page the equivalent query to exhaustion and count what comes back.
    /// This is the number `count_where` has to agree with.
    fn rows_returned(
        engine: &StorageEngine,
        kind: Option<&str>,
        owner: Option<&str>,
        requester: Option<&str>,
        predicate: Option<&Expr>,
    ) -> u64 {
        let mut total = 0u64;
        let mut after: Option<String> = None;

        for _ in 0..10_000 {
            let page = engine
                .query_where(
                    kind, owner, requester, predicate, "item", None, false,
                    after.as_deref(), 7, 0,
                )
                .expect("query_where ok");

            total += page.nodes.len() as u64;

            if page.next.is_empty() {
                return total;
            }

            after = Some(page.next);
        }

        panic!("paging did not terminate");
    }

    fn seed(engine: &mut StorageEngine, kind: &str, tag: &str) {
        for i in 0..40 {
            let author = if i % 4 == 0 { "alice" } else { "bob" };

            // A third of the rows are private and owned by someone else,
            // so the visibility filter has something to do.
            let (owner, public) = if i % 3 == 0 {
                ("carol", false)
            } else {
                ("alice", true)
            };

            engine
                .insert(node(
                    kind,
                    &format!("{tag}:{i:02}"),
                    owner,
                    &format!(r#"{{"author": "{author}", "n": {i}}}"#),
                    public,
                ))
                .expect("insert");
        }
    }

    /// Path 1: no predicate and no visibility filtering, so nothing needs
    /// to be read — the answer is how many entries the kind index holds.
    #[test]
    fn counting_a_kind_reads_no_records() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        seed(&mut e, "CountKind", "ck");

        assert_eq!(
            e.count_where(Some("CountKind"), None, None, None, "item")
                .expect("count ok"),
            40
        );

        assert_eq!(
            e.count_where(Some("CountKind"), None, None, None, "item")
                .expect("count ok"),
            rows_returned(&e, Some("CountKind"), None, None, None),
            "the fast path disagrees with the query it summarises"
        );
    }

    /// Path 3: the general scan, with a predicate the index cannot serve.
    #[test]
    fn counting_with_a_predicate_matches_the_query() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        seed(&mut e, "CountScan", "cs");

        let predicate = author_is("alice");

        assert_eq!(
            e.count_where(Some("CountScan"), None, None, Some(&predicate), "item")
                .expect("count ok"),
            rows_returned(&e, Some("CountScan"), None, None, Some(&predicate))
        );
    }

    /// Path 2: an index over the pinned field. The answer must not change
    /// when the access path does — that is the whole claim an index makes.
    #[test]
    fn an_index_changes_the_path_and_not_the_answer() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        seed(&mut e, "CountIndexed", "ci");

        let predicate = author_is("alice");

        let unindexed = e
            .count_where(Some("CountIndexed"), None, None, Some(&predicate), "item")
            .expect("count ok");

        e.create_index(IndexDef {
            name: "ci_author".to_string(),
            kind: "CountIndexed".to_string(),
            field: "author".to_string(),
    unique: false,
})
        .expect("create index");

        let indexed = e
            .count_where(Some("CountIndexed"), None, None, Some(&predicate), "item")
            .expect("count ok");

        assert_eq!(indexed, unindexed, "the index changed the count");
        assert_eq!(
            indexed,
            rows_returned(&e, Some("CountIndexed"), None, None, Some(&predicate))
        );
    }

    /// A count is a summary of what the caller may read, not of what
    /// exists. Counting rows a requester cannot see would leak their
    /// existence — the number is a side channel like any other.
    #[test]
    fn a_count_never_includes_a_row_the_caller_cannot_read() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        seed(&mut e, "CountVisible", "cv");

        let everything = e
            .count_where(Some("CountVisible"), None, None, None, "item")
            .expect("count ok");

        let as_dave = e
            .count_where(Some("CountVisible"), None, Some("dave"), None, "item")
            .expect("count ok");

        assert!(
            as_dave < everything,
            "the private rows were counted for someone who cannot read them"
        );

        assert_eq!(
            as_dave,
            rows_returned(&e, Some("CountVisible"), None, Some("dave"), None),
            "the count and the query disagree about what dave may see"
        );

        // And with an index in play, so the prefix path is held to the
        // same rule.
        e.create_index(IndexDef {
            name: "cv_author".to_string(),
            kind: "CountVisible".to_string(),
            field: "author".to_string(),
    unique: false,
})
        .expect("create index");

        let predicate = author_is("alice");

        assert_eq!(
            e.count_where(
                Some("CountVisible"),
                None,
                Some("dave"),
                Some(&predicate),
                "item"
            )
            .expect("count ok"),
            rows_returned(&e, Some("CountVisible"), None, Some("dave"), Some(&predicate)),
            "the indexed count leaks what the indexed query does not"
        );
    }

    /// An empty answer is zero, not an error — and a kind nobody has ever
    /// written is empty rather than missing.
    #[test]
    fn nothing_matching_counts_zero() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        e.insert(node("CountZero", "cz:1", "alice", r#"{"author":"alice"}"#, true))
            .expect("insert");

        assert_eq!(
            e.count_where(Some("CountNeverWritten"), None, None, None, "item")
                .expect("count ok"),
            0
        );

        assert_eq!(
            e.count_where(
                Some("CountZero"),
                None,
                None,
                Some(&author_is("nobody")),
                "item"
            )
            .expect("count ok"),
            0
        );
    }

    /// A predicate the engine cannot push down is an error, not a wrong
    /// number. Answering 0 would be indistinguishable from "nothing
    /// matched", which is the failure mode a count must never have.
    #[test]
    fn an_unpushable_predicate_errors_rather_than_counting_zero() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        e.insert(node("CountBad", "cb:1", "alice", r#"{"author":"alice"}"#, true))
            .expect("insert");

        let nonsense: Expr = serde_json::from_value(serde_json::json!({
            "kind": "wat"
        }))
        .expect("parses as an Expr");

        assert!(
            e.count_where(Some("CountBad"), None, None, Some(&nonsense), "item")
                .is_err(),
            "an unevaluatable predicate produced a number"
        );
    }

    /// Selecting through an index must not require the ordering to come
    /// from that index too.
    ///
    /// Conflating the two cost 714x. With `idx_Tweet_author` declared,
    /// `where author == 'u7'` was 2.4 ms unordered and **1.83 s** ordered
    /// by `created` over fifty thousand rows: the ordering disqualified
    /// the index for *selection* as well, so the query fell back to
    /// reading the whole kind to find a hundred rows it could have looked
    /// up. Selecting through the index and sorting what comes back is
    /// what a planner does.
    ///
    /// The assertion is equality of answers, not speed — a timing test is
    /// a flake, and the bug this guards against is a plan that is slow
    /// *and* still correct. What pins the plan is that the answer must
    /// not move when the index appears.
    #[test]
    fn an_ordering_on_another_field_still_selects_through_the_index() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        for i in 0..60 {
            let mut n = Node::new(
                Coordinate::new(0, 0, 0, 0),
                format!("sel:{i:03}"),
                "SelKind".to_string(),
                "owner".to_string(),
            );

            n.data = format!(
                r#"{{"author": "u{}", "created": {}}}"#,
                i % 5,
                // Deliberately not the address order: sorting by `created`
                // must reorder the rows, so a plan that quietly returned
                // index order would fail.
                1_000_000 - i
            );
            n.visibility = Visibility::Public;

            e.insert(n).expect("insert");
        }

        let predicate = author_is("u3");

        let page = |engine: &StorageEngine| -> Vec<String> {
            engine
                .query_where(
                    Some("SelKind"),
                    None,
                    None,
                    Some(&predicate),
                    "item",
                    Some("created"),
                    true,
                    None,
                    5,
                    0,
                )
                .expect("query_where ok")
                .nodes
                .iter()
                .map(|n| n.address.clone())
                .collect()
        };

        let scanned = page(&e);

        assert_eq!(scanned.len(), 5, "expected a full page of u3's rows");

        e.create_index(IndexDef {
            name: "sel_author".to_string(),
            kind: "SelKind".to_string(),
            field: "author".to_string(),
    unique: false,
})
        .expect("create index");

        assert_eq!(
            page(&e),
            scanned,
            "selecting through the index changed the ordered answer"
        );
    }
}

#[cfg(test)]
mod count_by_tests {
    //! Grouped counting, held to one rule: **every group must equal the
    //! count you would get by asking for that value on its own.**
    //!
    //! That is the whole contract. `count_by` exists to replace N calls
    //! to `count_where` with one, so the moment the two disagree the
    //! optimization has become a wrong answer — and a wrong answer that
    //! arrives faster, which is the worst kind. Each test below computes
    //! the answer both ways and compares.

    use super::*;
    use crate::core::coordinate::Coordinate;
    use crate::core::node::{Node, Visibility};
    use crate::storage::engine::test_support::disk_guard;
    use crate::storage::index::IndexDef;

    fn like(address: &str, tweet: i64, owner: &str, public: bool) -> Node {
        let mut n = Node::new(
            Coordinate::new(0, 0, 0, 0),
            address.to_string(),
            "GbLike".to_string(),
            owner.to_string(),
        );

        n.data = format!(r#"{{"tweet": {tweet}}}"#);

        if public {
            n.visibility = Visibility::Public;
        }

        n
    }

    /// The shape the feed actually asks: likes per tweet. Assert every
    /// group against the single-value count it replaces.
    fn assert_groups_match_individual_counts(
        engine: &StorageEngine,
        requester: Option<&str>,
    ) {
        let groups = engine
            .count_by(Some("GbLike"), None, requester, None, "item", "tweet", None)
            .expect("count_by ok");

        assert!(!groups.is_empty(), "no groups at all");

        for group in &groups {
            let value = group.value.clone().expect("a tweet id");

            let predicate: Expr = serde_json::from_value(serde_json::json!({
                "kind": "bin",
                "op": "==",
                "l": {
                    "kind": "get",
                    "field": "tweet",
                    "obj": { "kind": "ref", "name": "item" }
                },
                "r": { "kind": "lit", "val": value }
            }))
            .expect("valid predicate");

            let individually = engine
                .count_where(
                    Some("GbLike"),
                    None,
                    requester,
                    Some(&predicate),
                    "item",
                )
                .expect("count_where ok");

            assert_eq!(
                group.count, individually,
                "group {:?} counted {} but counting it on its own gives {}",
                group.value, group.count, individually
            );
        }
    }

    /// 3 likes on tweet 1, 2 on tweet 2, 1 on tweet 3. Two of them belong
    /// to carol, and `carol_public` decides whether anyone else can see
    /// them — which is what gives the visibility filter something to do.
    fn seed(engine: &mut StorageEngine, carol_public: bool) {
        let rows: &[(&str, i64, &str, bool)] = &[
            ("gb:1", 1, "alice", true),
            ("gb:2", 1, "alice", true),
            ("gb:3", 1, "carol", carol_public),
            ("gb:4", 2, "alice", true),
            ("gb:5", 2, "carol", carol_public),
            ("gb:6", 3, "alice", true),
        ];

        for (address, tweet, owner, public) in rows {
            engine
                .insert(like(address, *tweet, owner, *public))
                .expect("insert");
        }
    }

    /// The scan path.
    #[test]
    fn a_grouped_count_equals_the_individual_counts() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        seed(&mut e, true);

        let groups = e
            .count_by(Some("GbLike"), None, None, None, "item", "tweet", None)
            .expect("count_by ok");

        assert_eq!(
            groups.iter().map(|g| g.count).collect::<Vec<_>>(),
            vec![3, 2, 1],
            "groups should be ordered by value: tweet 1, 2, 3"
        );

        assert_groups_match_individual_counts(&e, None);
    }

    /// The index path. Adjacency in the index is the grouping, and only
    /// one record per group is read — so this is the path most able to
    /// drift from the scan, and the one most worth pinning.
    #[test]
    fn the_indexed_path_gives_the_same_groups_as_the_scan() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        seed(&mut e, true);

        let scanned = e
            .count_by(Some("GbLike"), None, None, None, "item", "tweet", None)
            .expect("count_by ok");

        e.create_index(IndexDef {
            name: "gb_tweet".to_string(),
            kind: "GbLike".to_string(),
            field: "tweet".to_string(),
    unique: false,
})
        .expect("create index");

        let indexed = e
            .count_by(Some("GbLike"), None, None, None, "item", "tweet", None)
            .expect("count_by ok");

        assert_eq!(
            indexed.iter().map(|g| (g.value.clone(), g.count)).collect::<Vec<_>>(),
            scanned.iter().map(|g| (g.value.clone(), g.count)).collect::<Vec<_>>(),
            "the index changed the grouping"
        );

        assert_groups_match_individual_counts(&e, None);
    }

    /// Two index runs that cannot name their value must still be one
    /// group.
    ///
    /// This is the state a delete racing the walk leaves: the run was
    /// counted from the index, and by the time the value is recovered
    /// the one record it would have been recovered from is gone. Two
    /// runs in that state used to emit two entries with `value: None`,
    /// and every caller that indexes the reply by value — fct's
    /// `countBy` builds a map straight out of it — kept only the last
    /// of them and lost the other count entirely. Staged by hand,
    /// because a real race cannot be scheduled.
    #[test]
    fn runs_that_cannot_name_their_value_are_one_group() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        seed(&mut e, true);

        e.create_index(IndexDef {
            name: "gb_vanished".to_string(),
            kind: "GbLike".to_string(),
            field: "tweet".to_string(),
            unique: false,
        })
        .expect("create index");

        let index = e
            .indexes
            .data_find("GbLike", "tweet")
            .expect("the index just declared");

        for (tweet, address) in [(7i64, "gb:gone-a"), (8, "gb:gone-b")] {
            index
                .tree
                .put(
                    &keys::data_key(Some(&serde_json::json!(tweet)), address),
                    &[],
                )
                .expect("stage an entry whose record is gone");
        }

        let groups = e
            .count_by(Some("GbLike"), None, None, None, "item", "tweet", None)
            .expect("count_by ok");

        let unnamed: Vec<&GroupCount> =
            groups.iter().filter(|g| g.value.is_none()).collect();

        assert_eq!(
            unnamed.len(),
            1,
            "one entry per value, `null` included: {groups:?}"
        );

        assert_eq!(unnamed[0].count, 2, "both runs belong in the total");

        // The invariant the caller depends on, asserted over the whole
        // reply rather than only over the entries this test staged.
        for (i, a) in groups.iter().enumerate() {
            for b in &groups[i + 1..] {
                assert_ne!(
                    keys::compare_order_values(a.value.as_ref(), b.value.as_ref()),
                    std::cmp::Ordering::Equal,
                    "two entries for one value: {a:?} and {b:?}"
                );
            }
        }

        assert_eq!(
            groups.iter().map(|g| g.count).sum::<u64>(),
            8,
            "the 6 seeded rows plus the 2 whose records are gone"
        );
    }

    /// A group must never count a row the caller cannot read — and the
    /// grouped answer must agree with the individual one about that too,
    /// because the two take different paths to decide it.
    #[test]
    fn grouping_respects_visibility() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        seed(&mut e, false); // carol's two likes are private

        let everything = e
            .count_by(Some("GbLike"), None, None, None, "item", "tweet", None)
            .expect("count_by ok");

        let as_dave = e
            .count_by(Some("GbLike"), None, Some("dave"), None, "item", "tweet", None)
            .expect("count_by ok");

        let total = |gs: &[GroupCount]| gs.iter().map(|g| g.count).sum::<u64>();

        assert!(
            total(&as_dave) < total(&everything),
            "private rows were counted for someone who cannot read them"
        );

        assert_groups_match_individual_counts(&e, Some("dave"));
    }

    /// A field the rows do not have groups as one `null` bucket rather
    /// than vanishing — a row that exists has to be counted somewhere, or
    /// the totals stop adding up.
    #[test]
    fn rows_missing_the_field_group_under_null() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        seed(&mut e, true);

        let groups = e
            .count_by(Some("GbLike"), None, None, None, "item", "nosuchfield", None)
            .expect("count_by ok");

        assert_eq!(groups.len(), 1, "expected one bucket, got {groups:?}");
        assert_eq!(groups[0].value, None);
        assert_eq!(groups[0].count, 6, "every row must land somewhere");
    }

    /// The grouped total must equal the ungrouped count. If it does not,
    /// a row was double-counted or dropped.
    #[test]
    fn the_groups_sum_to_the_plain_count() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        seed(&mut e, true);

        let grouped: u64 = e
            .count_by(Some("GbLike"), None, None, None, "item", "tweet", None)
            .expect("count_by ok")
            .iter()
            .map(|g| g.count)
            .sum();

        let plain = e
            .count_where(Some("GbLike"), None, None, None, "item")
            .expect("count ok");

        assert_eq!(grouped, plain);
    }

    /// Asking about the values a page renders must give the same answers
    /// as asking about each one alone — and must include a zero for a
    /// value with no rows, because the caller asked and an absent key is
    /// indistinguishable from one the engine forgot.
    #[test]
    fn restricting_to_named_values_answers_exactly_those() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        seed(&mut e, true);

        let wanted = vec![
            serde_json::json!(3),
            serde_json::json!(1),
            serde_json::json!(99), // nothing has this
        ];

        for indexed in [false, true] {
            if indexed {
                e.create_index(IndexDef {
                    name: "gb_named".to_string(),
                    kind: "GbLike".to_string(),
                    field: "tweet".to_string(),
    unique: false,
})
                .expect("create index");
            }

            let got = e
                .count_by(
                    Some("GbLike"),
                    None,
                    None,
                    None,
                    "item",
                    "tweet",
                    Some(&wanted),
                )
                .expect("count_by ok");

            let mut pairs: Vec<(i64, u64)> = got
                .iter()
                .map(|g| {
                    (
                        g.value.as_ref().and_then(|v| v.as_i64()).unwrap_or(-1),
                        g.count,
                    )
                })
                .collect();
            pairs.sort();

            assert_eq!(
                pairs,
                vec![(1, 3), (3, 1), (99, 0)],
                "indexed={indexed}: wrong answers for the named values"
            );
        }
    }

    /// The same value named twice is one question, on both paths.
    ///
    /// A page can render the same row in two places, so the `values`
    /// list it builds holds a duplicate. The indexed fast path used to
    /// answer each name separately and reply with two entries for one
    /// value — a shape the caller cannot represent, since it reads the
    /// reply into a map keyed by value — while the scan path had always
    /// merged them. `1` and `1.0` are the same duplicate: they are one
    /// key in the index, so they are one group everywhere.
    #[test]
    fn a_value_named_twice_is_answered_once() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        seed(&mut e, true);

        let wanted = vec![
            serde_json::json!(1),
            serde_json::json!(1),
            serde_json::json!(1.0),
            serde_json::json!(2),
        ];

        let scanned = e
            .count_by(
                Some("GbLike"),
                None,
                None,
                None,
                "item",
                "tweet",
                Some(&wanted),
            )
            .expect("count_by ok");

        e.create_index(IndexDef {
            name: "gb_twice".to_string(),
            kind: "GbLike".to_string(),
            field: "tweet".to_string(),
            unique: false,
        })
        .expect("create index");

        let indexed = e
            .count_by(
                Some("GbLike"),
                None,
                None,
                None,
                "item",
                "tweet",
                Some(&wanted),
            )
            .expect("count_by ok");

        let pairs = |groups: &[GroupCount]| {
            groups
                .iter()
                .map(|g| (g.value.clone(), g.count))
                .collect::<Vec<_>>()
        };

        assert_eq!(
            pairs(&scanned),
            vec![
                (Some(serde_json::json!(1)), 3),
                (Some(serde_json::json!(2)), 2),
            ],
            "the scan answered a repeated value more than once"
        );

        assert_eq!(
            pairs(&indexed),
            pairs(&scanned),
            "the index answered a repeated value differently from the scan"
        );
    }

    /// The restriction must not become a way to smuggle in an unbounded
    /// request: a caller naming more values than a page could render is
    /// asking for the grouped form and should say so.
    #[test]
    fn too_many_named_values_is_refused() {
        let _g = disk_guard();
        let mut e = StorageEngine::open().expect("open storage engine");

        seed(&mut e, true);

        let far_too_many: Vec<serde_json::Value> =
            (0..MAX_GROUP_VALUES as i64 + 1).map(|i| serde_json::json!(i)).collect();

        assert!(
            e.count_by(
                Some("GbLike"),
                None,
                None,
                None,
                "item",
                "tweet",
                Some(&far_too_many),
            )
            .is_err(),
            "an unbounded value list was accepted"
        );
    }
}
