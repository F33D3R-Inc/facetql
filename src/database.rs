use std::collections::VecDeque;
use std::fmt;
use std::io;
use std::sync::{Arc, Mutex};

use tokio::sync::broadcast;

use crate::config;
use crate::storage::engine::StorageEngine;
use crate::storage::recovery;

/// Why the database refused to open.
///
/// Startup is fail-closed and stays that way — this type adds no
/// tolerance, only diagnosis. It exists because "initialization failed"
/// is the same sentence for a missing directory, a wrong master key and
/// genuine bit-rot, and those three have nothing in common from the
/// operator's side: one is a `chown`, one is an environment variable,
/// one is a restore from backup.
///
/// Every variant keeps the underlying `io::Error` rather than a summary
/// of it. The storage layer already names the file, the byte offset and
/// the defect; that string is the most valuable thing we have, and a
/// tidier paraphrase would throw away the only part an operator can act
/// on directly.
#[derive(Debug)]
pub enum DatabaseError {
    /// The bytes could not be reached at all: a data directory that does
    /// not exist and cannot be created, a permissions problem, a full or
    /// read-only filesystem, a missing mount. Nothing has been read, so
    /// nothing is yet known to be wrong with the *contents*.
    Storage {
        /// What was being attempted, for the operator's first line.
        phase: &'static str,
        source: io::Error,
    },

    /// Bytes were read and did not verify: a frame checksum, an AEAD
    /// authentication tag, or a deserialization that failed.
    ///
    /// Two very different causes land here and the file cannot tell them
    /// apart: the server was started with the wrong
    /// `ENOCHIAN_MASTER_KEY` (common), or the file is genuinely damaged
    /// (rare). `authentication` records whether the failure was
    /// specifically a decrypt/auth failure, which is the case where the
    /// wrong-key explanation should be offered first.
    Integrity {
        authentication: bool,
        source: io::Error,
    },

    /// The log parsed and authenticated, but its contents violate the
    /// WAL's own rules: a sequence number that goes backwards, a COMMIT
    /// after an ABORT, a mutation before BEGIN. The bytes are intact;
    /// the history they describe is not one that can legally be
    /// replayed, so replaying it would invent a state that never
    /// existed.
    WalRecovery { source: io::Error },
}

impl DatabaseError {
    /// The underlying error's own words — file, byte offset, defect.
    /// Reported verbatim wherever this error is rendered.
    pub fn detail(&self) -> String {
        self.cause().to_string()
    }

    /// Process exit codes.
    ///
    /// This continues the policy already set by `src/cli/error.rs`
    /// (2 = usage error, 1 = runtime failure, 3 = operator declined)
    /// instead of competing with it: a storage failure is an ordinary
    /// runtime failure and keeps 1, and 2 and 3 are left alone so a
    /// script can rely on one meaning per code across the whole binary.
    /// The two classes a supervisor would genuinely branch on get codes
    /// of their own — 1 may be worth retrying once a mount comes back,
    /// 4 and 5 never are and need a human.
    pub fn exit_code(&self) -> i32 {
        match self {
            DatabaseError::Storage { .. } => 1,
            DatabaseError::Integrity { .. } => 4,
            DatabaseError::WalRecovery { .. } => 5,
        }
    }

    /// Classify a failure raised while loading the physical files.
    ///
    /// At this point no WAL semantics have been evaluated, so anything
    /// that is not an access failure is a verification failure.
    fn loading(source: io::Error) -> Self {
        if is_access_failure(source.kind()) {
            return DatabaseError::Storage {
                phase: "loading the storage files",
                source,
            };
        }

        DatabaseError::Integrity {
            authentication: integrity_failure(&source.to_string())
                .unwrap_or(false),
            source,
        }
    }

    /// Classify a failure raised by WAL recovery.
    ///
    /// Recovery can fail either way: a frame that will not decrypt or
    /// deserialize (integrity), or a set of frames that verified fine
    /// and describe an impossible transaction (lifecycle).
    fn recovering(source: io::Error) -> Self {
        if is_access_failure(source.kind()) {
            return DatabaseError::Storage {
                phase: "recovering the write-ahead log",
                source,
            };
        }

        match integrity_failure(&source.to_string()) {
            Some(authentication) => {
                DatabaseError::Integrity { authentication, source }
            }

            /*
             * Nothing in the message points at bad bytes, so what was
             * violated was one of recovery's own rules — sequence
             * ordering, transaction lifecycle, or an operation that
             * could not be replayed onto the engine.
             */
            None => DatabaseError::WalRecovery { source },
        }
    }

    fn cause(&self) -> &io::Error {
        match self {
            DatabaseError::Storage { source, .. }
            | DatabaseError::Integrity { source, .. }
            | DatabaseError::WalRecovery { source } => source,
        }
    }
}

/// Is this "I could not get at the bytes" rather than "I read the bytes
/// and they were wrong"?
///
/// Inverted deliberately: the four kinds listed below are the ones the
/// storage layer raises for content it has already read, and everything
/// else the OS can hand back (`NotFound`, `PermissionDenied`,
/// `StorageFull`, …) is a problem with reaching the file. Written this
/// way round so a kind nobody anticipated is treated as an access
/// failure — the class whose advice is harmless if wrong.
fn is_access_failure(kind: io::ErrorKind) -> bool {
    !matches!(
        kind,
        io::ErrorKind::InvalidData
            | io::ErrorKind::UnexpectedEof
            | io::ErrorKind::InvalidInput
            | io::ErrorKind::Other
    )
}

/// Does `detail` describe bytes that failed to verify, and if so, was it
/// specifically a decrypt/authentication failure?
///
/// The storage layer reports a bad checksum and an illegal transaction
/// as the same `ErrorKind::InvalidData`, so the kind alone cannot
/// separate them. We look for the vocabulary of verification instead.
/// These words are what those failures *are* rather than incidental
/// phrasing, so they survive a rewording of the storage layer; and if
/// they ever stop matching, a recovery-phase failure is reported as a
/// WAL recovery problem, which is still true, still fatal, and still
/// carries the original text verbatim.
fn integrity_failure(detail: &str) -> Option<bool> {
    let detail = detail.to_ascii_lowercase();

    // A wrong master key can only ever surface as one of these.
    const AUTHENTICATION: &[&str] =
        &["decrypt", "authentication", "master key", "master_key"];

    // Damage that framing or parsing caught before decryption.
    const VERIFICATION: &[&str] = &[
        "checksum",
        "crc",
        "corrupt",
        "deserialize",
        "invalid record",
        "valid hex",
        "magic",
        "truncated",
        "format version",
    ];

    if AUTHENTICATION.iter().any(|word| detail.contains(word)) {
        return Some(true);
    }

    if VERIFICATION.iter().any(|word| detail.contains(word)) {
        return Some(false);
    }

    None
}

/// Events the live channel retains before the slowest subscriber starts
/// missing them.
///
/// A retained event is memory: the ring holds every event until all
/// receivers have read past it, so this bound and the per-payload bound
/// in `api::routes` multiply to the channel's memory ceiling. Named
/// rather than inline so the two are visibly a pair — changing one
/// without the other silently changes that ceiling.
///
/// Lagging is a subscriber's problem, not the database's: a receiver
/// that falls behind gets a `Lagged` error, which `subscribe_events`
/// skips, and the stream continues. Events are best-effort
/// notifications, never the source of truth.
pub const BROADCAST_CAPACITY: usize = 1024;

/// Events the feed retains for replay, and therefore how far back
/// `GET /events?after=<seq>` can resume.
///
/// This is the resume *horizon*, and it is deliberately the same number
/// as [`BROADCAST_CAPACITY`]: the two rings hold the same events for
/// the same reason, and a horizon that differed from the live ring's
/// depth would be a second bound to reason about with no second
/// benefit. Together they mean the feed's memory ceiling is two rings
/// of this depth times the per-payload bound in `api::routes` — the
/// replay ring always full, the broadcast ring only while a subscriber
/// lags.
///
/// It is *in memory only*. Nothing published before this process
/// started is retained, which is why the horizon is stated to the
/// caller and a resume from behind it is refused (see [`ResumeTooOld`])
/// rather than answered from the live edge. Making it durable would
/// mean replaying the WAL to reconstruct events, which is a change to
/// what the feed *is* — a notification channel, never the source of
/// truth — not a bigger number here.
pub const EVENT_REPLAY_CAPACITY: usize = BROADCAST_CAPACITY;

/// Exit code for "a mutation panicked; in-memory state is untrustworthy".
///
/// Continues the numbering in [`DatabaseError::exit_code`] (1 storage,
/// 4 integrity, 5 WAL) and in `cli::error` (2 usage, 3 declined). Unlike
/// 4 and 5 this one is *expected* to be retried: the WAL holds every
/// acknowledged write and startup recovery replays it.
pub const EXIT_ENGINE_POISONED: i32 = 6;

/// End the process because a task panicked while holding the engine lock.
///
/// A `JoinError` from [`Database::with_engine_mut`] means the closure
/// panicked *inside* the lock, which is precisely the situation
/// [`poisoned`] describes — the panic poisons the lock, and every
/// subsequent request would meet it. The lock's own poison flag would
/// catch this on the next acquisition anyway; this reports it at the
/// point it happened rather than blaming the next request.
fn engine_task_panicked() -> ! {
    poisoned()
}

/// End the process because the engine lock is poisoned.
///
/// Never returns. Writes to stderr rather than returning an error
/// because there is no caller left that could act on one — the state
/// this would have to be reported through is the state that is broken.
pub(crate) fn poisoned() -> ! {
    eprintln!(
        "facetql: FATAL — a request panicked while holding the storage \
         engine lock, so the in-memory index and cache state is no longer \
         consistent with the disk. Exiting so this process is restarted: \
         every acknowledged write is durable in the write-ahead log, and \
         startup recovery replays it. Nothing is lost by restarting; \
         continuing to serve from inconsistent state would lose \
         correctness silently."
    );

    std::process::exit(EXIT_ENGINE_POISONED)
}

impl fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DatabaseError::Storage { phase, source } => {
                write!(f, "storage is unreachable while {phase}: {source}")
            }

            /*
             * The order of the two causes is not cosmetic. A wrong key
             * and a corrupt file are indistinguishable to the code, but
             * not in practice — the key is wrong far more often, and it
             * is the cause an operator can rule out in ten seconds and
             * without touching the data.
             */
            DatabaseError::Integrity { authentication: true, source } => {
                write!(
                    f,
                    "a stored record failed authentication: {source} — most \
                     likely the wrong ENOCHIAN_MASTER_KEY, otherwise the \
                     file is corrupt"
                )
            }

            DatabaseError::Integrity { authentication: false, source } => {
                write!(f, "a stored record failed its integrity check: {source}")
            }

            DatabaseError::WalRecovery { source } => write!(
                f,
                "the write-ahead log describes a history that cannot be \
                 replayed: {source}"
            ),
        }
    }
}

impl std::error::Error for DatabaseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.cause())
    }
}

/// Who is allowed to receive a live event.
///
/// Every event carries one of these because the notification channel is
/// a *read path*, and a read path without visibility rules is a leak: a
/// node the reader could never fetch over `GET /node/:address` must not
/// announce itself over `GET /events` either. Making the audience part
/// of the event (rather than something the subscriber tries to
/// reconstruct) means the decision is taken where the facts are — at the
/// handler that just performed the write and knows whose data it was.
#[derive(Clone, Debug)]
pub enum Audience {
    /// Any authenticated subscriber. For events that concern public
    /// nodes, and for explicit application broadcasts.
    Everyone,

    /// Only this owner — plus admins, who can already read everything.
    /// The default shape for anything touching a private node.
    Owner(String),
}

impl Audience {
    /// May a subscriber authenticated as `owner` receive this?
    pub fn admits(&self, owner: &str, is_admin: bool) -> bool {
        match self {
            Audience::Everyone => true,
            Audience::Owner(only) => is_admin || only == owner,
        }
    }

    /// The audience for an event about a node: public nodes are
    /// announced to everyone, private nodes only to their owner. This is
    /// the same rule `Node::can_read` applies to a direct fetch, which is
    /// the point — one visibility model, not two that can drift apart.
    pub fn for_node(node: &crate::core::node::Node) -> Audience {
        match node.visibility {
            crate::core::node::Visibility::Public => Audience::Everyone,
            crate::core::node::Visibility::Private => {
                Audience::Owner(node.owner.clone())
            }
        }
    }
}

/// A live notification, its position in the feed, and the audience
/// permitted to see it.
#[derive(Clone, Debug)]
pub struct LiveEvent {
    /// This event's position in the feed — strictly increasing in the
    /// order events were published, and the value a subscriber hands
    /// back as `GET /events?after=<seq>`.
    ///
    /// It comes from [`crate::storage::wal::next_operation_id`], the
    /// counter the WAL and [`crate::core::history::HistoryEntry`]
    /// already draw from, for the reason `HistoryEntry::version` gives:
    /// it is the one identifier in the process that is unique and
    /// increasing *across restarts*, because recovery advances it past
    /// every identifier in the durable WAL before the first new write.
    /// A second counter minted here would be a second source of truth
    /// for "when did this happen", and the two would disagree the first
    /// time one of them was reset.
    ///
    /// **Positions are not contiguous, by design.** The counter is
    /// shared with the WAL, which burns numbers on every record it
    /// writes, so consecutive events differ by more than one. A
    /// subscriber therefore cannot infer a gap from the numbers — that
    /// is what the explicit `feed_lagged` frame is for — and `after=`
    /// means "strictly greater than", never "the next one".
    pub seq: u64,
    pub payload: String,
    pub audience: Audience,
}

/// Why a resume was refused: the position asked for is older than
/// anything the server still holds.
///
/// Returned rather than quietly starting from "now", which is the whole
/// point. A resume that silently skips the events it cannot supply
/// looks *exactly* like a successful one — same 200, same live frames —
/// and the caller would carry on believing it had seen everything. The
/// refusal is the only honest answer, and it carries the numbers the
/// caller needs to choose what to do instead (reconcile from a full
/// read).
#[derive(Debug)]
pub struct ResumeTooOld {
    /// The `after` the caller asked for.
    pub requested: u64,

    /// The oldest position this feed can still resume from. Every event
    /// published after this one is still retained.
    pub earliest: u64,

    /// How many events are retained right now.
    pub retained: usize,
}

impl fmt::Display for ResumeTooOld {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "cannot resume the event feed from {}: the oldest position \
             still available is {} ({} of at most {} events retained, \
             in memory only — nothing published before this process \
             started is retained at all). The events between those two \
             positions are gone from the feed and this server will not \
             pretend otherwise by starting from the live edge. \
             Reconcile from a full read instead.",
            self.requested,
            self.earliest,
            self.retained,
            EVENT_REPLAY_CAPACITY,
        )
    }
}

impl std::error::Error for ResumeTooOld {}

/// The published-events ring: the live broadcast plus the recent
/// history a subscriber can resume from.
///
/// The two live in one struct, behind one mutex, because they must be
/// written *together*. Minting a position, appending to the replay ring
/// and sending on the broadcast are one atomic step, and taking a
/// snapshot of the ring and subscribing to the broadcast are another —
/// interleave either pair and a subscriber either misses an event that
/// fell between its snapshot and its subscription, or sees positions
/// arrive out of order. Neither is recoverable by the subscriber,
/// because both look exactly like a healthy stream.
pub struct EventFeed {
    sender: broadcast::Sender<LiveEvent>,
    state: Mutex<FeedState>,
}

struct FeedState {
    /// The most recent events, oldest first, capped at
    /// [`EVENT_REPLAY_CAPACITY`].
    retained: VecDeque<LiveEvent>,

    /// The oldest `after` this feed can still honour.
    ///
    /// Starts at the position the feed was created at, so a resume
    /// token minted by a *previous* process — whose events this
    /// in-memory ring never held — is refused rather than answered with
    /// a stream that begins at the live edge. Advances to the position
    /// of each event evicted from `retained`: once an event with
    /// position `s` is dropped, `after=s` is still answerable (every
    /// event after it is retained) but nothing older is.
    earliest_resumable: u64,
}

impl EventFeed {
    fn new() -> Self {
        let (sender, _) = broadcast::channel(BROADCAST_CAPACITY);

        Self {
            sender,
            state: Mutex::new(FeedState {
                retained: VecDeque::with_capacity(EVENT_REPLAY_CAPACITY),

                // Minting a position here rather than using 0 is what
                // makes a stale token from a previous process refusable:
                // recovery has already advanced the counter past every
                // identifier in the durable WAL, so every position that
                // process handed out is below this one.
                earliest_resumable: crate::storage::wal::next_operation_id(),
            }),
        }
    }

    /// Append an event carrying `payload` verbatim.
    fn emit(&self, audience: Audience, payload: String) -> u64 {
        self.emit_with(audience, |_| payload)
    }

    /// Append an event whose body is a JSON object, with the position
    /// inserted into that object under `seq`.
    ///
    /// A body that is not an object is published as it serializes,
    /// unchanged. Every internal publish site passes an object — the
    /// events are all `{"event": ...}` — so this is a total function
    /// rather than a case that happens; making it a silent panic
    /// instead would put a mutation site at risk for a malformed
    /// notification.
    fn emit_json(&self, audience: Audience, body: serde_json::Value) -> u64 {
        self.emit_with(audience, |seq| match body {
            serde_json::Value::Object(mut fields) => {
                fields.insert("seq".to_string(), seq.into());
                serde_json::Value::Object(fields).to_string()
            }
            other => other.to_string(),
        })
    }

    /// Append an event to the feed and hand it to every live
    /// subscriber. Returns the position it was given.
    ///
    /// `render` is called with that position, inside the lock, so a
    /// payload can carry the very number the frame is addressed by.
    fn emit_with<R>(&self, audience: Audience, render: R) -> u64
    where
        R: FnOnce(u64) -> String,
    {
        let mut state = self.locked();

        // Inside the lock: the position, the ring append and the send
        // are one step, so ring order, position order and delivery
        // order are the same order for every subscriber.
        let seq = crate::storage::wal::next_operation_id();

        let event = LiveEvent { seq, payload: render(seq), audience };

        // The ring is full: the oldest event leaves, and the horizon
        // moves up to it. `after=<evicted.seq>` is still answerable —
        // every event after that one is retained — and nothing older
        // is, which is exactly what `earliest_resumable` means.
        while state.retained.len() >= EVENT_REPLAY_CAPACITY {
            match state.retained.pop_front() {
                Some(evicted) => state.earliest_resumable = evicted.seq,
                None => break,
            }
        }

        state.retained.push_back(event.clone());

        /*
         * There may be no subscribers. broadcast::Sender::send()
         * returns an error in that case, but that does not mean the
         * database operation failed.
         *
         * Events are intentionally best-effort notifications.
         */
        let _ = self.sender.send(event);

        seq
    }

    /// Open a subscription, optionally resuming after position `after`.
    ///
    /// Returns the backlog to replay first (empty when `after` is
    /// `None`) and the live receiver, taken under one lock so no event
    /// can land between them. The caller still applies the audience
    /// filter to both halves — this returns everything, exactly as the
    /// broadcast does.
    pub fn subscribe(
        &self,
        after: Option<u64>,
    ) -> Result<(Vec<LiveEvent>, broadcast::Receiver<LiveEvent>), ResumeTooOld> {
        let state = self.locked();

        let backlog = match after {
            None => Vec::new(),

            Some(after) if after < state.earliest_resumable => {
                return Err(ResumeTooOld {
                    requested: after,
                    earliest: state.earliest_resumable,
                    retained: state.retained.len(),
                });
            }

            Some(after) => state
                .retained
                .iter()
                .filter(|event| event.seq > after)
                .cloned()
                .collect(),
        };

        Ok((backlog, self.sender.subscribe()))
    }

    /// The state, whether or not a previous holder panicked.
    ///
    /// What this mutex guards is a ring of already-formed events and one
    /// integer; a panic cannot leave those half-written the way it can
    /// leave the storage engine's index inconsistent with its pages
    /// (which is why *that* lock aborts the process instead — see
    /// [`poisoned`]). Refusing to serve the feed forever after one
    /// unrelated panic would be a worse answer than continuing.
    fn locked(&self) -> std::sync::MutexGuard<'_, FeedState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[derive(Clone)]
pub struct Database {
    pub engine: Arc<StorageEngine>,
    pub feed: Arc<EventFeed>,
}

impl Database {
    /// Open the database, or explain precisely why it cannot be opened.
    ///
    /// Returns [`DatabaseError`] rather than `io::Result` so the caller
    /// can tell a `chown` problem from a wrong master key from an
    /// unreplayable log. `main.rs` is the only caller, and it renders
    /// the classification to the operator and exits non-zero — this is
    /// still fail-closed, and every failure that aborted startup before
    /// aborts it now.
    pub fn new() -> Result<Self, DatabaseError> {
        config::ensure_data_dir().map_err(|source| DatabaseError::Storage {
            phase: "creating the data directory",
            source,
        })?;

        let mut engine =
            StorageEngine::load().map_err(DatabaseError::loading)?;

        /*
         * WAL recovery is part of opening the database.
         *
         * A recovery failure must prevent startup. Continuing after
         * an authentication, corruption, or format error could cause
         * the server to expose state that is not known to be durable
         * or valid.
         */
        recovery::recover(&mut engine)
            .map_err(DatabaseError::recovering)?;

        // Recovery is the last thing that can change the identity set,
        // so this is the first point at which the user log can be
        // rewritten to just the identities that survive. Non-fatal: an
        // uncompacted log is long, not wrong.
        if let Err(error) = engine.compact_user_log() {
            eprintln!(
                "warning: could not compact the user log: {error}. The \
                 existing log is intact and correct; it will simply stay \
                 longer than it needs to be."
            );
        }

        // After recovery, deliberately: the feed stamps its resume
        // horizon with a position minted from the WAL counter, and that
        // counter is only past the durable log once recovery has
        // advanced it.
        Ok(Self::attach(engine))
    }

    /// Wrap an already-open engine in a database with a fresh event
    /// feed.
    ///
    /// The one place a `Database` is assembled, so a new feed field
    /// cannot be forgotten by one of the several callers that build one
    /// (`new` here, and the router tests).
    pub fn attach(engine: StorageEngine) -> Self {
        Self {
            engine: Arc::new(engine),
            feed: Arc::new(EventFeed::new()),
        }
    }

    /// The engine, for a read.
    ///
    /// See [`Database::engine_mut`] for why this is a method rather than
    /// twenty copies of `.expect("storage engine lock poisoned")`.
    pub fn engine(&self) -> &StorageEngine {
        &self.engine
    }

    /// The engine, for a mutation.
    ///
    /// # Why a poisoned lock ends the process
    ///
    /// The lock is poisoned only if a thread panicked while holding it,
    /// which means it panicked partway through a mutation: some indexes
    /// updated, others not, the record cache possibly naming a version
    /// that was never indexed. The in-memory picture is no longer
    /// trustworthy.
    ///
    /// The durable picture still is. Every mutation writes its WAL
    /// record before it touches any of that state, so the operation is
    /// on disk and `recovery::recover` reapplies it — idempotently —
    /// at the next start. **Restarting is the repair**, and it is a
    /// complete one.
    ///
    /// Which makes the previous behaviour the worst of the three
    /// options. `.expect()` panics the *handler task*, and a panicked
    /// task in Tokio kills one connection and leaves the process
    /// running. So the server stayed up, never restarted, never
    /// recovered, and answered every subsequent request — forever — by
    /// panicking again. A permanent outage that looks, to a supervisor,
    /// like a healthy process.
    ///
    /// Exiting hands the repair to the thing that can perform it. The
    /// code is distinct from the startup failures in
    /// [`DatabaseError::exit_code`] so a supervisor can tell "crashed
    /// while serving, restart me" from "cannot open the data directory,
    /// stop trying".
    pub fn engine_mut(&self) -> &StorageEngine {
        &self.engine
    }

    /// Run a **read** against the engine, on a blocking thread.
    ///
    /// Takes no lock. Reads used to share an `RwLock` with writes, which
    /// made every read exclusive of every write in both directions; the
    /// engine now serves them concurrently, because the B+tree reads
    /// through pinned snapshots and the record cache is keyed by
    /// version rather than by address.
    ///
    /// What it does take is a [`ReadPin`](crate::storage::engine::ReadPin)
    /// for the length of the closure, which stops a concurrent
    /// checkpoint from deleting a heap segment this read may still be
    /// about to touch.
    ///
    /// Still on the blocking pool: a read is not fast merely because it
    /// is a read. It faults pages in, decrypts each one, and can visit a
    /// hundred thousand of them, and none of that belongs on a Tokio
    /// worker.
    pub async fn with_engine<R, F>(&self, act: F) -> R
    where
        F: FnOnce(&StorageEngine) -> R + Send + 'static,
        R: Send + 'static,
    {
        let engine = Arc::clone(&self.engine);

        tokio::task::spawn_blocking(move || {
            let _pin = engine.pin_read();

            act(&engine)
        })
        .await
        .unwrap_or_else(|_| engine_task_panicked())
    }

    /// Run a **write** against the engine, on a blocking thread.
    ///
    /// Writes are serialized against each other by the engine's own
    /// writer mutex, which is where the single-writer rule now lives.
    /// They are no longer serialized against reads.
    ///
    /// # Why this exists
    ///
    /// [`Database::engine_mut`] returns a `std` lock guard, and taking it
    /// inside an `async fn` blocks the Tokio worker thread that is
    /// running the task — not just the task. The engine then does an
    /// `fsync` while still holding it, and an `fsync` on a real
    /// filesystem is not fast: measured on this project's own hardware,
    /// **7.9 ms**.
    ///
    /// The consequence is worse than slow writes. Tokio's worker pool is
    /// sized to the core count, so a handful of concurrent writes park
    /// every worker on the same lock and the runtime has nothing left to
    /// run **reads** on. The whole server stalls on a workload that only
    /// writes, and the symptom is latency on endpoints that never touch
    /// the write path — the sort of thing that gets diagnosed as a
    /// network problem.
    ///
    /// `spawn_blocking` moves both the waiting and the `fsync` onto the
    /// blocking pool, which is separate from the async workers and grows
    /// on demand. Writes still serialize against each other — that is
    /// the engine's concurrency model and this does not change it — but
    /// they serialize somewhere that does not starve everything else,
    /// and they no longer hold reads up while they do it.
    pub async fn with_engine_mut<R, F>(&self, act: F) -> R
    where
        F: FnOnce(&StorageEngine) -> R + Send + 'static,
        R: Send + 'static,
    {
        let engine = Arc::clone(&self.engine);

        tokio::task::spawn_blocking(move || act(&engine))
            .await
            .unwrap_or_else(|_| engine_task_panicked())
    }

    /// Publish a database event to the subscribers `audience` admits.
    ///
    /// The database mutation itself is responsible for durability.
    /// This channel is only the live notification mechanism and must
    /// never be treated as the source of truth.
    ///
    /// The audience is a required argument rather than something with a
    /// default: a new publish site should have to say who may see its
    /// event, because the failure mode of getting it wrong is silent
    /// disclosure, and a default would be chosen by whoever was in a
    /// hurry.
    /// # This must stay lock-free
    ///
    /// Handlers call this from inside [`Database::with_engine_mut`],
    /// i.e. while holding the engine's write lock. That is safe only
    /// because the whole body is a non-blocking send on a broadcast
    /// channel and touches no engine state. If this ever grows a read of
    /// the engine, it will deadlock against the write lock the caller is
    /// already holding — `std::sync::RwLock` is not reentrant — and if it
    /// ever grows anything slow, it extends the window during which no
    /// other write can proceed.
    /// # The position goes in the body
    ///
    /// `body` is a JSON object and this inserts `seq` into it, so a
    /// consumer that reads only the `data` line of an SSE frame sees
    /// the position without having to also track the frame's `id:`
    /// field. Taking a `Value` rather than a pre-rendered `String` is
    /// what makes that a structured insert instead of string surgery on
    /// somebody else's JSON.
    pub fn publish(&self, audience: Audience, body: serde_json::Value) -> u64 {
        self.feed.emit_json(audience, body)
    }

    /// Publish an opaque payload — `POST /publish`'s arbitrary string,
    /// which is not required to be JSON at all.
    ///
    /// Its bytes reach subscribers verbatim, exactly as before: the
    /// position rides on the frame's `id:` field, which is where SSE
    /// puts a resume token and which no existing consumer of this
    /// payload parses. There is nowhere else to put it that would not
    /// change what a `Notify` listener receives.
    pub fn publish_opaque(&self, audience: Audience, payload: String) -> u64 {
        self.feed.emit(audience, payload)
    }
}
#[cfg(test)]
mod audience_tests {
    use super::*;
    use crate::core::coordinate::Coordinate;
    use crate::core::node::{Node, Visibility};

    fn node(owner: &str, visibility: Visibility) -> Node {
        let mut n = Node::new(
            Coordinate::new(0, 0, 0, 0),
            "aud:1".to_string(),
            "Thing".to_string(),
            owner.to_string(),
        );
        n.visibility = visibility;
        n
    }

    /// The rule that closes the `/events` leak: an event about a private
    /// node reaches its owner and admins, and nobody else.
    #[test]
    fn private_node_events_do_not_reach_other_identities() {
        let audience = Audience::for_node(&node("alice", Visibility::Private));

        assert!(audience.admits("alice", false), "the owner sees its own node");
        assert!(audience.admits("root", true), "an admin reads everything already");
        assert!(
            !audience.admits("bob", false),
            "another identity must not learn a private node exists"
        );
    }

    /// A public node is public on every read path, the event stream
    /// included — otherwise a feed could never be built from it.
    #[test]
    fn public_node_events_reach_everyone() {
        let audience = Audience::for_node(&node("alice", Visibility::Public));

        assert!(audience.admits("alice", false));
        assert!(audience.admits("bob", false));
        assert!(audience.admits("root", true));
    }

    /// An explicit application broadcast is addressed to everyone by
    /// construction — the caller chose the payload.
    #[test]
    fn explicit_broadcasts_reach_everyone() {
        assert!(Audience::Everyone.admits("anyone", false));
    }
}

#[cfg(test)]
mod event_feed_tests {
    //! What a subscriber has to be able to tell apart.
    //!
    //! The feed's whole job here is that "you missed nothing" and "you
    //! missed something" are different answers. Every test below is one
    //! way the old feed could not tell them apart.

    use super::*;

    fn feed() -> EventFeed {
        EventFeed::new()
    }

    /// The position a caller can legitimately resume from before
    /// anything at all has been published — which is also the proof
    /// that a position from before this feed existed is refused.
    fn floor(feed: &EventFeed) -> u64 {
        feed.subscribe(Some(0))
            .expect_err("position 0 predates this feed and must be refused")
            .earliest
    }

    fn payloads(events: Vec<LiveEvent>) -> Vec<String> {
        events.into_iter().map(|e| e.payload).collect()
    }

    /// Positions are strictly increasing in publication order. Without
    /// that, `after=` names an arbitrary point rather than a place in
    /// the stream.
    #[test]
    fn positions_increase_with_publication_order() {
        let feed = feed();

        let first = feed.emit(Audience::Everyone, "a".to_string());
        let second = feed.emit(Audience::Everyone, "b".to_string());

        assert!(second > first, "{second} must come after {first}");
    }

    /// A resume delivers what came after the named position, and only
    /// that — no replay of what the caller already had, no silent skip
    /// of what it did not.
    #[test]
    fn a_resume_replays_exactly_what_came_after() {
        let feed = feed();
        let start = floor(&feed);

        feed.emit(Audience::Everyone, "first".to_string());
        let after_second = feed.emit(Audience::Everyone, "second".to_string());
        feed.emit(Audience::Everyone, "third".to_string());

        let (all, _) = feed.subscribe(Some(start)).expect("resume from the start");
        assert_eq!(payloads(all), ["first", "second", "third"]);

        let (tail, _) =
            feed.subscribe(Some(after_second)).expect("resume mid-stream");
        assert_eq!(payloads(tail), ["third"]);
    }

    /// Opening without `after` is the live edge, exactly as before —
    /// a resume is opt-in, so no existing subscriber changes behaviour.
    #[test]
    fn opening_without_a_position_replays_nothing() {
        let feed = feed();
        feed.emit(Audience::Everyone, "already published".to_string());

        let (backlog, _) = feed.subscribe(None).expect("open at the live edge");
        assert!(backlog.is_empty());
    }

    /// The one that matters: past the horizon the answer is a refusal,
    /// not a stream that quietly begins at the live edge. A silent
    /// start would be indistinguishable from a complete resume.
    #[test]
    fn a_resume_from_before_the_horizon_is_refused() {
        let feed = feed();
        let start = floor(&feed);

        for i in 0..=EVENT_REPLAY_CAPACITY {
            feed.emit(Audience::Everyone, format!("event {i}"));
        }

        let refusal = feed
            .subscribe(Some(start))
            .expect_err("the oldest event has been evicted; this cannot be served");

        assert_eq!(refusal.requested, start);
        assert!(
            refusal.earliest > start,
            "the refusal must name a position that IS still available"
        );
        assert_eq!(refusal.retained, EVENT_REPLAY_CAPACITY);

        // And the position it names is honoured, so the caller has
        // somewhere to go rather than only a rejection.
        let (backlog, _) = feed
            .subscribe(Some(refusal.earliest))
            .expect("the horizon it reported must itself be resumable");

        assert_eq!(backlog.len(), EVENT_REPLAY_CAPACITY);
    }

    /// A JSON event carries its position inside the payload, so a
    /// consumer that reads only the `data` line still knows where it is.
    #[test]
    fn a_json_event_carries_its_own_position() {
        let feed = feed();
        let start = floor(&feed);

        let seq = feed.emit_json(
            Audience::Everyone,
            serde_json::json!({"event": "node_deleted", "address": "x"}),
        );

        let (backlog, _) = feed.subscribe(Some(start)).expect("resume");
        let body: serde_json::Value =
            serde_json::from_str(&backlog[0].payload).expect("valid JSON");

        assert_eq!(body["seq"], seq);
        assert_eq!(body["event"], "node_deleted");
    }

    /// An opaque `POST /publish` payload reaches subscribers byte for
    /// byte — the position rides on the frame, not in the bytes.
    #[test]
    fn an_opaque_payload_is_not_rewritten() {
        let feed = feed();
        let start = floor(&feed);

        feed.emit(Audience::Everyone, "not json at all".to_string());

        let (backlog, _) = feed.subscribe(Some(start)).expect("resume");
        assert_eq!(backlog[0].payload, "not json at all");
    }
}
