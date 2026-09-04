use std::fmt;
use std::io;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

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

/// Exit code for "a mutation panicked; in-memory state is untrustworthy".
///
/// Continues the numbering in [`DatabaseError::exit_code`] (1 storage,
/// 4 integrity, 5 WAL) and in `cli::error` (2 usage, 3 declined). Unlike
/// 4 and 5 this one is *expected* to be retried: the WAL holds every
/// acknowledged write and startup recovery replays it.
pub const EXIT_ENGINE_POISONED: i32 = 6;

/// End the process because the engine lock is poisoned.
///
/// Never returns. Writes to stderr rather than returning an error
/// because there is no caller left that could act on one — the state
/// this would have to be reported through is the state that is broken.
fn poisoned() -> ! {
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

/// A live notification and the audience permitted to see it.
#[derive(Clone, Debug)]
pub struct LiveEvent {
    pub payload: String,
    pub audience: Audience,
}

#[derive(Clone)]
pub struct Database {
    pub engine: Arc<RwLock<StorageEngine>>,
    pub broadcaster: broadcast::Sender<LiveEvent>,
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

        let (broadcaster, _) =
            broadcast::channel(BROADCAST_CAPACITY);

        Ok(Self {
            engine: Arc::new(RwLock::new(engine)),
            broadcaster,
        })
    }

    /// The engine, for a read.
    ///
    /// See [`Database::engine_mut`] for why this is a method rather than
    /// twenty copies of `.expect("storage engine lock poisoned")`.
    pub fn engine(&self) -> RwLockReadGuard<'_, StorageEngine> {
        self.engine.read().unwrap_or_else(|_| poisoned())
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
    pub fn engine_mut(&self) -> RwLockWriteGuard<'_, StorageEngine> {
        self.engine.write().unwrap_or_else(|_| poisoned())
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
    pub fn publish(&self, audience: Audience, payload: String) {
        /*
         * There may be no subscribers. broadcast::Sender::send()
         * returns an error in that case, but that does not mean the
         * database operation failed.
         *
         * Events are intentionally best-effort notifications.
         */
        let _ = self.broadcaster.send(LiveEvent { payload, audience });
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
