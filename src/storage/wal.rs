use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};

use crate::config;
use crate::core::edge::{Edge, EdgeId};
use crate::core::history::HistoryEntry;
use crate::core::node::Node;
use crate::core::user::UserRecord;
use crate::crypto;
use crate::storage::index::IndexDef;
use crate::storage::reference::ReferenceDef;
use crate::storage::text::TextIndexDef;

/// Current on-disk WAL record format.
///
/// Increment this when the serialized WAL representation changes in a
/// way that is not backwards-compatible.
///
/// v2 → v3: [`WalOperation::DeleteEdge`] was added, and added *beside*
/// `InsertEdge` rather than at the end of the enum, so every variant
/// after it moved by one bincode tag. A v2 record read by this build
/// would therefore decode an `InsertUser` as a `DeleteEdge` (or fail
/// outright) — which is exactly the "not backwards-compatible" case this
/// constant exists for. `recovery::validate_records` rejects a record
/// carrying any other version before it can be replayed, so a stale WAL
/// stops startup with a version complaint instead of replaying the wrong
/// mutations.
pub const WAL_FORMAT_VERSION: u16 = 3;

/// Reserved transaction ID for standalone operations.
///
/// Standalone operations don't need BEGIN/COMMIT framing because they
/// represent one complete mutation.
pub const STANDALONE_TRANSACTION_ID: u64 = 0;

/// Magic bytes that begin every on-disk WAL frame.
///
/// The magic lets recovery recognise the start of a well-formed frame and
/// distinguish a structurally-present frame from a torn trailing write.
pub const WAL_FRAME_MAGIC: [u8; 4] = *b"FQW1";

/// On-disk frame envelope version.
///
/// This is intentionally separate from `WAL_FORMAT_VERSION`: the record
/// layout (the bincode payload) and the outer frame envelope evolve
/// independently. Bump this only when the frame header layout changes.
pub const WAL_FRAME_VERSION: u16 = 1;

/// Size of the fixed frame header, in bytes:
///
/// ```text
///     magic(4) + frame_version(2) + payload_len(4) + payload_crc(4)
/// ```
const WAL_FRAME_HEADER_LEN: usize = 14;

/// Largest encrypted payload a single WAL frame may carry, in bytes.
///
/// This bound is enforced in BOTH directions, and the two directions
/// protect against two different failures:
///
///   * `encode_frame` refuses to *write* a payload above this size, so
/// ```text
///     one runaway record (a pathological node blob) can never be made
///     durable in a shape that a later reader would have to reject —
///     the write fails loudly at the moment the caller can still do
///     something about it, instead of bricking the next startup.
/// ```
///
///   * `decode_frame` treats a header *declaring* more than this as
/// ```text
///     `Corrupt`, and does so before allocating or slicing anything
///     sized by that declaration. A length prefix is the one field a
///     reader must trust before it has verified anything, so a
///     corrupted or hostile 4 GiB length must not be able to steer this
///     process into an unbounded allocation. The bound turns that class
///     of attack into an ordinary integrity failure.
/// ```
///
/// 64 MiB is far above any legitimate single-record payload while
/// staying comfortably allocatable.
pub const MAX_WAL_PAYLOAD_LEN: usize = 64 * 1024 * 1024;

/// Durability classification of a single on-disk WAL frame.
///
/// These are the explicit, code-level durability states recovery reasons
/// about. Every non-empty WAL line decodes into exactly one of them.
#[derive(Debug)]
pub enum FrameOutcome {
    /// The frame is fully present, its checksum verified, and its record
    /// authentic. This operation is durable and may be replayed.
    Durable(WalRecord),

    /// The frame is structurally incomplete — bad hex, a short header, or
    /// fewer payload bytes than the header declares. This is the exact
    /// signature of a crash that tore the final append. It is only ever
    /// safe to discard as the *trailing* frame (the crash point).
    Torn(String),

    /// The frame is structurally complete (full declared length present)
    /// but failed integrity — checksum mismatch, failed authentication,
    /// or an undecodable record. Consistent with bit-rot or tampering
    /// rather than a clean crash tail.
    Corrupt(String),
}

/// CRC-32 (IEEE 802.3, reflected, polynomial 0xEDB88320).
///
/// Implemented inline to keep the WAL frame self-checksumming without
/// pulling in an external crate — the same "one less dependency" stance
/// the crypto module's hand-written hex codec takes.
pub fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;

    for &byte in data {
        crc ^= byte as u32;

        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }

    !crc
}

/// Serialize, authenticate, checksum, and frame one WAL record into the
/// single hex-encoded line that is appended to the WAL file.
///
/// On-disk frame layout (before hex-encoding the whole line):
///
/// ```text
///     offset  size  field
///     ------  ----  ---------------------------------------------
///     0       4     MAGIC = b"FQW1"
///     4       2     FRAME_VERSION (u16, little-endian)
///     6       4     PAYLOAD_LEN   (u32, little-endian)
///     10      4     PAYLOAD_CRC32 (u32, little-endian, over PAYLOAD)
///     14      N     PAYLOAD = AES-256-GCM(bincode(WalRecord))
/// ```
///
/// The explicit length + CRC let recovery detect a torn trailing frame
/// structurally, before ever trusting its contents.
pub fn encode_frame(
    record: &WalRecord,
) -> io::Result<String> {
    let bytes = bincode::serialize(record)
        .map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                e,
            )
        })?;

    let payload = crypto::encrypt(&bytes);

    /*
     * Refuse to write what we would refuse to read.
     *
     * A frame larger than the reader's bound would be durable but
     * permanently unreadable — every subsequent startup would classify
     * it as Corrupt and refuse to recover. Failing here keeps the
     * failure inside the caller's mutation, where it is still a
     * recoverable error rather than an unrecoverable log.
     */
    if payload.len() > MAX_WAL_PAYLOAD_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "WAL payload of {} bytes exceeds the maximum of {MAX_WAL_PAYLOAD_LEN} bytes",
                payload.len(),
            ),
        ));
    }

    let payload_len = u32::try_from(payload.len())
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "WAL payload exceeds u32 length",
            )
        })?;

    let crc = crc32(&payload);

    let mut frame =
        Vec::with_capacity(WAL_FRAME_HEADER_LEN + payload.len());

    frame.extend_from_slice(&WAL_FRAME_MAGIC);
    frame.extend_from_slice(&WAL_FRAME_VERSION.to_le_bytes());
    frame.extend_from_slice(&payload_len.to_le_bytes());
    frame.extend_from_slice(&crc.to_le_bytes());
    frame.extend_from_slice(&payload);

    Ok(crypto::encode_hex(&frame))
}

/// Decode one WAL line into its explicit durability state.
///
/// This never returns an error: an unreadable line is *data*, and the
/// caller (recovery) decides whether a defect is an acceptable torn tail
/// or unacceptable mid-WAL corruption. The order of checks is deliberate
/// so that truncation is classified `Torn` before integrity is judged.
pub fn decode_frame(
    line: &str,
) -> FrameOutcome {
    let raw = match crypto::decode_hex(line) {
        Ok(bytes) => bytes,
        Err(e) => {
            return FrameOutcome::Torn(format!(
                "frame is not valid hex: {e}"
            ));
        }
    };

    if raw.len() < WAL_FRAME_HEADER_LEN {
        return FrameOutcome::Torn(format!(
            "frame header truncated: {} of {} header bytes present",
            raw.len(),
            WAL_FRAME_HEADER_LEN,
        ));
    }

    if raw[0..4] != WAL_FRAME_MAGIC {
        /*
         * The line decoded as hex and is long enough to hold a header,
         * yet does not start with the magic. By far the most likely
         * cause is not bit-rot but *history*: a WAL written before the
         * frame envelope existed, whose lines are bare
         * hex(encrypt(bincode(record))) with no header at all. Say so,
         * because the operator's action differs completely from the
         * bit-rot case — such a log has to be recovered (or checkpointed
         * and removed) by hand, and no amount of restarting will help.
         */
        return FrameOutcome::Corrupt(format!(
            "frame magic mismatch: expected {:?} ({}), found 0x{}. \
             This line carries no frame header, which means the WAL \
             predates the frame envelope (or was written by another \
             tool). It cannot be replayed safely and must be recovered \
             or removed by an operator.",
            String::from_utf8_lossy(&WAL_FRAME_MAGIC),
            crypto::encode_hex(&WAL_FRAME_MAGIC),
            crypto::encode_hex(&raw[0..4]),
        ));
    }

    let frame_version =
        u16::from_le_bytes([raw[4], raw[5]]);

    if frame_version != WAL_FRAME_VERSION {
        return FrameOutcome::Corrupt(format!(
            "unsupported WAL frame version {frame_version}"
        ));
    }

    let payload_len = u32::from_le_bytes([
        raw[6], raw[7], raw[8], raw[9],
    ]) as usize;

    /*
     * Bound the declared length BEFORE it is used for anything.
     *
     * `payload_len` is attacker- and bit-rot-controlled: it is read
     * straight off disk and nothing has been authenticated yet. Checking
     * it here — ahead of the truncation comparison below, and ahead of
     * any allocation or slice sized from it — means a corrupted length
     * prefix can never drive this process into an unbounded allocation,
     * and can never masquerade as a merely-`Torn` tail that recovery
     * would silently discard. An impossible length is an integrity
     * failure, so it is `Corrupt`.
     */
    if payload_len > MAX_WAL_PAYLOAD_LEN {
        return FrameOutcome::Corrupt(format!(
            "frame declares a payload of {payload_len} bytes, above the \
             maximum of {MAX_WAL_PAYLOAD_LEN} bytes"
        ));
    }

    let declared_crc = u32::from_le_bytes([
        raw[10], raw[11], raw[12], raw[13],
    ]);

    let available = raw.len() - WAL_FRAME_HEADER_LEN;

    if available < payload_len {
        return FrameOutcome::Torn(format!(
            "frame payload truncated: {available} of {payload_len} bytes present"
        ));
    }

    if available > payload_len {
        return FrameOutcome::Corrupt(format!(
            "frame has {} trailing byte(s) beyond declared payload length",
            available - payload_len,
        ));
    }

    let payload = &raw[WAL_FRAME_HEADER_LEN..];

    if crc32(payload) != declared_crc {
        return FrameOutcome::Corrupt(
            "frame checksum mismatch".to_string(),
        );
    }

    let plaintext = match crypto::decrypt(payload) {
        Ok(plaintext) => plaintext,
        Err(e) => {
            return FrameOutcome::Corrupt(format!(
                "frame failed authentication/decryption: {e}"
            ));
        }
    };

    let record: WalRecord =
        match bincode::deserialize(&plaintext) {
            Ok(record) => record,
            Err(e) => {
                return FrameOutcome::Corrupt(format!(
                    "frame contains an invalid record: {e}"
                ));
            }
        };

    if record.format_version != WAL_FORMAT_VERSION {
        return FrameOutcome::Corrupt(format!(
            "unsupported WAL record format version {}",
            record.format_version,
        ));
    }

    if record.sequence == 0 {
        return FrameOutcome::Corrupt(
            "record has invalid sequence 0".to_string(),
        );
    }

    if record.operation_id == 0 {
        return FrameOutcome::Corrupt(
            "record has invalid operation ID 0".to_string(),
        );
    }

    FrameOutcome::Durable(record)
}

/// Generates process-local transaction IDs.
///
/// The final durable transaction coordinator will recover the next ID
/// from the WAL rather than relying solely on this process-local counter.
static NEXT_TRANSACTION_ID: AtomicU64 = AtomicU64::new(1);

/// Generates process-local operation IDs.
static NEXT_OPERATION_ID: AtomicU64 = AtomicU64::new(1);

/// Generates process-local sequence numbers.
static NEXT_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalOperation {
    /// A transaction has begun.
    Begin,

    /// A transaction has durably committed.
    Commit,

    /// A transaction has explicitly been aborted.
    Abort,

    /// Archive a previous node state.
    Archive(HistoryEntry),

    /// Insert or replace a node.
    Insert(Node),

    /// Delete a node.
    Delete(String),

    /// Insert an edge.
    InsertEdge(Edge),

    /// Delete the edge with this identity.
    ///
    /// Carries an [`EdgeId`] — `(from, to, kind)` — rather than a whole
    /// `Edge`, because that triple is what an edge *is*; the owner is an
    /// authorization attribute the delete has already been checked
    /// against by the time this record is written, and including it
    /// would let two records name the same edge while comparing
    /// unequal.
    DeleteEdge(EdgeId),

    /// Insert a persistent user.
    InsertUser(UserRecord),

    /// Revoke a persistent user.
    RevokeUser(String),

    /// Declare an index over a `data` field.
    ///
    /// Carries the whole definition rather than a name, because replay
    /// has to be able to re-create the index from the WAL alone — the
    /// definition log it also writes is applied by the same operation,
    /// so a crash between the two must be repairable from this record.
    CreateIndex(IndexDef),

    /// Drop a declared index.
    DropIndex(String),

    /// Declare a reference.
    ///
    /// Carries the whole definition for the same reason `CreateIndex`
    /// does: the definition log it also writes is applied by the same
    /// operation, so a crash between the two must be repairable from
    /// this record alone.
    CreateReference(ReferenceDef),

    /// Drop a declared reference.
    DropReference(String),

    /// Declare an inverted index over a `data` field's text.
    ///
    /// Carries the whole definition for the reason `CreateIndex` does.
    ///
    /// Appended **at the end** of this enum rather than beside
    /// `CreateIndex`, deliberately: bincode tags variants by position, so
    /// inserting one in the middle renumbers every variant after it and
    /// makes an existing WAL decode into the wrong mutations — the exact
    /// mistake that forced the v2 → v3 bump documented on
    /// [`WAL_FORMAT_VERSION`]. Appending leaves every existing tag where
    /// it is, so a WAL written before this variant existed still replays
    /// correctly and the format version does not have to move.
    CreateTextIndex(TextIndexDef),

    /// Drop a declared inverted index.
    DropTextIndex(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalRecord {
    /// On-disk format version.
    pub format_version: u16,

    /// Globally ordered WAL sequence number.
    pub sequence: u64,

    /// Transaction this record belongs to.
    ///
    /// Zero means a standalone operation.
    pub transaction_id: u64,

    /// Unique operation identifier.
    ///
    /// BEGIN/COMMIT/ABORT records also receive operation IDs so every
    /// WAL record can be uniquely identified.
    pub operation_id: u64,

    /// Actual operation.
    pub operation: WalOperation,
}

impl WalRecord {
    pub fn new(
        sequence: u64,
        transaction_id: u64,
        operation_id: u64,
        operation: WalOperation,
    ) -> Self {
        Self {
            format_version: WAL_FORMAT_VERSION,
            sequence,
            transaction_id,
            operation_id,
            operation,
        }
    }

    /// Create a BEGIN record.
    pub fn begin(
        transaction_id: u64,
    ) -> Self {
        Self::new(
            next_sequence(),
            transaction_id,
            next_operation_id(),
            WalOperation::Begin,
        )
    }

    /// Create a COMMIT record.
    pub fn commit(
        transaction_id: u64,
    ) -> Self {
        Self::new(
            next_sequence(),
            transaction_id,
            next_operation_id(),
            WalOperation::Commit,
        )
    }

    /// Create an ABORT record.
    pub fn abort(
        transaction_id: u64,
    ) -> Self {
        Self::new(
            next_sequence(),
            transaction_id,
            next_operation_id(),
            WalOperation::Abort,
        )
    }

}

/// Allocate the next process-local sequence.
///
/// Sequence persistence across process restarts is handled by scanning
/// the existing WAL during database startup.
///
/// GAPS ARE INTENTIONAL — DO NOT "FIX" THEM.
///
/// A caller allocates a sequence (and an operation ID) *before* the
/// append that would make it durable, so any failed or torn append burns
/// its number: the durable log then reads 7, 8, 10. That is correct and
/// must stay that way. Recovery's contract is that sequences are
/// strictly *increasing*, never that they are contiguous
/// (`recovery::validate_sequence` checks exactly that), because the only
/// way to guarantee contiguity would be to reuse a burned number — and
/// reusing one is precisely what breaks the strictly-increasing
/// invariant when the earlier append actually did reach disk.
///
/// These three atomics are also the single source of truth for their
/// identifiers: `WalRecord::standalone/begin/commit/abort`, the engine's
/// `append_wal`, and the transaction frame all draw from them. Nothing
/// in this module may introduce a second counter, and nothing here
/// allocates a sequence on a path that cannot write it (`append` and
/// `read_all` allocate nothing at all).
pub fn next_sequence() -> u64 {
    NEXT_SEQUENCE.fetch_add(
        1,
        Ordering::Relaxed,
    )
}

/// Allocate the next operation ID.
pub fn next_operation_id() -> u64 {
    NEXT_OPERATION_ID.fetch_add(
        1,
        Ordering::Relaxed,
    )
}

/// Allocate the next transaction ID.
pub fn next_transaction_id() -> u64 {
    NEXT_TRANSACTION_ID.fetch_add(
        1,
        Ordering::Relaxed,
    )
}

/// The oldest position [`crate::storage::changes::scan`] can serve
/// **when the log holds no records at all**.
///
/// A non-empty log states its own horizon — the oldest record still in
/// it — and needs nothing from here. An empty one states nothing, and
/// the two ways to reach an empty log are opposites: a database that has
/// never been written (nothing was lost, so any position is answerable)
/// and a checkpointed log whose records were rotated or removed
/// (everything was lost, so nothing below "now" is answerable). Zero is
/// the first; recovery stamps the second by minting a position from the
/// counter that has already been advanced past every durable identifier
/// — the same trick `EventFeed::new` uses to make a resume token from a
/// previous process refusable.
static SCAN_HORIZON: AtomicU64 = AtomicU64::new(0);

/// Record that nothing at or below `position` can be scanned from the
/// log any more. Monotone: a horizon never moves backwards.
pub fn note_scan_horizon(position: u64) {
    SCAN_HORIZON.fetch_max(position, Ordering::Relaxed);
}

/// The horizon for an empty log. See [`SCAN_HORIZON`].
pub fn scan_horizon() -> u64 {
    SCAN_HORIZON.load(Ordering::Relaxed)
}

/// Advance the process-local counters beyond values already observed
/// in a durable WAL.
///
/// This prevents newly generated identifiers from colliding with IDs
/// recovered from a previous process.
pub fn initialize_counters(
    max_sequence: u64,
    max_transaction_id: u64,
    max_operation_id: u64,
) {
    advance_counter(
        &NEXT_SEQUENCE,
        max_sequence,
    );

    advance_counter(
        &NEXT_TRANSACTION_ID,
        max_transaction_id,
    );

    advance_counter(
        &NEXT_OPERATION_ID,
        max_operation_id,
    );
}

fn advance_counter(
    counter: &AtomicU64,
    observed_max: u64,
) {
    let desired = observed_max.saturating_add(1);

    let mut current = counter.load(
        Ordering::Relaxed,
    );

    while current < desired {
        match counter.compare_exchange(
            current,
            desired,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,

            Err(actual) => {
                current = actual;
            }
        }
    }
}

/// Path of the write-ahead log within the configured data dir.
///
/// Single source of truth for the file name: writer, reader and
/// truncation all resolve it through here, so relocating the data
/// directory (see `config.rs`) can never leave one of them addressing a
/// different file than the others.
pub fn wal_path() -> PathBuf {
    config::data_file("facetql.wal")
}

/// Append one authenticated WAL record as a complete on-disk frame.
///
/// Durability boundary:
///
/// ```text
///     serialize
///        ↓
///     encrypt/authenticate
///        ↓
///     frame (magic + version + length + CRC32)
///        ↓
///     append frame + '\n' as ONE write
///        ↓
///     return Ok(())
/// ```
///
/// **This does not make the record durable, and that is deliberate.**
/// A record's durability boundary belongs to its *transaction*, not to
/// the record: a framed mutation becomes durable when its frame's
/// COMMIT is synced ([`commit`]), and a standalone mutation — which is
/// its own whole transaction — is flushed by the [`sync_pending`] its
/// entry point runs after releasing the writer lock. A record that
/// reaches disk without its COMMIT is discarded by recovery, so syncing
/// it separately guarantees nothing that the COMMIT's own sync does not
/// already guarantee, and costs an `fsync` to guarantee it.
///
/// WHY THE FRAME MATTERS HERE
///
/// The line this writes is no longer bare `hex(encrypt(bincode(..)))`;
/// it is `hex(FRAME)` (see `encode_frame`). The difference is entirely
/// about what a *crash in the middle of this function* leaves behind.
///
/// Without the envelope, a torn append leaves a partial hex line that no
/// reader can tell apart from a whole one, and the next process — which
/// opens in append mode and writes straight onto the end of that partial
/// line — splices two half-records into a single line. The recovery path
/// itself manufactures unrecoverable mid-log corruption. With the
/// envelope, the frame carries its own declared length and CRC, so a
/// short tail is *structurally* recognisable as torn (`FrameOutcome::
/// Torn`) before its contents are trusted, and a splice shows up as
/// bytes beyond the declared length (`Corrupt`) rather than as a
/// plausible record.
///
/// Two deliberate details of the write itself:
///
///   * The frame and its newline go out in ONE `write_all`. Two writes
/// ```text
///     admit a window where the frame is durable but its terminator is
///     not; one write narrows the loss of the newline to the same tear
///     that would have damaged the frame bytes — which the frame's own
///     length and CRC already catch.
/// ```
///
///   * If the file does not already end on a newline, we emit one
/// ```text
///     first. Reaching that state requires a tear at exactly the last
///     byte, but it is the one state in which a following append would
///     splice onto a previous record. A separator turns that
///     never-should-happen case into an empty line, which the reader
///     skips, instead of into corruption that ends recovery forever.
/// ```
///
/// This allocates no sequence, operation or transaction ID: the caller
/// owns the record, so a failure here burns identifiers the caller
/// already took (see `next_sequence` on why those gaps are correct).
pub fn append(record: &WalRecord) -> io::Result<()> {
    let encoded = encode_frame(record)?;

    with_handle(|handle| handle.write_line(&encoded))?;

    APPENDED.fetch_max(record.sequence, Ordering::Relaxed);

    Ok(())
}

/// Highest sequence written to the log, durable or not.
static APPENDED: AtomicU64 = AtomicU64::new(0);

/// Completed `fsync` calls, and records covered by them.
///
/// The ratio is what group commit is: `records / flushes` is the average
/// number of writers that shared one flush. One means no grouping.
static FLUSHES: AtomicU64 = AtomicU64::new(0);
static FLUSHED_RECORDS: AtomicU64 = AtomicU64::new(0);

/// `(flushes, records covered)` since the process started.
pub fn flush_stats() -> (u64, u64) {
    (
        FLUSHES.load(Ordering::Relaxed),
        FLUSHED_RECORDS.load(Ordering::Relaxed),
    )
}

/// Coalescing state for [`sync_pending`].
struct SyncState {
    /// Highest sequence a completed `fsync` has covered.
    durable: u64,
    /// A thread is inside `sync_data` right now.
    syncing: bool,
}

fn sync_state() -> &'static (Mutex<SyncState>, Condvar) {
    static STATE: OnceLock<(Mutex<SyncState>, Condvar)> = OnceLock::new();

    STATE.get_or_init(|| {
        (
            Mutex::new(SyncState {
                durable: 0,
                syncing: false,
            }),
            Condvar::new(),
        )
    })
}

/// Make everything appended so far durable — sharing one `fsync` with
/// every other writer waiting for the same thing.
///
/// # Group commit
///
/// An `fsync` costs the same whether it flushes one record or a hundred:
/// on this project's own hardware, 7–24 ms either way. Paying it once per
/// writer therefore caps durable writes at roughly `1 / fsync` no matter
/// how many cores or clients there are, and no matter how much work each
/// writer did.
///
/// So the writers share it. The first one in becomes the syncer; the
/// others wait on the condvar, and when the syncer finishes they find
/// their own records already covered — because a single `fsync` flushes
/// the file's pending data, not merely the caller's share of it. `N`
/// concurrent writers cost one `fsync` between them instead of `N`.
///
/// This is only reachable because the engine's writer mutex is released
/// before this is called. While the fsync happened *inside* the mutex
/// there was never a second writer waiting to group with — which is why
/// this could not be built until reads stopped taking that lock.
pub fn sync_pending() -> io::Result<()> {
    let (mutex, condvar) = sync_state();

    let target = APPENDED.load(Ordering::Relaxed);
    let mut state = mutex.lock().unwrap_or_else(|e| e.into_inner());

    loop {
        if state.durable >= target {
            return Ok(());
        }

        if state.syncing {
            // Someone else is flushing. Their fsync may or may not cover
            // us — it covers whatever had been appended when it started
            // — so re-check the condition rather than assuming.
            state = condvar
                .wait(state)
                .unwrap_or_else(|e| e.into_inner());
            continue;
        }

        // Become the syncer. `covered` is read before the flush and
        // published after it: an fsync makes durable everything written
        // before it began, and claiming more than that would mark a
        // record durable that the flush had not yet seen.
        state.syncing = true;
        let covered = APPENDED.load(Ordering::Relaxed);
        drop(state);

        // Take the descriptor under the handle lock, flush outside it.
        // `sync_data`, not `sync_all`: the WAL's length is not metadata
        // a reader depends on — a frame carries its own length — so the
        // size does not have to be flushed alongside the bytes.
        let outcome = with_handle(|handle| Ok(handle.file()))
            .and_then(|file| file.sync_data());

        let mut state = mutex.lock().unwrap_or_else(|e| e.into_inner());
        state.syncing = false;

        if outcome.is_ok() {
            let gained = covered.saturating_sub(state.durable);

            state.durable = state.durable.max(covered);

            FLUSHES.fetch_add(1, Ordering::Relaxed);
            FLUSHED_RECORDS.fetch_add(gained, Ordering::Relaxed);
        }

        // Every waiter has to re-evaluate, including on failure: a
        // failed flush leaves them waiting for a syncer that has gone.
        condvar.notify_all();

        outcome?;

        return Ok(());
    }
}

/// Reset the group-commit bookkeeping. Called when the log file itself is
/// replaced, since sequence numbers no longer describe the same bytes.
fn reset_sync_state() {
    let (mutex, condvar) = sync_state();
    let mut state = mutex.lock().unwrap_or_else(|e| e.into_inner());

    state.durable = 0;
    state.syncing = false;

    condvar.notify_all();
}

/// Drop the cached file handle, so the next append reopens the log.
///
/// Anything that replaces the WAL *file* rather than appending to it —
/// [`rotate`], which renames a rebuilt log over the old one, and
/// [`truncate_torn_tail`], which shortens it during recovery — must call
/// this. A cached handle would otherwise keep writing into the previous
/// inode, or past a tail that no longer exists.
pub fn reset_handle() {
    *handle_slot() = None;

    reset_sync_state();
}

/// The open WAL, kept across appends.
///
/// Before this existed, every single record did its own
/// `open` + `metadata` + `seek` + 1-byte `read` + `write` + `fsync`. The
/// syscalls were not the expensive part; the `fsync` was, and there was
/// one per record. A 500-operation transaction performed 502 of them,
/// serially, while holding the engine's global write lock — which is why
/// a batch measured *slower per record* than the same records written
/// one at a time.
struct WalHandle {
    /// Shared so a flush can run on it without holding the handle lock.
    ///
    /// This is what makes group commit possible rather than merely
    /// coded: the first version kept the `File` by value and flushed it
    /// inside `with_handle`, which held the append lock for the whole
    /// 7 ms `fsync`. No other writer could append during it, so by the
    /// time the flush finished there was never anyone waiting to share
    /// it — measured at 1592 flushes for 1600 concurrent writes. Appends
    /// and the flush now use the same descriptor concurrently, which is
    /// exactly what the flush is for: it makes durable whatever has been
    /// written, and more arriving while it runs is the point.
    file: Arc<File>,

    /// The file does not currently end on a newline, so the next append
    /// has to emit a separator first. Probed once when the handle is
    /// opened; after that it is known, because every line this writes
    /// ends in one.
    needs_separator: bool,

    /// Records have been written since the last `sync_data`.
    unsynced: bool,
}

impl WalHandle {
    fn open() -> io::Result<WalHandle> {
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(wal_path())?;

        // Splice guard, paid once per handle rather than once per record.
        // Reaching a WAL that does not end on a newline requires a tear
        // at exactly the last byte, but it is the one state in which a
        // following append would splice onto a previous record.
        let needs_separator = if file.metadata()?.len() > 0 {
            file.seek(SeekFrom::End(-1))?;

            let mut last = [0u8; 1];
            file.read_exact(&mut last)?;

            last[0] != b'\n'
        } else {
            false
        };

        Ok(WalHandle {
            file: Arc::new(file),
            needs_separator,
            unsynced: false,
        })
    }

    /// The frame and its newline go out in ONE `write_all`. Two writes
    /// admit a window where the frame is durable but its terminator is
    /// not; one write narrows the loss of the newline to the same tear
    /// that would have damaged the frame bytes — which the frame's own
    /// declared length and CRC already catch.
    fn write_line(&mut self, encoded: &str) -> io::Result<()> {
        let mut line = Vec::with_capacity(encoded.len() + 2);

        if self.needs_separator {
            line.push(b'\n');
        }

        line.extend_from_slice(encoded.as_bytes());
        line.push(b'\n');

        // `impl Write for &File` — the descriptor is opened in append
        // mode, so every write lands at end-of-file regardless of what
        // any concurrent flush is doing with the same fd.
        (&*self.file).write_all(&line)?;

        self.needs_separator = false;
        self.unsynced = true;

        Ok(())
    }

    /// The descriptor, for a flush that must not hold the append lock.
    fn file(&self) -> Arc<File> {
        Arc::clone(&self.file)
    }
}

fn handle_slot() -> MutexGuard<'static, Option<WalHandle>> {
    static HANDLE: Mutex<Option<WalHandle>> = Mutex::new(None);

    HANDLE.lock().unwrap_or_else(|e| e.into_inner())
}

fn with_handle<R>(
    act: impl FnOnce(&mut WalHandle) -> io::Result<R>,
) -> io::Result<R> {
    let mut slot = handle_slot();

    if slot.is_none() {
        *slot = Some(WalHandle::open()?);
    }

    act(slot.as_mut().expect("just opened"))
}

// ---------------------------------------------------------------------
// READING THE LOG BACK
//
// Everything below is the reader half of the envelope above. Recovery
// consumes this API and nothing else — it must never re-derive the
// on-disk shape for itself, because the whole point of the frame is
// that exactly one piece of code decides what "durable" means.
// ---------------------------------------------------------------------

/// The durability state of the WAL file's final bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalTail {
    /// The file ends exactly on a frame boundary.
    Clean,

    /// The file ends in a structurally incomplete frame — the signature
    /// of a crash mid-append. `durable_len` is the byte length of the
    /// fully-durable prefix (i.e. the offset the file must be truncated
    /// to before any further append).
    Torn { durable_len: u64, detail: String },
}

/// Read every durable WAL record in file order, plus the tail state.
/// Returns `(Vec::new(), WalTail::Clean)` when the file does not exist.
///
/// THE THREE OUTCOMES, AND WHY THEY ARE TREATED DIFFERENTLY
///
///   * `Durable` — the frame's length matched, its CRC matched, its
/// ```text
///     AEAD authenticated and its record deserialized. Replayable.
/// ```
///
///   * `Torn` — the frame is structurally short. This is what a crash
/// ```text
///     mid-`append` looks like and it is tolerable in exactly ONE
///     position: the last non-empty line. There it is the crash point,
///     and the caller drops it with `truncate_torn_tail`. Anywhere else
///     it means a short frame was later written *past* — mid-log
///     corruption, not a crash tail — and we refuse to guess which side
///     of it is real.
/// ```
///
///   * `Corrupt` — the frame is structurally complete but failed
/// ```text
///     integrity (CRC, authentication, version, or an impossible
///     length). This is bit-rot or tampering, never a clean crash, so it
///     is ALWAYS an error — including at the tail. Silently truncating
///     here would turn "your disk is lying to you" into "some writes
///     quietly vanished", which is the one outcome a WAL exists to
///     prevent. The operator gets told.
/// ```
///
/// Offsets are tracked as real file offsets — the running sum of every
/// line's bytes including its `\n` — so a returned `durable_len` can be
/// handed straight to `truncate_torn_tail`.
pub fn read_all() -> io::Result<(Vec<WalRecord>, WalTail)> {
    let path = wal_path();

    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,

        /*
         * A missing WAL is a legitimate state: first boot, or a
         * checkpointed log an operator removed. It is not an error.
         */
        Err(e)
            if e.kind() == io::ErrorKind::NotFound =>
        {
            return Ok((
                Vec::new(),
                WalTail::Clean,
            ));
        }

        Err(e) => return Err(e),
    };

    let mut records: Vec<WalRecord> = Vec::new();

    /*
     * Byte offset of the start of the line currently being examined.
     * Every branch below advances this by the line's own bytes plus its
     * terminator, so it stays a true file offset even across skipped
     * blank lines.
     */
    let mut offset: u64 = 0;

    /*
     * A torn frame is provisional: it is only acceptable once we know
     * nothing non-empty follows it. Hold it here until end of file.
     */
    let mut torn: Option<(u64, usize, String)> = None;

    let segments: Vec<&[u8]> =
        bytes.split(|&b| b == b'\n').collect();

    /*
     * `split` yields one more segment than there are newlines, so the
     * final segment is the bytes after the last '\n' — empty when the
     * file ends on a newline. Only that final segment lacks a
     * terminator, which is what the `+ 1` below encodes.
     */
    let last_index = segments.len() - 1;

    for (index, segment) in segments.iter().enumerate() {
        let line_start = offset;

        let line_number = index + 1;

        offset += segment.len() as u64
            + if index < last_index { 1 } else { 0 };

        /*
         * Our own writer emits nothing but hex and '\n'. Bytes outside
         * UTF-8 therefore cannot come from a torn append of a frame we
         * wrote — something else damaged or wrote this file — so it is
         * reported rather than absorbed as a torn tail.
         */
        let text = match std::str::from_utf8(segment) {
            Ok(text) => text,

            Err(e) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "WAL line {line_number} at byte offset \
                         {line_start} is not valid UTF-8 ({e}); the log \
                         contains bytes no WAL writer produces and must \
                         be recovered or removed by an operator"
                    ),
                ));
            }
        };

        let text = text.trim();

        /*
         * Blank lines carry no record but do carry bytes: they still
         * advanced `offset` above. They are produced by the writer's
         * splice guard and by an operator's editor, and are harmless.
         */
        if text.is_empty() {
            continue;
        }

        /*
         * A torn frame with anything real after it is not a crash tail.
         * Checked before decoding this line so the error names the line
         * that proved it, whatever that line turns out to contain.
         */
        if let Some((_, torn_line, detail)) = &torn {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "WAL line {torn_line} is a torn frame ({detail}) but \
                     line {line_number} follows it; a torn frame is only \
                     valid as the final line, so this is mid-log \
                     corruption rather than a crash tail and must be \
                     recovered by an operator"
                ),
            ));
        }

        match decode_frame(text) {
            FrameOutcome::Durable(record) => {
                records.push(record);
            }

            FrameOutcome::Torn(detail) => {
                /*
                 * Provisional. `line_start` — the offset at the START of
                 * this line — is the length of the fully-durable prefix,
                 * because everything before it was a complete frame and
                 * nothing from this line can be trusted.
                 */
                torn = Some((
                    line_start,
                    line_number,
                    detail,
                ));
            }

            FrameOutcome::Corrupt(detail) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "WAL line {line_number} at byte offset \
                         {line_start} is corrupt: {detail}"
                    ),
                ));
            }
        }
    }

    /*
     * A final line with no trailing '\n' that decoded Durable IS
     * durable. The frame verified its own declared length and CRC, so
     * the only way the newline could be missing while the frame is
     * whole is a tear at the very last byte — and `append`'s splice
     * guard makes that state safe to append onto anyway.
     */
    match torn {
        Some((durable_len, _, detail)) => Ok((
            records,
            WalTail::Torn {
                durable_len,
                detail,
            },
        )),

        None => Ok((records, WalTail::Clean)),
    }
}

/// Physically drop a torn trailing frame by truncating the WAL to
/// `durable_len`, then fsync so the truncation itself is durable.
///
/// WHY THE DIRECTORY FSYNC
///
/// `set_len` changes the file's *metadata* (its length), and
/// `sync_all` on the file forces that file's data and metadata out. But
/// on POSIX filesystems the guarantee that a size change is visible
/// after a crash also depends on the directory entry that names the
/// file being flushed: the inode update and the directory block can be
/// journalled independently. If we skipped it, a crash immediately after
/// truncation could leave the WAL back at its old length — with the torn
/// frame restored, and now with fresh appends written after it. That is
/// the exact mid-log corruption `read_all` refuses to replay, so the
/// truncation must be as durable as the appends it is protecting.
pub fn truncate_torn_tail(
    durable_len: u64,
) -> io::Result<()> {
    // A cached handle would still believe the log ends where it did
    // before the truncation, including whether it ends on a newline.
    reset_handle();

    let path = wal_path();

    let file = OpenOptions::new()
        .write(true)
        .open(&path)?;

    file.set_len(durable_len)?;

    /*
     * `sync_all`, not `sync_data`: the length IS metadata here, so
     * flushing data alone would not persist the truncation.
     */
    file.sync_all()?;

    let directory = match path.parent() {
        Some(parent) => parent.to_path_buf(),
        None => PathBuf::from("."),
    };

    /*
     * Opening a directory read-only and fsyncing the handle is the
     * standard POSIX way to force a metadata change out; there is
     * nothing to write to it.
     */
    let directory = File::open(directory)?;

    directory.sync_all()?;

    Ok(())
}

/// Append BEGIN.
pub fn begin(
    transaction_id: u64,
) -> io::Result<WalRecord> {
    if transaction_id == STANDALONE_TRANSACTION_ID {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "transaction ID 0 is reserved for standalone operations",
        ));
    }

    let record =
        WalRecord::begin(
            transaction_id,
        );

    append(&record)?;

    Ok(record)
}

/// Append COMMIT.
///
/// COMMIT is itself a durable WAL record. A transaction must never be
/// considered committed merely because its mutation records exist.
pub fn commit(
    transaction_id: u64,
) -> io::Result<WalRecord> {
    if transaction_id == STANDALONE_TRANSACTION_ID {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "transaction ID 0 cannot be committed",
        ));
    }

    let record =
        WalRecord::commit(
            transaction_id,
        );

    // The COMMIT record is written here and made durable by
    // `sync_pending`, which the mutation entry point calls after
    // releasing the writer lock. Both halves matter:
    //
    // * one flush per *frame* rather than per record, because an fsync
    //   makes durable everything written before it — so BEGIN, every
    //   staged mutation and this COMMIT go together; and
    // * one flush per *group of writers* rather than per frame, because
    //   the flush happens outside the lock where concurrent writers can
    //   share it.
    //
    // Neither is a shortcut: recovery commits a frame only when it finds
    // BEGIN, the mutations and COMMIT, and discards it otherwise, so a
    // frame that was appended but not yet flushed is simply not a
    // transaction yet — which is exactly true, since its caller has not
    // been told it committed.
    append(&record)?;

    Ok(record)
}

/// Append ABORT.
pub fn abort(
    transaction_id: u64,
) -> io::Result<WalRecord> {
    if transaction_id == STANDALONE_TRANSACTION_ID {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "transaction ID 0 cannot be aborted",
        ));
    }

    let record =
        WalRecord::abort(
            transaction_id,
        );

    // Deliberately not synced. An ABORT that does not survive a crash
    // leaves a frame with no durable COMMIT, and recovery discards such
    // a frame whether or not it is marked aborted — the outcome is the
    // same, so paying an fsync to reach it is not worth it.
    append(&record)?;

    Ok(record)
}
// ---------------------------------------------------------------------
// BOUNDING THE LOG
// ---------------------------------------------------------------------

/// Default size at which a checkpoint will rewrite the WAL, in bytes.
///
/// The WAL is append-only and, until this existed, was never shortened:
/// a long-lived database grew one indefinitely, and since `read_all`
/// reads the whole file into memory before replaying it, startup cost —
/// in both time and RAM — grew with the *lifetime* of the database
/// rather than with its size. That is the one resource in this engine an
/// ordinary, entirely legitimate workload exhausts on its own.
///
/// 64 MiB is well above the volume a single checkpoint interval
/// produces, so rotation is a rare event rather than something every
/// checkpoint pays for, and it is small enough to read back at startup
/// without thinking about it.
const DEFAULT_ROTATE_BYTES: u64 = 64 * 1024 * 1024;

const ROTATE_BYTES_ENV: &str = "FACETQL_WAL_ROTATE_BYTES";

/// The configured rotation threshold.
pub fn rotate_threshold() -> u64 {
    std::env::var(ROTATE_BYTES_ENV)
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|bytes| *bytes > 0)
        .unwrap_or(DEFAULT_ROTATE_BYTES)
}

/// Current size of the WAL on disk, or 0 when it does not exist.
pub fn size() -> io::Result<u64> {
    match fs::metadata(wal_path()) {
        Ok(meta) => Ok(meta.len()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(0),
        Err(e) => Err(e),
    }
}

/// Rewrite the WAL so it holds only the records a future recovery would
/// still replay — those with `sequence > durable_checkpoint`.
///
/// # Why this is safe to drop records
///
/// Recovery replays exactly `sequence > checkpoint`, and the checkpoint
/// only advances once the heap, the catalog and every index are fsync'd
/// (`StorageEngine::checkpoint`). A record at or below it is therefore
/// already reflected in physical storage by definition; keeping it costs
/// disk and startup time and buys nothing. This is the standard
/// checkpoint-then-truncate cycle, done as a whole-file rewrite because
/// a POSIX file cannot have a prefix removed in place.
///
/// # Why it is a rewrite and not a `set_len(0)`
///
/// Emptying the file would also discard the records *above* the
/// checkpoint — the ones a crash still needs — and would discard an open
/// transaction frame's staged records, which the checkpoint fence
/// deliberately keeps the boundary below. Those are carried over
/// verbatim (re-framed, since a frame is written as one hex line), in
/// their original order and with their original sequence numbers, so the
/// log a later recovery reads is byte-for-byte equivalent in meaning to
/// the tail it replaces.
///
/// # Crash safety
///
/// Temp file → `sync_all` → `rename` → `sync_all` the directory: the
/// same protocol the catalog and the checkpoint use. A crash at any
/// point leaves either the full old log or the rewritten one, never a
/// partial file — and either one recovers to the same state, because the
/// records the rewrite dropped are the ones recovery would have skipped.
///
/// # Refusals
///
/// A torn tail is left alone. Recovery repairs a tear at startup, before
/// anything else, so seeing one here means the file changed underneath a
/// running process; rewriting it would be guessing about bytes we have
/// no explanation for. Returns the number of records carried over.
pub fn rotate(durable_checkpoint: u64) -> io::Result<usize> {
    let path = wal_path();

    if !path.exists() {
        return Ok(0);
    }

    let (records, tail) = read_all()?;

    if let WalTail::Torn { detail, .. } = tail {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "refusing to rotate the WAL: its tail is torn ({detail}). \
                 A tear is repaired during startup recovery, so one here \
                 means the log changed underneath a running process."
            ),
        ));
    }

    let kept: Vec<&WalRecord> = records
        .iter()
        .filter(|record| record.sequence > durable_checkpoint)
        .collect();

    // Nothing to reclaim: every record is still live. Leaving the file
    // untouched is not just an optimization — rewriting it would burn a
    // full read/write cycle to produce an identical log.
    if kept.len() == records.len() {
        return Ok(kept.len());
    }

    /*
     * The change scan's horizon moves with this, and it has to move
     * *before* the file does: everything about to be dropped stops
     * being answerable the instant the rename lands, and a scan that
     * ran in between would otherwise report a complete answer built on
     * a log it no longer has. The highest dropped position is the
     * boundary — `after=` that value is still honest, because every
     * record above it survives the rewrite.
     *
     * Only consulted when the rewrite leaves the log empty (a checkpoint
     * that covered everything); a log with records left in it states its
     * own horizon. Noting it unconditionally is what makes that case
     * correct without a second code path to forget.
     */
    let dropped = records
        .iter()
        .filter(|record| record.sequence <= durable_checkpoint)
        .map(|record| record.operation_id)
        .max()
        .unwrap_or(0);

    note_scan_horizon(dropped);

    let mut body = Vec::new();

    for record in &kept {
        body.extend_from_slice(encode_frame(record)?.as_bytes());
        body.push(b'\n');
    }

    let tmp = config::data_file(&format!(
        "facetql.wal.{}.{}.tmp",
        std::process::id(),
        NEXT_ROTATION_ID.fetch_add(1, Ordering::Relaxed),
    ));

    {
        let mut file = File::create(&tmp)?;
        file.write_all(&body)?;
        // sync_all, not sync_data: the rename below publishes this inode
        // and its length has to be durable with it, or a crash leaves
        // the WAL name pointing at a file whose size never landed.
        file.sync_all()?;
    }

    if let Err(e) = fs::rename(&tmp, &path) {
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }

    sync_parent_dir(&path)?;

    // The cached handle still refers to the inode the rename just
    // unlinked. Anything appended through it would land in a file with
    // no name and be lost at the next restart.
    reset_handle();

    Ok(kept.len())
}

static NEXT_ROTATION_ID: AtomicU64 = AtomicU64::new(0);

/// `fsync` the directory holding `path`, making the rename above
/// durable. Without it the directory entry can survive a crash still
/// naming the pre-rotation inode — which is harmless here (the old log
/// recovers to the same state) but would silently make the rotation a
/// no-op forever on a filesystem that always replays that way.
#[cfg(unix)]
fn sync_parent_dir(path: &std::path::Path) -> io::Result<()> {
    let dir = path.parent().unwrap_or_else(|| std::path::Path::new("."));

    File::open(dir)?.sync_all()
}

#[cfg(not(unix))]
fn sync_parent_dir(_path: &std::path::Path) -> io::Result<()> {
    Ok(())
}
