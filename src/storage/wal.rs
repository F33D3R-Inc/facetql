use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::config;
use crate::core::edge::{Edge, EdgeId};
use crate::core::history::HistoryEntry;
use crate::core::node::Node;
use crate::core::user::UserRecord;
use crate::crypto;
use crate::storage::index::IndexDef;

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
///     magic(4) + frame_version(2) + payload_len(4) + payload_crc(4)
const WAL_FRAME_HEADER_LEN: usize = 14;

/// Largest encrypted payload a single WAL frame may carry, in bytes.
///
/// This bound is enforced in BOTH directions, and the two directions
/// protect against two different failures:
///
///   * `encode_frame` refuses to *write* a payload above this size, so
///     one runaway record (a pathological node blob) can never be made
///     durable in a shape that a later reader would have to reject —
///     the write fails loudly at the moment the caller can still do
///     something about it, instead of bricking the next startup.
///
///   * `decode_frame` treats a header *declaring* more than this as
///     `Corrupt`, and does so before allocating or slicing anything
///     sized by that declaration. A length prefix is the one field a
///     reader must trust before it has verified anything, so a
///     corrupted or hostile 4 GiB length must not be able to steer this
///     process into an unbounded allocation. The bound turns that class
///     of attack into an ordinary integrity failure.
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
///     offset  size  field
///     ------  ----  ---------------------------------------------
///     0       4     MAGIC = b"FQW1"
///     4       2     FRAME_VERSION (u16, little-endian)
///     6       4     PAYLOAD_LEN   (u32, little-endian)
///     10      4     PAYLOAD_CRC32 (u32, little-endian, over PAYLOAD)
///     14      N     PAYLOAD = AES-256-GCM(bincode(WalRecord))
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
///     serialize
///        ↓
///     encrypt/authenticate
///        ↓
///     frame (magic + version + length + CRC32)
///        ↓
///     append frame + '\n' as ONE write
///        ↓
///     sync_data()
///        ↓
///     return Ok(())
///
/// Once this function returns successfully, the WAL record has been
/// handed to the filesystem's durable-data synchronization boundary.
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
///     admit a window where the frame is durable but its terminator is
///     not; one write narrows the loss of the newline to the same tear
///     that would have damaged the frame bytes — which the frame's own
///     length and CRC already catch.
///
///   * If the file does not already end on a newline, we emit one
///     first. Reaching that state requires a tear at exactly the last
///     byte, but it is the one state in which a following append would
///     splice onto a previous record. A separator turns that
///     never-should-happen case into an empty line, which the reader
///     skips, instead of into corruption that ends recovery forever.
///
/// This allocates no sequence, operation or transaction ID: the caller
/// owns the record, so a failure here burns identifiers the caller
/// already took (see `next_sequence` on why those gaps are correct).
pub fn append(
    record: &WalRecord,
) -> io::Result<()> {
    let encoded = encode_frame(record)?;

    let mut file =
        OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(wal_path())?;

    /*
     * Splice guard.
     *
     * In append mode every write lands at end-of-file regardless of the
     * seek position, so probing the last byte here is safe and costs one
     * seek plus one 1-byte read — nothing next to the fsync below.
     */
    let existing_len = file.metadata()?.len();

    let mut line =
        Vec::with_capacity(encoded.len() + 2);

    if existing_len > 0 {
        file.seek(SeekFrom::End(-1))?;

        let mut last = [0u8; 1];

        file.read_exact(&mut last)?;

        if last[0] != b'\n' {
            line.push(b'\n');
        }
    }

    line.extend_from_slice(encoded.as_bytes());
    line.push(b'\n');

    file.write_all(&line)?;

    /*
     * Explicit durability boundary.
     *
     * We don't acknowledge the WAL append until the data has been
     * synchronized. `sync_data` (not `sync_all`) is deliberate: the
     * file's length metadata is updated by the append itself and the
     * data is what must survive; the directory entry already exists.
     */
    file.sync_data()?;

    Ok(())
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
///     AEAD authenticated and its record deserialized. Replayable.
///
///   * `Torn` — the frame is structurally short. This is what a crash
///     mid-`append` looks like and it is tolerable in exactly ONE
///     position: the last non-empty line. There it is the crash point,
///     and the caller drops it with `truncate_torn_tail`. Anywhere else
///     it means a short frame was later written *past* — mid-log
///     corruption, not a crash tail — and we refuse to guess which side
///     of it is real.
///
///   * `Corrupt` — the frame is structurally complete but failed
///     integrity (CRC, authentication, version, or an impossible
///     length). This is bit-rot or tampering, never a clean crash, so it
///     is ALWAYS an error — including at the tail. Silently truncating
///     here would turn "your disk is lying to you" into "some writes
///     quietly vanished", which is the one outcome a WAL exists to
///     prevent. The operator gets told.
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
