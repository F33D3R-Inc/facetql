use std::fs::{File, OpenOptions};
use std::io::{Error, ErrorKind, Read, Seek, SeekFrom, Write};
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use crate::core::edge::{Edge, EdgeId};
use crate::core::node::Node;
use crate::core::user::UserRecord;
use crate::config;
use crate::crypto;

// ─────────────────────────────────────────────────────────────────────
// On-disk record frame
// ─────────────────────────────────────────────────────────────────────
//
// Every persisted record — a node op, an edge op, a user op, a history
// entry — is written as one self-describing frame so that a read can prove a
// record is intact (not truncated, not silently corrupted, not written
// by an incompatible build) BEFORE it is ever handed to `crypto::decrypt`
// / `bincode`. AES-GCM already authenticates the payload on decrypt, but
// that only fires once we've read exactly the right bytes; a torn or
// mis-length-prefixed frame can leave the reader reading the wrong bytes
// entirely. The explicit frame is what makes "torn/partial final record"
// distinguishable from "clean end of file", and mid-file corruption
// distinguishable from both.
//
// Layout (all multi-byte integers little-endian):
//
//   offset  size  field
//   ------  ----  -----------------------------------------------------
//   0       4     MAGIC marker bytes ("FQR1")
//   4       1     format version byte
//   5       4     payload length (u32) — length of the encrypted payload
//   9       4     CRC-32 (IEEE) of the encrypted payload (u32)
//   13      N     payload = encrypted blob (nonce || ciphertext || tag)
//
// The header is FRAME_HEADER_LEN (13) bytes. The checksum covers exactly
// the on-disk payload bytes, so a bit-flip anywhere in the payload — or a
// wrong length prefix that would slice the payload short — is caught at
// read time, independently of and before decryption.
//
// These constants are `pub` so the WAL/recovery layer can reason about
// the frame (e.g. to physically truncate a torn trailing record) without
// re-deriving the byte math, and so sibling logs reuse this envelope
// instead of inventing their own: `facetql.data`, `.edges`, `.users` and
// `.history` all go through `append_record` / `read_all_records_framed`
// here rather than growing a second framing (and a second CRC) that
// would have to be audited separately for the same torn-tail and
// corruption cases.
//
// ─── What the payload of each log is, and why ────────────────────────
//
// Each of the three mutable logs stores an OPERATION, not a bare value:
// [`NodeRecord`], [`EdgeRecord`] and [`UserOpRecord`] below. That is the
// v2 format change, and it is a correctness fix rather than a tidy-up.
//
// Deletes used to live in a fourth log, `facetql.tombstones`, shared by
// nodes and users. Two append-only logs carry no shared ordering, so
// nothing on disk could say whether a delete happened before or after
// the create it sat next to — and `load()` had no choice but to apply
// every tombstone last. A tombstone therefore always won:
//
//     create X → delete X → create X again → restart → X is gone.
//
// It also made edge deletion unbuildable: a follow/unfollow/follow graph
// cannot be expressed by a permanent tombstone at all.
//
// Folding the delete into the same log as the value it deletes makes
// **file order within that one log the total order for that entity
// type**. Replay is then a straight last-write-wins walk — `Put`
// inserts, `Delete`/`Revoke` removes — with no cross-log reconciliation
// pass to get wrong, and delete-then-recreate falls out for free in
// every entity type at once.

/// Marker bytes at the start of every record frame: "FQR1" =
/// FacetQL Record, frame generation 1. A read that does not find these
/// bytes where a frame is expected treats the file as corrupt rather
/// than guessing at the bytes.
pub const RECORD_MAGIC: [u8; 4] = *b"FQR1";

/// Frame format version.
///
/// Bump this for an incompatible change to the *frame* (header shape /
/// checksum algorithm) **or** to the shape of the payloads the frames in
/// these logs carry — the version byte is the only thing on disk that
/// says how the bytes behind it are meant to be read, and a payload
/// change is exactly as unreadable to an old build as a header change.
///
/// v1 → v2: the mutable logs went from storing bare values (`Node`,
/// `Edge`, `UserRecord`) to storing per-entity operations
/// ([`NodeRecord`], [`EdgeRecord`], [`UserOpRecord`]), and the separate
/// `facetql.tombstones` log was retired. A v1 `facetql.data` frame is
/// structurally valid and would bincode-decode *into something*, which
/// is precisely why the version must be checked: silently reading a v1
/// payload as a v2 `NodeRecord` is worse than refusing to read it.
///
/// A frame carrying any other version is surfaced as an
/// unsupported-version error naming the format change, never decoded on
/// a guess. See [`FrameOutcome::UnsupportedVersion`].
pub const RECORD_FORMAT_VERSION: u8 = 2;

/// Bytes of fixed header in front of every payload: magic(4) +
/// version(1) + payload_len(4) + crc(4).
pub const FRAME_HEADER_LEN: usize = 4 + 1 + 4 + 4;

// ─────────────────────────────────────────────────────────────────────
// Per-log operation records
// ─────────────────────────────────────────────────────────────────────
//
// One enum per append-only log, each carrying that log's own deletes.
// They live here, beside the paths of the files that hold them and the
// version byte that describes them, rather than beside the core types
// they wrap: `Node`/`Edge`/`UserRecord` are the *domain* shapes and know
// nothing about being logged, while these are the *storage* shapes and
// exist only because the log they go in has to be totally ordered.
// Keeping the three together also keeps them honest — they are one
// format decision, and a future entity type gets its own variant here
// rather than reaching for a second tombstone log.
//
// A log's records are replayed in file order and the last one for a key
// wins. There is nothing else to reconcile, so "when did this happen"
// never has to be answered by anything other than the file offset.

/// One operation in `facetql.data`, the node log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeRecord {
    /// Insert or replace the node at `address` (upsert semantics,
    /// matching `StorageEngine::insert`).
    Put(Node),

    /// Remove the node at this address.
    ///
    /// Carries the address rather than the node: a delete is about
    /// identity, and re-writing the whole value would make an
    /// already-append-only log grow by a full record for a removal, as
    /// well as inviting a reader to resurrect the value it names.
    Delete(String),
}

/// One operation in `facetql.edges`, the edge log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EdgeRecord {
    /// Insert or replace the edge with this identity. Replacement, not
    /// duplication: `(from, to, kind)` is the key, so re-asserting an
    /// existing relationship lands on the same edge.
    Put(Edge),

    /// Remove the edge with this identity.
    ///
    /// Carries an [`EdgeId`] and not an [`Edge`] for the same reason
    /// [`NodeRecord::Delete`] carries an address — and because the owner
    /// is deliberately not part of an edge's identity, so a delete that
    /// carried a whole `Edge` would be naming a field the lookup must
    /// ignore.
    Delete(EdgeId),
}

/// One operation in `facetql.users`, the persistent-user log.
///
/// Named `UserOpRecord` rather than `UserRecord` because
/// [`crate::core::user::UserRecord`] already means "a user" — this is
/// "something that happened to a user".
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UserOpRecord {
    /// Create or replace a persistent user.
    Put(UserRecord),

    /// Revoke the user holding this token hash.
    ///
    /// Revocation lives in this log now instead of as a `user:`-prefixed
    /// entry in the shared tombstone log. That prefix existed only to
    /// keep user hashes from colliding with node addresses in a log that
    /// should never have held both; with each entity's deletes in its own
    /// log there are no two key spaces to keep apart, and a revoked token
    /// can be re-issued and revoked again like any other key.
    Revoke(String),
}

/// Largest encrypted payload a single record frame may carry, in bytes.
///
/// The frame header's 4-byte length prefix is the one field a reader has
/// to act on *before* it has verified anything: the CRC cannot be checked
/// until the payload has been read, and the payload cannot be read until
/// a buffer sized by the declared length exists. Validating that length
/// only against "bytes remaining in the file" is not enough — in a large
/// data file (and these are append-only logs that only grow), a corrupt
/// or hostile length prefix can name a value that *does* fit inside the
/// file and still drive a correspondingly large allocation and read for
/// every startup that touches the file.
///
/// So the bound is enforced in BOTH directions, for two different
/// reasons:
///
///   * [`append_record`] refuses to *write* a payload above this size.
///     A record that cannot be read back must never be made durable: it
///     would be a permanently unreadable frame that bricks every
///     subsequent load, instead of an error returned to the caller at a
///     point where the mutation can still be rejected cleanly.
///
///   * [`read_one_frame`] classifies a header *declaring* more than this
///     as [`FrameOutcome::Corrupt`] before allocating anything sized by
///     that declaration, turning an unbounded-allocation vector into an
///     ordinary integrity failure.
///
/// 64 MiB is far above any legitimate single record (a node, edge, user,
/// history entry or edge identity) while staying comfortably
/// allocatable. It deliberately matches `wal::MAX_WAL_PAYLOAD_LEN`: the
/// two logs carry the same mutations, so a record that fits in one must
/// fit in the other, or a write could be accepted by the WAL and then
/// rejected by physical storage after the intent was already durable.
pub const MAX_RECORD_PAYLOAD_LEN: usize = 64 * 1024 * 1024;

/// Outcome of the state of the file *after* the last fully-decoded
/// record, returned alongside the records by [`read_all_records_framed`].
///
/// This is the contract that lets a caller (notably the WAL/recovery
/// layer) tell the two benign terminal states apart from real damage:
///
/// * [`TailState::Clean`] — the file ended exactly on a frame boundary.
///   Every byte was accounted for.
/// * [`TailState::Truncated`] — the file ended *inside* a frame: the
///   trailing header or payload is short. This is the signature of a
///   crash during an append (the write was never acknowledged as
///   durable), and the returned records are every fully-durable record
///   before it. A caller may safely truncate the file back to `offset`.
///
/// Mid-file corruption (bad magic / version / checksum on a frame that is
/// fully present on disk) is NOT represented here: it is returned as an
/// `Err` from the read functions, because unlike a torn tail it cannot be
/// safely recovered from by dropping a trailing record.
#[derive(Debug)]
pub enum TailState {
    Clean,
    Truncated {
        /// Byte offset at which the torn trailing frame begins — the
        /// point a caller would truncate the file back to.
        offset: u64,
        /// Human-readable detail of what was short.
        detail: String,
    },
}

/// Result of attempting to decode a single frame at a known offset.
enum FrameOutcome {
    /// A complete, checksum-verified frame. `consumed` is the total
    /// on-disk size (header + payload), i.e. how far to advance.
    Record { payload: Vec<u8>, consumed: u64 },
    /// The frame is truncated — the file ended before the full header or
    /// the full declared payload was present. Only ever legitimately the
    /// last thing in the file (a torn trailing record).
    Truncated(String),
    /// The frame is fully present on disk but structurally invalid:
    /// wrong magic, or a payload whose checksum does not match. This is
    /// corruption, not a torn tail.
    Corrupt(String),

    /// The frame is intact but was written by a build that used a
    /// different [`RECORD_FORMAT_VERSION`].
    ///
    /// Kept apart from [`FrameOutcome::Corrupt`] because it is not
    /// damage and the operator response is completely different. A
    /// corrupt frame says "this disk, or these bytes, went bad"; an
    /// unsupported version says "these bytes are fine, this build cannot
    /// interpret them" — and telling an operator to go hunting for
    /// hardware faults when the real answer is a format change wastes
    /// exactly the time an unavailable database does not have. Both are
    /// fatal to the read, and neither is ever decoded on a guess: a v1
    /// payload fed to a v2 decoder does not reliably fail, it silently
    /// means something else.
    UnsupportedVersion { found: u8, offset: u64 },
}

/// CRC-32 (IEEE 802.3 / zlib, reflected, polynomial 0xEDB88320).
///
/// Hand-rolled rather than pulling in a `crc` crate — the same stance
/// crypto.rs takes for its hex codec, keeping the dependency surface (and
/// version-drift risk) small for a few lines of well-known logic. The
/// bitwise form needs no lookup table and is more than fast enough for
/// the record sizes this engine writes.
fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            // Branchless: build a mask of all-ones when the low bit is
            // set, all-zeros otherwise, then conditionally XOR the poly.
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

/// The error an unreadable-because-too-old (or too-new) frame is
/// surfaced as, written for the operator staring at a database that will
/// not start.
///
/// It has one job: make sure nobody concludes their disk is failing, and
/// nobody goes looking for a migration that does not exist. The v1 → v2
/// change reshaped what every frame in these logs *contains* (bare
/// values became per-entity operations, and the tombstone log went
/// away), so there is no in-place upgrade — the honest instruction is to
/// recreate the data directory. That instruction is only safe to give
/// because these logs are the whole database: nothing survives
/// underneath them that recreating the directory would orphan.
///
/// The message names the file, both versions and the offset, so an
/// operator can tell "the entire file is from an older build" (offset 0)
/// from the much stranger "one frame partway in is" — which would mean
/// two builds appended to the same file and is a different problem.
fn unsupported_version_error(
    path: &std::path::Path,
    found: u8,
    offset: u64,
) -> Error {
    Error::new(
        ErrorKind::InvalidData,
        format!(
            "{} holds a v{found} record frame at offset {offset}, but this \
             build reads and writes v{RECORD_FORMAT_VERSION}. The on-disk \
             record format changed: each log now stores per-entity \
             operations (put/delete) instead of bare values, and the \
             separate facetql.tombstones log was removed — deletes live in \
             the log of the thing they delete, so file order is the total \
             order and a delete no longer outranks a later re-create. \
             There is no in-place upgrade for this: stop the server and \
             recreate the data directory (or restore a backup taken with a \
             matching build). Nothing here is corrupt — refusing to read \
             is deliberate, because a v{found} payload decoded as \
             v{RECORD_FORMAT_VERSION} would not fail, it would quietly \
             mean the wrong thing.",
            path.display()
        ),
    )
}

/// Wraps an already-encrypted payload in a record frame ready to append.
///
/// Refuses to encode what the read path would refuse to accept: a payload
/// over [`MAX_RECORD_PAYLOAD_LEN`] is rejected here, at the write, rather
/// than becoming a durable frame that every future
/// [`read_all_records_framed`] would have to report as corrupt. The
/// caller still holds the mutation at this point, so this is a
/// recoverable error; a written one would not be.
fn encode_frame(payload: &[u8]) -> std::io::Result<Vec<u8>> {
    if payload.len() > MAX_RECORD_PAYLOAD_LEN {
        return Err(Error::new(
            ErrorKind::InvalidData,
            format!(
                "record payload of {} bytes exceeds the maximum of \
                 {MAX_RECORD_PAYLOAD_LEN} bytes; refusing to write a record \
                 the read path would reject as corrupt",
                payload.len()
            ),
        ));
    }

    // Structural bound of the frame format itself: the length prefix is a
    // u32. MAX_RECORD_PAYLOAD_LEN sits far below this, so the check above
    // fires first — this one stays as the invariant that guards the cast
    // below if that constant is ever raised.
    if payload.len() > u32::MAX as usize {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            format!(
                "record payload of {} bytes exceeds the {}-byte frame limit",
                payload.len(),
                u32::MAX
            ),
        ));
    }

    let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + payload.len());
    frame.extend_from_slice(&RECORD_MAGIC);
    frame.push(RECORD_FORMAT_VERSION);
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&crc32(payload).to_le_bytes());
    frame.extend_from_slice(payload);
    Ok(frame)
}

/// Reads and verifies one frame from `file`, which must already be
/// positioned at `offset`. `file_len` is the total file length, used to
/// tell "truncated" apart from "corrupt" without ever reading past the
/// end. Never decrypts — it only proves the on-disk bytes are a
/// well-formed, checksum-valid frame and returns the raw payload.
fn read_one_frame(
    file: &mut File,
    offset: u64,
    file_len: u64,
) -> std::io::Result<FrameOutcome> {
    let remaining = file_len - offset;

    // Not even a full header left — the header itself is torn.
    if remaining < FRAME_HEADER_LEN as u64 {
        return Ok(FrameOutcome::Truncated(format!(
            "record header at offset {offset} is truncated: {remaining} of \
             {FRAME_HEADER_LEN} header bytes present"
        )));
    }

    let mut header = [0u8; FRAME_HEADER_LEN];
    file.read_exact(&mut header)?;

    if header[0..4] != RECORD_MAGIC {
        return Ok(FrameOutcome::Corrupt(format!(
            "bad record magic at offset {offset}: expected {RECORD_MAGIC:?}, \
             found {:?}",
            &header[0..4]
        )));
    }

    let version = header[4];
    if version != RECORD_FORMAT_VERSION {
        return Ok(FrameOutcome::UnsupportedVersion { found: version, offset });
    }

    let payload_len =
        u32::from_le_bytes(header[5..9].try_into().unwrap()) as u64;
    let stored_crc = u32::from_le_bytes(header[9..13].try_into().unwrap());

    // Absolute cap on the declared length, checked BEFORE anything is
    // sized by it.
    //
    // This check exists specifically so a corrupted (or hostile) 4-byte
    // length prefix cannot drive an unbounded allocation: the `vec![0u8;
    // payload_len]` below is the first place the reader has to trust a
    // number it has not yet been able to verify, because the CRC that
    // would expose the corruption covers the payload the length names.
    //
    // It is deliberately ordered before the length-vs-remaining-bytes
    // check that follows. That check alone is not a bound — it only says
    // the declaration fits inside the file, and these are append-only
    // logs that grow without limit, so on a large file a garbage length
    // can pass it and still name hundreds of megabytes. Ordering also
    // decides the *classification*: an over-cap length is `Corrupt`, not
    // `Truncated`. That is the correct call, because a torn tail is a
    // benign, recoverable crash signature the caller may respond to by
    // discarding the trailing bytes, whereas a length prefix that names
    // more than any record this build will ever write is evidence the
    // header bytes themselves are damaged — the file needs an operator,
    // not a silent truncation.
    if payload_len > MAX_RECORD_PAYLOAD_LEN as u64 {
        return Ok(FrameOutcome::Corrupt(format!(
            "record at offset {offset} declares a payload of {payload_len} \
             bytes, over the {MAX_RECORD_PAYLOAD_LEN}-byte maximum — the \
             length prefix is corrupt (no record this build writes can be \
             that large)"
        )));
    }

    // Is the whole declared payload actually present on disk?
    let payload_available = remaining - FRAME_HEADER_LEN as u64;
    if payload_len > payload_available {
        return Ok(FrameOutcome::Truncated(format!(
            "record payload at offset {offset} is truncated: header declares \
             {payload_len} bytes but only {payload_available} remain"
        )));
    }

    let mut payload = vec![0u8; payload_len as usize];
    file.read_exact(&mut payload)?;

    let actual_crc = crc32(&payload);
    if actual_crc != stored_crc {
        return Ok(FrameOutcome::Corrupt(format!(
            "record checksum mismatch at offset {offset}: stored \
             {stored_crc:#010x}, computed {actual_crc:#010x} — payload corrupted"
        )));
    }

    Ok(FrameOutcome::Record {
        payload,
        consumed: FRAME_HEADER_LEN as u64 + payload_len,
    })
}

/// Appends any serializable record to `path` as one framed, encrypted,
/// checksummed record (see the frame layout at the top of this file) and
/// returns the byte offset the frame was written at.
///
/// The payload the length prefix and checksum cover is the AES-256-GCM
/// blob (nonce + ciphertext + auth tag), not the plaintext — the read
/// side verifies the frame, then decrypts, and neither side needs to know
/// the plaintext size.
///
/// DURABILITY: on a successful return the frame — and the file-length
/// change the append implies — has been flushed to stable storage via
/// `sync_all`. The engine relies on this: it only advances the WAL
/// checkpoint and makes a value visible in memory *after* this returns,
/// so "append_record returned Ok" means "this record is durable". A crash
/// before the return leaves at most a torn trailing frame, which the read
/// path reports as [`TailState::Truncated`] rather than mistaking for
/// good data.
///
/// SIZE LIMIT: an encrypted payload larger than [`MAX_RECORD_PAYLOAD_LEN`]
/// is rejected with `ErrorKind::InvalidData` naming the actual and maximum
/// size, and nothing is written. The write side is bounded because the
/// read side is: a record that cannot be read back must never become
/// durable, or the next startup fails on a frame no version of this build
/// can accept.
///
/// FORMAT: frames are stamped with [`RECORD_FORMAT_VERSION`], and a
/// frame carrying any other version is refused on read (see
/// [`unsupported_version_error`]) rather than silently misread. Older
/// files still on the length-prefix-only layout — or on the
/// pre-encryption plaintext bincode layout before that — lack the frame
/// header entirely and are reported as corrupt for the same reason.
pub fn append_record<T: Serialize>(path: &std::path::Path, record: &T) -> std::io::Result<u64> {
    let bytes = bincode::serialize(record).map_err(|e| {
        Error::new(ErrorKind::InvalidData, format!("failed to serialize record: {e}"))
    })?;
    let encrypted = crypto::encrypt(&bytes);
    let frame = encode_frame(&encrypted)?;

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;

    let offset = file.metadata()?.len();

    file.write_all(&frame)?;
    // Flush the record AND the metadata length change to stable storage
    // before acknowledging the write. `sync_all` (not `sync_data`) so the
    // grown file length is durable too — a reader on the next open must be
    // able to see the frame's bytes and the size that bounds them.
    file.sync_all()?;

    Ok(offset)
}

/// Reads a single record back from a known offset in `path`, verifying
/// its frame (magic, version, length, checksum) before decrypting.
/// Any framing problem — truncated or corrupt — is surfaced as an error;
/// a record is never returned from bytes that failed verification.
///
/// Kept for future point-reads once a dataset outgrows an in-memory
/// index — reads today go through load()/read_all_records() at startup.
#[allow(dead_code)]
pub fn read_record_at<T: DeserializeOwned>(path: &std::path::Path, offset: u64) -> std::io::Result<T> {
    let mut file = File::open(path)?;
    let file_len = file.metadata()?.len();
    file.seek(SeekFrom::Start(offset))?;

    match read_one_frame(&mut file, offset, file_len)? {
        FrameOutcome::Record { payload, .. } => {
            let decrypted = crypto::decrypt(&payload).map_err(|e| {
                Error::new(
                    ErrorKind::InvalidData,
                    format!("failed to decrypt record at offset {offset} in {}: {e}", path.display()),
                )
            })?;
            bincode::deserialize(&decrypted).map_err(|e| {
                Error::new(
                    ErrorKind::InvalidData,
                    format!("failed to deserialize record at offset {offset} in {}: {e}", path.display()),
                )
            })
        }
        FrameOutcome::Truncated(detail) => Err(Error::new(
            ErrorKind::UnexpectedEof,
            format!("truncated record at offset {offset} in {}: {detail}", path.display()),
        )),
        FrameOutcome::Corrupt(detail) => Err(Error::new(
            ErrorKind::InvalidData,
            format!("corrupt record at offset {offset} in {}: {detail}", path.display()),
        )),
        FrameOutcome::UnsupportedVersion { found, offset } => {
            Err(unsupported_version_error(path, found, offset))
        }
    }
}

/// Sequentially replays `path` from the start, returning every
/// (offset, record) in file order together with the [`TailState`] of the
/// file after the last complete record.
///
/// Corruption-detection contract:
///
/// * Each frame's magic, version, length and CRC-32 are checked before
///   the payload is decrypted. A frame that is fully present on disk but
///   fails the magic, length or CRC check is **corruption** and is
///   returned as an `Err` — it is never skipped, and no record is
///   produced from it. The same applies if a verified frame fails to
///   decrypt or deserialize.
/// * A frame stamped with a different [`RECORD_FORMAT_VERSION`] is also
///   an `Err`, but a distinct one naming the format change and the fix
///   (see [`unsupported_version_error`]). It is *not* folded into
///   corruption: the bytes are intact, this build simply cannot read
///   them, and pointing an operator at a disk fault that isn't there
///   costs an outage's worth of time.
/// * A frame whose header or payload runs off the end of the file is a
///   **torn/partial final record** — the signature of a crash mid-append.
///   Reading stops there and every fully-durable record before it is
///   returned, paired with [`TailState::Truncated`] naming the offset. It
///   is detected and reported, never silently accepted as data.
/// * Reaching the end exactly on a frame boundary returns
///   [`TailState::Clean`].
///
/// Returns an empty list + `Clean` if the file doesn't exist yet.
pub fn read_all_records_framed<T: DeserializeOwned>(
    path: &std::path::Path,
) -> std::io::Result<(Vec<(u64, T)>, TailState)> {
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok((Vec::new(), TailState::Clean));
        }
        Err(e) => return Err(e),
    };

    let file_len = file.metadata()?.len();
    let mut records = Vec::new();
    let mut offset: u64 = 0;

    while offset < file_len {
        file.seek(SeekFrom::Start(offset))?;

        match read_one_frame(&mut file, offset, file_len)? {
            FrameOutcome::Record { payload, consumed } => {
                let decrypted = crypto::decrypt(&payload).map_err(|e| {
                    Error::new(
                        ErrorKind::InvalidData,
                        format!(
                            "failed to decrypt record at offset {offset} in {}: {e} \
                             — wrong ENOCHIAN_MASTER_KEY, or this file predates \
                             encryption at rest",
                            path.display()
                        ),
                    )
                })?;

                let record: T = bincode::deserialize(&decrypted).map_err(|e| {
                    Error::new(
                        ErrorKind::InvalidData,
                        format!(
                            "failed to deserialize record at offset {offset} in {}: {e} \
                             — file may be corrupted",
                            path.display()
                        ),
                    )
                })?;

                records.push((offset, record));
                offset += consumed;
            }

            FrameOutcome::Truncated(detail) => {
                return Ok((records, TailState::Truncated { offset, detail }));
            }

            FrameOutcome::Corrupt(detail) => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("corrupt record in {} at offset {offset}: {detail}", path.display()),
                ));
            }

            // A frame this build cannot interpret stops the replay dead,
            // exactly like corruption — but with its own message, because
            // the fix is a format migration and not a disk investigation.
            FrameOutcome::UnsupportedVersion { found, offset } => {
                return Err(unsupported_version_error(path, found, offset));
            }
        }
    }

    Ok((records, TailState::Clean))
}

/// Back-compatible view over [`read_all_records_framed`] for callers that
/// only want the records. A torn/partial final record (crash-during-
/// append) is not fatal here: it is logged and the fully-durable prefix
/// is returned, which is the correct recovery for an append-only log
/// whose torn trailing write was never acknowledged as durable. Real
/// (mid-file) corruption still propagates as an `Err`.
pub fn read_all_records<T: DeserializeOwned>(path: &std::path::Path) -> std::io::Result<Vec<(u64, T)>> {
    let (records, tail) = read_all_records_framed::<T>(path)?;

    if let TailState::Truncated { offset, detail } = &tail {
        eprintln!(
            "warning: {} has a torn/partial final record at offset {offset} \
             ({detail}); ignoring the incomplete trailing record and recovering \
             the {} record(s) before it. This is the expected outcome of a crash \
             during append — the record was never acknowledged as durable.",
            path.display(),
            records.len()
        );
    }

    Ok(records)
}

/// Node-log convenience wrappers over the generic functions above, kept
/// so call sites that only deal with nodes (the common case) don't need
/// to name the storage path or turbofish the type at every call.
///
/// They speak [`NodeRecord`], not `Node`: the log's unit is an operation,
/// and a wrapper that took a bare `Node` would be an inviting way to
/// append a put while forgetting deletes exist in the same file.
pub fn append_node_record(record: &NodeRecord) -> std::io::Result<u64> {
    append_record(&nodes_path(), record)
}

#[allow(dead_code)]
pub fn read_node_record_at(offset: u64) -> std::io::Result<NodeRecord> {
    read_record_at(&nodes_path(), offset)
}

pub fn read_all() -> std::io::Result<Vec<(u64, NodeRecord)>> {
    read_all_records(&nodes_path())
}

/// Full paths under the configured data directory (see `config.rs`) —
/// these replace what used to be hardcoded "facetql.data" /
/// "facetql.edges" literals in the repo root.
/// The node log: a sequence of [`NodeRecord`]s.
pub fn nodes_path() -> std::path::PathBuf {
    config::data_file("facetql.data")
}

/// The edge log: a sequence of [`EdgeRecord`]s.
pub fn edges_path() -> std::path::PathBuf {
    config::data_file("facetql.edges")
}

/// Persistent, admin-manageable user records — see core/user.rs. A
/// sequence of [`UserOpRecord`]s; `StorageEngine::load()` replays it the
/// same last-write-wins way as the node and edge logs.
pub fn users_path() -> std::path::PathBuf {
    config::data_file("facetql.users")
}

/// Archived previous node states — see core/history.rs.
///
/// The one log with no operation enum, because it has no deletes and no
/// keys: history is a pure, strictly-additive record of what a node used
/// to be, and nothing ever supersedes an entry. Giving it a `Put`
/// wrapper would add a variant that no writer could ever produce a
/// counterpart to.
pub fn history_path() -> std::path::PathBuf {
    config::data_file("facetql.history")
}
