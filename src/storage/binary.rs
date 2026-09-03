use std::fs::{File, OpenOptions};
use std::io::{Error, ErrorKind, Read, Seek, SeekFrom, Write};
use serde::Serialize;
use serde::de::DeserializeOwned;
use crate::core::node::Node;
use crate::config;
use crate::crypto;

// ─────────────────────────────────────────────────────────────────────
// On-disk record frame
// ─────────────────────────────────────────────────────────────────────
//
// Every persisted record — node, edge, user, history entry, tombstone —
// is written as one self-describing frame so that a read can prove a
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
// re-deriving the byte math.

/// Marker bytes at the start of every record frame: "FQR1" =
/// FacetQL Record, frame generation 1. A read that does not find these
/// bytes where a frame is expected treats the file as corrupt rather
/// than guessing at the bytes.
pub const RECORD_MAGIC: [u8; 4] = *b"FQR1";

/// Frame format version. Bump this only for an incompatible change to
/// the *frame* (header shape / checksum algorithm), not for changes to
/// the record payloads themselves. A frame carrying any other version is
/// surfaced as corruption, never decoded on a guess.
pub const RECORD_FORMAT_VERSION: u8 = 1;

/// Bytes of fixed header in front of every payload: magic(4) +
/// version(1) + payload_len(4) + crc(4).
pub const FRAME_HEADER_LEN: usize = 4 + 1 + 4 + 4;

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
    /// The frame is fully present on disk but structurally invalid: wrong
    /// magic, an unsupported version, or a payload whose checksum does
    /// not match. This is corruption, not a torn tail.
    Corrupt(String),
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

/// Wraps an already-encrypted payload in a record frame ready to append.
fn encode_frame(payload: &[u8]) -> std::io::Result<Vec<u8>> {
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
        return Ok(FrameOutcome::Corrupt(format!(
            "unsupported record format version {version} at offset {offset} \
             (this build reads/writes frame v{RECORD_FORMAT_VERSION})"
        )));
    }

    let payload_len =
        u32::from_le_bytes(header[5..9].try_into().unwrap()) as u64;
    let stored_crc = u32::from_le_bytes(header[9..13].try_into().unwrap());

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
/// This is a breaking on-disk format change from the previous
/// length-prefix-only layout (which itself replaced the pre-encryption
/// plaintext bincode files): older `facetql.data`/`.edges`/`.users`/
/// `.history`/`.tombstones` files lack the frame header and will be
/// reported as corrupt on load rather than silently misread.
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
///   fails any of those checks is **corruption** and is returned as an
///   `Err` — it is never skipped, and no record is produced from it. The
///   same applies if a verified frame fails to decrypt or deserialize.
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

/// Node-specific convenience wrappers over the generic functions above,
/// kept so call sites that only deal with nodes (the common case) don't
/// need to name the storage path or turbofish the type at every call.
pub fn append_node(node: &Node) -> std::io::Result<u64> {
    append_record(&nodes_path(), node)
}

#[allow(dead_code)]
pub fn read_node_at(offset: u64) -> std::io::Result<Node> {
    read_record_at(&nodes_path(), offset)
}

pub fn read_all() -> std::io::Result<Vec<(u64, Node)>> {
    read_all_records(&nodes_path())
}

/// Full paths under the configured data directory (see `config.rs`) —
/// these replace what used to be hardcoded "facetql.data" /
/// "facetql.edges" literals in the repo root.
pub fn nodes_path() -> std::path::PathBuf {
    config::data_file("facetql.data")
}

pub fn edges_path() -> std::path::PathBuf {
    config::data_file("facetql.edges")
}

/// Persistent, admin-manageable user records — see core/user.rs.
/// Same append-only, offset-indexed format as nodes and edges;
/// StorageEngine::load() replays it the same way.
pub fn users_path() -> std::path::PathBuf {
    config::data_file("facetql.users")
}

/// Archived previous node states — see core/history.rs.
pub fn history_path() -> std::path::PathBuf {
    config::data_file("facetql.history")
}
