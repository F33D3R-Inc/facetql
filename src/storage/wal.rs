use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::config;
use crate::core::edge::Edge;
use crate::core::history::HistoryEntry;
use crate::core::node::Node;
use crate::core::user::UserRecord;
use crate::crypto;

/// Current on-disk WAL record format.
///
/// Increment this when the serialized WAL representation changes in a
/// way that is not backwards-compatible.
pub const WAL_FORMAT_VERSION: u16 = 2;

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
        return FrameOutcome::Corrupt(
            "frame magic mismatch".to_string(),
        );
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

    /// Insert a persistent user.
    InsertUser(UserRecord),

    /// Revoke a persistent user.
    RevokeUser(String),
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

    /// Create a standalone WAL record.
    pub fn standalone(
        operation: WalOperation,
    ) -> Self {
        Self::new(
            next_sequence(),
            STANDALONE_TRANSACTION_ID,
            next_operation_id(),
            operation,
        )
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

    /// Returns true when this record starts a transaction.
    pub fn is_begin(&self) -> bool {
        matches!(
            self.operation,
            WalOperation::Begin
        )
    }

    /// Returns true when this record commits a transaction.
    pub fn is_commit(&self) -> bool {
        matches!(
            self.operation,
            WalOperation::Commit
        )
    }

    /// Returns true when this record aborts a transaction.
    pub fn is_abort(&self) -> bool {
        matches!(
            self.operation,
            WalOperation::Abort
        )
    }

    /// Returns true when this record contains a mutation rather than a
    /// transaction-control marker.
    pub fn is_mutation(&self) -> bool {
        matches!(
            self.operation,
            WalOperation::Archive(_)
                | WalOperation::Insert(_)
                | WalOperation::Delete(_)
                | WalOperation::InsertEdge(_)
                | WalOperation::InsertUser(_)
                | WalOperation::RevokeUser(_)
        )
    }
}

/// Allocate the next process-local sequence.
///
/// Sequence persistence across process restarts is handled by scanning
/// the existing WAL during database startup.
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

/// Append one authenticated WAL record.
///
/// Durability boundary:
///
///     serialize
///        ↓
///     encrypt/authenticate
///        ↓
///     append
///        ↓
///     sync_data()
///        ↓
///     return Ok(())
///
/// Once this function returns successfully, the WAL record has been
/// handed to the filesystem's durable-data synchronization boundary.
pub fn append(
    record: &WalRecord,
) -> io::Result<()> {
    let bytes = bincode::serialize(record)
        .map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                e,
            )
        })?;

    let encrypted =
        crypto::encrypt(&bytes);

    let encoded =
        crypto::encode_hex(&encrypted);

    let path =
        config::data_file("facetql.wal");

    let mut file =
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;

    file.write_all(
        encoded.as_bytes(),
    )?;

    file.write_all(b"\n")?;

    /*
     * Explicit durability boundary.
     *
     * We don't acknowledge the WAL append until the data has been
     * synchronized.
     */
    file.sync_data()?;

    Ok(())
}

/// Append a standalone operation.
pub fn append_standalone(
    operation: WalOperation,
) -> io::Result<WalRecord> {
    if matches!(
        operation,
        WalOperation::Begin
            | WalOperation::Commit
            | WalOperation::Abort
    ) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "transaction control records cannot be standalone operations",
        ));
    }

    let record =
        WalRecord::standalone(
            operation,
        );

    append(&record)?;

    Ok(record)
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