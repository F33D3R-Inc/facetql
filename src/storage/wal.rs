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