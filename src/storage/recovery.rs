use std::collections::HashMap;
use std::fs;
use std::io;

use crate::config;
use crate::crypto;
use crate::storage::checkpoint;
use crate::storage::engine::StorageEngine;
use crate::storage::wal::{
    WalOperation,
    WalRecord,
    WAL_FORMAT_VERSION,
};

/// Recover the database from the durable WAL.
///
/// Recovery is intentionally separate from normal mutation.
///
/// Normal write path:
///
///     mutation
///        ↓
///       WAL
///        ↓
///     storage
///        ↓
///      memory
///
/// Recovery path:
///
///     WAL
///      ↓
///    validate
///      ↓
///    determine committed transactions
///      ↓
///    replay directly into memory
///
/// Recovery MUST NOT call normal mutation methods because those methods
/// append new WAL records.
pub fn recover(engine: &mut StorageEngine) -> io::Result<()> {
    let path = config::data_file("facetql.wal");

    if !path.exists() {
        return Ok(());
    }

    let raw = fs::read_to_string(&path)?;

    let records = read_records(&raw)?;

    validate_sequence(&records)?;

    /*
     * Make sure the in-process sequence counter continues past every
     * sequence number already durable in this WAL file, so the first
     * write of the new process doesn't reuse (and thus write
     * out-of-order) a sequence number a previous process already used.
     */
    if let Some(max_sequence) =
        records.iter().map(|r| r.sequence).max()
    {
        crate::storage::engine::advance_wal_sequence(max_sequence);
    }

    /*
     * Records already reflected in physical storage (facetql.data,
     * facetql.edges, facetql.users, facetql.history) don't need to be
     * replayed again — replaying them would duplicate edges and history
     * entries on every single restart. Only records past the last
     * confirmed-durable checkpoint represent operations that might not
     * have made it to physical storage before a crash.
     */
    let checkpoint = checkpoint::read()?;

    let records: Vec<WalRecord> = records
        .into_iter()
        .filter(|r| r.sequence > checkpoint)
        .collect();

    /*
     * transaction_id == 0:
     *
     * Standalone operation. It is immediately replayable.
     *
     * transaction_id != 0:
     *
     * Multi-operation transaction. We collect its records and only replay
     * the mutations if the WAL contains a valid BEGIN + COMMIT sequence.
     */
    let mut transactions: HashMap<u64, Vec<WalRecord>> =
        HashMap::new();

    for record in records {
        if record.transaction_id == 0 {
            match record.operation {
                // Transaction control records are never valid with
                // transaction_id == 0.
                WalOperation::Begin
                | WalOperation::Commit
                | WalOperation::Abort => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "transaction control record {} uses reserved transaction ID 0",
                            record.sequence
                        ),
                    ));
                }

                operation => {
                    apply_recovery_operation(
                        engine,
                        operation,
                    )?;
                }
            }
        } else {
            transactions
                .entry(record.transaction_id)
                .or_default()
                .push(record);
        }
    }

    /*
     * Evaluate every multi-operation transaction.
     *
     * Only this exact lifecycle is committed:
     *
     * BEGIN
     *   mutation...
     * COMMIT
     *
     * Anything else is discarded:
     *
     * BEGIN
     *   mutation...
     * EOF
     *
     * BEGIN
     *   mutation...
     * ABORT
     *
     * BEGIN
     *   mutation...
     * ABORT
     * COMMIT
     */
    for transaction_id in sorted_transaction_ids(&transactions) {
        let records = transactions
            .get(&transaction_id)
            .expect("transaction ID came from the map");

        replay_committed_transaction(
            engine,
            transaction_id,
            records,
        )?;
    }

    Ok(())
}

/// Return transaction IDs in deterministic order.
///
/// HashMap iteration order is intentionally nondeterministic. Recovery must
/// never depend on HashMap iteration order.
fn sorted_transaction_ids(
    transactions: &HashMap<u64, Vec<WalRecord>>,
) -> Vec<u64> {
    let mut ids: Vec<u64> =
        transactions.keys().copied().collect();

    ids.sort_unstable();

    ids
}

/// Validate and replay one transaction.
///
/// A transaction is committed only if:
///
///     first record = BEGIN
///     last control record = COMMIT
///     no ABORT occurred
///
/// All mutation records between BEGIN and COMMIT are replayed in WAL
/// sequence order.
fn replay_committed_transaction(
    engine: &mut StorageEngine,
    transaction_id: u64,
    records: &[WalRecord],
) -> io::Result<()> {
    if records.is_empty() {
        return Ok(());
    }

    let mut ordered =
        records.to_vec();

    ordered.sort_by_key(
        |record| record.sequence,
    );

    // -------------------------------------------------------------
    // BEGIN must be the first record.
    // -------------------------------------------------------------

    if !matches!(
        ordered.first().map(|record| &record.operation),
        Some(WalOperation::Begin)
    ) {
        // No valid BEGIN means this transaction is incomplete/corrupt.
        //
        // Do not replay it.
        return Ok(());
    }

    // -------------------------------------------------------------
    // Validate transaction IDs.
    // -------------------------------------------------------------

    for record in &ordered {
        if record.transaction_id != transaction_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "transaction {} contains record {} belonging to transaction {}",
                    transaction_id,
                    record.sequence,
                    record.transaction_id
                ),
            ));
        }
    }

    // -------------------------------------------------------------
    // Walk transaction lifecycle.
    // -------------------------------------------------------------

    let mut committed = false;
    let mut aborted = false;

    for (index, record) in ordered.iter().enumerate() {
        match &record.operation {
            WalOperation::Begin => {
                if index != 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "transaction {} contains BEGIN after transaction start",
                            transaction_id
                        ),
                    ));
                }
            }

            WalOperation::Commit => {
                if aborted {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "transaction {} contains COMMIT after ABORT",
                            transaction_id
                        ),
                    ));
                }

                if committed {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "transaction {} contains duplicate COMMIT",
                            transaction_id
                        ),
                    ));
                }

                // COMMIT must be the final record.
                if index != ordered.len() - 1 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "transaction {} contains records after COMMIT",
                            transaction_id
                        ),
                    ));
                }

                committed = true;
            }

            WalOperation::Abort => {
                if committed {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "transaction {} contains ABORT after COMMIT",
                            transaction_id
                        ),
                    ));
                }

                if aborted {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "transaction {} contains duplicate ABORT",
                            transaction_id
                        ),
                    ));
                }

                // ABORT must be the final record.
                if index != ordered.len() - 1 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "transaction {} contains records after ABORT",
                            transaction_id
                        ),
                    ));
                }

                aborted = true;
            }

            operation => {
                // Mutations cannot occur before BEGIN.
                if index == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "transaction {} begins with a mutation instead of BEGIN",
                            transaction_id
                        ),
                    ));
                }

                // Mutations after COMMIT/ABORT are rejected by the
                // control-record checks above.
                if committed || aborted {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "transaction {} contains mutation after transaction termination",
                            transaction_id
                        ),
                    ));
                }

                let _ = operation;
            }
        }
    }

    // -------------------------------------------------------------
    // Incomplete transaction.
    // -------------------------------------------------------------

    if !committed {
        // This includes:
        //
        // BEGIN
        // mutation
        // EOF
        //
        // and:
        //
        // BEGIN
        // mutation
        // ABORT
        //
        // Neither is replayed.
        return Ok(());
    }

    // -------------------------------------------------------------
    // Replay only mutations.
    // -------------------------------------------------------------

    for record in ordered {
        match record.operation {
            WalOperation::Begin
            | WalOperation::Commit
            | WalOperation::Abort => {
                // Control records affect transaction state only.
            }

            operation => {
                apply_recovery_operation(
                    engine,
                    operation,
                )?;
            }
        }
    }

    Ok(())
}

/// Decode and deserialize every WAL record.
///
/// Malformed or unauthenticated records are treated as corruption rather
/// than silently ignored.
fn read_records(
    raw: &str,
) -> io::Result<Vec<WalRecord>> {
    let mut records = Vec::new();

    for (line_number, line) in raw.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }

        let line_number =
            line_number + 1;

        let encrypted =
            crypto::decode_hex(line)
                .map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "WAL line {line_number} is not valid hex: {e}"
                        ),
                    )
                })?;

        let plaintext =
            crypto::decrypt(&encrypted)
                .map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "WAL line {line_number} failed authentication/decryption: {e}"
                        ),
                    )
                })?;

        let record: WalRecord =
            bincode::deserialize(&plaintext)
                .map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "WAL line {line_number} contains an invalid record: {e}"
                        ),
                    )
                })?;

        if record.format_version != WAL_FORMAT_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "unsupported WAL format version {} at line {}",
                    record.format_version,
                    line_number
                ),
            ));
        }

        if record.sequence == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "WAL line {line_number} has invalid sequence 0"
                ),
            ));
        }

        if record.operation_id == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "WAL line {line_number} has invalid operation ID 0"
                ),
            ));
        }

        records.push(record);
    }

    Ok(records)
}

/// Verify that WAL sequence numbers are strictly increasing.
fn validate_sequence(
    records: &[WalRecord],
) -> io::Result<()> {
    let mut previous_sequence =
        None;

    for record in records {
        if let Some(previous) =
            previous_sequence
        {
            if record.sequence <= previous {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "WAL sequence violation: {} follows {}",
                        record.sequence,
                        previous
                    ),
                ));
            }
        }

        previous_sequence =
            Some(record.sequence);
    }

    Ok(())
}

/// Apply a WAL mutation directly to the in-memory storage engine.
///
/// Recovery MUST NOT call normal mutation methods because those methods
/// append additional WAL records.
fn apply_recovery_operation(
    engine: &mut StorageEngine,
    operation: WalOperation,
) -> io::Result<()> {
    match operation {
        WalOperation::Archive(entry) => {
            engine
                .replay_archive(entry)
                .map_err(storage_error)?;
        }

        WalOperation::Insert(node) => {
            engine
                .replay_insert(node)
                .map_err(storage_error)?;
        }

        WalOperation::Delete(address) => {
            engine
                .replay_delete(&address)
                .map_err(storage_error)?;
        }

        WalOperation::InsertEdge(edge) => {
            engine
                .replay_insert_edge(edge)
                .map_err(storage_error)?;
        }

        WalOperation::InsertUser(record) => {
            engine
                .replay_insert_user(record)
                .map_err(storage_error)?;
        }

        WalOperation::RevokeUser(token_hash) => {
            engine
                .replay_revoke_user(&token_hash)
                .map_err(storage_error)?;
        }

        // Transaction-control records are consumed by the transaction
        // state machine and never reach this function.
        WalOperation::Begin
        | WalOperation::Commit
        | WalOperation::Abort => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "transaction control record reached mutation replay",
            ));
        }
    }

    Ok(())
}

fn storage_error(
    error: String,
) -> io::Error {
    io::Error::new(
        io::ErrorKind::Other,
        error,
    )
}