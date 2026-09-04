use std::collections::HashMap;
use std::io;

use crate::core::edge::Edge;
use crate::core::history::HistoryEntry;
use crate::core::node::Node;
use crate::storage::checkpoint;
use crate::storage::engine::StorageEngine;
use crate::storage::wal;
use crate::storage::wal::{
    WalOperation,
    WalRecord,
    WalTail,
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
///    read framed records (repair a torn tail)
///      ↓
///    validate
///      ↓
///    pass 1: classify transactions (no mutation)
///      ↓
///    pass 2: replay in strict WAL sequence order
///      ↓
///    memory
///
/// Recovery MUST NOT call normal mutation methods because those methods
/// append new WAL records.
///
/// Every error returned here prevents startup. That is deliberate: a WAL
/// we cannot fully explain must never be silently partially applied and
/// then served as if it were the truth. The one crash artefact we *can*
/// explain — a torn trailing frame — is repaired rather than refused
/// (see `WalTail::Torn` below).
pub fn recover(engine: &mut StorageEngine) -> io::Result<()> {
    let path = wal::wal_path();

    if !path.exists() {
        return Ok(());
    }

    /*
     * Read through the WAL module's framed reader rather than parsing
     * lines here.
     *
     * Framing is a WAL-format concern, and only the reader that knows
     * the frame layout can tell the difference between the two failures
     * that look identical to a naive line parser:
     *
     *   - a structurally incomplete FINAL frame, which is the expected
     *     signature of a crash partway through an append. The record was
     *     never acknowledged durable to any caller, so discarding it
     *     loses nothing a client was promised.
     *
     *   - a frame that is structurally complete but fails integrity, or
     *     any damaged frame that is NOT last. That is bit-rot or
     *     tampering, not a clean crash tail, and `read_all` returns an
     *     `Err` for it — which we propagate, refusing startup.
     */
    let (records, tail) = wal::read_all()?;

    if let WalTail::Torn { durable_len, detail } = tail {
        eprintln!(
            "facetql: WAL tail is torn at durable length {durable_len} bytes: {detail}"
        );

        eprintln!(
            "facetql: this is the expected signature of a crash during a WAL append; \
             the incomplete trailing record was never acknowledged durable and is discarded"
        );

        /*
         * Truncating is not cosmetic and its failure is not survivable.
         *
         * The torn frame has no complete trailing byte run, so the next
         * `append` would write its bytes directly onto the partial one,
         * splicing two frames into a single unparsable blob. That blob
         * is NOT a trailing tear — it sits mid-log the moment anything
         * is appended after it — so `read_all` would classify it as real
         * corruption forever after, and the database would refuse to
         * start with no way back.
         *
         * Truncating to the last frame boundary is therefore a hard
         * precondition of accepting traffic. If it fails, fail startup.
         */
        wal::truncate_torn_tail(durable_len)?;
    }

    validate_records(&records)?;

    validate_sequence(&records)?;

    /*
     * Make sure the in-process counters continue past every identifier
     * already durable in this WAL file.
     *
     * sequence: the first write of the new process must not reuse (and
     * thus write out-of-order) a sequence number a previous process
     * already used — `validate_sequence` requires strictly increasing
     * sequences, so a reused one makes the *next* restart unrecoverable.
     *
     * transaction_id: a restarted process must not reissue a transaction
     * ID that is still present in the WAL. If it did, the records of a
     * previous run's frame and a new frame would be classified as one
     * transaction below, and the lifecycle walk would reject the merged
     * group ("BEGIN after transaction start") — turning a routine
     * restart into a failed recovery. This matters most for exactly the
     * case the frame exists to survive: an incomplete `BEGIN … EOF` frame
     * left behind by a crash.
     *
     * operation_id: every record must stay uniquely identifiable.
     *
     * These maxima are computed over ALL records read, BEFORE the
     * checkpoint filter below. Checkpointed records are still physically
     * in the file, so their identifiers are still taken; filtering first
     * would let a restart reissue an identifier that a later `read_all`
     * would then see twice.
     */
    let max_sequence =
        records.iter().map(|r| r.sequence).max().unwrap_or(0);

    let max_transaction_id =
        records.iter().map(|r| r.transaction_id).max().unwrap_or(0);

    let max_operation_id =
        records.iter().map(|r| r.operation_id).max().unwrap_or(0);

    wal::initialize_counters(
        max_sequence,
        max_transaction_id,
        max_operation_id,
    );

    /*
     * Records already reflected in physical storage (facetql.data,
     * facetql.edges, facetql.users, facetql.history) don't need to be
     * replayed. Only records past the last confirmed-durable checkpoint
     * represent operations that might not have made it to physical
     * storage before a crash.
     *
     * The checkpoint is an OPTIMIZATION, not a correctness barrier. It
     * is advanced *after* the physical write and on a best-effort basis,
     * so a record whose physical write did land can still fall on the
     * replay side of this filter. Correctness therefore rests on replay
     * being idempotent (see `apply_recovery_operation`), never on the
     * checkpoint being exact.
     */
    let checkpoint = checkpoint::read()?;

    let records: Vec<WalRecord> = records
        .into_iter()
        .filter(|r| r.sequence > checkpoint)
        .collect();

    /*
     * ---------------------------------------------------------------
     * Pass 1 — classify. No mutation happens here.
     * ---------------------------------------------------------------
     *
     * Decide, for every non-zero transaction ID, whether its frame is
     * committed. Doing this before touching the engine means a malformed
     * frame anywhere in the WAL aborts startup with memory untouched,
     * rather than half-applied.
     */
    let committed = classify_transactions(&records)?;

    /*
     * ---------------------------------------------------------------
     * Pass 2 — apply, in strict ascending WAL sequence order.
     * ---------------------------------------------------------------
     *
     * The WAL is a totally ordered log and replay must honour that total
     * order. Applying all standalone operations first and all
     * transactional ones afterwards (or vice versa) reorders writes
     * against each other: a committed transaction that deleted address
     * X at sequence 10 would land after a standalone insert of X at
     * sequence 20 and the node would vanish on restart.
     *
     * `records` is in file order, and `validate_sequence` has already
     * proven file order is strictly ascending by sequence, so walking it
     * once IS walking the total order.
     *
     * Visibility rule:
     *
     *   - a standalone mutation applies at its own position;
     *
     *   - a transaction's mutations are buffered and applied at the
     *     position of that transaction's COMMIT record, in their own
     *     ascending-sequence order.
     *
     * That is the standard rule — a transaction becomes visible at its
     * commit point — and it is what makes interleaving with standalone
     * operations well defined: everything before the COMMIT in the log
     * is applied before the transaction, everything after it is applied
     * after.
     *
     * Uncommitted and aborted frames are never buffered and never
     * applied.
     */
    replay_in_sequence_order(engine, records, &committed)
}

/// Buffered mutations of one in-flight transaction, keyed by
/// transaction ID.
///
/// Values are pushed in encounter order, which pass 2 walks in ascending
/// sequence order, so each buffer is already sorted by sequence. The map
/// is only ever accessed by key — recovery never iterates it — so no
/// behaviour depends on `HashMap` iteration order.
type TransactionBuffers = HashMap<u64, Vec<WalOperation>>;

/// Sequence number of the COMMIT record of each committed transaction.
///
/// A transaction ID that is absent was never committed (incomplete or
/// aborted) and must be discarded entirely.
type CommittedTransactions = HashMap<u64, u64>;

/// Walk the WAL in total order and apply it.
///
/// See the pass 2 rationale in `recover` for the ordering contract this
/// implements.
fn replay_in_sequence_order(
    engine: &mut StorageEngine,
    records: Vec<WalRecord>,
    committed: &CommittedTransactions,
) -> io::Result<()> {
    let mut buffers: TransactionBuffers =
        HashMap::new();

    for record in records {
        let sequence = record.sequence;

        let transaction_id =
            record.transaction_id;

        if transaction_id == 0 {
            match record.operation {
                /*
                 * Transaction control records are never valid with the
                 * reserved standalone transaction ID. Pass 1 already
                 * rejected this, so reaching it here would mean the two
                 * passes disagree; keep the hard error rather than
                 * silently trusting one of them.
                 */
                WalOperation::Begin
                | WalOperation::Commit
                | WalOperation::Abort => {
                    return Err(reserved_transaction_id_error(
                        sequence,
                    ));
                }

                operation => {
                    apply_recovery_operation(
                        engine,
                        operation,
                    )?;
                }
            }

            continue;
        }

        /*
         * A transaction that never committed contributes nothing — not
         * its mutations, and not the buffer they would sit in.
         */
        let Some(&commit_sequence) =
            committed.get(&transaction_id)
        else {
            continue;
        };

        match record.operation {
            WalOperation::Begin => {
                /*
                 * Nothing becomes visible at BEGIN. The buffer is
                 * created lazily by the first mutation.
                 */
            }

            WalOperation::Abort => {
                /*
                 * Unreachable: pass 1 does not mark an aborted frame
                 * committed. Treated as "contributes nothing" for the
                 * same reason BEGIN is.
                 */
            }

            WalOperation::Commit => {
                if sequence != commit_sequence {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "transaction {transaction_id} COMMIT at sequence {sequence} \
                             disagrees with classified commit sequence {commit_sequence}"
                        ),
                    ));
                }

                /*
                 * The commit point. Everything this transaction buffered
                 * becomes visible here, in its own ascending-sequence
                 * order, before the log continues.
                 */
                let mutations = buffers
                    .remove(&transaction_id)
                    .unwrap_or_default();

                for operation in mutations {
                    apply_recovery_operation(
                        engine,
                        operation,
                    )?;
                }
            }

            operation => {
                buffers
                    .entry(transaction_id)
                    .or_default()
                    .push(operation);
            }
        }
    }

    Ok(())
}

/// Pass 1: decide which transactions committed, without mutating
/// anything.
///
/// Returns the COMMIT sequence of every committed transaction. Any
/// transaction absent from the result is discarded by pass 2.
///
/// Only this exact lifecycle is committed:
///
///     BEGIN
///       mutation...
///     COMMIT
///
/// Anything else is discarded:
///
///     BEGIN
///       mutation...
///     EOF
///
///     BEGIN
///       mutation...
///     ABORT
///
/// and a frame whose shape is not merely incomplete but impossible
/// (records after COMMIT, a second BEGIN, a mutation before BEGIN) is a
/// hard error — the log claims something that cannot have happened, so
/// we refuse to guess.
fn classify_transactions(
    records: &[WalRecord],
) -> io::Result<CommittedTransactions> {
    let mut frames: HashMap<u64, Vec<&WalRecord>> =
        HashMap::new();

    for record in records {
        if record.transaction_id == 0 {
            /*
             * Reject a control record carrying the reserved standalone
             * transaction ID here, in the non-mutating pass, so it can
             * never abort startup halfway through applying the log.
             */
            if matches!(
                record.operation,
                WalOperation::Begin
                    | WalOperation::Commit
                    | WalOperation::Abort
            ) {
                return Err(reserved_transaction_id_error(
                    record.sequence,
                ));
            }

            continue;
        }

        frames
            .entry(record.transaction_id)
            .or_default()
            .push(record);
    }

    let mut committed: CommittedTransactions =
        HashMap::new();

    /*
     * Classify in ascending transaction ID order. Nothing here depends
     * on the order — classification is independent per frame — but a
     * malformed frame aborts startup, and which error the operator sees
     * must not change from run to run. HashMap iteration order is
     * intentionally nondeterministic; recovery never relies on it.
     */
    for transaction_id in sorted_transaction_ids(&frames) {
        let frame = frames
            .get(&transaction_id)
            .expect("transaction ID came from the map");

        if let Some(commit_sequence) =
            classify_transaction(
                transaction_id,
                frame,
            )?
        {
            committed.insert(
                transaction_id,
                commit_sequence,
            );
        }
    }

    Ok(committed)
}

/// Return transaction IDs in deterministic order.
///
/// HashMap iteration order is intentionally nondeterministic. Recovery
/// must never depend on HashMap iteration order.
fn sorted_transaction_ids(
    frames: &HashMap<u64, Vec<&WalRecord>>,
) -> Vec<u64> {
    let mut ids: Vec<u64> =
        frames.keys().copied().collect();

    ids.sort_unstable();

    ids
}

/// Validate one transaction frame.
///
/// A transaction is committed only if:
///
///     first record = BEGIN
///     last control record = COMMIT
///     no ABORT occurred
///
/// Returns the sequence number of the COMMIT record when the frame
/// committed, `None` when it is merely incomplete or aborted (discard,
/// not an error), and `Err` when the frame is structurally impossible.
fn classify_transaction(
    transaction_id: u64,
    records: &[&WalRecord],
) -> io::Result<Option<u64>> {
    if records.is_empty() {
        return Ok(None);
    }

    let mut ordered: Vec<&WalRecord> =
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
        return Ok(None);
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

    let mut commit_sequence: Option<u64> =
        None;

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

                if commit_sequence.is_some() {
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

                commit_sequence =
                    Some(record.sequence);
            }

            WalOperation::Abort => {
                if commit_sequence.is_some() {
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

            _ => {
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
                if commit_sequence.is_some() || aborted {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "transaction {} contains mutation after transaction termination",
                            transaction_id
                        ),
                    ));
                }
            }
        }
    }

    // -------------------------------------------------------------
    // Incomplete transaction.
    // -------------------------------------------------------------
    //
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
    // Neither is replayed. `commit_sequence` is already `None` for both.

    Ok(commit_sequence)
}

/// A transaction control record carrying the reserved standalone
/// transaction ID.
///
/// BEGIN/COMMIT/ABORT describe a transaction, and transaction ID 0 means
/// "no transaction", so such a record cannot be interpreted at all.
fn reserved_transaction_id_error(
    sequence: u64,
) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "transaction control record {sequence} uses reserved transaction ID 0"
        ),
    )
}

/// Re-assert the per-record invariants recovery depends on.
///
/// The framed reader authenticates and decodes records; these checks are
/// about what the record *says* once decoded, and recovery asserts them
/// itself so its own preconditions do not silently depend on which
/// reader produced the records.
fn validate_records(
    records: &[WalRecord],
) -> io::Result<()> {
    for record in records {
        if record.format_version != WAL_FORMAT_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "unsupported WAL format version {} at sequence {}",
                    record.format_version,
                    record.sequence
                ),
            ));
        }

        if record.sequence == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "WAL record has invalid sequence 0",
            ));
        }

        if record.operation_id == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "WAL record at sequence {} has invalid operation ID 0",
                    record.sequence
                ),
            ));
        }
    }

    Ok(())
}

/// Verify that WAL sequence numbers are strictly increasing.
///
/// This is also what lets pass 2 treat file order as the total order:
/// once file order is proven strictly ascending, walking the records as
/// read IS walking them in sequence order, with no sort needed.
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
///
/// # Idempotence
///
/// Replay must be safe to run repeatedly against physical storage that
/// may ALREADY contain the record being replayed. That is the property
/// recovery actually depends on; the checkpoint only reduces how much
/// work it takes.
///
/// The reason is the checkpoint's own ordering: it is advanced after the
/// physical write completes, and on a best-effort basis, so a record
/// whose physical write did land can still be replayed after a crash —
/// and `StorageEngine::load()` has already read that landed write back
/// out of facetql.history / facetql.edges into memory.
///
/// Insert and Delete are naturally idempotent (a map insert and a map
/// remove both converge). Archive and InsertEdge are NOT: they push onto
/// a `Vec`, so a re-applied record appends a second, identical history
/// entry or edge — permanently, and again on every subsequent restart.
/// They are de-duplicated here, on the recovery side of the boundary,
/// against the state `load()` produced.
///
/// InsertUser and RevokeUser are map operations and converge like Insert
/// and Delete. DeleteEdge converges too: it removes by identity from
/// both adjacency lists, so a second application removes nothing.
fn apply_recovery_operation(
    engine: &mut StorageEngine,
    operation: WalOperation,
) -> io::Result<()> {
    match operation {
        WalOperation::Archive(entry) => {
            /*
             * Identity of a history entry is (address, archived_at_unix,
             * node): the same node archived twice at different instants
             * is two real entries, so the timestamp is part of the key
             * and only a byte-for-byte repeat of the same archival is a
             * duplicate.
             */
            if history_contains(engine, &entry) {
                return Ok(());
            }

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
            /*
             * An edge is fully described by (from, to, kind, owner);
             * there is no separate edge identity to compare, so a second
             * edge with all four equal is indistinguishable from the
             * first and adds nothing but a duplicate traversal result.
             */
            if edge_exists(engine, &edge) {
                return Ok(());
            }

            engine
                .replay_insert_edge(edge)
                .map_err(storage_error)?;
        }

        WalOperation::DeleteEdge(id) => {
            /*
             * No existence check and no de-duplication needed: removal
             * from both adjacency lists converges the same way `Delete`
             * converges for a node. Replaying this against an edge that
             * `load()` already resolved away — or that an earlier replay
             * removed — finds nothing to remove and is a no-op, not an
             * error. That is the idempotence the checkpoint's
             * best-effort advance depends on.
             */
            engine
                .replay_delete_edge(&id)
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

/// Is this exact history entry already present for its address?
///
/// Read-only against `StorageEngine::history`, which is `pub`, so
/// recovery can answer this without engine.rs growing a
/// recovery-specific method.
fn history_contains(
    engine: &StorageEngine,
    entry: &HistoryEntry,
) -> bool {
    engine
        .history
        .get(&entry.address)
        .is_some_and(|existing| {
            existing.iter().any(|candidate| {
                candidate.address == entry.address
                    && candidate.archived_at_unix
                        == entry.archived_at_unix
                    && nodes_equal(
                        &candidate.node,
                        &entry.node,
                    )
            })
        })
}

/// Is this exact edge already present in the outgoing adjacency list?
///
/// Checking `edges_out` alone is sufficient: `StorageEngine` indexes
/// every edge into `edges_out` and `edges_in` together, so the two are
/// never out of step and one is a faithful witness for the other.
fn edge_exists(
    engine: &StorageEngine,
    edge: &Edge,
) -> bool {
    engine
        .edges_out
        .get(&edge.from)
        .is_some_and(|existing| {
            existing.iter().any(|candidate| {
                candidate.from == edge.from
                    && candidate.to == edge.to
                    && candidate.kind == edge.kind
                    && candidate.owner == edge.owner
            })
        })
}

/// Structural equality of two nodes.
///
/// Compared field by field rather than with a derived `PartialEq`:
/// `Node` lives in core/node.rs, which recovery does not own, and a
/// derive there is a wider change than this one call site justifies.
///
/// Every field participates. A history entry records the *whole*
/// previous node, so two archives that differ only in, say, `owner` or
/// `visibility` are genuinely different snapshots and must not collapse
/// into one.
fn nodes_equal(
    left: &Node,
    right: &Node,
) -> bool {
    left.address == right.address
        && left.coordinate == right.coordinate
        && left.value == right.value
        && left.kind == right.kind
        && left.data == right.data
        && left.owner == right.owner
        && left.claimed_by == right.claimed_by
        && left.visibility == right.visibility
}

fn storage_error(
    error: String,
) -> io::Error {
    io::Error::new(
        io::ErrorKind::Other,
        error,
    )
}
