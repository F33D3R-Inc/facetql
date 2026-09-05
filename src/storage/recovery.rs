use std::collections::HashMap;
use std::io;

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
/// ```text
///     mutation
///        ↓
///       WAL
///        ↓
///     storage
///        ↓
///      memory
/// ```
///
/// Recovery path:
///
/// ```text
///     WAL
///      ↓
/// ```
///    read framed records (repair a torn tail)
/// ```text
///      ↓
/// ```
///    validate
/// ```text
///      ↓
/// ```
///    pass 1: classify transactions (no mutation)
/// ```text
///      ↓
/// ```
///    pass 2: replay in strict WAL sequence order
/// ```text
///      ↓
/// ```
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

    /*
     * The durable checkpoint is a FLOOR on the identifier space, and it
     * has to be applied before anything else — including before the
     * early return below, which is the case that made this necessary.
     *
     * Recovery replays exactly the records with `sequence > checkpoint`.
     * That filter is only sound while every sequence this process hands
     * out is above the checkpoint. Derive the counters from the WAL's
     * contents alone and that stops being true the moment the WAL is
     * shorter than the checkpoint implies — which is not a hypothetical:
     * a checkpointed WAL removed by an operator is documented as a
     * legitimate state, and `read_all` treats a missing file as one.
     * Restart there and the counters begin at 1 while the checkpoint
     * still reads (say) 5000, so every subsequent write is stamped with
     * a sequence the next recovery filters out. Those writes are durable
     * in the WAL, acknowledged to the client, and silently skipped on
     * replay: data loss with no error anywhere.
     *
     * Seeding both counters from the checkpoint closes that. It is also
     * what makes a history `version` safe across the same event, since
     * versions are drawn from the operation-id counter and must never
     * revisit a `(address, version)` key an earlier run already wrote.
     */
    let floor = checkpoint::read()?;

    wal::initialize_counters(floor, 0, floor);

    if !path.exists() {
        note_scan_horizon(floor);

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

    if records.is_empty() {
        // Same state as a missing file, reached a different way: an
        // emptied or never-written log. See `note_scan_horizon`.
        note_scan_horizon(floor);
    }

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
     * Records already reflected in the heap and the indexes don't need
     * to be replayed. Only records past the last confirmed-durable
     * checkpoint represent operations that might not have reached the
     * disk before a crash.
     *
     * The checkpoint is an OPTIMIZATION, not a correctness barrier. It
     * advances only after a flush, and a flush covers many mutations, so
     * a record whose effects did reach the disk routinely falls on the
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
    replay_in_sequence_order(engine, records, &committed)?;

    /*
     * ---------------------------------------------------------------
     * Settle what was just replayed.
     * ---------------------------------------------------------------
     *
     * Replay put everything back in the buffer pool; this pushes it to
     * the disk and moves the durability checkpoint across it. Without
     * it, the same records would be replayed again on the next start,
     * and again after that — the WAL would never stop being redone, and
     * every redo would leave another superseded copy of each record in
     * the heap.
     */
    engine.checkpoint()
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
pub(crate) type CommittedTransactions = HashMap<u64, u64>;

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

                    engine.note_recovered(sequence);
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

                engine.note_recovered(sequence);
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
/// `pub(crate)` because recovery is no longer its only caller:
/// [`crate::storage::changes::scan`] has to answer the same question —
/// which of the log's mutations actually happened — for a read rather
/// than a replay, and a change feed that surfaced an uncommitted
/// mutation would report a write that never occurred. There must be
/// exactly one definition of "committed" in this crate, and it is this
/// one. The scan differs only in what it does with an *incomplete*
/// frame: recovery runs after a crash, so an incomplete frame is always
/// dead, while a scan runs against a live log where the last frame may
/// simply not have reached its COMMIT yet.
///
/// Only this exact lifecycle is committed:
///
/// ```text
///     BEGIN
///       mutation...
///     COMMIT
/// ```
///
/// Anything else is discarded:
///
/// ```text
///     BEGIN
///       mutation...
///     EOF
///
///     BEGIN
///       mutation...
///     ABORT
/// ```
///
/// and a frame whose shape is not merely incomplete but impossible
/// (records after COMMIT, a second BEGIN, a mutation before BEGIN) is a
/// hard error — the log claims something that cannot have happened, so
/// we refuse to guess.
pub(crate) fn classify_transactions(
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
/// ```text
///     first record = BEGIN
///     last control record = COMMIT
///     no ABORT occurred
/// ```
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
        /*
         * A frame whose first surviving record is not BEGIN comes in two
         * kinds, and they are opposites — one is routine, the other is
         * the loss this whole module exists to prevent. Which one it is
         * is decided by whether a COMMIT survived alongside it.
         *
         *   * NO COMMIT — nothing was ever promised. The batch was in
         *     flight when the process died, or it aborted; either way no
         *     caller was told it happened, so discarding it is the
         *     correct outcome and needs no comment.
         *
         *   * A COMMIT IS PRESENT — the log is asserting that a
         *     transaction committed while the record that opened it is
         *     gone. A correct writer cannot produce that: BEGIN is the
         *     first record of the frame and carries the lowest sequence
         *     in it, so anything that removed BEGIN while leaving the
         *     COMMIT removed a *middle* of the log. That is either a
         *     checkpoint/rotation that sliced through a live frame (the
         *     fence in `storage::checkpoint` exists to make this
         *     impossible) or external damage. Returning `Ok(None)` here
         *     would silently drop a batch that was acknowledged to a
         *     client — data loss with no signal anywhere — so it is a
         *     hard startup failure instead. The operator gets told which
         *     transaction and which sequences, because the surviving
         *     records ARE the batch and recovering them by hand is the
         *     only remedy.
         */
        if let Some(commit) = ordered
            .iter()
            .find(|record| matches!(record.operation, WalOperation::Commit))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "transaction {transaction_id} has a durable COMMIT at \
                     sequence {} but no BEGIN record; its first surviving \
                     record is sequence {}. A committed transaction whose \
                     opening record is missing means the log was truncated \
                     through the middle of a frame, so replaying what is \
                     left would apply part of a batch and discarding it \
                     would silently lose one that was acknowledged. \
                     Recovery refuses both and must be resolved by an \
                     operator.",
                    commit.sequence,
                    ordered[0].sequence,
                ),
            ));
        }

        // No BEGIN and no COMMIT: an incomplete frame. Nothing was
        // promised, so it is discarded rather than replayed.
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
/// The reason is the checkpoint's own ordering: it is advanced only
/// after a flush, so every operation applied since the last flush is
/// replayed even though some of it did reach the disk.
///
/// Every operation is now idempotent **by key**, which is what the
/// durable indexes bought: an insert repoints one primary-index entry,
/// a delete removes one, an edge is keyed by its identity, and a history
/// entry is keyed by `(address, version)` with the version carried in
/// the record itself. Re-applying any of them lands on the entry it
/// already wrote. The cost of a redundant replay is a superseded record
/// in the heap, which compaction reclaims — not a duplicate that
/// survives forever and grows with every restart, which is what the
/// previous `Vec`-backed history and adjacency lists produced and why
/// replay used to need explicit de-duplication.
fn apply_recovery_operation(
    engine: &mut StorageEngine,
    operation: WalOperation,
) -> io::Result<()> {
    match operation {
        WalOperation::Archive(entry) => {
            /*
             * No existence check: the history index is keyed
             * (address, version), and the version travels with the entry
             * through the WAL, so a replayed archive re-derives the key
             * it already wrote and lands on itself. This used to require
             * scanning a node's whole history for a byte-for-byte match
             * before every replay, because history was a `Vec` that a
             * second apply would simply push onto again.
             */
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
             * Keyed by (from, kind, to) in both edge indexes, so a
             * re-applied insert replaces its own entry rather than
             * adding a second copy of the relationship.
             */
            engine
                .replay_insert_edge(edge)
                .map_err(storage_error)?;
        }

        WalOperation::DeleteEdge(id) => {
            /*
             * No existence check needed: removing a key from both edge
             * indexes converges the same way `Delete` converges for a
             * node. Replaying this against an edge already gone finds
             * nothing to remove and is a no-op, not an error.
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

        WalOperation::CreateIndex(def) => {
            /*
             * Re-declaring an index that already exists reopens the same
             * tree and re-populates it, and every entry the backfill
             * writes is keyed by (value, address) — so the second pass
             * lands on the keys the first one wrote. Which is what makes
             * it safe to replay a create whose effects partly reached
             * disk: there is no state that "already backfilled" would
             * have to be remembered to avoid corrupting.
             */
            engine
                .replay_create_index(def)
                .map_err(storage_error)?;
        }

        WalOperation::DropIndex(name) => {
            engine
                .replay_drop_index(&name)
                .map_err(storage_error)?;
        }

        WalOperation::CreateReference(def) => {
            /*
             * Last-write-wins by name in both the log and the resident
             * set, so re-declaring a reference that is already there
             * converges on the same definition. A reference builds no
             * structure, so unlike an index there is not even a
             * backfill to redo.
             */
            engine
                .replay_create_reference(def)
                .map_err(storage_error)?;
        }

        WalOperation::DropReference(name) => {
            engine
                .replay_drop_reference(&name)
                .map_err(storage_error)?;
        }

        WalOperation::CreateTextIndex(def) => {
            /*
             * Same convergence as `CreateIndex`, one level finer: the
             * backfill writes one key per trigram per row, every one of
             * them a `put` keyed by (gram, address), so a replay lands
             * on the keys the interrupted pass already wrote and fills
             * in the ones it did not reach.
             */
            engine
                .replay_create_text_index(def)
                .map_err(storage_error)?;
        }

        WalOperation::DropTextIndex(name) => {
            engine
                .replay_drop_text_index(&name)
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

/// Stamp the change scan's horizon for a log that holds no records.
///
/// Two opposite states reach an empty log, and only the checkpoint tells
/// them apart:
///
///   * **no checkpoint** — this database has never made a mutation
///     durable, so nothing has been lost and every position is
///     answerable. The horizon stays at zero and
///     `crate::storage::changes::scan` truthfully answers "no changes".
///
///   * **a checkpoint exists** — records were written, made durable, and
///     then rotated away or removed by an operator. Whatever they said
///     is gone, and *nothing here can bound how far it went*: the
///     positions those records carried are not derivable from a
///     checkpoint (which counts sequences, not operation IDs) nor from a
///     file with no records in it. So the scan can vouch for nothing at
///     all, and says so, rather than answering an old `after=` with an
///     empty page that is indistinguishable from "you are up to date".
///
/// The second case stamps `u64::MAX`, which refuses every scan **while
/// the log is empty** and nothing longer: the moment a mutation is
/// appended the log is non-empty and states its own horizon (see
/// `changes::horizon`), so this is a refusal that repairs itself with
/// the first write rather than a latch.
fn note_scan_horizon(checkpoint_floor: u64) {
    if checkpoint_floor == 0 {
        return;
    }

    wal::note_scan_horizon(u64::MAX);
}
