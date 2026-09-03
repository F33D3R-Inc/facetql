use std::collections::BTreeSet;
use std::fs;
use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

use crate::config;

/// Tracks the highest WAL sequence number that is already durably
/// reflected in the physical storage files (facetql.data,
/// facetql.edges, facetql.users, facetql.history, facetql.tombstones).
///
/// Why this exists:
///
/// Every mutation writes its WAL record first, then writes the matching
/// physical record. `StorageEngine::load()` reconstructs state directly
/// from the physical files, so by the time it runs, every WAL record at
/// or below the checkpoint has already been applied through the normal
/// physical-storage path.
///
/// Without this checkpoint, `recovery::recover()` would replay the
/// *entire* WAL on every startup — including operations already present
/// in physical storage. `Insert`/`Delete`/user operations happen to be
/// idempotent (replaying them just overwrites/removes the same key), but
/// `Archive` and `InsertEdge` are not: replaying them again duplicates
/// history entries and edge adjacency lists on every single restart.
///
/// The checkpoint is advanced only after a physical write has completed,
/// so it always trails (or matches) what's actually durable on disk. If
/// the process crashes between a WAL append and the matching physical
/// write, the checkpoint stays behind, and recovery correctly replays
/// that operation from the WAL.
///
/// # Cooperation with staged (uncommitted) transactions
///
/// A multi-operation transaction stages its mutations to the WAL under a
/// single `BEGIN … COMMIT` frame (see [`crate::storage::commit`]). While
/// that frame is open — BEGIN durable, COMMIT not yet durable — recovery
/// would *discard* the whole transaction on a crash. The checkpoint must
/// therefore never advance past the transaction's BEGIN sequence while
/// the frame is open, otherwise recovery would filter those still-in-
/// flight records out (`sequence > checkpoint`) and could lose them even
/// though nothing has replaced them in physical storage.
///
/// To enforce that, an open transaction registers a *fence* at its BEGIN
/// sequence via [`begin_fence`]. [`advance`] never writes a checkpoint
/// value at or beyond the lowest active fence; it clamps to
/// `min_fence - 1`. When the transaction is fully committed (COMMIT
/// durable AND every op reflected in physical storage) or aborted, it
/// calls [`release_fence`], after which the checkpoint may advance past
/// those sequences normally.

/// In-process registry of open-transaction fence sequences.
///
/// Each entry is the BEGIN sequence of a transaction whose frame is not
/// yet fully settled in physical storage. The checkpoint boundary is not
/// permitted to cross the smallest of these values, so that the snapshot
/// the checkpoint represents is always a *transactionally consistent*
/// prefix of the WAL — never one that slices through the middle of a
/// staged batch.
///
/// This is process-local state, which is correct: fences only matter
/// while a transaction is in flight in *this* process. A crash drops the
/// registry entirely, and recovery re-derives committed-vs-incomplete
/// purely from the durable WAL frame, so nothing needs to be persisted
/// here.
fn fences() -> &'static Mutex<BTreeSet<u64>> {
    static FENCES: OnceLock<Mutex<BTreeSet<u64>>> = OnceLock::new();
    FENCES.get_or_init(|| Mutex::new(BTreeSet::new()))
}

/// Register a checkpoint fence at an open transaction's BEGIN sequence.
///
/// Must be called immediately after the BEGIN record is durable and
/// before any of the transaction's mutations advance the checkpoint. A
/// registered fence prevents [`advance`] from moving the durable
/// checkpoint at or beyond `begin_sequence` until [`release_fence`] is
/// called, so a crash while the frame is open always leaves recovery a
/// full copy of the transaction's WAL records to discard.
pub fn begin_fence(begin_sequence: u64) {
    if let Ok(mut set) = fences().lock() {
        set.insert(begin_sequence);
    }
}

/// Release a previously registered checkpoint fence.
///
/// Called once the transaction is fully settled — either COMMIT is
/// durable and every operation is reflected in physical storage, or the
/// transaction was aborted. After release, the checkpoint may advance
/// past the transaction's sequences on the next [`advance`] call.
pub fn release_fence(begin_sequence: u64) {
    if let Ok(mut set) = fences().lock() {
        set.remove(&begin_sequence);
    }
}

/// The lowest active fence sequence, or `None` when no transaction frame
/// is currently open. The checkpoint may advance up to (but never reach)
/// this value.
fn min_fence() -> Option<u64> {
    fences().lock().ok().and_then(|set| set.iter().next().copied())
}

/// The highest checkpoint value that is currently permitted, given the
/// active fences. With no open transaction this is unbounded
/// (`u64::MAX`); with an open transaction it is `min_fence - 1`.
fn ceiling() -> u64 {
    match min_fence() {
        Some(fence) => fence.saturating_sub(1),
        None => u64::MAX,
    }
}

pub fn read() -> io::Result<u64> {
    let path = config::data_file("facetql.checkpoint");

    if !path.exists() {
        return Ok(0);
    }

    let raw = fs::read_to_string(&path)?;

    let trimmed = raw.trim();

    if trimmed.is_empty() {
        return Ok(0);
    }

    trimmed.parse::<u64>().map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("corrupt checkpoint file: {e}"),
        )
    })
}

/// Advance the checkpoint to `sequence`, if it's newer than what's
/// already recorded — and only as far as the active transaction fences
/// allow.
///
/// Two clamps apply:
///
/// 1. Monotonicity: the checkpoint only ever moves forward, so a request
///    at or below the current value is a no-op.
///
/// 2. Transaction fence: the checkpoint is never advanced to a value at
///    or beyond the lowest open-transaction BEGIN sequence (see
///    [`begin_fence`]). If `sequence` would cross an open frame, it is
///    clamped to `min_fence - 1`. This keeps the checkpoint a
///    transactionally consistent boundary: everything at or below it is
///    both physically durable *and* not part of a still-staging batch.
///
/// Writes are whole-file overwrites (the value is a single small
/// integer) via a temp file + `sync_data()` + atomic rename, so a
/// checkpoint update is itself crash-safe: readers either see the old
/// value or the new one, never a torn write.
pub fn advance(sequence: u64) -> io::Result<()> {
    let current = read()?;

    // Clamp the request so it never crosses an open transaction frame.
    let target = sequence.min(ceiling());

    if target <= current {
        return Ok(());
    }

    write_value(target)
}

/// Force the checkpoint to `sequence` regardless of the active fences,
/// still respecting monotonicity.
///
/// This is the settlement step a committed transaction uses once its
/// COMMIT is durable *and* every operation is reflected in physical
/// storage: the caller releases its own fence and then advances past its
/// COMMIT sequence. It bypasses the fence ceiling because the whole
/// point is to move the boundary across the frame that just settled —
/// any *other* still-open frame has a lower BEGIN sequence and would
/// have blocked this transaction's own commit ordering, so in the
/// single-writer mutation path there is never an unsettled frame below
/// `sequence` at this point.
///
/// Prefer [`advance`] for ordinary per-operation checkpointing; this
/// exists specifically for the transaction-commit settlement boundary.
pub fn settle(sequence: u64) -> io::Result<()> {
    let current = read()?;

    if sequence <= current {
        return Ok(());
    }

    write_value(sequence)
}

/// Whole-file, crash-safe write of the checkpoint value.
///
/// The temp file name is unique per write, not a fixed
/// `facetql.checkpoint.tmp`. Two writers sharing one temp path race
/// destructively: the second `File::create` truncates the first's temp
/// file, the first's `rename` consumes it, and the second's `rename`
/// then fails with `ENOENT` — a spurious error on a checkpoint that was
/// otherwise fine. Checkpoints are advanced from every mutation path, so
/// "only one thread is ever in here" is not an assumption this engine
/// can make.
fn write_value(sequence: u64) -> io::Result<()> {
    static NEXT_TMP_ID: AtomicU64 = AtomicU64::new(0);

    let path = config::data_file("facetql.checkpoint");

    let tmp_path = config::data_file(&format!(
        "facetql.checkpoint.{}.{}.tmp",
        std::process::id(),
        NEXT_TMP_ID.fetch_add(1, Ordering::Relaxed),
    ));

    {
        let mut file = fs::File::create(&tmp_path)?;
        file.write_all(sequence.to_string().as_bytes())?;
        file.sync_data()?;
    }

    // On failure the temp file would otherwise be left behind in the
    // data directory, so clean it up before surfacing the error.
    if let Err(e) = fs::rename(&tmp_path, &path) {
        let _ = fs::remove_file(&tmp_path);
        return Err(e);
    }

    Ok(())
}
