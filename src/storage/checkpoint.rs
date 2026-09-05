use std::collections::BTreeSet;
use std::fs;
use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock};

use crate::config;

/// Tracks the highest WAL sequence number that is already durably
/// reflected in physical storage — the record heap, the catalog that
/// describes it, and all six indexes.
///
/// Why this exists:
///
/// Every mutation writes its WAL record first, then applies itself to
/// the heap and the indexes. Those land in the buffer pool and reach the
/// disk at the engine's next flush; this file records how far that flush
/// got. Everything at or below the value here is on stable storage,
/// which is exactly the prefix of the WAL a restart may skip.
///
/// Without it, `recovery::recover()` would replay the *entire* WAL on
/// every startup. Replay is idempotent — every operation is keyed, so
/// re-applying one lands on the entry it already wrote — so the
/// checkpoint is a cost control rather than a correctness requirement.
/// It is still a hard rule that it may never run ahead: a checkpoint
/// written before the flush would claim durability for state a crash
/// discards, and recovery would skip exactly the operations needed to
/// rebuild it.
///
/// The checkpoint is therefore advanced only by
/// `StorageEngine::checkpoint`, and only after the heap, the catalog and
/// every index have been fsync'd.
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

/// The fence set, recovered rather than abandoned if the lock is
/// poisoned.
///
/// Every other lock in the storage layer takes its guard this way
/// (`wal::handle_slot`, `wal::sync_state`), and here the reason is
/// sharper than consistency. The three critical sections this lock
/// protects are an insert, a remove and reading the minimum — none of
/// which can leave a `BTreeSet<u64>` half-updated — so a poisoned lock
/// here means a panic happened *elsewhere* on a thread that merely held
/// this one, and the set behind it is intact.
///
/// The previous form swallowed the poison with `.ok()`, which turned
/// that into the worst possible outcome: `begin_fence` would silently
/// not fence, `min_fence` would silently report "no open transaction",
/// and [`advance`] would then be free to write a checkpoint into the
/// middle of a live `BEGIN … COMMIT` frame. Recovery filters on
/// `sequence > checkpoint`, so the next start would read a frame with
/// its opening record missing — the one shape
/// `recovery::classify_transaction` cannot resolve without either
/// losing an acknowledged batch or applying half of one. A durability
/// fence that stops fencing without saying so is worse than one that
/// panics, and this one has no reason to do either.
fn fence_set() -> MutexGuard<'static, BTreeSet<u64>> {
    fences().lock().unwrap_or_else(|e| e.into_inner())
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
    fence_set().insert(begin_sequence);
}

/// Release a previously registered checkpoint fence.
///
/// Called once the transaction is fully settled — either COMMIT is
/// durable and every operation is reflected in physical storage, or the
/// transaction was aborted. After release, the checkpoint may advance
/// past the transaction's sequences on the next [`advance`] call.
pub fn release_fence(begin_sequence: u64) {
    fence_set().remove(&begin_sequence);
}

/// The lowest active fence sequence, or `None` when no transaction frame
/// is currently open. The checkpoint may advance up to (but never reach)
/// this value.
fn min_fence() -> Option<u64> {
    fence_set().iter().next().copied()
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

/// Largest checkpoint file this reader will even look at, in bytes.
///
/// The file holds one ASCII u64 and nothing else, so 20 digits is the
/// widest legitimate content (`u64::MAX` is 18446744073709551615) and a
/// trailing newline is the only decoration a hand-edit is likely to add.
/// 64 bytes is generous room for that while making "this is not a
/// checkpoint file at all" — a log, a JSON blob, another file renamed
/// over it — a cheap, allocation-free rejection instead of something the
/// reader slurps into memory and then tries to parse.
const MAX_CHECKPOINT_FILE_LEN: u64 = 64;

/// Read the durable checkpoint: the highest WAL sequence already
/// reflected in physical storage.
///
/// # Why this fails closed
///
/// The checkpoint is a durability high-water mark, and recovery replays
/// exactly the WAL records with `sequence > checkpoint`. That makes the
/// two directions of error wildly asymmetric:
///
/// * Reading a value that is **too low** costs a little redundant
///   replay. `Insert`/`Delete`/user ops are idempotent; `Archive` and
///   `InsertEdge` are not, so it is not free — but it is bounded,
///   visible, and never loses a write.
/// * Reading a value that is **too high** silently *skips* recovery of
///   real, durable-in-the-WAL records. Those mutations are simply gone,
///   with no error anywhere, and the database looks healthy while
///   missing writes an operator was told had committed.
///
/// Guessing is therefore not acceptable. Anything that is not
/// unambiguously an ASCII u64 — a non-numeric body, a partial or padded
/// number, a stray sign, an overlong file, non-UTF-8 bytes, or a
/// whitespace-only body — is a hard `InvalidData` error naming the file
/// and quoting the offending content, so a human decides what happened
/// instead of the process inventing a number. In particular, note that
/// falling back to `0` is NOT the safe default it looks like: `0` means
/// "replay the entire WAL", which re-applies every non-idempotent
/// `Archive`/`InsertEdge` ever recorded and duplicates history entries
/// and edges.
///
/// # The one benign case
///
/// A **zero-length** file is treated as `0` with a warning. That is the
/// exact residue of an interrupted create — the temp file existed and
/// was renamed (or was created directly by an older build) before any
/// bytes were written — and it is indistinguishable from "no checkpoint
/// has ever been written", which the missing-file branch above already
/// treats as `0`. Refusing to start on a state that carries no
/// information, and that a fresh install can legitimately reach, would
/// be a self-inflicted outage. It is warned about rather than silent
/// because if it appears on a *populated* data directory it means a
/// checkpoint write was interrupted, and the extra WAL replay that
/// follows is worth an operator knowing about.
pub fn read() -> io::Result<u64> {
    let path = config::data_file("facetql.checkpoint");

    if !path.exists() {
        return Ok(0);
    }

    let len = fs::metadata(&path)?.len();

    if len == 0 {
        eprintln!(
            "warning: {} is zero-length — treating the checkpoint as 0 and \
             replaying the WAL from the beginning. This is what an \
             interrupted checkpoint write leaves behind; it is safe (replay \
             is redundant, never lossy) but on a non-empty data directory it \
             means a checkpoint update did not complete.",
            path.display()
        );

        return Ok(0);
    }

    if len > MAX_CHECKPOINT_FILE_LEN {
        return Err(corrupt(
            &path,
            &format!(
                "file is {len} bytes; a checkpoint is a single ASCII u64 and \
                 cannot exceed {MAX_CHECKPOINT_FILE_LEN} bytes"
            ),
        ));
    }

    let raw = fs::read(&path)?;

    let text = match std::str::from_utf8(&raw) {
        Ok(text) => text,

        Err(e) => {
            return Err(corrupt(
                &path,
                &format!(
                    "content is not valid UTF-8 ({e}): {}",
                    quote(&raw)
                ),
            ));
        }
    };

    let trimmed = text.trim();

    // Whitespace-only is NOT the zero-length case. Zero bytes carries no
    // information; whitespace means something wrote bytes here that are
    // not a number, so the file's history is unknown and the previous
    // value has already been lost. Fail closed.
    if trimmed.is_empty() {
        return Err(corrupt(
            &path,
            &format!(
                "content is whitespace only: {} — this is not an \
                 interrupted create (that leaves a zero-length file), so the \
                 previous checkpoint value has been overwritten by something \
                 that is not a number",
                quote(&raw)
            ),
        ));
    }

    // Reject anything that is not purely digits *before* parsing.
    // `u64::from_str` already refuses most of this, but doing it here
    // means the error message quotes what was actually on disk instead
    // of "invalid digit found in string", and it makes the accepted
    // grammar explicit: digits only, no sign, no radix prefix, no
    // separators, no trailing units.
    if !trimmed.bytes().all(|b| b.is_ascii_digit()) {
        return Err(corrupt(
            &path,
            &format!(
                "content is not a plain decimal integer: {}",
                quote(&raw)
            ),
        ));
    }

    trimmed.parse::<u64>().map_err(|e| {
        corrupt(
            &path,
            &format!(
                "content does not fit in a u64 ({e}): {}",
                quote(&raw)
            ),
        )
    })
}

/// Operator-facing error for an unreadable checkpoint.
///
/// Names the file and says what to do about it, because the recipient of
/// this message is someone whose database just refused to start and who
/// has to choose between "restore the file" and "delete it and accept a
/// full WAL replay" — a choice they can only make if they know which
/// file and what was in it.
fn corrupt(path: &std::path::Path, detail: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "corrupt checkpoint file {}: {detail}. Refusing to guess: a \
             checkpoint that reads too high silently skips WAL recovery of \
             durable records. Restore the file from a backup, or delete it \
             to replay the whole WAL (safe, but re-applies non-idempotent \
             archive/edge operations).",
            path.display()
        ),
    )
}

/// Renders raw checkpoint bytes for an error message: lossy-decoded,
/// escaped so control characters cannot mangle a terminal or a log line.
/// The file is length-capped above, so this can never be large.
fn quote(raw: &[u8]) -> String {
    format!("{:?}", String::from_utf8_lossy(raw))
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
/// integer) via a temp file + `sync_all()` + atomic rename + a
/// `sync_all()` of the parent directory, so a checkpoint update is itself
/// crash-safe: readers either see the old value or the new one, never a
/// torn write, and the rename cannot be undone by a crash. See
/// [`write_value`] for why each of those steps is required.
pub fn advance(sequence: u64) -> io::Result<()> {
    let current = read()?;

    // Clamp the request so it never crosses an open transaction frame.
    let target = sequence.min(ceiling());

    if target <= current {
        return Ok(());
    }

    write_value(target)
}

/// Whole-file, crash-safe write of the checkpoint value.
///
/// The sequence is: write a temp file → `sync_all` it → atomically
/// `rename` it over `facetql.checkpoint` → `sync_all` the *directory*.
/// Each of those four steps is load-bearing:
///
/// 1. **Temp file, not in-place truncate-and-write.** A reader must see
///    either the whole old value or the whole new one. Rewriting in
///    place can be observed (and can crash) mid-write, leaving a partial
///    number — and a partial number is a *plausible* number, e.g. `1234`
///    truncated to `12`, which reads as a checkpoint that went
///    backwards.
///
/// 2. **`sync_all` on the temp file before the rename**, not
///    `sync_data`. `sync_data` may flush the bytes without flushing the
///    inode's size. Rename the file in that state, crash, and the
///    directory entry points at an inode whose length is 0 — the classic
///    "empty file after atomic rename". A zero-length checkpoint is
///    recoverable here (see [`read`]) but only by replaying the entire
///    WAL, which is exactly the cost this file exists to avoid.
///
/// 3. **`rename`**, which is atomic with respect to readers: the name
///    `facetql.checkpoint` resolves to the old inode or the new one,
///    never to a half-written file.
///
/// 4. **`sync_all` on the parent directory.** This is the step that is
///    almost always missing, so: `rename` does not modify the file, it
///    modifies the *directory* that names it. Syncing the file — however
///    thoroughly — flushes the file's own data and inode and says
///    nothing about the directory block holding the new name→inode
///    mapping. That mapping can still be sitting in the page cache when
///    the machine loses power, and the filesystem is entitled to replay
///    the directory to its previous state: fully-synced new contents, a
///    successful `rename()` that returned `Ok`, and after reboot the name
///    still points at the OLD inode. The checkpoint silently rolls back
///    to an earlier value.
///
///    A rolled-back checkpoint is "merely" redundant WAL replay, but the
///    same durability hole would be data loss if it were `advance` in the
///    other direction, and the comment is here so this call is not
///    deleted later as a pointless extra fsync. It is not: without it,
///    `advance` returning `Ok` does not mean the new value survives a
///    crash. `sync_all` on a directory handle is the portable way to say
///    "make the name change durable"; opening a directory read-only for
///    this is standard and does not conflict with other writers.
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
        // sync_all, not sync_data: the file's *length* has to be durable
        // before the rename publishes this inode under the real name.
        file.sync_all()?;
    }

    // On failure the temp file would otherwise be left behind in the
    // data directory, so clean it up before surfacing the error.
    if let Err(e) = fs::rename(&tmp_path, &path) {
        let _ = fs::remove_file(&tmp_path);
        return Err(e);
    }

    // Make the rename itself durable. See step 4 above — without this
    // the directory entry can survive a crash still pointing at the old
    // inode, silently rolling the checkpoint back.
    sync_parent_dir(&path)?;

    Ok(())
}

/// `fsync` the directory containing `path`, making a just-completed
/// `rename`/`create`/`unlink` of that name durable.
///
/// A directory entry is filesystem metadata living in the *parent*, so
/// no amount of syncing the file itself covers it — this is the only
/// call that does. It is done through a read-only handle on the
/// directory, which is the conventional (and on Linux, the only) way to
/// obtain a descriptor to fsync.
///
/// Errors propagate: a caller that cannot confirm the rename is durable
/// must not report success, because the entire point of the checkpoint
/// protocol is that "advance returned Ok" is a durability claim.
#[cfg(unix)]
fn sync_parent_dir(path: &std::path::Path) -> io::Result<()> {
    let dir = path.parent().unwrap_or_else(|| std::path::Path::new("."));

    fs::File::open(dir)?.sync_all()
}

/// Non-Unix builds: opening a directory as a file is not permitted on
/// Windows, where `MoveFileEx` with `MOVEFILE_WRITE_THROUGH` (or the
/// filesystem's own metadata journaling) is the equivalent guarantee and
/// there is no directory handle to sync through `std`. Treated as a
/// no-op rather than an error so the checkpoint path still works there;
/// the Unix builds this engine targets get the real fsync above.
#[cfg(not(unix))]
fn sync_parent_dir(_path: &std::path::Path) -> io::Result<()> {
    Ok(())
}
