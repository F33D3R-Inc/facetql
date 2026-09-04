//! Commit staging for multi-operation transactions.
//!
//! A `POST /transaction` batch is validate-all-then-apply. Validation
//! already happens before anything is written. What was missing is
//! *crash-mid-commit safety*: the previous apply path pushed each
//! operation to the WAL as a standalone (`transaction_id == 0`) record
//! and advanced the durability checkpoint after every single one, so a
//! crash partway through a validated batch could leave some operations
//! durable and some not — with no way for recovery to tell that those
//! survivors were only ever meant to be applied together.
//!
//! This module closes that gap with an explicit **staging frame**:
//!
//! ```text
//! BEGIN(tx)          ← durable, opens the frame; registers a checkpoint fence
//!   mutation 1(tx)   ← durable, framed under tx
//!   mutation 2(tx)   ← durable, framed under tx
//!   …
//! COMMIT(tx)         ← durable; THIS record is the atomic commit point
//! ```
//!
//! Every record is appended through [`crate::storage::wal`], which
//! `sync_data()`s before returning, so "the function returned Ok" means
//! "the record crossed the filesystem durability boundary".
//!
//! ## The COMMIT marker
//!
//! The commit marker is a single `WalOperation::Commit` WAL record
//! carrying the transaction's id, appended and fsync'd as the **last**
//! record of the frame. It is written to `facetql.wal` (the same log as
//! every mutation), so it is ordered by the same strictly-increasing
//! sequence numbers and validated by the same recovery reader. Recovery
//! (`storage::recovery`) already implements the matching rule: a
//! transaction's mutations are replayed **iff** its records form exactly
//! `BEGIN … COMMIT` with no `ABORT`; anything else (BEGIN + mutations +
//! EOF, or an `ABORT`) is discarded.
//!
//! ## Crash before vs. after the marker
//!
//! * **Crash before COMMIT is durable** — recovery sees `BEGIN` +
//!   however many mutations happened to reach disk, then end-of-log (or
//!   a torn final record the recovery/WAL layer stops at). No COMMIT ⇒
//!   the frame is incomplete ⇒ **none** of the operations become
//!   visible. Because the checkpoint fence held the durable checkpoint
//!   below this transaction's BEGIN, those records are still `>
//!   checkpoint` and are the ones recovery inspects and discards. Full
//!   rollback.
//!
//! * **Crash after COMMIT is durable** — recovery sees a complete
//!   `BEGIN … COMMIT` frame and replays **all** of the transaction's
//!   mutations. If the crash also happened before physical storage was
//!   updated (or before the checkpoint advanced), the checkpoint is
//!   still below BEGIN, so the frame is `> checkpoint` and gets replayed
//!   in full. If the checkpoint had already advanced past COMMIT, the
//!   data is already in physical storage and recovery correctly skips
//!   it. Either way: all-or-nothing, and here it is all.
//!
//! ## Ordering contract for the caller
//!
//! The caller (the transaction coordinator in `transaction.rs`, driven
//! by the engine's `execute_transaction` and by every mutation the
//! engine's `apply_atomic` routes here) must observe this order:
//!
//! 1. [`StagedCommit::open`] — after validation succeeds.
//! 2. [`StagedCommit::stage`] once per resolved mutation, **before**
//!    touching in-memory / physical state.
//! 3. [`StagedCommit::commit`] — the atomic durability point.
//! 4. Apply every operation to memory + physical storage. This step is
//!    the engine's, using a **non-WAL, non-self-checkpointing** apply
//!    primitive: `StorageEngine::apply_committed`, which writes memory
//!    and the physical record but deliberately appends no WAL record and
//!    does not advance the checkpoint — the frame has already logged the
//!    intent, and the checkpoint is this module's to move at step 5.
//! 5. [`StagedCommit::settle`] — releases the checkpoint fence and
//!    advances the durable checkpoint past COMMIT, now that physical
//!    storage reflects the whole batch.
//!
//! If anything fails between steps 1 and 3, the frame is simply
//! dropped. There is no explicit abort call, because there is nothing
//! for one to do that `Drop` does not: an uncommitted frame has no
//! durable `COMMIT`, and recovery discards a frame without one whether
//! or not an `ABORT` marker follows it. So [`Drop`] releases the fence
//! and writes the `ABORT` marker best-effort — the marker records the
//! intent explicitly in the log, but the missing `COMMIT` is what
//! actually makes the frame invisible.

use std::io;

use crate::storage::checkpoint;
use crate::storage::wal::{self, WalOperation, WalRecord};

/// An open commit frame for one multi-operation transaction.
///
/// Constructed by [`StagedCommit::open`], which allocates the
/// transaction id, makes the `BEGIN` marker durable, and registers a
/// checkpoint fence at the BEGIN sequence. A successful frame is
/// consumed by [`StagedCommit::settle`]; a failed one is dropped, and
/// [`Drop`] releases the fence for it.
#[must_use = "an open commit frame must be settled, or dropped so its checkpoint fence is released"]
pub struct StagedCommit {
    /// The transaction id every record in this frame carries. Non-zero
    /// and distinct from the standalone id `0`.
    transaction_id: u64,

    /// WAL sequence of the `BEGIN` record. Doubles as the checkpoint
    /// fence key and as the lower bound recovery uses to recognise the
    /// frame.
    begin_sequence: u64,

    /// WAL sequence of the `COMMIT` record, once written. `None` until
    /// [`StagedCommit::commit`] succeeds.
    commit_sequence: Option<u64>,
}

impl StagedCommit {
    /// Open a new commit frame.
    ///
    /// Allocates a fresh non-zero transaction id, appends+fsyncs the
    /// `BEGIN` marker, and registers a checkpoint fence so the durable
    /// checkpoint cannot advance into this frame while it is open.
    ///
    /// Call this only after the batch has fully validated — an open
    /// frame writes a `BEGIN` to the log, and leaving one un-settled
    /// would pin the checkpoint.
    pub fn open() -> io::Result<StagedCommit> {
        let transaction_id = wal::next_transaction_id();

        let begin = wal::begin(transaction_id)?;
        let begin_sequence = begin.sequence;

        // Pin the checkpoint below this frame before any mutation can
        // advance it. Ordering matters: the fence must exist before the
        // first staged mutation, otherwise a concurrent/standalone
        // checkpoint advance could cross BEGIN.
        checkpoint::begin_fence(begin_sequence);

        Ok(StagedCommit {
            transaction_id,
            begin_sequence,
            commit_sequence: None,
        })
    }

    /// Stage one resolved mutation into the frame.
    ///
    /// The operation is written to the WAL as a record framed under this
    /// transaction's id (not the standalone id `0`) and fsync'd before
    /// returning. Returns the record's WAL sequence number.
    ///
    /// The operation MUST be a mutation. Passing a transaction-control
    /// operation (`Begin`/`Commit`/`Abort`) is a programming error — the
    /// frame writes its own control records — and is rejected so a
    /// caller can never smuggle a second BEGIN/COMMIT into the frame and
    /// corrupt recovery's lifecycle check.
    ///
    /// Stage every mutation *before* applying anything to memory or
    /// physical storage: the whole point is that the durable intent
    /// exists in full before any of it becomes visible.
    pub fn stage(&mut self, operation: WalOperation) -> io::Result<u64> {
        if matches!(
            operation,
            WalOperation::Begin | WalOperation::Commit | WalOperation::Abort
        ) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "transaction control records cannot be staged as mutations",
            ));
        }

        if self.commit_sequence.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot stage a mutation after the frame has committed",
            ));
        }

        let sequence = wal::next_sequence();

        let record = WalRecord::new(
            sequence,
            self.transaction_id,
            wal::next_operation_id(),
            operation,
        );

        wal::append(&record)?;

        Ok(sequence)
    }

    /// Write the atomic COMMIT marker.
    ///
    /// Appends+fsyncs a `Commit` WAL record for this transaction as the
    /// final record of the frame. Once this returns `Ok`, the
    /// transaction is durably committed: a crash from this instant
    /// onward replays the entire frame on recovery. Returns the COMMIT
    /// record's WAL sequence.
    ///
    /// After this succeeds the caller applies the operations to memory +
    /// physical storage and then calls [`StagedCommit::settle`]. The
    /// checkpoint is deliberately *not* advanced here — physical storage
    /// does not yet reflect the batch, so the checkpoint must stay below
    /// the frame until [`StagedCommit::settle`].
    pub fn commit(&mut self) -> io::Result<u64> {
        if self.commit_sequence.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "transaction already committed",
            ));
        }

        let commit = wal::commit(self.transaction_id)?;
        self.commit_sequence = Some(commit.sequence);

        Ok(commit.sequence)
    }

    /// Settle a committed frame: release the checkpoint fence and report
    /// the COMMIT sequence.
    ///
    /// Call this only after [`StagedCommit::commit`] has returned `Ok`
    /// **and** every operation in the batch has been applied. Releasing
    /// the fence lets the durability boundary move across this frame
    /// once the storage layer has flushed it.
    ///
    /// Returns an error if the frame was never committed — settling an
    /// uncommitted frame would unpin the checkpoint over operations that
    /// recovery is going to discard, losing them. An uncommitted frame
    /// is dropped instead; see [`Drop`].
    ///
    /// # Why this does not advance the checkpoint
    ///
    /// It used to, and it could when applying an operation meant
    /// appending an fsync'd record: "applied" and "durable" were the
    /// same instant. They are not any more. An applied batch is in the
    /// buffer pool — heap pages and index pages that reach the disk at
    /// the engine's next flush — and a checkpoint written before that
    /// flush would claim durability for state a crash still discards,
    /// which is the one thing a checkpoint must never do. So the
    /// sequence is handed back instead, and `StorageEngine::checkpoint`
    /// advances the boundary to it after flushing the heap, the catalog
    /// and every index.
    pub fn settle(self) -> io::Result<u64> {
        let commit_sequence = self.commit_sequence.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot settle a transaction that has not committed",
            )
        })?;

        checkpoint::release_fence(self.begin_sequence);

        // Suppress the drop-guard warning path — the fence is already
        // released above.
        std::mem::forget(self);

        Ok(commit_sequence)
    }

}

impl Drop for StagedCommit {
    /// The disposal path for a frame that was not settled — a failure
    /// between BEGIN and COMMIT, or an early `?` unwinding the caller.
    /// What it does depends on whether COMMIT reached disk, because that
    /// decides what recovery will do with the frame:
    ///
    /// * **No durable COMMIT** — recovery discards the frame, so the
    ///   fence has nothing left to protect and is released (plus a
    ///   best-effort ABORT marker).
    ///
    /// * **COMMIT durable, never settled** — recovery will *replay* the
    ///   frame, and the fence is the only thing keeping the checkpoint
    ///   from moving past it. It is deliberately NOT released. See the
    ///   body.
    fn drop(&mut self) {
        /*
         * SEAM: a frame that COMMITted durably but was dropped before
         * `settle` must KEEP its fence.
         *
         * `Transaction::commit` drops the frame without settling when
         * step 4 (apply to memory + physical storage) fails *after*
         * COMMIT is durable. The batch is committed — recovery will
         * replay it — but physical storage does not reflect it. If the
         * fence were released here, the very next single-op mutation
         * would call `checkpoint::advance` with its own (higher)
         * sequence, the ceiling would be unbounded, and the checkpoint
         * would move past this frame's COMMIT. Recovery filters on
         * `sequence > checkpoint`, so the committed batch would be
         * skipped on the next start and lost outright — the one outcome
         * a WAL exists to prevent.
         *
         * Keeping the fence pins the checkpoint below this frame's
         * BEGIN for the rest of the process's life. That is deliberate:
         * the cost is redundant (idempotent) WAL replay on the next
         * start, and the alternative is silent data loss. The process
         * has already returned an I/O error to the caller, so it is
         * not in a healthy state to begin with.
         *
         * Only an uncommitted frame is safe to unpin, because recovery
         * discards it outright.
         */
        if self.commit_sequence.is_none() {
            checkpoint::release_fence(self.begin_sequence);

            // Best-effort ABORT so the intent is explicit in the log as
            // well as implicit from the missing COMMIT. Ignored on
            // failure — the missing COMMIT alone already guarantees
            // recovery discards the frame.
            let _ = wal::abort(self.transaction_id);
        }
    }
}
