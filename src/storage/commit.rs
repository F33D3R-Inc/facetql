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
//! by the engine's `execute_transaction`) must observe this order:
//!
//! 1. [`StagedCommit::open`] — after validation succeeds.
//! 2. [`StagedCommit::stage`] once per resolved mutation, **before**
//!    touching in-memory / physical state.
//! 3. [`StagedCommit::commit`] — the atomic durability point.
//! 4. Apply every operation to memory + physical storage. This step is
//!    the engine's, using a **non-WAL, non-self-checkpointing** apply
//!    primitive (see the module note at the bottom of `transaction.rs`
//!    about the engine hook this requires).
//! 5. [`StagedCommit::settle`] — releases the checkpoint fence and
//!    advances the durable checkpoint past COMMIT, now that physical
//!    storage reflects the whole batch.
//!
//! If anything fails between steps 1 and 3, call
//! [`StagedCommit::abort`]: it writes a durable `ABORT` marker and
//! releases the fence, and recovery discards the frame.

use std::io;

use crate::storage::checkpoint;
use crate::storage::wal::{self, WalOperation, WalRecord};

/// An open commit frame for one multi-operation transaction.
///
/// Constructed by [`StagedCommit::open`], which allocates the
/// transaction id, makes the `BEGIN` marker durable, and registers a
/// checkpoint fence at the BEGIN sequence. It must be consumed by
/// exactly one of [`StagedCommit::settle`] (success) or
/// [`StagedCommit::abort`] (failure) so the fence is always released.
#[must_use = "an open commit frame must be settled or aborted to release its checkpoint fence"]
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

    /// The transaction id of this frame.
    pub fn transaction_id(&self) -> u64 {
        self.transaction_id
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

    /// Settle a committed frame: release the checkpoint fence and advance
    /// the durable checkpoint past the COMMIT marker.
    ///
    /// Call this only after [`StagedCommit::commit`] has returned `Ok`
    /// **and** every operation in the batch has been reflected in
    /// physical storage. It moves the durability boundary across the now
    /// fully-settled frame so recovery won't redundantly replay it (and
    /// won't duplicate history/edges) on the next restart.
    ///
    /// Returns an error if the frame was never committed — settling an
    /// uncommitted frame would advance the checkpoint past operations
    /// that recovery is going to discard, losing them. Abort uncommitted
    /// frames with [`StagedCommit::abort`] instead.
    ///
    /// A failure to *write* the advanced checkpoint is deliberately not
    /// an error: by this point the COMMIT record is durable and the batch
    /// is in physical storage, so the transaction has succeeded and must
    /// be reported as such. All a stuck checkpoint costs is that the next
    /// startup replays a frame already reflected on disk — safe, since
    /// recovery replays a committed frame in full or not at all. Failing
    /// here instead would tell the caller its transaction failed when the
    /// data is durably committed, which is the one answer that is simply
    /// untrue.
    pub fn settle(self) -> io::Result<()> {
        let commit_sequence = self.commit_sequence.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot settle a transaction that has not committed",
            )
        })?;

        // Release this frame's fence first, then push the checkpoint
        // across the frame. `settle` bypasses the fence ceiling because
        // this frame is, by construction, the lowest open one in the
        // single-writer mutation path.
        checkpoint::release_fence(self.begin_sequence);

        // Best-effort, mirroring the engine's existing standalone
        // checkpoint policy: the data is already durable in the WAL
        // (COMMIT is fsync'd) and in physical storage, so a failure to
        // move the checkpoint only means recovery replays a little extra
        // — which is safe and idempotent under the frame rule.
        if let Err(e) = checkpoint::settle(commit_sequence) {
            eprintln!(
                "warning: failed to advance checkpoint to {commit_sequence} \
                 after committing transaction {}: {e}",
                self.transaction_id
            );
        }

        // Suppress the drop-guard warning path — the fence is already
        // released above.
        std::mem::forget(self);

        Ok(())
    }

    /// Abort an open, uncommitted frame.
    ///
    /// Appends+fsyncs an `Abort` marker for this transaction and releases
    /// the checkpoint fence. Recovery discards a `BEGIN … ABORT` frame
    /// (and equally a `BEGIN … EOF` frame if even the ABORT never
    /// reached disk), so none of the staged operations become visible.
    ///
    /// The checkpoint is intentionally left where it was: the aborted
    /// records sit above it and will simply be filtered out by any later
    /// advance, never replayed.
    pub fn abort(self) -> io::Result<()> {
        // Best-effort ABORT marker: even if this append fails, the frame
        // has no COMMIT, so recovery already discards it. The fence
        // release below is the part that must always happen.
        let abort_result = wal::abort(self.transaction_id).map(|_| ());

        checkpoint::release_fence(self.begin_sequence);

        std::mem::forget(self);

        abort_result
    }
}

impl Drop for StagedCommit {
    /// Safety net: if a frame is dropped without an explicit
    /// [`StagedCommit::settle`] or [`StagedCommit::abort`] (e.g. an early
    /// `?` unwinds the caller), release the checkpoint fence so it can't
    /// pin the durable checkpoint forever. The frame has no durable
    /// COMMIT in that path, so recovery discards it — dropping only needs
    /// to undo the in-process fence.
    fn drop(&mut self) {
        checkpoint::release_fence(self.begin_sequence);

        // Best-effort ABORT so the intent is explicit in the log as well
        // as implicit from the missing COMMIT. Ignored on failure — the
        // missing COMMIT alone already guarantees recovery discards the
        // frame.
        if self.commit_sequence.is_none() {
            let _ = wal::abort(self.transaction_id);
        }
    }
}
